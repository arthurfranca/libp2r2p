import { createEventStoreChunkStorage, createEventStoreMessageStorage } from '../event-store/index.js'
import { createPrivateFileTransfer } from '../file/index.js'
import { createPrivateMessenger } from '../index.js'
import { getEventHash, isValidEvent, isSerializableEvent } from '../../event/index.js'
import { decodeIrfsChunk, IRFS_CHUNK_BYTES } from '../../irfs/index.js'
import { getIykcProofs } from '../../content-key/index.js'
import { onOnline } from '../../network/index.js'
import { decodeFileMetadata } from '../../nip94/index.js'
import { createChatOutbox } from './helpers/work-storage.js'
import { messengerSigner } from './helpers/signer.js'
import { assertMessagePublished } from '../helpers/publication.js'
import { normalizeFallbackRelays } from '../helpers/send-routing.js'

export function wireEvent (value, owner) {
  const event = { kind: value.kind, created_at: value.created_at, tags: structuredClone(value.tags), content: value.content, pubkey: value.pubkey || owner }
  if (value.sig) return { ...event, id: value.id, sig: value.sig }
  return event
}
// Relay rejection text must not be interpreted as a local signer denial.
const retryable = error => error?.code === 'MESSAGE_NOT_PUBLISHED' || !/DENIED|PERMISSION|REVOKED|READ_ONLY|INVALID|BLOCKED|EXPIRED|NOT_IN_PERSONA/i.test(`${error?.code || ''} ${error?.message || ''}`)
const unavailableSend = /PRIVATE_MESSENGER_PAUSED|CHAT_UNAVAILABLE|MESSENGER_UNAVAILABLE/i
const isUnavailableSend = error => unavailableSend.test(`${error?.code || ''} ${error?.message || ''}`)
const SEND_RETRY_MIN_MS = 1000
const SEND_RETRY_MAX_MS = 30000
// A paused or unavailable messenger must keep the outbox entry retryable
// instead of surfacing a send failure the user has to retry by hand.
const unavailableError = (error, signal) => Object.assign(new Error(error?.message || 'CHAT_UNAVAILABLE'), {
  code: error?.code,
  cause: error,
  retryWhenAvailable: !signal?.aborted,
  retryWhenOnline: false
})

export function createPrivateMessageSession ({ owner, signer, eventStore, messageStorage, chunkStorage, recoveryStorage, fallbackRelays = [], mode = 'seeder', seedersForPeer = peer => [peer], allowedKinds = [5, 9, 1063, 34601], useContentKeys = true, onMedia = () => {}, onOutbox = () => {}, onError = () => {}, onSendError = () => {}, Messenger = createPrivateMessenger, openOutbox = createChatOutbox, FileTransfer = createPrivateFileTransfer, _onOnline = onOnline, _getIykcProofs = getIykcProofs, _setTimeout = setTimeout, _clearTimeout = clearTimeout, _random = Math.random, openDownloads = options => createChatOutbox({ ...options, namespace: 'downloads' }) }) {
  fallbackRelays = normalizeFallbackRelays(fallbackRelays)
  const reportedErrors = new WeakSet()
  const reportError = error => {
    if (error && (typeof error === 'object' || typeof error === 'function')) {
      if (reportedErrors.has(error)) return
      reportedErrors.add(error)
    }
    onError(error)
  }
  const userSigner = messengerSigner(signer)
  messageStorage ||= createEventStoreMessageStorage({ eventStore })
  chunkStorage ||= eventStore ? createEventStoreChunkStorage({ eventStore }) : undefined
  const peers = new Set()
  const channels = new Map()
  const deniedPeers = new Set()
  const entries = new Map()
  const cancelled = new Set()
  const sendControllers = new Map()
  const offlineSends = new Set()
  let stopOnline
  let sendRetryTimer
  let sendRetryDelay = SEND_RETRY_MIN_MS
  let messenger
  let fileTransfers
  let downloads
  const downloadIntents = new Map()
  const downloadEpochs = new Map()
  const activeDownloads = new Map()
  const pendingPriorityRanges = new Map()
  let downloadWriteTail = Promise.resolve()
  const writeDownloadIntent = operation => {
    const work = downloadWriteTail.catch(() => {}).then(operation)
    downloadWriteTail = work
    return work
  }
  let storage
  let initialized
  let closed = false
  let available = false
  let lifecycle = 0
  let configuring = Promise.resolve()
  let sending
  let repump
  let draining
  const idleWaiters = new Set()
  const settled = () => { if (!sending && !draining) { for (const resolve of idleWaiters) resolve(); idleWaiters.clear() } }
  const emit = () => onOutbox([...entries.values()].filter(entry => !entry.deletion))
  const ready = () => (initialized ??= (async () => {
    storage = await openOutbox({ owner, signer })
    for (const entry of await storage.list()) entries.set(entry.id, entry)
    emit()
  })().catch(async error => { await storage?.close(); storage = null; initialized = null; throw error }))
  async function configure () {
    if (closed || !available) return
    await ready()
    const version = lifecycle
    const values = []
    for (const peer of peers) {
      if (deniedPeers.has(peer)) continue
      let channel = channels.get(peer)
      if (!channel) {
        try {
          const scoped = messengerSigner(signer.withSharedKey(peer, 'dm'))
          channel = { signer: scoped, pubkey: await scoped.getPublicKey(), mode, seeders: seedersForPeer(peer) }
          channels.set(peer, channel)
        } catch (error) { if (!retryable(error)) deniedPeers.add(peer); reportError(error); continue }
      }
      if (closed || !available || version !== lifecycle) return
      values.push(channel)
    }
    if (!messenger) messenger = await Messenger({ fallbackRelays, seedStorage: recoveryStorage?.seeds, userSigner, channels: [], useContentKeys, onMessageQueued: () => drain(), onError: reportError })
    if (closed || !available || version !== lifecycle) { await messenger.pause('signer'); return }
    await messenger.update({ channels: values })
    if (pendingPriorityRanges.size && typeof messenger.prioritizeRange === 'function') {
      for (const [peer, list] of [...pendingPriorityRanges]) {
        const channel = channels.get(peer)
        if (!channel) continue
        for (const request of list) {
          try { await messenger.prioritizeRange(channel.pubkey, request) } catch (error) { reportError(error) }
        }
        pendingPriorityRanges.delete(peer)
      }
    }
    await messenger.resume('signer')
    if (!fileTransfers) {
      fileTransfers = FileTransfer({
        messenger, onError: reportError,
        resolveChannel: ({ peerPubkey, info }) => messengerSigner(signer.withSharedKey(peerPubkey, info)),
        storage: chunkStorage,
        seedStorage: recoveryStorage?.seeds,
        authorizationStorage: recoveryStorage?.authorizations
      })
      fileTransfers.observe(state => {
        const peer = [...channels].find(([, value]) => value.pubkey === state.controlChannelPubkey)?.[0]
        if (peer) {
          onMedia({ ...state, peer })
          if (state.status === 'complete') {
            const id = intentId({ peer, root: state.root })
            if (downloadIntents.delete(id)) writeDownloadIntent(() => downloads?.remove(id)).catch(reportError)
          }
        }
      })
      downloads = await openDownloads({ owner, signer })
      for (const entry of await downloads.list()) downloadIntents.set(entry.id, entry)
    }
    for (const entry of downloadIntents.values()) {
      if (!peers.has(entry.file.peer) || activeDownloads.has(entry.id)) continue
      const work = download(entry.file, { manual: true })
        .catch(error => {
          // A failed intent stays queued for the next configure; report each
          // real attempt once even if configure runs while it is in flight.
          if (downloadIntents.has(entry.id)) reportError(error)
        })
        .finally(() => { if (activeDownloads.get(entry.id) === work) activeDownloads.delete(entry.id) })
      activeDownloads.set(entry.id, work)
    }
    drain(); pump()
  }
  function schedule () {
    const work = configuring.catch(() => {}).then(configure)
    configuring = work
    work.catch(reportError)
    return work
  }
  async function drain () {
    if (draining || closed || !available || !messenger) return
    draining = true
    const deferred = []
    try {
      while (true) {
        if (closed || !available) break
        const delivery = await messenger.nextMessage()
        if (!delivery) break
        const { message, ack, nack } = delivery
        try {
          const peer = [...channels].find(([, value]) => value.pubkey === message.channelPubkey)?.[0]
          if (!peer) { deferred.push(nack); continue }
          if (message.senderPubkey !== peer) { await ack(); continue }
          if (!peers.has(peer)) { deferred.push(nack); continue }
          const event = wireEvent(message.event, peer)
          const valid = event.pubkey === peer || message.provenance === 'hearsay' || message.provenance === 'signed'
          if (!valid || !isSerializableEvent(event) || !allowedKinds.includes(event.kind) || (event.sig && !isValidEvent(event)) || getEventHash(event) !== message.event.id) { await ack(); continue }
          if (event.kind === 34601) {
            try { decodeIrfsChunk(event) } catch (error) { reportError(error); await ack(); continue }
          }
          if (event.kind === 5 && (message.provenance === 'hearsay' || event.pubkey !== peer)) { await ack(); continue }
          if (!available || closed) { await nack(); break }
          const saved = await messageStorage.save(event, { peerPubkey: peer, hearsay: message.provenance === 'hearsay' })
          if (!saved?.result?.ok && !['blocked', 'expired'].includes(saved?.result?.code)) throw Object.assign(new Error('INBOX_STORAGE_FAILED'), { code: saved?.result?.code?.toUpperCase() })
          await ack()
        } catch (error) {
          await nack()
          reportError(error)
          // Retain the failed record and suspend network ingestion until recovery.
          await messenger.pause('storage')
          break
        }
      }
    } finally { await Promise.all(deferred.map(nack => nack())); draining = false; settled() }
  }
  async function publish (peer, event, signal) {
    if (deniedPeers.has(peer)) throw new Error('PERMISSION_DENIED')
    if (!available || !peers.has(peer) || !channels.has(peer)) throw unavailableError(new Error('CHAT_UNAVAILABLE'), signal)
    const options = { channelPubkey: channels.get(peer)?.pubkey, receiverPubkeys: [peer], signal }
    let report
    try {
      report = event.sig ? await messenger.broadcastEvent({ ...options, event }) : await messenger.broadcastRumor({ ...options, rumor: event })
    } catch (error) {
      if (isUnavailableSend(error)) throw unavailableError(error, signal)
      throw error
    }
    // Published is relay acceptance, never a peer receipt.
    await assertMessagePublished(report, event)
  }
  async function sendEntry (entry) {
    entry.status = 'pending'; emit()
    const controller = new AbortController()
    sendControllers.set(entry.id, controller)
    try {
      const active = async () => {
        if (storage.has && !await storage.has(entry.id)) cancelled.add(entry.id)
        if (closed || !available || cancelled.has(entry.id) || (entry.peer !== owner && !peers.has(entry.peer))) throw new Error('CHAT_UNAVAILABLE')
      }
      // Personal copies must commit even if publishing attachment bytes
      // fails offline. Remote progress remains independently retryable.
      for (let index = 0; index < entry.events.length; index++) {
        await active()
        const event = entry.events[index]
        if (!entry.localSaved[index]) {
          const hearsay = !event.sig && event.pubkey !== owner
          const result = await messageStorage.save(event, { peerPubkey: entry.peer, hearsay })
          if (result?.result?.code === 'blocked') { cancelled.add(entry.id); await active() }
          if (!result?.result?.ok) throw Object.assign(new Error('MESSAGE_STORAGE_FAILED'), { code: result?.result?.code?.toUpperCase() })
          entry.localSaved[index] = true
          await storage.put(entry, { existing: true })
        }
      }
      // Read/publish one chunk at a time. Progress is durable after each
      // accepted chunk; replay after interruption keeps its original identity.
      for (; entry.peer !== owner && entry.fileIndex < entry.files.length; entry.fileIndex++, entry.chunkIndex = 0) {
        const file = entry.files[entry.fileIndex]
        await active()
        await fileTransfers.authorizeSeeding({ controlChannelPubkey: channels.get(entry.peer).pubkey, peerPubkey: entry.peer, root: file.root, size: file.size }, { receiverPubkeys: [entry.peer], sharedAt: entry.event.created_at })
        for (; entry.chunkIndex < Math.ceil(file.size / IRFS_CHUNK_BYTES); entry.chunkIndex++) {
          await active()
          const chunk = chunkStorage ? await chunkStorage.read(file.root, entry.chunkIndex, file) : await fileTransfers.readChunk(file.root, entry.chunkIndex)
          if (!chunk || decodeIrfsChunk(chunk).root !== file.root) { if (file.optional) break; throw new Error('FILE_UNAVAILABLE') }
          if (!fileTransfers) throw new Error('FILE_TRANSFER_UNAVAILABLE')
          await fileTransfers.publishChunk({ controlChannelPubkey: channels.get(entry.peer).pubkey, peerPubkey: entry.peer, root: file.root, size: file.size }, wireEvent(chunk, owner), { signal: controller.signal })
          const completed = entry.files.slice(0, entry.fileIndex).reduce((sum, value) => sum + value.size, 0) + Math.min(file.size, (entry.chunkIndex + 1) * IRFS_CHUNK_BYTES)
          entry.uploadProgress = { completed, total: entry.files.reduce((sum, value) => sum + value.size, 0) }
          emit()
          await active()
          await storage.put({ ...entry, chunkIndex: entry.chunkIndex + 1 }, { existing: true })
          // Newly queued text and deletion controls can pass an in-flight file.
          let admitted = 0
          for (const pending of entries.values()) {
            if (closed || !available || admitted >= 8) break
            if (pending.files.length || pending.failed || cancelled.has(pending.id) || (pending.peer !== owner && !peers.has(pending.peer))) continue
            admitted++
            await sendEntry(pending)
          }
        }
      }
      for (; entry.index < entry.events.length; entry.index++) {
        await active()
        const event = entry.events[entry.index]
        if (entry.peer !== owner) await publish(entry.peer, event, controller.signal)
        await active()
        await storage.put({ ...entry, index: entry.index + 1 }, { existing: true })
      }
      await storage.remove(entry.id)
      entries.delete(entry.id)
      emit()
    } catch (cause) {
      // Pauses can surface at any stage (text publication, file authorization
      // or chunk delivery). Normalize all of them into the retryable path.
      const error = cause?.retryWhenAvailable ? cause : (isUnavailableSend(cause) ? unavailableError(cause, controller.signal) : cause)
      if (cancelled.has(entry.id)) { await storage.remove(entry.id); entries.delete(entry.id) } else {
        entry.status = error.retryWhenAvailable ? 'pending' : 'error'; entry.failed = true; entry.retryable = retryable(error)
        await storage.put(entry, { existing: true }).catch(reportError)
        reportError(error)
        // The failed wire event may be a quote or file chunk. Report the
        // owning outbox item separately, without mutating the native error.
        if (error.retryWhenOnline) offlineSends.add(entry.id)
        if (error.retryWhenAvailable) scheduleSendRetry()
        if (!closed && !entry.deletion && !cancelled.has(entry.id) && !error.retryWhenAvailable) onSendError(error, { id: entry.id, peer: entry.peer })
      }
      emit()
    } finally { sendControllers.delete(entry.id) }
  }
  function stopOnlineRetry () { stopOnline?.(); stopOnline = undefined }
  // Entries that failed only because the messenger was paused/unavailable do
  // not receive a connectivity transition, so they own a bounded backoff timer
  // instead of waiting for the next account-state change.
  function clearSendRetry () {
    if (sendRetryTimer) _clearTimeout(sendRetryTimer)
    sendRetryTimer = undefined
  }
  function resetSendRetry () {
    clearSendRetry()
    sendRetryDelay = SEND_RETRY_MIN_MS
  }
  const pendingRetryable = entry => Boolean(entry.failed && !cancelled.has(entry.id) && (entry.retryable || entry.status === 'pending'))
  function scheduleSendRetry () {
    if (closed || !available || sendRetryTimer) return
    const delay = Math.min(SEND_RETRY_MAX_MS, sendRetryDelay * (0.8 + _random() * 0.4))
    sendRetryDelay = Math.min(SEND_RETRY_MAX_MS, sendRetryDelay * 2)
    sendRetryTimer = _setTimeout(() => {
      sendRetryTimer = undefined
      if (closed || !available) return
      let retried = false
      for (const entry of entries.values()) {
        if (!pendingRetryable(entry)) continue
        entry.failed = false
        retried = true
      }
      if (retried) pump()
    }, delay)
    sendRetryTimer.unref?.()
  }
  function watchOfflineSends () {
    if (closed || !available || !offlineSends.size) { stopOnlineRetry(); return }
    if (stopOnline) return
    // A fresh listener receives confirmed connectivity even when a brief outage
    // occurred between the app's long-lived monitor checks.
    stopOnline = _onOnline(() => {
      stopOnlineRetry()
      if (closed || !available) return
      for (const id of offlineSends) {
        const entry = entries.get(id)
        if (entry?.retryable) entry.failed = false
      }
      offlineSends.clear()
      return schedule()
    })
  }
  async function pump () {
    if (!available || closed || !messenger || !storage) return
    // A retry that lands while a pump is already running must not be skipped
    // just because that pass already walked past the entry.
    if (sending) { repump = true; return }
    sending = true
    try {
      do {
        repump = false
        for (const entry of entries.values()) {
          if (closed || !available) break
          if ((entry.peer !== owner && !peers.has(entry.peer)) || entry.failed || cancelled.has(entry.id)) continue
          await sendEntry(entry)
        }
      } while (repump && !closed && available)
    } finally {
      sending = false
      watchOfflineSends()
      if (![...entries.values()].some(pendingRetryable)) resetSendRetry()
      settled()
    }
  }
  const intentId = file => getEventHash({ kind: 0, pubkey: owner, created_at: 0, tags: [], content: `${file.peer}:${file.root}` })
  async function download (file, options = {}) {
    if (file.peer === owner) return file
    if (!available || closed || !peers.has(file.peer) || !fileTransfers) throw new Error('CHAT_UNAVAILABLE')
    const id = intentId(file)
    if (options.manual) {
      const epoch = downloadEpochs.get(id) || 0
      const cancelled = () => { if ((downloadEpochs.get(id) || 0) !== epoch) throw new DOMException('Download cancelled', 'AbortError') }
      await writeDownloadIntent(async () => {
        cancelled()
        await downloads.put({ id, file })
        cancelled()
        downloadIntents.set(id, { id, file })
      })
    }
    if (!available || closed || !peers.has(file.peer)) throw new Error('CHAT_UNAVAILABLE')
    const result = await fileTransfers.download({ controlChannelPubkey: channels.get(file.peer).pubkey, peerPubkey: file.peer, root: file.root, size: file.size, sharedAt: file.sharedAt }, options)
    if (downloadIntents.delete(id)) await writeDownloadIntent(() => downloads.remove(id))
    return result
  }
  async function cancelDownload (file) {
    fileTransfers?.cancel(channels.get(file.peer)?.pubkey, file.root)
    const id = intentId(file)
    downloadEpochs.set(id, (downloadEpochs.get(id) || 0) + 1)
    downloadIntents.delete(id)
    await writeDownloadIntent(() => downloads?.remove(id))
  }
  return {
    download, cancelDownload,
    // Peers-based so callers can prefetch as soon as a chat route opens, before
    // the shared-key channel exists. When group chats get real membership, add
    // a thin prefetchChannelContentKeys(channelPubkey) wrapper that resolves
    // the channel's receivers and delegates here.
    async prefetchContentKeys (values = [...peers]) {
      if (closed || !useContentKeys) return {}
      const requested = [...new Set([owner, ...(Array.isArray(values) ? values : [values])])]
        .filter(value => typeof value === 'string' && value)
      if (!requested.length) return {}
      try {
        return await _getIykcProofs(requested)
      } catch (error) {
        // Prefetch only warms the lookup cache; callers must keep working with
        // sender-content until a proof is available. Keep the failure visible.
        console.warn('private-messenger content-key prefetch failed', error?.message ?? error)
        return {}
      }
    },
    async prioritizeRange (peer, options = {}) {
      if (!/^[0-9a-f]{64}$/.test(peer || '')) throw new TypeError('INVALID_PRIORITY_PEER')
      const { since, until, type = 'unread-page' } = options || {}
      if (type !== 'unread-page' && type !== 'tail') throw new TypeError('INVALID_PRIORITY_TYPE')
      if (since !== undefined && !Number.isSafeInteger(since)) throw new TypeError('INVALID_PRIORITY_RANGE')
      if (until !== undefined && !Number.isSafeInteger(until)) throw new TypeError('INVALID_PRIORITY_RANGE')
      if (type === 'unread-page' && since === undefined) throw new TypeError('PRIORITY_SINCE_REQUIRED')
      const request = { since, until, type }
      const channel = channels.get(peer)
      if (channel && typeof messenger?.prioritizeRange === 'function') {
        try { return await messenger.prioritizeRange(channel.pubkey, request) } catch (error) { reportError(error); return false }
      }
      const list = pendingPriorityRanges.get(peer) || []
      list.push(request)
      pendingPriorityRanges.set(peer, list)
      return true
    },
    async setPeers (values) {
      peers.clear(); for (const peer of values) if (peer !== owner) peers.add(peer)
      for (const peer of deniedPeers) if (!peers.has(peer)) deniedPeers.delete(peer)
      lifecycle++
      return schedule()
    },
    async setAvailable (value) {
      available = value === true
      stopOnlineRetry(); offlineSends.clear()
      if (available) resetSendRetry()
      else clearSendRetry()
      lifecycle++
      if (!available) { await messenger?.pause('signer'); return }
      await ready()
      for (const entry of entries.values()) if (pendingRetryable(entry)) entry.failed = false
      await messenger?.resume('storage')
      return schedule()
    },
    async enqueue ({ peer, event, context = [], requiredFiles = [], deletion = false }) {
      if (closed || !available || (peer !== owner && !peers.has(peer))) throw new Error('CHAT_UNAVAILABLE')
      await ready()
      const main = wireEvent(event, owner)
      const id = getEventHash(main)
      const events = [...context.map(value => wireEvent(value, owner)), main]
      const files = events.filter(value => value.kind === 1063).map(value => ({ ...decodeFileMetadata(value), optional: !requiredFiles.includes(getEventHash(value)) })).filter(file => file.service === 'irfs').flatMap(file => [file, ...(file.thumbnail?.root ? [{ ...file.thumbnail, optional: file.optional }] : [])])
      const entry = { id, peer, event: { ...main, id }, events, files, index: 0, fileIndex: 0, chunkIndex: 0, localSaved: context.map(() => true).concat(false), status: 'pending', deletion }
      await storage.put(entry)
      entries.set(id, entry); emit(); pump()
      return id
    },
    async retry (id) {
      offlineSends.delete(id); watchOfflineSends()
      await ready()
      const entry = entries.get(id)
      if (entry) { entry.failed = false; resetSendRetry(); if (deniedPeers.delete(entry.peer)) await schedule(); return pump() }
    },
    async cancel (id) {
      cancelled.add(id)
      sendControllers.get(id)?.abort()
      offlineSends.delete(id); watchOfflineSends()
      await ready()
      entries.delete(id)
      await storage.remove(id)
      emit()
    },
    async close () {
      closed = true; available = false; lifecycle++
      stopOnlineRetry(); offlineSends.clear(); activeDownloads.clear(); pendingPriorityRanges.clear()
      clearSendRetry()
      for (const controller of sendControllers.values()) controller.abort()
      await Promise.allSettled([configuring, initialized])
      await messenger?.pause('closed')
      if (sending || draining) await new Promise(resolve => idleWaiters.add(resolve))
      await messenger?.close()
      await storage?.close()
      await downloadWriteTail.catch(reportError)
      await downloads?.close()
    }
  }
}

export { createChatOutbox as createPrivateSessionStorage } from './helpers/work-storage.js'
export { messengerSigner as createMessengerSigner } from './helpers/signer.js'
