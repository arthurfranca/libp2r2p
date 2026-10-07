import { createEventStoreChunkStorage, createEventStoreMessageStorage } from '../event-store/index.js'
import { createPrivateFileTransfer } from '../file/index.js'
import { createPrivateMessenger } from '../index.js'
import { getEventHash, isValidEvent, isSerializableEvent } from '../../event/index.js'
import { decodeIrfsChunk, IRFS_CHUNK_BYTES } from '../../irfs/index.js'
import { getIykcProofs } from '../../content-key/index.js'
import { isOnline, onOnline } from '../../network/index.js'
import { createPeerPreparation } from './helpers/peer-preparation.js'
import { createForegroundWork } from './helpers/foreground-work.js'
import { isRetryableRelayFailure } from '../../relay/index.js'
import { ValidationError } from '../../error/index.js'
import { decodeFileMetadata } from '../../nip94/index.js'
import { createChatOutbox } from './helpers/work-storage.js'
import { messengerSigner } from './helpers/signer.js'
import { assertMessagePublished } from '../helpers/publication.js'
import { normalizeFallbackRelays, normalizeFallbackDelay } from '../helpers/send-routing.js'
import { createPauseRecovery, isRecoverableStorageFailure } from '../helpers/pause-recovery.js'

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
  ...(Array.isArray(error?.pauseReasons) ? { pauseReasons: [...error.pauseReasons] } : {}),
  retryWhenAvailable: !signal?.aborted,
  retryWhenOnline: false
})

function messengerStatusSnapshot (value) {
  if (!value || typeof value.closed !== 'boolean' || typeof value.paused !== 'boolean' ||
      !Array.isArray(value.pauseReasons) || value.pauseReasons.some(reason => typeof reason !== 'string' || !reason.trim()) ||
      value.paused !== Boolean(value.pauseReasons.length)) throw new ValidationError('INVALID_MESSENGER_STATUS')
  return { closed: value.closed, paused: value.paused, pauseReasons: [...value.pauseReasons] }
}

export function createPrivateMessageSession ({ owner, signer, eventStore, messageStorage, chunkStorage, recoveryStorage, fallbackRelays = [], fallbackDelayMs = null, mode = 'seeder', seedersForPeer = peer => [peer], allowedKinds = [5, 9, 1063, 34601], useContentKeys = true, onMedia = () => {}, onOutbox = () => {}, onError = () => {}, onSendError = () => {}, Messenger = createPrivateMessenger, openOutbox = createChatOutbox, FileTransfer = createPrivateFileTransfer, _onOnline = onOnline, _isOnline = isOnline, _getIykcProofs = getIykcProofs, _setTimeout = setTimeout, _clearTimeout = clearTimeout, _random = Math.random, openDownloads = options => createChatOutbox({ ...options, namespace: 'downloads' }) }) {
  fallbackRelays = normalizeFallbackRelays(fallbackRelays)
  fallbackDelayMs = normalizeFallbackDelay(fallbackDelayMs)
  const reportedErrors = new WeakSet()
  const reportError = error => {
    if (error && (typeof error === 'object' || typeof error === 'function')) {
      if (reportedErrors.has(error)) return
      reportedErrors.add(error)
    }
    onError(error)
  }
  const userSigner = messengerSigner(signer)
  const foreground = createForegroundWork()
  messageStorage ||= createEventStoreMessageStorage({ eventStore })
  chunkStorage ||= eventStore ? createEventStoreChunkStorage({ eventStore }) : undefined
  const peers = new Set()
  const channels = new Map()
  const entries = new Map()
  const cancelled = new Set()
  const sendControllers = new Map()
  const offlineSends = new Set()
  const pausedSends = new Set()
  let messengerStatus
  let messengerBinding
  let storageBlocked = false
  const inboxRecovery = createPauseRecovery({
    attempt: async signal => { signal.throwIfAborted(); await drain(); if (storageBlocked) throw inboxError },
    retryable: isRecoverableStorageFailure, onError: reportError,
    setTimer: _setTimeout, clearTimer: _clearTimeout, random: _random
  })
  let inboxError
  const stateChanged = state => {
    if (closed) return
    messengerStatus = messengerStatusSnapshot(state)
    if (!available || state.closed) return
    if (state.paused) {
      for (const entry of entries.values()) {
        if (entry.peer !== owner && entry.failed && entry.status === 'pending') pausedSends.add(entry.id)
      }
      if (![...entries.values()].some(entry => pendingRetryable(entry) && !pausedSends.has(entry.id))) clearSendRetry()
      return
    }
    clearSendRetry()
    for (const id of pausedSends) { const entry = entries.get(id); if (entry) entry.failed = false }
    pausedSends.clear()
    // The callback can run inside construction/resume, before configure settles.
    queueMicrotask(() => pump())
  }
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
  let closeWork
  let available = false
  let lifecycle = 0
  let configuring = Promise.resolve()
  let configurationFailed = false
  let configureQueued = false
  let configureWork
  let messengerWork
  let fileSetupWork
  let sending
  let repump
  let draining
  let redrain = false
  const idleWaiters = new Set()
  const settled = () => { if (!sending && !draining) { for (const resolve of idleWaiters) resolve(); idleWaiters.clear() } }
  const emit = () => onOutbox([...entries.values()].filter(entry => !entry.deletion))
  const ready = () => (initialized ??= (async () => {
    storage = await openOutbox({ owner, signer })
    for (const entry of await storage.list()) entries.set(entry.id, entry)
    emit()
  })().catch(async error => { await storage?.close(); storage = null; initialized = null; throw error }))
  async function createMessenger () {
    const binding = {}
    messengerBinding = binding
    let notified = false
    let candidate
    try {
      candidate = await Messenger({
        fallbackRelays, fallbackDelayMs, seedStorage: recoveryStorage?.seeds, userSigner, channels: [], useContentKeys, _waitForForeground: foreground.wait,
        onMessageQueued: () => { if (messengerBinding === binding) return drain() },
        onStateChanged: state => {
          if (closed || messengerBinding !== binding) return
          stateChanged(state)
          notified = true
        },
        onError: reportError
      })
      if (closed) { messengerBinding = null; await candidate?.close?.(); return }
      if (typeof candidate?.readStatus !== 'function' || !notified) throw new ValidationError('MESSENGER_STATE_CONTRACT_REQUIRED')
      const status = messengerStatusSnapshot(candidate.readStatus())
      messenger = candidate
      stateChanged(status)
    } catch (error) {
      messengerBinding = null
      messengerStatus = undefined
      try { await candidate?.close?.() } catch (cleanupError) { reportError(cleanupError) }
      throw error
    }
  }

  const preparation = createPeerPreparation({
    prepare: async (peer, signal) => {
      const scoped = messengerSigner(signer.withSharedKey(peer, 'dm'))
      const pubkey = await scoped.getPublicKey()
      signal.throwIfAborted()
      const channel = { signer: scoped, pubkey, readerPubkey: pubkey, mode, seeders: seedersForPeer(peer) }
      channels.set(peer, channel)
      await schedule()
      signal.throwIfAborted()
      return channel
    },
    onReady: () => { drain(); pump() }, onError: reportError,
    isRetryable: error => ['connection', 'transport', 'timeout'].includes(error?.category) && isRetryableRelayFailure(error),
    online: _isOnline, onOnline: _onOnline, setTimer: _setTimeout, clearTimer: _clearTimeout, random: _random
  })
  async function ensureMessenger () {
    if (!messenger && !messengerWork) {
      const work = createMessenger()
      messengerWork = work
      work.finally(() => { if (messengerWork === work) messengerWork = null }).catch(() => {})
    }
    await messengerWork
  }
  async function configure () {
    if (closed || !available) return
    const version = lifecycle
    await ready()
    if (closed || !available || version !== lifecycle) return
    await ensureMessenger()
    if (!messenger) return
    if (closed || !available) { await messenger.pause('signer'); return }
    if (version !== lifecycle) { configureQueued = true; return }
    const values = [...channels].filter(([peer]) => peers.has(peer)).map(([, channel]) => channel)
    await messenger.update({ channels: values })
    if (closed || !available || version !== lifecycle) return
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
    if (closed || !available || version !== lifecycle) return
    preparation.setAvailable(true)
    if (storageBlocked) inboxRecovery.start({ immediate: true })
    if (!fileSetupWork) {
      fileSetupWork = setupFiles().catch(error => { fileSetupWork = null; if (!closed) reportError(error) })
    }
    fileSetupWork.then(() => { if (!closed && available) restoreDownloads() }).catch(reportError)
    drain(); pump()
  }
  async function setupFiles () {
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
    }
    if (!downloads) {
      const candidate = await openDownloads({ owner, signer })
      try {
        const intents = await candidate.list()
        downloads = candidate
        for (const entry of intents) downloadIntents.set(entry.id, entry)
      } catch (error) { await candidate.close?.(); throw error }
    }
  }
  function restoreDownloads () {
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
    pump()
  }
  function schedule () {
    configureQueued = true
    if (configureWork) return configureWork
    const work = Promise.resolve().then(async () => {
      try {
        do {
          configureQueued = false
          await configure()
          if (closed || !available) break
        } while (configureQueued)
      } finally { if (configureWork === work) configureWork = null }
    })
    configureWork = configuring = work
    work.then(() => {
      if (configureWork === work) configureWork = null
      configurationFailed = false
    }, error => {
      if (configureWork === work) configureWork = null
      configurationFailed = true
      if (!closed) reportError(error)
    }).catch(() => {})
    return work
  }
  async function preparePeer (peer, { retry = true } = {}) {
    if (typeof peer !== 'string' || !/^[0-9a-f]{64}$/i.test(peer)) throw new ValidationError('INVALID_PRIVATE_PEER')
    if (closed || !available || !peers.has(peer)) throw new Error('CHAT_UNAVAILABLE')
    const work = preparation.request(peer, { retry })
    work.catch(() => {})
    if (!messenger || configurationFailed) await schedule()
    return work
  }
  async function drain () {
    if (draining) { redrain = true; return }
    if (closed || !available || !messenger) return
    draining = true
    const deferred = []
    let persisted = false
    let passFailed = false
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
          if (!available || closed) { await nack(); break }
          if (await ack() === false) throw new Error('INBOX_ACK_FAILED')
          persisted = true
        } catch (error) {
          await nack()
          reportError(error)
          // Retain the failed record and suspend network ingestion until recovery.
          passFailed = true
          storageBlocked = true
          inboxError = error
          await messenger.pause('session-storage')
          if (isRecoverableStorageFailure(error)) inboxRecovery.start()
          break
        }
      }
      if (storageBlocked && persisted && !passFailed) {
        // A retry can save one record and fail the next. Only release after the
        // whole currently admissible pass completes without another failure.
        storageBlocked = false; inboxError = null
        await messenger.resume('session-storage')
      }
    } finally {
      await Promise.all(deferred.map(nack => nack()))
      draining = false; settled()
      const again = redrain; redrain = false
      if (again && !storageBlocked && !closed && available) queueMicrotask(() => drain())
    }
  }
  async function publish (peer, event, signal) {
    if (preparation.error(peer)) throw preparation.error(peer)
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
    const releaseForeground = foreground.enter()
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
      if (entry.peer !== owner && !preparation.get(entry.peer)) {
        if (preparation.error(entry.peer)) throw preparation.error(entry.peer)
        preparation.request(entry.peer).catch(() => {})
        return
      }
      if (entry.peer !== owner && messengerStatus?.paused) {
        throw unavailableError(Object.assign(new Error('PRIVATE_MESSENGER_PAUSED'), { pauseReasons: [...messengerStatus.pauseReasons] }), controller.signal)
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
        // The final durable operation is removal. Avoid encrypting a checkpoint
        // that would immediately be deleted; earlier context stages still checkpoint.
        if (entry.index + 1 < entry.events.length) await storage.put({ ...entry, index: entry.index + 1 }, { existing: true })
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
        if (entry.peer !== owner && error.retryWhenAvailable && messenger?.readStatus().paused) pausedSends.add(entry.id)
        else if (isUnavailableSend(cause) && cause.pauseReasons?.length && available && !closed) {
          // Release can race persistence of the failed attempt. Do not lose
          // its notification and spend a retry delay after the pause is gone.
          entry.failed = false
          queueMicrotask(() => pump())
        }
        reportError(error)
        // The failed wire event may be a quote or file chunk. Report the
        // owning outbox item separately, without mutating the native error.
        if (error.retryWhenOnline) offlineSends.add(entry.id)
        if (entry.failed && error.retryWhenAvailable && !pausedSends.has(entry.id)) scheduleSendRetry()
        if (!closed && !entry.deletion && !cancelled.has(entry.id) && !error.retryWhenAvailable) onSendError(error, { id: entry.id, peer: entry.peer })
      }
      emit()
    } finally { sendControllers.delete(entry.id); releaseForeground() }
  }
  function stopOnlineRetry () { stopOnline?.(); stopOnline = undefined }
  // Transient availability failures without an active messenger pause retain
  // bounded retries. Observed pauses wait exclusively for their release.
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
        if (!pendingRetryable(entry) || pausedSends.has(entry.id)) continue
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
    if (!available || closed || !storage) return
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
          if (entry.peer !== owner && entry.localSaved.every(Boolean) && !preparation.get(entry.peer) && !preparation.error(entry.peer)) {
            preparation.request(entry.peer).catch(() => {})
            continue
          }
          if (entry.peer !== owner && entry.files.length && !fileTransfers) continue
          await sendEntry(entry)
        }
      } while (repump)
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
    if (!available || closed || !peers.has(file.peer)) throw new Error('CHAT_UNAVAILABLE')
    await preparePeer(file.peer)
    await fileSetupWork
    if (!fileTransfers || !downloads) throw new Error('CHAT_UNAVAILABLE')
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
    download, cancelDownload, preparePeer,
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
      if (channel && preparation.get(peer) && !pendingPriorityRanges.has(peer) && typeof messenger?.prioritizeRange === 'function') {
        try { return await messenger.prioritizeRange(channel.pubkey, request) } catch (error) { reportError(error); return false }
      }
      const list = pendingPriorityRanges.get(peer) || []
      list.push(request)
      pendingPriorityRanges.set(peer, list)
      if (channel && available) schedule().catch(() => {})
      return true
    },
    async setPeers (values) {
      const next = new Set([...values].filter(peer => peer !== owner))
      if (next.size === peers.size && [...next].every(peer => peers.has(peer))) return configuring
      peers.clear(); for (const peer of next) peers.add(peer)
      preparation.reconcile(peers)
      for (const peer of channels.keys()) {
        if (!peers.has(peer)) {
          channels.delete(peer); pendingPriorityRanges.delete(peer)
          for (const entry of entries.values()) {
            if (entry.peer === peer) sendControllers.get(entry.id)?.abort()
          }
        }
      }
      return schedule()
    },
    async setAvailable (value) {
      const changed = available !== (value === true)
      available = value === true
      if (!changed && !available) return
      stopOnlineRetry(); offlineSends.clear()
      if (available) resetSendRetry()
      else clearSendRetry()
      if (changed) lifecycle++
      if (!available) {
        preparation.setAvailable(false)
        for (const peer of channels.keys()) if (!preparation.get(peer)) channels.delete(peer)
        inboxRecovery.stop(); await messenger?.pause('signer'); return
      }
      await ready()
      for (const entry of entries.values()) if (pendingRetryable(entry)) entry.failed = false
      if (storageBlocked) inboxRecovery.start({ immediate: true })
      if (!changed && messenger && !configurationFailed) { preparation.setAvailable(true); pump(); return configuring }
      return schedule()
    },
    async enqueue ({ peer, event, context = [], requiredFiles = [], deletion = false }) {
      if (closed || !available || (peer !== owner && !peers.has(peer))) throw new Error('CHAT_UNAVAILABLE')
      const releaseForeground = foreground.enter()
      try {
        await ready()
        const main = wireEvent(event, owner)
        const id = getEventHash(main)
        const events = [...context.map(value => wireEvent(value, owner)), main]
        const files = events.filter(value => value.kind === 1063).map(value => ({ ...decodeFileMetadata(value), optional: !requiredFiles.includes(getEventHash(value)) })).filter(file => file.service === 'irfs').flatMap(file => [file, ...(file.thumbnail?.root ? [{ ...file.thumbnail, optional: file.optional }] : [])])
        const entry = { id, peer, event: { ...main, id }, events, files, index: 0, fileIndex: 0, chunkIndex: 0, localSaved: context.map(() => true).concat(false), status: 'pending', deletion }
        await storage.put(entry)
        entries.set(id, entry); emit(); pump()
        if (peer !== owner) preparePeer(peer).catch(() => {})
        return id
      } finally { releaseForeground() }
    },
    async retry (id) {
      offlineSends.delete(id); pausedSends.delete(id); watchOfflineSends()
      if (storageBlocked) inboxRecovery.start({ immediate: true })
      await ready()
      const entry = entries.get(id)
      if (entry) { entry.failed = false; resetSendRetry(); if (entry.peer !== owner) preparePeer(entry.peer).catch(() => {}); return pump() }
    },
    async cancel (id) {
      cancelled.add(id)
      sendControllers.get(id)?.abort()
      offlineSends.delete(id); pausedSends.delete(id); watchOfflineSends()
      await ready()
      entries.delete(id)
      await storage.remove(id)
      emit()
    },
    close () {
      return (closeWork ??= (async () => {
        closed = true; available = false; lifecycle++
        stopOnlineRetry(); offlineSends.clear(); activeDownloads.clear(); pendingPriorityRanges.clear()
        clearSendRetry(); inboxRecovery.stop(); preparation.close(); foreground.close(); pausedSends.clear()
        for (const controller of sendControllers.values()) controller.abort()
        await Promise.allSettled([configuring, initialized, messengerWork, fileSetupWork])
        await messenger?.pause('closed')
        if (sending || draining) await new Promise(resolve => idleWaiters.add(resolve))
        await messenger?.close()
        await storage?.close()
        await downloadWriteTail.catch(reportError)
        await downloads?.close()
      })())
    }
  }
}

export { createChatOutbox as createPrivateSessionStorage } from './helpers/work-storage.js'
export { messengerSigner as createMessengerSigner } from './helpers/signer.js'
