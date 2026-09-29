import NMMR from 'nmmr'
import { createFileCatalog } from './helpers/catalog.js'
import { ValidationError } from '../../error/index.js'
import { IRFS_CHUNK_BYTES, decodeIrfsChunk } from '../../irfs/index.js'
import { createQueue } from '../../idb-queue/index.js'
import * as channelTransport from '../../private-channel/index.js'
import * as messages from '../../private-message/index.js'
import { readFileChunkIndex } from '../../private-channel/helpers/event.js'
import { compactSeedRouterRows, compactRecordsFromSeed, createEventReplyPacker, ROUTER_SEED_RECORD_TYPE } from '../recovery/index.js'

export const FILE_CHUNKS_REQUEST_CODE = 'fileChunksRequest_p5cc'
export const FILE_CHUNKS_REPLY_CODE = 'fileChunksReply_p5cc'
export const IRFS_CHUNK_RECORD_TYPE = 'irfsChunk_v1'
export const AUTO_DOWNLOAD_BYTES = 1024 * 1024
export const FILE_REQUEST_CHUNKS = 16
const HEX = /^[0-9a-f]{64}$/
const now = () => Math.floor(Date.now() / 1000)

export function fileChannelInfo (root) {
  if (typeof root !== 'string' || !HEX.test(root)) throw new ValidationError('INVALID_FILE_ROOT')
  return `dm:media:${root}`
}

export function decodeMissingRanges (ranges) {
  if (!Array.isArray(ranges) || !ranges.length) throw new ValidationError('INVALID_FILE_CHUNK_RANGES')
  const indices = []
  let previous = -1
  for (const range of ranges) {
    if (!Array.isArray(range) || range.length !== 2) throw new ValidationError('INVALID_FILE_CHUNK_RANGES')
    const [first, last] = range
    if (![first, last].every(Number.isSafeInteger) || first < 0 || first <= previous || last < first || last - first + 1 > FILE_REQUEST_CHUNKS - indices.length) throw new ValidationError('INVALID_FILE_CHUNK_RANGES')
    for (let index = first; index <= last; index++) indices.push(index)
    previous = last
  }
  return indices
}

function rangesFor (indices) {
  const ranges = []
  for (const index of indices) {
    const last = ranges.at(-1)
    if (last && last[1] + 1 === index) last[1] = index
    else ranges.push([index, index])
  }
  return ranges
}

function assertPublished (result) {
  const reports = result?.delivery?.reports
  if (!Array.isArray(reports) || !reports.length || !reports.every(report => report?.success === true)) throw Object.assign(new Error('FILE_NOT_PUBLISHED'), { code: 'MESSAGE_NOT_PUBLISHED' })
}

function waitForTransfer (promise, signal) {
  signal?.throwIfAborted()
  if (!signal) return promise
  return new Promise((resolve, reject) => {
    const aborted = () => reject(signal.reason)
    signal.addEventListener('abort', aborted, { once: true })
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', aborted))
  })
}

// One coordinator per account. Signer derivation and the local IRFS database
// belong to the caller; recovery, validation and scheduling belong here.
export function createPrivateFileTransfer ({ messenger, resolveChannel, storage, onError = () => {}, _transport = channelTransport, _messages = messages, _hedgeMs = 2000, _idleMs = 30000, _now = now }) {
  if (!messenger?.extensions || typeof resolveChannel !== 'function' || !storage?.read || !storage?.save) throw new ValidationError('INVALID_FILE_TRANSFER_OPTIONS')
  const registrations = new Map()
  const jobs = new Map()
  const listeners = new Set()
  const successes = new Map()
  const work = new Set()
  const requests = []
  let replying = 0
  let active = 0
  let closed = false
  let queuePromise
  const queue = () => (queuePromise ??= createQueue({ prefix: `${messenger.prefix}:file-seeds`, indexedDB: messenger._indexedDB, maxBytes: 64 * 1024 * 1024, evictionPolicy: 'fifo', indexes: { key: { keyPath: 'key', unique: true }, channel: 'fileChannelPubkey' } }))
  const catalog = createFileCatalog({ messenger, now: _now })
  catalog.ready.catch(onError)
  const keyFor = (control, root) => `${control}:${root}`
  const emit = job => { for (const listener of listeners) listener({ root: job.file.root, controlChannelPubkey: job.file.controlChannelPubkey, status: job.status, completed: job.completed, total: job.file.size, error: job.error }) }
  const usable = file => {
    if (closed) throw new Error('FILE_TRANSFER_CLOSED')
    messenger.requireWritableChannel(file.controlChannelPubkey)
    if (!messenger.desiredChannels.has(file.controlChannelPubkey)) throw new Error('FILE_TRANSFER_CHANNEL_UNWATCHED')
  }
  async function register ({ controlChannelPubkey, peerPubkey, root, size, sharedAt }) {
    fileChannelInfo(root)
    if (!HEX.test(controlChannelPubkey) || !HEX.test(peerPubkey) || (size !== undefined && (!Number.isSafeInteger(size) || size < 1))) throw new ValidationError('INVALID_FILE_TRANSFER_DESCRIPTOR')
    if (sharedAt !== undefined && (!Number.isSafeInteger(sharedAt) || sharedAt < 0)) throw new ValidationError('INVALID_FILE_SHARED_AT')
    const key = keyFor(controlChannelPubkey, root)
    const previous = registrations.get(key)
    if (previous && (previous.peerPubkey !== peerPubkey || (size !== undefined && previous.size !== undefined && previous.size !== size))) throw new ValidationError('CONFLICTING_FILE_TRANSFER_DESCRIPTOR')
    const file = previous || { controlChannelPubkey, peerPubkey, root }
    if (size !== undefined) file.size = size
    if (sharedAt !== undefined) file.sharedAt = Math.max(file.sharedAt ?? 0, sharedAt)
    registrations.set(key, file)
    file.ready ||= (async () => {
      file.signer = await resolveChannel({ controlChannelPubkey, peerPubkey, root, info: fileChannelInfo(root) })
      file.fileChannelPubkey = await file.signer.getPublicKey()
      if (!HEX.test(file.fileChannelPubkey)) throw new ValidationError('INVALID_FILE_CHANNEL_PUBKEY')
    })().catch(error => { if (registrations.get(key) === file) registrations.delete(key); throw error })
    await file.ready
    return file
  }
  function seedEnabled (file) {
    const parent = messenger.channels.get(file.controlChannelPubkey)
    return ['seeder', 'watchtower'].includes(parent?.mode) && messenger.offlineRecoverySecondsFor(parent) > 0
  }
  async function authorizeSeeding (descriptor, { receiverPubkeys, sharedAt } = {}) {
    if (!Array.isArray(receiverPubkeys) || !receiverPubkeys.length || !receiverPubkeys.every(key => typeof key === 'string' && HEX.test(key))) throw new ValidationError('INVALID_FILE_AUTHORIZATION_RECEIVERS')
    if (!Number.isSafeInteger(sharedAt) || sharedAt < 0) throw new ValidationError('INVALID_FILE_SHARED_AT')
    const file = await register(descriptor)
    usable(file)
    await catalog.authorize(file, [...new Set(receiverPubkeys)], sharedAt)
  }
  async function prune () {
    await catalog.prune()
    if (!queuePromise) return
    await (await queue()).removeWhere(row => {
      const parent = messenger.channels.get(row.controlChannelPubkey)
      return parent?.mode !== 'watchtower' || !seedEnabled(row) || row.expiresAt <= _now() || row.receivedAt + messenger.offlineRecoverySecondsFor(parent) <= _now()
    })
  }
  async function saveSeed (file, seed) {
    if (messenger.channels.get(file.controlChannelPubkey)?.mode !== 'watchtower' || !seedEnabled(file)) return
    const index = readFileChunkIndex(seed.router)
    if (index === undefined) return
    const db = await queue()
    for (const row of compactSeedRouterRows(seed)) {
      if (![file.peerPubkey, messenger.userPubkey].includes(row.receiverPubkey)) continue
      const key = `${file.controlChannelPubkey}:${file.fileChannelPubkey}:${row.receiverPubkey}:${index}`
      await db.putBy('key', { ...row, key, root: file.root, controlChannelPubkey: file.controlChannelPubkey, fileChannelPubkey: file.fileChannelPubkey, peerPubkey: file.peerPubkey, chunkIndex: index, receivedAt: _now(), expiresAt: _now() + messenger.offlineRecoverySecondsFor(file.controlChannelPubkey) })
    }
    await prune()
  }
  function checked (file, event, index) {
    const decoded = decodeIrfsChunk(event)
    if (decoded.root !== file.root || (index !== undefined && decoded.index !== index) || (file.size !== undefined && (decoded.total !== Math.ceil(file.size / IRFS_CHUNK_BYTES) || (decoded.index === decoded.total - 1 && decoded.contentBytes.length !== file.size - decoded.index * IRFS_CHUNK_BYTES)))) throw new ValidationError('FILE_CHUNK_DESCRIPTOR_MISMATCH')
    return decoded
  }
  async function routing (file, receiverPubkey) {
    const parent = messenger.requireWritableChannel(file.controlChannelPubkey)
    return messenger.resolveSendRouting({ channel: parent, receiverPubkeys: [receiverPubkey] })
  }
  function sendOptions (file, routes) {
    return { senderSigner: messenger.userSigner, imkcSigner: messenger.contentKeySigner, privateChannelSigner: file.signer, ...routes, temporaryStorageArea: messenger.temporaryStorageArea, expirationSeconds: messenger.eventExpirationSecondsFor(file.controlChannelPubkey), _getIykcProofs: messenger.contentKeyLookup() }
  }
  async function publishChunk (descriptor, event) {
    const file = await register(descriptor)
    usable(file)
    const decoded = checked(file, event)
    await catalog.ready
    const options = { ...sendOptions(file, await routing(file, file.peerPubkey)), receiverPubkeys: [file.peerPubkey], fileChunkIndex: decoded.index, ...(messenger.channels.get(file.controlChannelPubkey)?.mode === 'watchtower' ? { onPreparedSeed: seed => saveSeed(file, seed) } : {}) }
    const result = event.sig ? await _messages.broadcastEvent({ ...options, event }) : await _messages.broadcastRumor({ ...options, rumor: event })
    assertPublished(result)
    // A scheduler turn between chunks lets queued text/control publications run.
    await new Promise(resolve => setTimeout(resolve, 0))
    return result
  }
  async function replyToRequest (controlChannelPubkey, message) {
    if (message.payload?.code !== FILE_CHUNKS_REQUEST_CODE) return false
    const question = message.question || message.event
    if (message.provenance === 'hearsay' || message.senderPubkey !== question?.pubkey || question.tags?.find(tag => tag[0] === 'r')?.[1] !== messenger.userPubkey) return true
    const payload = message.payload.payload
    if (!HEX.test(payload?.fileChannelPubkey)) throw new ValidationError('INVALID_FILE_TRANSFER_DESCRIPTOR')
    const indices = new Set(decodeMissingRanges(payload.missingRanges))
    await prune()
    const grant = await catalog.find(controlChannelPubkey, payload.fileChannelPubkey, question.pubkey)
    if (grant) {
      const file = await register(grant)
      usable(file)
      if (file.fileChannelPubkey !== payload.fileChannelPubkey) throw new ValidationError('FILE_CHANNEL_MISMATCH')
      const routes = await routing(file, question.pubkey)
      const packer = createEventReplyPacker({
        messenger: {
          reply: async options => {
            usable(file)
            if (!await catalog.find(controlChannelPubkey, file.fileChannelPubkey, question.pubkey)) return
            return _messages.reply({ ...sendOptions(file, routes), ...options })
          }
        },
        channelPubkey: file.fileChannelPubkey, question, receiverPubkey: question.pubkey,
        code: FILE_CHUNKS_REPLY_CODE, eventsPerChunk: 1, recordsFromInput: record => [record]
      })
      for (const index of indices) {
        usable(file)
        if (!await catalog.find(controlChannelPubkey, file.fileChannelPubkey, question.pubkey)) break
        const event = await storage.read(file.root, index, file)
        if (!event) continue
        let decoded
        try { decoded = checked(file, event, index) } catch (error) { if (error instanceof ValidationError) { onError(error); continue } throw error }
        const proof = event.tags.find(tag => tag[0] === 'mmr')[3]
        await packer.update({ recordType: IRFS_CHUNK_RECORD_TYPE, index, total: decoded.total, proof, content: event.content })
        await new Promise(resolve => setTimeout(resolve, 0))
      }
      await packer.finalize()
      return true
    }
    if (messenger.channels.get(controlChannelPubkey)?.mode !== 'watchtower') return true
    const db = await queue()
    let packer
    for await (const seed of db.storedItemsBy('channel', payload.fileChannelPubkey)) {
      if (seed.controlChannelPubkey !== controlChannelPubkey || seed.receiverPubkey !== question.pubkey || !indices.has(seed.chunkIndex) || !seedEnabled(seed)) continue
      const file = await register(seed)
      usable(file)
      if (file.fileChannelPubkey !== payload.fileChannelPubkey) throw new ValidationError('FILE_CHANNEL_MISMATCH')
      if (!packer) {
        const routes = await routing(file, question.pubkey)
        packer = createEventReplyPacker({ messenger: { reply: async options => { usable(file); return _messages.reply({ ...sendOptions(file, routes), ...options }) } }, channelPubkey: file.fileChannelPubkey, question, receiverPubkey: question.pubkey, code: FILE_CHUNKS_REPLY_CODE, eventsPerChunk: 1, recordsFromInput: row => compactRecordsFromSeed(row, { receiverPubkey: question.pubkey }) })
      }
      await packer.update(seed)
      await new Promise(resolve => setTimeout(resolve, 0))
    }
    await packer?.finalize()
    return true
  }
  async function run (job) {
    const file = job.file
    const signal = job.controller.signal
    let sub
    const historyController = new AbortController()
    let ingest = Promise.resolve()
    let total = file.size === undefined ? undefined : Math.ceil(file.size / IRFS_CHUNK_BYTES)
    const persisted = new Set()
    let lastProgress = Date.now()
    let requestRoundAt = Date.now()
    const requestedSeeders = new Set()
    const validSeeders = new Set([file.peerPubkey, ...messenger.recoverySeeders(file.controlChannelPubkey)])
    let terminalError
    const fail = error => { terminalError ||= error }
    async function accept (event, index, { save = true } = {}) {
      signal.throwIfAborted()
      usable(file)
      const chunk = checked(file, event, index)
      if (total !== undefined && chunk.total !== total) throw new ValidationError('FILE_CHUNK_TOTAL_MISMATCH')
      total = chunk.total
      if (persisted.has(chunk.index)) return
      if (save) await storage.save(event, file)
      signal.throwIfAborted()
      persisted.add(chunk.index)
      job.completed += chunk.contentBytes.length
      lastProgress = Date.now()
      if (chunk.index === total - 1 && file.size === undefined) file.size = (total - 1) * IRFS_CHUNK_BYTES + chunk.contentBytes.length
      emit(job)
    }
    const consume = (event, outer, meta) => {
      const task = ingest.then(async () => {
        if (signal.aborted) return
        if (event.kind === messages.REPLY_KIND) {
          const reply = messages.parseRumorContent(event)
          if (reply.code !== FILE_CHUNKS_REPLY_CODE || meta.provenance === 'hearsay' || meta.senderPubkey !== event.pubkey || !validSeeders.has(event.pubkey) || event.tags?.find(tag => tag[0] === 'r')?.[1] !== messenger.userPubkey) return
          const jsonl = reply.payload?.jsonl
          if (typeof jsonl !== 'string') throw new ValidationError('INVALID_FILE_CHUNK_REPLY')
          const rows = jsonl.split('\n').filter(Boolean)
          if (rows.length > FILE_REQUEST_CHUNKS) throw new ValidationError('INVALID_FILE_CHUNK_REPLY')
          for (const line of rows) {
            let record
            try { record = JSON.parse(line) } catch { throw new ValidationError('INVALID_FILE_CHUNK_REPLY') }
            if (record.recordType === IRFS_CHUNK_RECORD_TYPE) {
              if (!Number.isSafeInteger(record.index) || record.index < 0 || !Number.isSafeInteger(record.total) || record.total <= record.index || typeof record.content !== 'string' || record.content.length > 100000 || typeof record.proof !== 'string' || record.proof.length > 8192) throw new ValidationError('INVALID_FILE_CHUNK_REPLY')
              const chunk = { kind: 34601, created_at: event.created_at, tags: [['d', NMMR.deriveChunkId(file.root, record.index)], ['mmr', String(record.index), String(record.total), record.proof]], content: record.content }
              await accept(chunk, record.index)
              successes.set(event.pubkey, Date.now())
              continue
            }
            if (record.recordType !== ROUTER_SEED_RECORD_TYPE) continue
            const index = readFileChunkIndex(record.router)
            if (index === undefined) throw new ValidationError('INVALID_FILE_CHUNK_INDEX')
            const original = await _transport.unwrapRouterEvent({ router: record.router, receiverSigner: messenger.userSigner, receiverPubkey: messenger.userPubkey, channelPubkey: file.fileChannelPubkey })
            if (original) { await accept(original, index); successes.set(event.pubkey, Date.now()) }
          }
        } else {
          if (meta.senderPubkey !== file.peerPubkey) return
          const index = readFileChunkIndex(meta.router)
          if (index === undefined) throw new ValidationError('INVALID_FILE_CHUNK_INDEX')
          await accept(event, index)
          // Only direct, validated delivery proves this identity was a recipient.
          // Local cache hits and recovery replies never create/renew a grant.
          if (file.sharedAt !== undefined) await authorizeSeeding(file, { receiverPubkeys: [messenger.userPubkey], sharedAt: file.sharedAt })
        }
      })
      // Malformed remote records do not poison the ingestion chain. Operational
      // persistence failures stop the job so completion cannot outrun storage.
      ingest = task.catch(error => { if (error instanceof ValidationError) onError(error); else if (!signal.aborted) fail(error) })
      return ingest
    }
    try {
      job.status = 'downloading'; emit(job)
      // Resume from independently verified local chunks, never from an ACK.
      for (let index = 0; index < (total ?? 1); index++) {
        signal.throwIfAborted()
        const event = await storage.read(file.root, index, file)
        if (event) await accept(event, index, { save: false })
      }
      if (total !== undefined && persisted.size === total) return
      const parent = messenger.requireWritableChannel(file.controlChannelPubkey)
      const relays = await messenger.resolveWatchRelays(parent)
      const options = { receiverSigner: messenger.userSigner, privateChannelSigner: file.signer, privateChannelPubkey: file.fileChannelPubkey, receiverPubkey: messenger.userPubkey, relays, receivedChunkIndexedDB: messenger._indexedDB, receivedChunkScope: `${messenger.userPubkey}:file:${file.fileChannelPubkey}`, onEvent: consume, onSeedEvent: seed => saveSeed(file, seed), mode: messenger.channels.get(file.controlChannelPubkey)?.mode === 'watchtower' ? 'watchtower' : 'leecher', onError }
      sub = _transport.subscribe({ ...options, liveOnly: true })
      const stop = () => { sub?.close() }
      signal.addEventListener('abort', stop, { once: true })
      try {
        await sub.ready
        signal.throwIfAborted()
        // Partial/failed history still permits recovery from peers.
        const history = _transport.fetch({ ...options, signal: AbortSignal.any([signal, historyController.signal]), since: 0 }).catch(error => { if (!signal.aborted) onError(error) })
        work.add(history); history.finally(() => work.delete(history))
        const seeders = [...validSeeders].sort((a, b) => (successes.get(b) || 0) - (successes.get(a) || 0) || Math.random() - 0.5)
        while (true) {
          signal.throwIfAborted(); usable(file)
          await ingest
          if (terminalError) throw terminalError
          if (total !== undefined && persisted.size === total) break
          const missing = []
          for (let index = 0; index < (total ?? 1) && missing.length < FILE_REQUEST_CHUNKS; index++) if (!persisted.has(index)) missing.push(index)
          const remaining = seeders.filter(peer => !requestedSeeders.has(peer))
          if (remaining.length && Date.now() >= requestRoundAt) {
            const peer = remaining[0]
            requestedSeeders.add(peer)
            requestRoundAt = Date.now() + _hedgeMs
            // The request is on dm; its compact reply is on the file channel.
            await messenger.ask({ channelPubkey: file.controlChannelPubkey, receiverPubkey: peer, code: FILE_CHUNKS_REQUEST_CODE, payload: { fileChannelPubkey: file.fileChannelPubkey, missingRanges: rangesFor(missing) } }).catch(onError)
          }
          if (!remaining.length && Date.now() - Math.max(lastProgress, requestRoundAt - _hedgeMs) >= _idleMs) throw new Error('FILE_DOWNLOAD_STALLED')
          // Start a fresh batch when all positions of the previous batch arrived.
          if (job.batch?.every(index => persisted.has(index))) { requestedSeeders.clear(); requestRoundAt = Date.now(); job.batch = null }
          job.batch ||= missing
          await new Promise(resolve => { const timer = setTimeout(done, Math.min(100, _hedgeMs)); function done () { clearTimeout(timer); signal.removeEventListener('abort', done); resolve() } signal.addEventListener('abort', done, { once: true }) })
        }
      } finally { signal.removeEventListener('abort', stop) }
    } finally { historyController.abort(); await sub?.close(); await ingest }
    if (job.completed !== file.size) throw new ValidationError('FILE_SIZE_MISMATCH')
  }
  function pump () {
    if (closed) return
    for (const job of [...jobs.values()].filter(job => job.status === 'queued').sort((a, b) => b.priority - a.priority)) {
      if (active >= 2) break
      active++; job.status = 'starting'
      const task = run(job).then(() => { job.status = 'complete'; emit(job); job.resolve(job.file) }, error => { job.status = job.pauseRequested ? 'paused' : job.controller.signal.aborted ? 'cancelled' : 'error'; job.error = error; emit(job); job.reject(error) }).finally(() => { active--; work.delete(task); pump() })
      work.add(task)
    }
  }
  async function download (descriptor, { manual = false, thumbnail = false, signal } = {}) {
    const file = await register(descriptor)
    usable(file)
    const existing = jobs.get(keyFor(file.controlChannelPubkey, file.root))
    if (existing && !['error', 'cancelled', 'paused'].includes(existing.status)) return waitForTransfer(existing.promise, signal)
    if (!manual && (file.size === undefined || file.size > (thumbnail ? IRFS_CHUNK_BYTES : AUTO_DOWNLOAD_BYTES))) {
      // Locally complete files need no network consent, including the sender's
      // own originals and files already downloaded before a restart.
      let total, completed = 0
      for (let index = 0; index < (total ?? 1); index++) {
        signal?.throwIfAborted(); usable(file)
        const event = await storage.read(file.root, index, file)
        if (!event) throw new Error('FILE_DOWNLOAD_REQUIRES_ACTION')
        const chunk = checked(file, event, index)
        if (total !== undefined && total !== chunk.total) throw new ValidationError('FILE_CHUNK_TOTAL_MISMATCH')
        total = chunk.total; completed += chunk.contentBytes.length
      }
      file.size ??= completed
      if (completed !== file.size) throw new ValidationError('FILE_SIZE_MISMATCH')
      const job = { file, completed, status: 'complete', promise: Promise.resolve(file) }
      jobs.set(keyFor(file.controlChannelPubkey, file.root), job); emit(job)
      return file
    }
    const key = keyFor(file.controlChannelPubkey, file.root)
    let job = jobs.get(key)
    if (!job || ['error', 'cancelled', 'paused'].includes(job.status)) {
      const controller = new AbortController()
      const { promise, resolve, reject } = Promise.withResolvers()
      job = { file, controller, resolve, reject, promise, completed: 0, manual, thumbnail, priority: thumbnail ? 2 : manual ? 1 : 0, status: 'queued' }
      jobs.set(key, job); emit(job); pump()
    }
    // A view leaving does not cancel another consumer's shared download.
    return waitForTransfer(job.promise, signal)
  }
  const cancelJob = job => {
    if (['complete', 'error', 'cancelled'].includes(job.status)) return
    const error = new DOMException('Download cancelled', 'AbortError')
    job.controller.abort(error)
    if (job.status === 'queued') { job.status = job.pauseRequested ? 'paused' : 'cancelled'; job.reject(error); emit(job) }
  }
  function drainRequests () {
    if (closed) return
    while (replying < 2 && requests.length) {
      const [channel, message] = requests.shift()
      replying++
      const task = replyToRequest(channel, message).catch(error => { if (!(closed && error.message === 'FILE_TRANSFER_CLOSED')) onError(error) }).finally(() => { replying--; work.delete(task); drainRequests() })
      work.add(task)
    }
  }
  const extension = {
    handleAsk (channel, message) {
      if (message.payload?.code !== FILE_CHUNKS_REQUEST_CODE) return false
      // Never hold the DM dispatcher while publishing a batch of file bytes.
      // Bound both workers and queued requests; discarded excess is retryable.
      if (!closed && requests.length < 16) { requests.push([channel, message]); drainRequests() }
      return true
    },
    pause () { requests.length = 0; for (const job of jobs.values()) { if (!['complete', 'error', 'cancelled'].includes(job.status)) job.pauseRequested = true; cancelJob(job) } },
    async resume () {
      await Promise.allSettled([...work])
      for (const job of jobs.values()) if (job.pauseRequested && job.status !== 'complete') download(job.file, { manual: job.manual, thumbnail: job.thumbnail }).catch(onError)
    },
    unwatch (channels) { for (const job of jobs.values()) if (channels.includes(job.file.controlChannelPubkey)) cancelJob(job) },
    async close () { if (closed) return; closed = true; extension.pause(); clearInterval(timer); await Promise.allSettled([...work]); await catalog.close(); if (queuePromise) await (await queuePromise).close(); messenger.extensions.delete(extension); listeners.clear() }
  }
  const timer = setInterval(() => { prune().catch(onError) }, 60000)
  timer.unref?.()
  messenger.extensions.add(extension)
  return { register, authorizeSeeding, publishChunk, download, close: extension.close, cancel (control, root) { const job = jobs.get(keyFor(control, root)); if (job) { job.pauseRequested = false; cancelJob(job) } }, observe (listener) { listeners.add(listener); for (const job of jobs.values()) emit(job); return () => listeners.delete(listener) } }
}
