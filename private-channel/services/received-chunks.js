import { bytesToBase64 } from '../../base64/index.js'
import { run } from '../../idb/index.js'

export const DEFAULT_RECEIVED_CHUNK_TTL_MS = 60 * 60 * 1000 // 1 hour
export const DEFAULT_RECEIVED_CHUNK_MAX_BYTES = 16 * 1024 * 1024 // 16 MiB

const DEFAULT_PREFIX = 'libp2r2p:private-channel:received'
const DATABASE_VERSION = 1
const GROUPS_STORE = 'groups'
const CHUNKS_STORE = 'chunks'
const STATE_STORE = 'state'
const USAGE_KEY = 'usage'
const DRAIN_LEASE_MS = 30_000
const decoder = new TextDecoder()

/*
IndexedDB schema, scoped per received-chunk prefix:

database `${prefix}:idb`, version 1

groups, keyPath "groupKey"
  groupKey                  `${channelPubkey}:${routerPubkey}`
  channelPubkey/routerPubkey private-channel coordinates
  total                     expected chunk count
  received/receivedCount    sparse received-index map and its count
  nextIndex/rowIndex/carry  incremental payload decoding state
  payloadCiphertext         accumulated encrypted payload text
  receiverPubkeys           deduplicated intended receivers
  byteSize                  logical bytes charged to this group
  createdAt/updatedAt        lifecycle timestamps in milliseconds
  drainToken/drainUntil      optional drain lease owner and expiry

groups indexes
  byUpdatedAt  updatedAt

chunks, keyPath ["groupKey", "index"]
  groupKey  owning group coordinate
  index     zero-based chunk position
  bytes     raw chunk bytes stored as Uint8Array

chunks indexes
  byGroup  groupKey

state, keyPath "key"
  key        state record name, currently "usage"
  usedBytes  total logical bytes held by all groups
*/

function deferred () {
  let resolve
  let reject
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

function transactionDone (tx) {
  const p = deferred()
  tx.oncomplete = () => p.resolve()
  tx.onabort = () => p.reject(tx.error || new Error('IDB_TRANSACTION_ABORTED'))
  tx.onerror = () => p.reject(tx.error || new Error('IDB_TRANSACTION_FAILED'))
  return p.promise
}

function openDatabase (indexedDB, name) {
  if (!indexedDB?.open) return Promise.reject(new Error('IDB_UNAVAILABLE'))
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, DATABASE_VERSION)
    request.onerror = () => reject(request.error || new Error('IDB_OPEN_FAILED'))
    request.onblocked = () => reject(new Error('IDB_DATABASE_BLOCKED'))
    request.onupgradeneeded = () => {
      const db = request.result
      const tx = request.transaction
      let groups
      if (!db.objectStoreNames.contains(GROUPS_STORE)) {
        groups = db.createObjectStore(GROUPS_STORE, { keyPath: 'groupKey' })
      } else {
        groups = tx.objectStore(GROUPS_STORE)
      }
      if (!groups.indexNames.contains('byUpdatedAt')) groups.createIndex('byUpdatedAt', 'updatedAt')

      let chunks
      if (!db.objectStoreNames.contains(CHUNKS_STORE)) {
        chunks = db.createObjectStore(CHUNKS_STORE, { keyPath: ['groupKey', 'index'] })
      } else {
        chunks = tx.objectStore(CHUNKS_STORE)
      }
      if (!chunks.indexNames.contains('byGroup')) chunks.createIndex('byGroup', 'groupKey')

      if (!db.objectStoreNames.contains(STATE_STORE)) db.createObjectStore(STATE_STORE, { keyPath: 'key' })
    }
    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      resolve(db)
    }
  })
}

function normalizeBytes (value) {
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  throw new Error('RECEIVED_CHUNK_BYTES_REQUIRED')
}

function uniq (values) {
  return [...new Set((values || []).filter(Boolean))]
}

function normalizeReceived (received) {
  if (!received || typeof received !== 'object' || Array.isArray(received)) return {}
  return Object.fromEntries(
    Object.entries(received)
      .filter(([index, hasChunk]) => hasChunk && Number.isSafeInteger(Number(index)) && Number(index) >= 0)
      .map(([index]) => [String(Number(index)), true])
  )
}

function normalizeMeta (meta) {
  if (!meta || typeof meta !== 'object') return null
  const total = Number(meta.total)
  const nextIndex = Number(meta.nextIndex)
  const rowIndex = Number(meta.rowIndex)
  if (!meta.groupKey || !Number.isSafeInteger(total) || total < 1) return null
  return {
    groupKey: String(meta.groupKey),
    channelPubkey: String(meta.channelPubkey || ''),
    routerPubkey: String(meta.routerPubkey || ''),
    total,
    received: normalizeReceived(meta.received),
    receivedCount: Math.max(0, Number(meta.receivedCount) || 0),
    nextIndex: Number.isSafeInteger(nextIndex) && nextIndex >= 0 ? nextIndex : 0,
    rowIndex: Number.isSafeInteger(rowIndex) && rowIndex >= 0 ? rowIndex : 0,
    carry: typeof meta.carry === 'string' ? meta.carry : '',
    payloadCiphertext: typeof meta.payloadCiphertext === 'string' ? meta.payloadCiphertext : '',
    receiverPubkeys: uniq(meta.receiverPubkeys),
    byteSize: Math.max(0, Number(meta.byteSize) || 0),
    createdAt: Number(meta.createdAt) || Date.now(),
    updatedAt: Number(meta.updatedAt) || Date.now(),
    drainToken: typeof meta.drainToken === 'string' ? meta.drainToken : '',
    drainUntil: Math.max(0, Number(meta.drainUntil) || 0)
  }
}

function normalizeUsage (value) {
  return {
    key: USAGE_KEY,
    usedBytes: Number.isSafeInteger(value?.usedBytes) && value.usedBytes >= 0 ? value.usedBytes : 0
  }
}

function isQuotaExceeded (err) {
  return err?.name === 'QuotaExceededError' ||
    err?.name === 'NS_ERROR_DOM_QUOTA_REACHED' ||
    err?.code === 22 ||
    err?.code === 1014 ||
    /quota/i.test(err?.message || '')
}

function randomToken () {
  const bytes = globalThis.crypto?.getRandomValues?.(new Uint8Array(16))
  if (bytes) return [...bytes].map(value => value.toString(16).padStart(2, '0')).join('')
  return `${Date.now().toString(16)}:${Math.random().toString(16).slice(2)}`
}

function wait (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export function createReceivedChunkStore ({
  prefix = DEFAULT_PREFIX,
  indexedDB = globalThis.indexedDB,
  ttlMs = DEFAULT_RECEIVED_CHUNK_TTL_MS,
  maxBytes = DEFAULT_RECEIVED_CHUNK_MAX_BYTES
} = {}) {
  const configuredTtlMs = Number.isFinite(ttlMs) && ttlMs >= 0 ? ttlMs : DEFAULT_RECEIVED_CHUNK_TTL_MS
  const configuredMaxBytes = Number.isFinite(maxBytes) && maxBytes > 0 ? maxBytes : Infinity
  let dbPromise
  let readyPromise

  function database () {
    dbPromise ||= openDatabase(indexedDB, `${prefix}:idb`)
    return dbPromise
  }

  async function transaction (storeNames, mode, work) {
    const db = await database()
    const tx = db.transaction(storeNames, mode)
    const done = transactionDone(tx)
    try {
      // `work` may await only IDB requests issued through `run` with this tx.
      const result = await work(tx)
      await done
      return result
    } catch (err) {
      try { tx.abort() } catch {}
      try { await done } catch {}
      throw err
    }
  }

  function groupKeyFor (channelPubkey, routerPubkey) {
    return `${channelPubkey}:${routerPubkey}`
  }

  async function readUsage (tx) {
    return normalizeUsage((await run('get', [USAGE_KEY], STATE_STORE, null, { tx })).result)
  }

  async function writeUsage (tx, usage) {
    await run('put', [normalizeUsage(usage)], STATE_STORE, null, { tx })
  }

  async function deleteGroupInTransaction (tx, groupKey, usage, knownMeta) {
    const meta = knownMeta || normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
    const keys = (await run('getAllKeys', [globalThis.IDBKeyRange?.only?.(groupKey) ?? groupKey], CHUNKS_STORE, 'byGroup', { tx })).result
    for (const key of keys) await run('delete', [key], CHUNKS_STORE, null, { tx })
    await run('delete', [groupKey], GROUPS_STORE, null, { tx })
    if (usage && meta) usage.usedBytes = Math.max(0, usage.usedBytes - meta.byteSize)
    return Boolean(meta || keys.length)
  }

  async function oldestGroupsInTransaction (tx, except = '') {
    const values = (await run('getAll', [], GROUPS_STORE, null, { tx })).result
    return values
      .map(normalizeMeta)
      .filter(meta => meta && meta.groupKey !== except)
      .sort((left, right) => left.updatedAt - right.updatedAt || left.groupKey.localeCompare(right.groupKey))
  }

  async function cleanupStaleRaw (nowMs = Date.now()) {
    const cutoff = nowMs - configuredTtlMs
    return transaction([GROUPS_STORE, CHUNKS_STORE, STATE_STORE], 'readwrite', async tx => {
      const usage = await readUsage(tx)
      const groups = await oldestGroupsInTransaction(tx)
      let removed = 0
      for (const meta of groups) {
        if (meta.updatedAt > cutoff && (!Number.isFinite(configuredMaxBytes) || usage.usedBytes <= configuredMaxBytes)) continue
        await deleteGroupInTransaction(tx, meta.groupKey, usage, meta)
        removed++
      }
      await writeUsage(tx, usage)
      return removed
    })
  }

  function ready () {
    readyPromise ||= cleanupStaleRaw()
    return readyPromise
  }

  async function cleanupStale (nowMs = Date.now()) {
    await ready()
    return cleanupStaleRaw(nowMs)
  }

  async function putOnce ({ channelPubkey, routerPubkey, index, total, contentBytes }) {
    const groupKey = groupKeyFor(channelPubkey, routerPubkey)
    const bytes = normalizeBytes(contentBytes)
    const now = Date.now()

    return transaction([GROUPS_STORE, CHUNKS_STORE, STATE_STORE], 'readwrite', async tx => {
      const usage = await readUsage(tx)
      let meta = normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
      const staleCutoff = now - configuredTtlMs
      if (meta && meta.updatedAt <= staleCutoff) {
        await deleteGroupInTransaction(tx, groupKey, usage, meta)
        meta = null
      }
      if (meta && meta.total !== total) {
        await deleteGroupInTransaction(tx, groupKey, usage, meta)
        meta = null
      }
      if (!meta) {
        meta = normalizeMeta({
          groupKey,
          channelPubkey,
          routerPubkey,
          total,
          received: {},
          receivedCount: 0,
          nextIndex: 0,
          rowIndex: 0,
          carry: '',
          payloadCiphertext: '',
          receiverPubkeys: [],
          byteSize: 0,
          createdAt: now,
          updatedAt: now
        })
      }

      const existing = (await run('get', [[groupKey, index]], CHUNKS_STORE, null, { tx })).result
      if (!existing) {
        if (bytes.byteLength > configuredMaxBytes || meta.byteSize + bytes.byteLength > configuredMaxBytes) {
          await deleteGroupInTransaction(tx, groupKey, usage, meta)
          await writeUsage(tx, usage)
          return { tooLarge: true, meta: null }
        }

        const requiredBytes = bytes.byteLength
        const candidates = await oldestGroupsInTransaction(tx, groupKey)
        for (const candidate of candidates) {
          if (candidate.updatedAt > staleCutoff) continue
          await deleteGroupInTransaction(tx, candidate.groupKey, usage, candidate)
        }
        for (const candidate of candidates) {
          if (candidate.updatedAt <= staleCutoff) continue
          if (!Number.isFinite(configuredMaxBytes) || usage.usedBytes + requiredBytes <= configuredMaxBytes) break
          await deleteGroupInTransaction(tx, candidate.groupKey, usage, candidate)
        }
        if (Number.isFinite(configuredMaxBytes) && usage.usedBytes + requiredBytes > configuredMaxBytes) {
          await deleteGroupInTransaction(tx, groupKey, usage, meta)
          await writeUsage(tx, usage)
          return { tooLarge: true, meta: null }
        }

        await run('put', [{ groupKey, index, bytes }], CHUNKS_STORE, null, { tx })
        meta.received[String(index)] = true
        meta.receivedCount++
        meta.byteSize += requiredBytes
        usage.usedBytes += requiredBytes
      }

      meta.total = total
      meta.updatedAt = now
      await run('put', [meta], GROUPS_STORE, null, { tx })
      await writeUsage(tx, usage)
      return { tooLarge: false, meta }
    })
  }

  async function evictOldestGroup (except = '') {
    return transaction([GROUPS_STORE, CHUNKS_STORE, STATE_STORE], 'readwrite', async tx => {
      const usage = await readUsage(tx)
      const oldest = (await oldestGroupsInTransaction(tx, except))[0]
      if (!oldest) return false
      await deleteGroupInTransaction(tx, oldest.groupKey, usage, oldest)
      await writeUsage(tx, usage)
      return true
    })
  }

  async function put ({ channelPubkey, routerPubkey, index, total, contentBytes }) {
    if (!channelPubkey || !routerPubkey) throw new Error('RECEIVED_CHUNK_GROUP_REQUIRED')
    if (!Number.isSafeInteger(index) || !Number.isSafeInteger(total) || index < 0 || total < 1 || index >= total) {
      throw new Error('INVALID_RECEIVED_CHUNK_INDEX')
    }
    const bytes = normalizeBytes(contentBytes)
    await ready()
    while (true) {
      try {
        const result = await putOnce({ channelPubkey, routerPubkey, index, total, contentBytes: bytes })
        if (result.tooLarge) throw new Error('RECEIVED_CHUNK_GROUP_TOO_LARGE')
        return result.meta
      } catch (err) {
        if (!isQuotaExceeded(err) || !await evictOldestGroup(groupKeyFor(channelPubkey, routerPubkey))) throw err
      }
    }
  }

  async function readMeta (groupKey) {
    await ready()
    return transaction([GROUPS_STORE], 'readonly', async tx => {
      return normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
    })
  }

  async function status (metaOrGroupKey) {
    const meta = typeof metaOrGroupKey === 'string' ? await readMeta(metaOrGroupKey) : normalizeMeta(metaOrGroupKey)
    if (!meta) return { received: 0, missing: [] }
    const missing = []
    let received = 0
    for (let index = 0; index < meta.total; index++) {
      if (index < meta.nextIndex || meta.received[String(index)]) received++
      else missing.push(index)
    }
    return { received, missing }
  }

  function rememberReceiverPubkey (meta, pubkey) {
    if (pubkey && !meta.receiverPubkeys.includes(pubkey)) meta.receiverPubkeys.push(pubkey)
  }

  function rememberPayloadCiphertext (meta, ciphertext) {
    if (!meta.payloadCiphertext) meta.payloadCiphertext = ciphertext
  }

  async function claimDrain (groupKey, token) {
    while (true) {
      const now = Date.now()
      const result = await transaction([GROUPS_STORE], 'readwrite', async tx => {
        const meta = normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
        if (!meta) return { meta: null, waitMs: 0 }
        if (meta.drainToken && meta.drainToken !== token && meta.drainUntil > now) {
          return { meta: null, waitMs: Math.min(DRAIN_LEASE_MS, meta.drainUntil - now) }
        }
        meta.drainToken = token
        meta.drainUntil = now + DRAIN_LEASE_MS
        await run('put', [meta], GROUPS_STORE, null, { tx })
        return { meta, waitMs: 0 }
      })
      if (!result.waitMs) return result.meta
      await wait(result.waitMs)
    }
  }

  async function readDrainChunk (groupKey, token) {
    return transaction([GROUPS_STORE, CHUNKS_STORE], 'readonly', async tx => {
      const meta = normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
      if (!meta || meta.drainToken !== token) return { meta: null, bytes: null }
      if (meta.nextIndex >= meta.total) return { meta, bytes: null }
      const chunk = (await run('get', [[groupKey, meta.nextIndex]], CHUNKS_STORE, null, { tx })).result
      return { meta, bytes: chunk ? normalizeBytes(chunk.bytes) : null }
    })
  }

  async function commitDrainMeta (nextMeta, token) {
    return transaction([GROUPS_STORE], 'readwrite', async tx => {
      const current = normalizeMeta((await run('get', [nextMeta.groupKey], GROUPS_STORE, null, { tx })).result)
      if (!current || current.drainToken !== token) return null
      current.nextIndex = nextMeta.nextIndex
      current.rowIndex = nextMeta.rowIndex
      current.carry = nextMeta.carry
      current.payloadCiphertext = nextMeta.payloadCiphertext
      current.receiverPubkeys = uniq(nextMeta.receiverPubkeys)
      current.updatedAt = nextMeta.updatedAt
      current.drainUntil = Date.now() + DRAIN_LEASE_MS
      await run('put', [current], GROUPS_STORE, null, { tx })
      return current
    })
  }

  async function releaseDrain (groupKey, token) {
    return transaction([GROUPS_STORE], 'readwrite', async tx => {
      const meta = normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
      if (!meta || meta.drainToken !== token) return false
      meta.drainToken = ''
      meta.drainUntil = 0
      await run('put', [meta], GROUPS_STORE, null, { tx })
      return true
    })
  }

  async function drainAvailable (groupKey, { onLine } = {}) {
    await ready()
    const token = randomToken()
    let meta = await claimDrain(groupKey, token)
    if (!meta) return { complete: false, stopped: false, meta: null }
    try {
      while (meta.nextIndex < meta.total) {
        const snapshot = await readDrainChunk(groupKey, token)
        meta = snapshot.meta
        if (!meta || !snapshot.bytes) break

        const text = `${meta.carry}${decoder.decode(snapshot.bytes)}`
        let start = 0
        let end = text.indexOf('\n', start)
        while (end !== -1) {
          const line = text.slice(start, end)
          start = end + 1
          if (line) {
            const result = await onLine?.(line, meta.rowIndex, meta, { rememberPayloadCiphertext, rememberReceiverPubkey })
            meta.rowIndex++
            if (result?.stop) {
              meta.updatedAt = Date.now()
              meta = await commitDrainMeta(meta, token)
              return { complete: false, stopped: true, meta }
            }
          }
          end = text.indexOf('\n', start)
        }
        meta.carry = text.slice(start)
        meta.nextIndex++
        meta.updatedAt = Date.now()
        meta = await commitDrainMeta(meta, token)
        if (!meta) break
      }

      if (meta && meta.nextIndex >= meta.total) {
        if (meta.carry) {
          const result = await onLine?.(meta.carry, meta.rowIndex, meta, { rememberPayloadCiphertext, rememberReceiverPubkey })
          meta.rowIndex++
          meta.carry = ''
          if (result?.stop) {
            meta.updatedAt = Date.now()
            meta = await commitDrainMeta(meta, token)
            return { complete: false, stopped: true, meta }
          }
        }
        meta.updatedAt = Date.now()
        meta = await commitDrainMeta(meta, token)
        return { complete: Boolean(meta), stopped: false, meta }
      }
      return { complete: false, stopped: false, meta }
    } finally {
      await releaseDrain(groupKey, token).catch(() => {})
    }
  }

  async function readChunkBytes (groupKey) {
    await ready()
    return transaction([GROUPS_STORE, CHUNKS_STORE], 'readonly', async tx => {
      const meta = normalizeMeta((await run('get', [groupKey], GROUPS_STORE, null, { tx })).result)
      if (!meta) return { meta: null, chunks: [] }
      const chunks = []
      for (let index = 0; index < meta.total; index++) {
        const chunk = (await run('get', [[groupKey, index]], CHUNKS_STORE, null, { tx })).result
        if (!chunk) throw new Error('RECEIVED_CHUNK_MISSING')
        chunks.push(normalizeBytes(chunk.bytes))
      }
      return { meta, chunks }
    })
  }

  async function readChunkContents (groupKey) {
    return (await readChunkBytes(groupKey)).chunks.map(bytes => decoder.decode(bytes))
  }

  async function readEnvelopeBundleContent (groupKey) {
    const { chunks } = await readChunkBytes(groupKey)
    return bytesToBase64(joinChunks(chunks))
  }

  function joinChunks (chunks) {
    let length = 0
    for (const chunk of chunks) length += chunk.byteLength
    const joined = new Uint8Array(length)
    let offset = 0
    for (const chunk of chunks) {
      joined.set(chunk, offset)
      offset += chunk.byteLength
    }
    return joined
  }

  async function readEnvelopeBundleText (groupKey) {
    return decoder.decode(joinChunks((await readChunkBytes(groupKey)).chunks))
  }

  async function removeGroup (groupKey) {
    await ready()
    return transaction([GROUPS_STORE, CHUNKS_STORE, STATE_STORE], 'readwrite', async tx => {
      const usage = await readUsage(tx)
      const removed = await deleteGroupInTransaction(tx, groupKey, usage)
      await writeUsage(tx, usage)
      return removed
    })
  }

  return {
    cleanupStale,
    drainAvailable,
    groupKeyFor,
    put,
    readChunkContents,
    readEnvelopeBundleContent,
    readEnvelopeBundleText,
    removeGroup,
    status
  }
}
