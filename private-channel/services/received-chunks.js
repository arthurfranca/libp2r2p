import { base64ToBytes, bytesToBase64 } from '../../base64/index.js'
import { JSONL_CHUNK_BYTES } from '../helpers/chunk-size.js'

export const DEFAULT_RECEIVED_CHUNK_TTL_MS = 60 * 60 * 1000 // 1 hour
// For illustration purposes: A 280-character rumor would take approximately
// 23 chunks (~0.65 MiB) to be sent if encrypted to 1000 participants.
export const DEFAULT_RECEIVED_CHUNK_MAX_BYTES = Math.min(JSONL_CHUNK_BYTES * 64, 3 * 1024 * 1024 /* 3 MiB cap */)

const DEFAULT_PREFIX = 'libp2r2p:private-channel:received'
const encoder = new TextEncoder()
const decoder = new TextDecoder()

// Receive buffers survive reloads briefly, unlike send-side temporary chunks.
// Stale cleanup handles peers that never publish the remaining chunks.

function byteLength (value) {
  return encoder.encode(String(value)).length
}

function parseJson (raw, fallback) {
  try { return JSON.parse(raw || '') } catch { return fallback }
}

function uniq (values) {
  return [...new Set((values || []).filter(Boolean))]
}

function normalizeGroupKeys (value) {
  return Array.isArray(value) ? uniq(value.filter(key => typeof key === 'string' && key)) : []
}

function normalizeReceived (received) {
  if (!received || typeof received !== 'object' || Array.isArray(received)) return {}
  return Object.fromEntries(
    Object.entries(received)
      .filter(([index, hasChunk]) => hasChunk && Number.isSafeInteger(Number(index)) && Number(index) >= 0)
      .map(([index]) => [String(Number(index)), true])
  )
}

function normalizeMeta (meta, groupKey) {
  if (!meta || typeof meta !== 'object') return null
  const total = Number(meta.total)
  const nextIndex = Number(meta.nextIndex)
  const rowIndex = Number(meta.rowIndex)
  if (!Number.isSafeInteger(total) || total < 1) return null
  return {
    groupKey,
    channelPubkey: String(meta.channelPubkey || ''),
    routerPubkey: String(meta.routerPubkey || ''),
    total,
    received: normalizeReceived(meta.received),
    receivedCount: Math.max(0, Number(meta.receivedCount) || 0),
    nextIndex: Number.isSafeInteger(nextIndex) && nextIndex >= 0 ? nextIndex : 0,
    rowIndex: Number.isSafeInteger(rowIndex) && rowIndex >= 0 ? rowIndex : 0,
    carry: typeof meta.carry === 'string' ? meta.carry : '',
    payloadCiphertext: typeof meta.payloadCiphertext === 'string' ? meta.payloadCiphertext : '',
    receiverPubkeys: uniq(meta.receiverPubkeys || []),
    byteSize: Math.max(0, Number(meta.byteSize) || 0),
    createdAt: Number(meta.createdAt) || Date.now(),
    updatedAt: Number(meta.updatedAt) || Date.now()
  }
}

function isQuotaExceeded (err) {
  return err?.name === 'QuotaExceededError' ||
    err?.name === 'NS_ERROR_DOM_QUOTA_REACHED' ||
    err?.code === 22 ||
    err?.code === 1014 ||
    /quota/i.test(err?.message || '')
}

function joinBase64Chunks (parts) {
  let out = ''
  let carry = new Uint8Array()

  for (const part of parts) {
    const bytes = base64ToBytes(part)
    const joined = new Uint8Array(carry.length + bytes.length)
    joined.set(carry)
    joined.set(bytes, carry.length)

    const completeLength = joined.length - (joined.length % 3)
    if (completeLength) out += bytesToBase64(joined.slice(0, completeLength))
    carry = joined.slice(completeLength)
  }

  if (carry.length) out += bytesToBase64(carry)
  return out
}

export function createReceivedChunkStore ({
  prefix = DEFAULT_PREFIX,
  storageArea = globalThis.localStorage,
  ttlMs = DEFAULT_RECEIVED_CHUNK_TTL_MS,
  maxBytes = DEFAULT_RECEIVED_CHUNK_MAX_BYTES
} = {}) {
  const groupsKey = `${prefix}:groups`
  const configuredTtlMs = Number.isFinite(ttlMs) && ttlMs >= 0 ? ttlMs : DEFAULT_RECEIVED_CHUNK_TTL_MS
  const configuredMaxBytes = Number.isFinite(maxBytes) && maxBytes > 0 ? maxBytes : Infinity

  function storage () {
    return storageArea
  }

  function metaKey (groupKey) {
    return `${prefix}:group:${groupKey}:meta`
  }

  function chunkKey (groupKey, index) {
    return `${prefix}:group:${groupKey}:chunk:${index}`
  }

  function groupKeyFor (channelPubkey, routerPubkey) {
    return `${channelPubkey}:${routerPubkey}`
  }

  function readGroupKeys () {
    return normalizeGroupKeys(parseJson(storage().getItem(groupsKey), []))
  }

  function writeGroupKeys (keys) {
    const normalized = normalizeGroupKeys(keys)
    if (normalized.length) storage().setItem(groupsKey, JSON.stringify(normalized))
    else storage().removeItem(groupsKey)
  }

  function addGroupKey (groupKey) {
    const keys = readGroupKeys()
    if (!keys.includes(groupKey)) writeGroupKeys(keys.concat(groupKey))
  }

  function removeGroupKey (groupKey) {
    writeGroupKeys(readGroupKeys().filter(key => key !== groupKey))
  }

  function readMeta (groupKey) {
    return normalizeMeta(parseJson(storage().getItem(metaKey(groupKey)), null), groupKey)
  }

  function writeMeta (meta) {
    storage().setItem(metaKey(meta.groupKey), JSON.stringify(meta))
  }

  function removeGroup (groupKey) {
    const meta = readMeta(groupKey)
    if (meta) {
      for (const index of Object.keys(meta.received)) {
        storage().removeItem(chunkKey(groupKey, index))
      }
    }
    storage().removeItem(metaKey(groupKey))
    removeGroupKey(groupKey)
  }

  function allMetas () {
    const keys = readGroupKeys()
    const metas = []
    const liveKeys = []
    for (const groupKey of keys) {
      const meta = readMeta(groupKey)
      if (!meta) continue
      metas.push(meta)
      liveKeys.push(groupKey)
    }
    if (liveKeys.length !== keys.length) writeGroupKeys(liveKeys)
    return metas
  }

  function totalStoredBytes () {
    return allMetas().reduce((total, meta) => {
      return total + meta.byteSize + byteLength(JSON.stringify(meta))
    }, 0)
  }

  function oldestGroupKey ({ except } = {}) {
    return allMetas()
      .filter(meta => meta.groupKey !== except)
      .sort((a, b) => a.updatedAt - b.updatedAt)[0]?.groupKey || ''
  }

  function evictOldestUntilFits (requiredBytes = 0, { except } = {}) {
    if (!Number.isFinite(configuredMaxBytes)) return
    while (totalStoredBytes() + requiredBytes > configuredMaxBytes) {
      const groupKey = oldestGroupKey({ except })
      if (!groupKey) break
      removeGroup(groupKey)
    }
  }

  function writeChunk (key, value, requiredBytes, except) {
    evictOldestUntilFits(requiredBytes, { except })
    try {
      storage().setItem(key, value)
      return
    } catch (err) {
      if (!isQuotaExceeded(err)) throw err
    }

    let groupKey = oldestGroupKey({ except })
    while (groupKey) {
      removeGroup(groupKey)
      try {
        storage().setItem(key, value)
        return
      } catch (err) {
        if (!isQuotaExceeded(err)) throw err
      }
      groupKey = oldestGroupKey({ except })
    }

    storage().setItem(key, value)
  }

  function cleanupStale (nowMs = Date.now()) {
    const cutoff = nowMs - configuredTtlMs
    for (const meta of allMetas()) {
      if (meta.updatedAt <= cutoff) removeGroup(meta.groupKey)
    }
    evictOldestUntilFits(0)
  }

  function put ({ channelPubkey, routerPubkey, index, total, content }) {
    if (!channelPubkey || !routerPubkey) throw new Error('RECEIVED_CHUNK_GROUP_REQUIRED')
    if (!Number.isSafeInteger(index) || !Number.isSafeInteger(total) || index < 0 || total < 1 || index >= total) {
      throw new Error('INVALID_RECEIVED_CHUNK_INDEX')
    }

    cleanupStale()

    const groupKey = groupKeyFor(channelPubkey, routerPubkey)
    const now = Date.now()
    const chunk = String(content || '')
    const nextBytes = byteLength(chunk)
    let meta = readMeta(groupKey)

    if (meta && meta.total !== total) {
      removeGroup(groupKey)
      meta = null
    }

    if (!meta) {
      meta = normalizeMeta({
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
      }, groupKey)
      addGroupKey(groupKey)
      writeMeta(meta)
    }

    if (!meta.received[String(index)]) {
      if (meta.byteSize + nextBytes > configuredMaxBytes) {
        removeGroup(groupKey)
        throw new Error('RECEIVED_CHUNK_GROUP_TOO_LARGE')
      }
      writeChunk(chunkKey(groupKey, index), chunk, nextBytes, groupKey)
      meta.received[String(index)] = true
      meta.receivedCount++
      meta.byteSize += nextBytes
    }

    meta.total = total
    meta.updatedAt = now
    writeMeta(meta)
    evictOldestUntilFits(0, { except: groupKey })
    return meta
  }

  function status (metaOrGroupKey) {
    const meta = typeof metaOrGroupKey === 'string' ? readMeta(metaOrGroupKey) : metaOrGroupKey
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
    if (!pubkey || meta.receiverPubkeys.includes(pubkey)) return
    meta.receiverPubkeys.push(pubkey)
  }

  function rememberPayloadCiphertext (meta, ciphertext) {
    if (!meta.payloadCiphertext) meta.payloadCiphertext = ciphertext
  }

  async function drainAvailable (groupKey, { onLine } = {}) {
    const meta = readMeta(groupKey)
    if (!meta) return { complete: false, stopped: false, meta: null }

    while (meta.nextIndex < meta.total) {
      const index = meta.nextIndex
      const raw = storage().getItem(chunkKey(groupKey, index))
      if (raw == null) break

      const text = `${meta.carry}${decoder.decode(base64ToBytes(raw))}`
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
            writeMeta(meta)
            return { complete: false, stopped: true, meta }
          }
        }
        end = text.indexOf('\n', start)
      }

      meta.carry = text.slice(start)

      meta.nextIndex++
      meta.updatedAt = Date.now()
      writeMeta(meta)
    }

    if (meta.nextIndex >= meta.total) {
      if (meta.carry) {
        const result = await onLine?.(meta.carry, meta.rowIndex, meta, { rememberPayloadCiphertext, rememberReceiverPubkey })
        meta.rowIndex++
        meta.carry = ''
        if (result?.stop) {
          meta.updatedAt = Date.now()
          writeMeta(meta)
          return { complete: false, stopped: true, meta }
        }
      }
      meta.updatedAt = Date.now()
      writeMeta(meta)
      return { complete: true, stopped: false, meta }
    }

    return { complete: false, stopped: false, meta }
  }

  function readEnvelopeBundleContent (groupKey) {
    const meta = readMeta(groupKey)
    if (!meta) return ''
    const parts = []
    for (let index = 0; index < meta.total; index++) {
      const chunk = storage().getItem(chunkKey(groupKey, index))
      if (chunk == null) throw new Error('RECEIVED_CHUNK_MISSING')
      parts.push(chunk)
    }
    return joinBase64Chunks(parts)
  }

  function readEnvelopeBundleText (groupKey) {
    const content = readEnvelopeBundleContent(groupKey)
    return decoder.decode(base64ToBytes(content))
  }

  function readChunkContents (groupKey) {
    const meta = readMeta(groupKey)
    if (!meta) return []
    const parts = []
    for (let index = 0; index < meta.total; index++) {
      const chunk = storage().getItem(chunkKey(groupKey, index))
      if (chunk == null) throw new Error('RECEIVED_CHUNK_MISSING')
      parts.push(chunk)
    }
    return parts
  }

  cleanupStale()

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
