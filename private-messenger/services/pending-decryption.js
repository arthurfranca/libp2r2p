import { sha256 } from '@noble/hashes/sha2.js'
import { bytesToHex } from '../../base16/index.js'
import { base64ToBytes } from '../../base64/index.js'
import { createQueue } from '../../idb-queue/index.js'
import { isDeferredDecryptionError } from '../../private-channel/helpers/decryption-error.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
export const PENDING_DECRYPTION_MAX_BYTES = 16 * 1024 * 1024

function storedBytes (item) {
  let byteSize = 0
  while (true) {
    const size = encoder.encode(JSON.stringify({ byteSize, item })).length
    if (size === byteSize) return size
    byteSize = size
  }
}

function packetSnapshot (packet) {
  const event = (value, router = false) => {
    const copy = Object.fromEntries(['id', 'kind', 'pubkey', 'created_at', 'content', 'sig'].filter(key => value?.[key] !== undefined).map(key => [key, value[key]]))
    if (value?.tags) {
      copy.tags = router
        ? value.tags.filter(tag => ['f', 'p', 'imkc', 'c', 'i'].includes(tag[0])).map(tag => tag.slice(0, tag[0] === 'imkc' || tag[0] === 'c' ? 3 : 2))
        : structuredClone(value.tags)
    }
    return copy
  }
  return { format: packet.format, channelPubkey: packet.channelPubkey, outer: event(packet.outer), ...(packet.router ? { router: event(packet.router, true) } : {}) }
}

// Stable across live, history and synthesized recovery wrappers.
function packetId (record) {
  const rows = record.format === 'routerRow_v1'
    ? decoder.decode(base64ToBytes(record.router.content)).split('\n').filter(Boolean).map(line => {
      const row = JSON.parse(line)
      return row.length === 1 ? row : [row[0], row[1], row[2] || '', row[3] || '']
    })
    : null
  const value = rows ? [record.channelPubkey, record.router.pubkey, record.router.tags.filter(tag => tag[0] !== 'c'), rows] : [record.channelPubkey, record.outer.id]
  return bytesToHex(sha256(encoder.encode(JSON.stringify(value))))
}

export async function createPendingDecryption ({ prefix, indexedDB, maxBytes = PENDING_DECRYPTION_MAX_BYTES, report = () => {} }) {
  const queue = await createQueue({
    prefix: `${prefix}:pending-decryption`, indexedDB, maxBytes, evictionPolicy: 'fifo',
    indexes: { byId: { keyPath: 'id', unique: true }, byChannel: 'channelPubkey' }
  })
  let tail = Promise.resolve()
  let timer
  let counts = {}
  const flush = () => {
    clearTimeout(timer); timer = null
    if (Object.keys(counts).length) {
      const summary = counts; counts = {}
      report({ action: 'summary', counts: summary })
      console.debug('private-messenger pending decryption', JSON.stringify(summary))
    }
  }
  const serial = work => { const result = tail.then(work); tail = result.catch(() => {}); return result }
  const notify = (action, detail) => {
    report({ action, ...detail })
    const key = `${action}:${detail.code || detail.reason || ''}`
    counts[key] = { count: (counts[key]?.count || 0) + 1, example: detail }
    if (!timer) { timer = setTimeout(flush, 1000); timer.unref?.() }
  }
  return {
    queue,
    has: record => serial(() => queue.someBy('byId', packetId(packetSnapshot(record)))),
    save ({ error, record }) {
      record = packetSnapshot(record)
      return serial(async () => {
        const id = packetId(record)
        if (await queue.someBy('byId', id)) return true
        const item = { id, channelPubkey: record.channelPubkey, packet: structuredClone(record), code: error.code, macAttempts: error.code === 'INVALID_MAC' ? 1 : 0 }
        const requiredBytes = storedBytes(item)
        if (requiredBytes > maxBytes) {
          notify('discarded', { reason: 'oversized', channelPubkey: item.channelPubkey, id }); return true
        }
        const before = await queue.getCapacity()
        try { await queue.enqueue(item) } catch (error) {
          if (error.name === 'ConstraintError') return true
          throw error
        }
        const after = await queue.getCapacity()
        const evictedBytes = Math.max(0, before.usedBytes + requiredBytes - after.usedBytes)
        if (evictedBytes) notify('discarded', { reason: 'capacity', bytes: evictedBytes })
        const coordinates = Object.fromEntries(['eventId', 'eventKind', 'senderPubkey', 'receiverPubkey', 'ownContentPubkey', 'peerContentPubkey', 'hasOwnContentKey']
          .filter(key => ['string', 'boolean', 'number'].includes(typeof error[key])).map(key => [key, error[key]]))
        notify('deferred', { ...coordinates, code: error.code, stage: error.stage, id, channelPubkey: item.channelPubkey, capacityBytesBefore: before.usedBytes, capacityBytesAfter: after.usedBytes })
        return true
      })
    },
    async records ({ force = false } = {}) {
      await tail
      const records = []
      for await (const item of queue.storedItems()) if (force || item.macAttempts < 3) records.push(item)
      return records
    },
    current (item, { force = false } = {}) {
      return serial(async () => {
        const stored = await queue.getBy('byId', item.id)
        return stored && (force || stored.macAttempts < 3) ? stored : null
      })
    },
    recovered (item) {
      return serial(async () => {
        await queue.removeBy('byId', item.id)
        notify('recovered', { id: item.id, channelPubkey: item.channelPubkey })
      })
    },
    failed (item, error) {
      return serial(async () => {
        if (!isDeferredDecryptionError(error)) throw error
        const current = await queue.getBy('byId', item.id)
        if (!current) return
        const updated = { ...current, code: error.code, macAttempts: current.macAttempts + (error.code === 'INVALID_MAC' ? 1 : 0) }
        await queue.putBy('byId', updated, { existingOnly: true })
        if (updated.macAttempts >= 3 && item.macAttempts < 3) notify('suspended', { id: item.id, code: error.code, channelPubkey: item.channelPubkey })
      })
    },
    clearChannel: channel => serial(() => queue.removeBy('byChannel', channel)),
    async close () { await tail; flush(); await queue.close() }
  }
}
