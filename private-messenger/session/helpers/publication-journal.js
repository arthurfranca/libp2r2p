import { compactSeedRouterRows } from '../../index.js'
import { encodeRecord } from '../../event-store/helpers/codec.js'
import { isRecoverableStorageFailure } from '../../helpers/pause-recovery.js'

export function createPublicationJournal ({ entries, store, archive, changed, onError, debug, setTimer = setTimeout, clearTimer = clearTimeout, random = Math.random }) {
  const writes = new Map()
  const waitingWrites = new Map()
  const failed = new Set()
  const durable = new WeakSet()
  const queued = new WeakMap()
  let working; let stopped = false; let enabled = false; let timer; let delay = 1000
  function write (entry) {
    if (waitingWrites.has(entry.id)) return waitingWrites.get(entry.id)
    const previous = writes.get(entry.id) || Promise.resolve()
    const work = previous.catch(() => {}).then(() => {
      waitingWrites.delete(entry.id)
      return store().put(structuredClone(entry), { existing: true })
    })
    waitingWrites.set(entry.id, work)
    writes.set(entry.id, work)
    work.finally(() => { if (writes.get(entry.id) === work) writes.delete(entry.id) }).catch(() => {})
    return work
  }
  function rows (entry) {
    const packet = entry.prepared
    if (!packet?.recover) return []
    return packet.publication.seeds.flatMap(seed => compactSeedRouterRows(seed).map(row => {
      const descriptor = packet.target.file
      const value = {
        ...row, channelPubkey: seed.channelPubkey, receivedAt: packet.receivedAt,
        expiresAt: row.router.created_at + packet.retentionSeconds,
        ...(descriptor
          ? {
              root: descriptor.root, controlChannelPubkey: descriptor.controlChannelPubkey,
              fileChannelPubkey: packet.publication.channelPubkey, peerPubkey: descriptor.peerPubkey,
              chunkIndex: packet.target.chunkIndex,
              key: `${descriptor.controlChannelPubkey}:${packet.publication.channelPubkey}:${row.receiverPubkey}:${packet.target.chunkIndex}`
            }
          : {})
      }
      value.recordId = encodeRecord('seed', value).tags[0][1]
      return value
    }))
  }
  function retryLater () {
    if (stopped || !enabled || timer || !failed.size) return
    timer = setTimer(() => { timer = null; failed.clear(); wake() }, Math.min(30000, delay * (0.8 + random() * 0.4)))
    timer?.unref?.()
    delay = Math.min(30000, delay * 2)
  }
  function wake () {
    if (stopped || !enabled || working) return working
    const work = Promise.resolve().then(async () => {
      for (const entry of entries.values()) {
        if (stopped || !enabled) break
        const packet = entry.prepared
        if (!packet || failed.has(entry.id)) continue
        if (packet.archived && !packet.accepted && !entry.cancelled) continue
        const started = Date.now()
        try {
          if (!durable.has(packet)) { await write(entry); durable.add(packet) }
          if (!packet.archived) {
            debug(entry, 'recovery-archive-start', queued.get(packet), { publicationId: packet.publication.events[0].id })
            if (packet.recover) await archive(entry, rows(entry))
            packet.archived = true
            debug(entry, 'recovery-archived', started)
          }
          if (packet.accepted || entry.cancelled) delete entry.prepared
          await write(entry)
          changed(entry)
        } catch (error) {
          failed.add(entry.id)
          onError(error)
          if (isRecoverableStorageFailure(error)) retryLater()
        }
      }
    }).finally(() => {
      if (working === work) working = null
      if (!failed.size) delay = 1000
      // An ACK can arrive while another entry is being archived.
      if (!stopped && [...entries.values()].some(entry => entry.prepared && !failed.has(entry.id) && (!entry.prepared.archived || entry.prepared.accepted || entry.cancelled))) wake()
    })
    working = work
    return work
  }
  return {
    write,
    async flush (entry) { while (writes.has(entry.id)) await writes.get(entry.id) },
    adopt (entry) { if (entry.prepared) { durable.add(entry.prepared); queued.set(entry.prepared, Date.now()) } },
    async stage (entry, seed, target, retentionSeconds) {
      const started = Date.now()
      if (entry.prepared && entry.prepared.publication.events[0].id !== seed.publication.events[0].id) throw new Error('PUBLICATION_ARCHIVE_PENDING')
      entry.prepared ||= { publication: structuredClone(seed.publication), target, receivedAt: Math.floor(Date.now() / 1000), retentionSeconds, recover: retentionSeconds > 0, accepted: false, archived: retentionSeconds <= 0 }
      if (durable.has(entry.prepared)) return
      queued.set(entry.prepared, Date.now())
      await write(entry)
      durable.add(entry.prepared)
      debug(entry, 'publication-checkpoint', started, { publicationId: seed.publication.events[0].id })
      for (const row of rows(entry)) debug(entry, 'recovery-pending', undefined, { seedRecordId: row.recordId, publicationId: seed.publication.events[0].id })
      wake()
    },
    wake,
    accept (entry) {
      entry.prepared.accepted = true
      // An unrelated archive may be blocked. Its worker must not hold this ACK.
      write(entry).catch(error => { failed.add(entry.id); onError(error); retryLater() }).finally(wake)
      wake()
    },
    setAvailable (value) {
      enabled = value
      if (!enabled) { if (timer) clearTimer(timer); timer = null } else { failed.clear(); wake() }
    },
    retry () { failed.clear(); if (timer) clearTimer(timer); timer = null; return wake() },
    pendingRows () { return [...entries.values()].flatMap(rows).filter(row => row.expiresAt > Math.floor(Date.now() / 1000)) },
    async close () {
      stopped = true
      if (timer) clearTimer(timer)
      await working
      await Promise.allSettled([...writes.values()])
    }
  }
}

export function pendingSeedMatches (row, query) {
  for (const key of ['channelPubkey', 'controlChannelPubkey', 'fileChannelPubkey', 'receiverPubkey', 'recordId']) if (query[key] && row[key] !== query[key]) return false
  if (query.indices && !query.indices.includes(row.chunkIndex)) return false
  if (query.since !== undefined && (row.lastSeenAt ?? row.router.created_at) < query.since) return false
  if (query.until !== undefined && (row.firstSeenAt ?? row.router.created_at) > query.until) return false
  return true
}

export function withPendingSeeds (storage, pending) {
  async function hasMany (rows) {
    const pendingIds = new Set(pending().map(row => row.recordId).filter(Boolean))
    const unresolved = rows.filter(row => !row.recordId || !pendingIds.has(row.recordId))
    let persisted
    if (typeof storage.hasMany === 'function') persisted = await storage.hasMany(unresolved)
    else {
      persisted = []
      for (const row of unresolved) persisted.push(await storage.has(row))
    }
    if (!Array.isArray(persisted) || persisted.length !== unresolved.length) throw new Error('INVALID_RECOVERY_STORAGE_RESULT')
    let index = 0
    return rows.map(row => Boolean(row.recordId && pendingIds.has(row.recordId)) || Boolean(persisted[index++]))
  }
  return {
    ...storage,
    async * iterate (query = {}) {
      const seen = new Set()
      // Pending durable rows are immediately usable, even while store.put waits.
      for (const row of pending()) if (pendingSeedMatches(row, query)) { seen.add(row.recordId); yield row }
      for await (const row of storage.iterate(query)) if (!seen.has(row.recordId)) yield row
    },
    hasMany,
    async has (row) { return (await hasMany([row]))[0] }
  }
}
