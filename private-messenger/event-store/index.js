import NMMR from 'nmmr'
import { chunkStream } from '../file/helpers/stream.js'
import { ValidationError } from '../../error/index.js'
import { decodeIrfsChunk } from '../../irfs/index.js'
import { encodeRecord, decodeRecord, selector, days, PREFIX } from './helpers/codec.js'
export { encodeRecord as encodeRecoveryRecord, decodeRecord as decodeRecoveryRecord } from './helpers/codec.js'

const nowSeconds = () => Math.floor(Date.now() / 1000)
function assertOptions (eventStore, signer) {
  if (!eventStore?.query || !eventStore?.addPersonalCopy || !signer?.getPublicKey || !signer?.obfuscate || !signer?.nip44v3?.decrypt) throw new ValidationError('INVALID_EVENT_STORE_OPTIONS')
}
function committed (value) {
  if (!value?.result?.ok) throw Object.assign(new Error(value?.result?.message || 'EVENT_STORE_WRITE_FAILED'), { code: value?.result?.code || 'EVENT_STORE_WRITE_FAILED' })
  return value
}
// Bound each page and retain only the IDs at the timestamp boundary.
async function * pages (eventStore, filter, signal) {
  let until = filter.until, excluded = []
  while (true) {
    signal?.throwIfAborted()
    const { results } = await eventStore.query({ ...filter, ...(until === undefined ? {} : { until }), ...(excluded.length ? { '!ids': excluded } : {}), limit: 128, search: 'sort:desc' })
    if (!results.length) return
    for (const event of results) { signal?.throwIfAborted(); yield event }
    const boundary = results.at(-1).created_at
    const ids = results.filter(e => e.created_at === boundary).map(e => e.id)
    excluded = boundary === until ? [...excluded, ...ids] : ids
    until = boundary
    if (results.length < 128) return
  }
}
export function createPersonalCopyRecoveryStorage ({ eventStore, signer, now = nowSeconds }) {
  assertOptions(eventStore, signer)
  let closed = false
  let opening
  const ready = () => (opening ??= Promise.all([signer.getPublicKey(), signer.obfuscate('', '1006', '')]).catch(error => { opening = undefined; throw error }))
  const check = () => { if (closed) throw new Error('RECOVERY_STORAGE_CLOSED') }
  const mirrors = new Map(), grants = new Map()
  const mirror = (name, value) => {
    const key = `${name}:${value}`
    if (!mirrors.has(key)) {
      if (mirrors.size >= 256) mirrors.delete(mirrors.keys().next().value)
      const promise = Promise.resolve().then(() => signer.obfuscate(value, '1006', `#${name}`)).catch(error => { if (mirrors.get(key) === promise) mirrors.delete(key); throw error })
      mirrors.set(key, promise)
    }
    return mirrors.get(key)
  }
  async function filterFor (type) {
    check()
    const [owner, context] = await ready()
    return { kinds: [1006], authors: [owner], '#k': ['30078'], '#c': [context], '#v': ['0', '1'], '&o': [await mirror('t', `${PREFIX}${type}`)] }
  }
  async function read (wrapper) {
    check()
    // Query the store before every use; only decryption of an immutable wrapper
    // is cached. A removed wrapper therefore cannot authorize another reply.
    const cached = grants.get(wrapper.id)
    if (cached) return { ...cached, row: { ...cached.row }, wrapper }
    const [owner] = await ready()
    const bytes = await signer.nip44v3.decrypt(owner, 30078, '', wrapper.content)
    const decoded = decodeRecord(JSON.parse(new TextDecoder().decode(bytes)))
    if (decoded.type === 'grant') {
      if (grants.size >= 128) grants.delete(grants.keys().next().value)
      grants.set(wrapper.id, { ...decoded, row: { ...decoded.row } })
    }
    return { ...decoded, wrapper }
  }
  async function * records (type, query = {}) {
    const filter = await filterFor(type)
    if (query.recordId) filter['&o'].push(await mirror('d', query.recordId))
    else if (query.fileChannelPubkey && query.receiverPubkey) {
      const indices = type === 'seed' ? query.indices : [undefined]
      if (indices?.length) filter['#o'] = await Promise.all(indices.map(chunkIndex => mirror('s', selector(type, { ...query, chunkIndex }))))
    } else if (query.channelPubkey) {
      filter['&o'].push(await mirror('c', query.channelPubkey))
      if (Number.isFinite(query.since) && Number.isFinite(query.until)) {
        const buckets = days(query.since, query.until)
        // A wide query must include daily records too, so skip the day prefilter.
        if (!buckets.includes('*')) filter['#o'] = await Promise.all([...buckets, '*'].map(d => mirror('D', d)))
      }
    }
    for await (const wrapper of pages(eventStore, filter, query.signal)) {
      const { row } = await read(wrapper)
      if (query.channelPubkey && row.channelPubkey !== query.channelPubkey) continue
      if (query.controlChannelPubkey && row.controlChannelPubkey !== query.controlChannelPubkey) continue
      if (query.fileChannelPubkey && row.fileChannelPubkey !== query.fileChannelPubkey) continue
      if (query.receiverPubkey && row.receiverPubkey && row.receiverPubkey !== query.receiverPubkey) continue
      if (query.indices && !query.indices.includes(row.chunkIndex)) continue
      if (query.since !== undefined && row.lastSeenAt < query.since) continue
      if (query.until !== undefined && row.firstSeenAt > query.until) continue
      if (!query.includeExpired && row.expiresAt <= now()) continue
      yield { row, wrapper }
    }
  }
  function store (type) {
    return {
      async put (row) {
        check()
        const event = encodeRecord(type, row)
        if (row.expiresAt <= now()) return
        const recordId = event.tags[0][1]
        const existing = records(type, { recordId, includeExpired: true })
        try { const first = await existing.next(); if (!first.done) return first.value.row } finally { await existing.return() }
        const saved = committed(await eventStore.addPersonalCopy(event, { context: '' }))
        // A pending kind-5 tombstone can reject the insertion; do not cache it.
        if (saved.result.stored === false && saved.result.code === 'blocked') return
        return { ...row, recordId }
      },
      async * iterate (query = {}) { for await (const { row } of records(type, query)) yield row },
      async has (row) {
        const iterator = records(type, { recordId: row.recordId || encodeRecord(type, row).tags[0][1] })
        try { return !(await iterator.next()).done } finally { await iterator.return() }
      },
      async removeLocal (query = {}) {
        for await (const { wrapper } of records(type, { ...query, includeExpired: true })) await eventStore.remove([['e', wrapper.id]])
      },
      async prune ({ now: at = now() } = {}) {
        // Personal copies preserve the inner expiration on their signed wrapper.
        // Cleanup must not decrypt the entire seed archive on startup/request.
        for await (const wrapper of pages(eventStore, await filterFor(type))) {
          const expiry = Number(wrapper.tags.find(tag => tag[0] === 'expiration')?.[1])
          if (Number.isSafeInteger(expiry) && expiry <= at) await eventStore.remove([['e', wrapper.id]])
        }
      },
      async revoke (rows) {
        check()
        const [owner] = await ready()
        for (let i = 0; i < rows.length; i += 100) {
          const group = rows.slice(i, i + 100).filter(row => row.expiresAt > now())
          if (!group.length) continue
          const tags = []; let createdAt = now(), expiresAt = 0
          for (const row of group) {
            const recordId = row.recordId || encodeRecord(type, row).tags[0][1]
            for await (const { wrapper } of records(type, { recordId })) createdAt = Math.max(createdAt, wrapper.created_at)
            tags.push(['a', `30078:${owner}:${recordId}`]); expiresAt = Math.max(expiresAt, row.expiresAt)
          }
          if (expiresAt <= createdAt) continue
          committed(await eventStore.addPersonalCopy({ kind: 5, created_at: createdAt, tags: [...tags, ['k', '30078'], ['expiration', String(expiresAt)]], content: '' }, { context: '' }))
        }
      }
    }
  }
  const seeds = store('seed'), authorizations = store('grant')
  authorizations.find = async query => {
    let best
    for await (const row of authorizations.iterate(query)) if (!best || row.sharedAt > best.sharedAt || (row.sharedAt === best.sharedAt && (row.expiresAt < best.expiresAt || (row.expiresAt === best.expiresAt && row.recordId < best.recordId)))) best = row
    return best
  }
  return { seeds, authorizations, close () { closed = true; mirrors.clear(); grants.clear() } }
}

export function createEventStoreChunkStorage ({ eventStore }) {
  if (!eventStore?.query || !eventStore?.addPersonalCopy) throw new ValidationError('INVALID_EVENT_STORE_OPTIONS')
  const storage = {
    async read (root, index) {
      const { results } = await eventStore.query({ kinds: [34601], '#d': [NMMR.deriveChunkId(root, index)], limit: 1 })
      return results[0]
    },
    async save (event, descriptor) {
      const decoded = decodeIrfsChunk(event)
      if (decoded.root !== descriptor.root) throw new ValidationError('FILE_CHUNK_DESCRIPTOR_MISMATCH')
      committed(await eventStore.addPersonalCopy({ kind: 34601, created_at: event.created_at, tags: structuredClone(event.tags), content: event.content }, { context: `dm:${descriptor.peerPubkey}` }))
    }
  }
  storage.stream = (descriptor, { signal } = {}) => chunkStream({ read: storage.read, ...descriptor, signal })
  return storage
}
export function createEventStoreMessageStorage ({ eventStore }) {
  if (!eventStore?.addPersonalCopy) throw new ValidationError('INVALID_EVENT_STORE_OPTIONS')
  return {
    async save (event, { peerPubkey, hearsay = false } = {}) {
      const result = await eventStore.addPersonalCopy(event, { context: `dm:${peerPubkey}`, hearsay })
      if (!result?.result?.ok && !['blocked', 'expired'].includes(result?.result?.code)) committed(result)
      return result
    }
  }
}
