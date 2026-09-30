import { sha256 } from '@noble/hashes/sha2.js'
import { bytesToHex } from '../../../base16/index.js'
import { ValidationError } from '../../../error/index.js'
import { isValidEvent } from '../../../event/index.js'

const encoder = new TextEncoder()
const HEX = /^[0-9a-f]{64}$/
const DAY = 86400
export const PREFIX = 'libp2r2p:recovery:'
export const digest = value => bytesToHex(sha256(encoder.encode(JSON.stringify(value))))
const fail = () => { throw new ValidationError('INVALID_RECOVERY_RECORD') }
const hex = value => { if (!HEX.test(value || '')) fail(); return value }
const integer = value => { const n = Number(value); if (!Number.isSafeInteger(n) || n < 0 || String(n) !== String(value)) fail(); return n }
// Protect original values from NostrDB's reserved CRDT metadata syntax.
const escape = value => String(value).replace(/^%/, '%25').replace(/^~/, '%7E')
const unescape = value => value.startsWith('%25') ? `%${value.slice(3)}` : value.startsWith('%7E') ? `~${value.slice(3)}` : value
export function days (since, until) {
  const first = Math.floor(since / DAY), last = Math.floor(until / DAY)
  return last - first >= 32 ? ['*'] : Array.from({ length: Math.max(0, last - first + 1) }, (_, i) => String(first + i))
}
export function selector (type, row) {
  return digest(type === 'grant'
    ? ['grant', row.controlChannelPubkey, row.fileChannelPubkey, row.receiverPubkey]
    : row.fileChannelPubkey
      ? ['file', row.controlChannelPubkey, row.fileChannelPubkey, row.receiverPubkey, row.chunkIndex]
      : ['seed', row.channelPubkey, row.receiverPubkey || ''])
}
function body (type, row) {
  const tags = []
  const add = (name, ...values) => tags.push([name, ...values.map(escape)])
  const expires = integer(row.expiresAt)
  if (type === 'grant') {
    add('c', hex(row.controlChannelPubkey)); add('channel', hex(row.fileChannelPubkey))
    add('peer', hex(row.peerPubkey)); add('p', hex(row.receiverPubkey)); add('root', hex(row.root))
    if (row.size !== undefined) { if (!integer(row.size)) fail(); add('size', row.size) }
    add('shared', integer(row.sharedAt))
    if (expires <= row.sharedAt) fail()
  } else if (type === 'seed') {
    add('c', hex(row.fileChannelPubkey || row.channelPubkey))
    add('type', row.recordType)
    if (row.recordType === 'routerEnvelopeRow_v1') {
      const payload = JSON.parse(row.payloadRow), envelope = JSON.parse(row.row)
      if (!Array.isArray(payload) || payload.length !== 1 || typeof payload[0] !== 'string' || !Array.isArray(envelope) || envelope.length < 2 || envelope.length > 4 || !envelope.every(v => typeof v === 'string') || envelope[0] !== row.receiverPubkey) fail()
      add('p', hex(row.receiverPubkey))
      if (row.router?.kind !== 26300) fail()
      add('router', hex(row.router.pubkey), integer(row.router.created_at))
      if (!Array.isArray(row.router.tags)) fail()
      row.router.tags.filter(tag => tag[0] !== 'c').forEach((tag, i) => {
        if (!Array.isArray(tag) || !tag.length || !tag.every(v => typeof v === 'string')) fail()
        add('router-tag', i, ...tag)
      })
      add('payload', payload[0]); add('key', envelope[1])
      if (envelope.length > 2) add('iykc', ...envelope.slice(2))
      if (row.innerEventId) add('event', hex(row.innerEventId))
      const first = integer(row.firstSeenAt ?? row.router.created_at), last = integer(row.lastSeenAt ?? row.router.created_at)
      if (first > last) fail()
      add('range', first, last)
      if (row.fileChannelPubkey) {
        add('file', hex(row.controlChannelPubkey), hex(row.peerPubkey), hex(row.root))
        add('received', integer(row.receivedAt))
        const index = row.router.tags.find(tag => tag[0] === 'i')?.[1]
        if (integer(index) !== row.chunkIndex) fail()
      }
    } else if (row.recordType === 'nymCarrier_v1') {
      if (!row.carriers?.length) fail()
      row.carriers.forEach((carrier, i) => {
        if (!isValidEvent(carrier) || carrier.kind !== 26400) fail()
        add('carrier', i, carrier.kind, carrier.pubkey, carrier.created_at, carrier.id, carrier.sig, carrier.content)
        carrier.tags.forEach((tag, j) => add('carrier-tag', `${i}:${j}`, ...tag))
      })
      const at = Math.max(...row.carriers.map(c => c.created_at))
      add('range', at, at)
    } else fail()
  } else fail()
  add('expiration', expires)
  return tags
}
export function encodeRecord (type, row) {
  try {
    const tags = body(type, row)
    const id = digest([type, tags])
    const range = tags.find(t => t[0] === 'range')
    const recordedAt = type === 'grant' ? row.sharedAt : row.fileChannelPubkey ? row.receivedAt : Number(range[2])
    return { kind: 30078, created_at: recordedAt, tags: [['d', `${PREFIX}${type}:${id}`], ['t', `${PREFIX}${type}`], ['s', selector(type, row)], ...tags, ...(type === 'seed' && !row.fileChannelPubkey ? days(Number(range[1]), Number(range[2])).map(day => ['D', day]) : [])], content: '' }
  } catch (error) { if (error instanceof ValidationError) throw error; fail() }
}
export function decodeRecord (event) {
  try {
    if (event?.kind !== 30078 || event.content !== '' || !Array.isArray(event.tags)) fail()
    const tags = event.tags.filter(t => !['~', 'z', 'zz'].includes(t[0])).map(t => {
      if (!Array.isArray(t) || !t.every(v => typeof v === 'string')) fail()
      return t.filter((v, i) => i === 0 || !v.startsWith('~')).map((v, i) => i ? unescape(v) : v)
    })
    const one = (name, required = true) => {
      const matches = tags.filter(t => t[0] === name)
      if (matches.length > 1 || (required && !matches.length)) fail()
      return matches[0]?.slice(1)
    }
    const coordinate = one('d')[0]
    const type = coordinate?.startsWith(`${PREFIX}grant:`) ? 'grant' : coordinate?.startsWith(`${PREFIX}seed:`) ? 'seed' : fail()
    const row = { expiresAt: integer(one('expiration')[0]), recordId: coordinate }
    if (type === 'grant') {
      Object.assign(row, { controlChannelPubkey: one('c')[0], fileChannelPubkey: one('channel')[0], peerPubkey: one('peer')[0], receiverPubkey: one('p')[0], root: one('root')[0], sharedAt: integer(one('shared')[0]) })
      const size = one('size', false); if (size) row.size = integer(size[0])
    } else {
      row.channelPubkey = one('c')[0]; row.recordType = one('type')[0]
      const range = one('range'); row.firstSeenAt = integer(range[0]); row.lastSeenAt = integer(range[1])
      if (row.recordType === 'routerEnvelopeRow_v1') {
        row.receiverPubkey = one('p')[0]
        const router = one('router')
        const list = tags.filter(t => t[0] === 'router-tag').sort((a, b) => Number(a[1]) - Number(b[1]))
        list.forEach((t, i) => { if (t[1] !== String(i)) fail() })
        row.router = { kind: 26300, pubkey: router[0], created_at: integer(router[1]), tags: list.map(t => t.slice(2)) }
        row.payloadRow = JSON.stringify(one('payload'))
        const envelope = [row.receiverPubkey, ...one('key'), ...(one('iykc', false) || [])]
        row.row = JSON.stringify(envelope); row.iykcPubkey = envelope[2] || ''
        row.innerEventId = one('event', false)?.[0] || ''
        const file = one('file', false)
        if (file) Object.assign(row, { controlChannelPubkey: file[0], peerPubkey: file[1], root: file[2], fileChannelPubkey: row.channelPubkey, chunkIndex: integer(row.router.tags.find(t => t[0] === 'i')?.[1]), receivedAt: integer(one('received')[0]) })
      } else if (row.recordType === 'nymCarrier_v1') {
        row.carriers = tags.filter(t => t[0] === 'carrier').sort((a, b) => Number(a[1]) - Number(b[1])).map((t, i) => {
          if (t[1] !== String(i)) fail()
          const original = tags.filter(v => v[0] === 'carrier-tag' && v[1].startsWith(`${i}:`)).sort((a, b) => Number(a[1].split(':')[1]) - Number(b[1].split(':')[1]))
          original.forEach((v, j) => { if (v[1] !== `${i}:${j}`) fail() })
          return { kind: integer(t[2]), pubkey: t[3], created_at: integer(t[4]), id: t[5], sig: t[6], content: t[7], tags: original.map(v => v.slice(2)) }
        })
      } else fail()
    }
    const encoded = encodeRecord(type, row)
    // Compare the complete semantic tag multiset, including derived indices.
    const canonical = list => list.map(t => JSON.stringify(t)).sort()
    const cleanEncoded = encoded.tags.map(t => t.map((v, i) => i ? unescape(v) : v))
    if (JSON.stringify(canonical(tags)) !== JSON.stringify(canonical(cleanEncoded))) fail()
    return { type, row }
  } catch (error) { if (error instanceof ValidationError) throw error; fail() }
}
