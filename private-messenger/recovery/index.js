import { getEventHash } from '../../event/index.js'
import { bytesToBase64, base64ToBytes } from '../../base64/index.js'
import { ValidationError } from '../../error/index.js'
import { ASK_KIND, parseRumorContent } from '../../private-message/index.js'

export const SEEDER_PRESENCE_CODE = 'seederPresence_8mj8'
export const MISSING_MESSAGES_ASK_CODE = 'missingMessages_ask_8mj8'
export const MISSING_MESSAGES_REPLY_CODE = 'missingMessages_reply_8mj8'
export const ROUTER_SEED_RECORD_TYPE = 'routerEnvelopeRow_v1'
export const NYM_CARRIER_SEED_RECORD_TYPE = 'nymCarrier_v1'

const DEFAULT_EVENTS_PER_CHUNK = 100
const encoder = new TextEncoder()
const decoder = new TextDecoder()
const HASH_PUBKEY = '0'.repeat(64)

function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

function isPlainObject (value) {
  return value && typeof value === 'object' && !Array.isArray(value)
}

function parseEventContent (event) {
  return parseRumorContent(event)
}

function splitJsonl (jsonl) {
  return String(jsonl || '').split('\n').filter(Boolean)
}

function decodeJsonl (content) {
  try { return decoder.decode(base64ToBytes(content || '')) } catch { return '' }
}

function encodeJsonlRows (...rows) {
  return bytesToBase64(encoder.encode(rows.map(row => String(row).endsWith('\n') ? row : `${row}\n`).join('')))
}

function isEventInRange (event, since, until) {
  if (!Number.isFinite(event?.created_at)) return true
  if (since != null && event.created_at < since) return false
  if (until != null && event.created_at > until) return false
  return true
}

function compactRouter (router = {}) {
  return {
    kind: router.kind,
    pubkey: router.pubkey,
    created_at: router.created_at,
    tags: (router.tags || []).filter(tag => tag[0] !== 'c')
  }
}

function cloneTags (tags) {
  return (tags || []).map(tag => Array.isArray(tag) ? [...tag] : tag)
}

export function compactSeedRouter (router = {}) {
  return {
    ...compactRouter(router),
    content: router.content || ''
  }
}

function parseRouterRow (line) {
  try {
    const record = JSON.parse(line)
    if (!Array.isArray(record) || record.length === 1) return null
    return {
      receiverPubkey: record[0] || '',
      iykcPubkey: record[2] || ''
    }
  } catch {
    return null
  }
}

function parsePayloadRow (line) {
  try {
    const record = JSON.parse(line)
    if (!Array.isArray(record) || record.length !== 1 || typeof record[0] !== 'string') return ''
    return line
  } catch {
    return ''
  }
}

function rowHash (payloadRow, row) {
  return getEventHash({
    kind: 0,
    pubkey: HASH_PUBKEY,
    created_at: 0,
    tags: [],
    content: `${payloadRow}\n${row}`
  })
}

export function compactSeedRouterRows (seed = {}) {
  const router = compactRouter(seed.router)
  const createdAt = router.created_at || seed.outer?.created_at || nowSeconds()
  const rows = []
  const innerEventIdsByRowIndex = seed.innerEventIdsByRowIndex || {}
  const lines = splitJsonl(seed.jsonl || decodeJsonl(seed.router?.content))
  const payloadRow = parsePayloadRow(lines[0])
  if (!payloadRow) return []
  for (let index = 0; index < lines.length; index++) {
    const row = lines[index]
    const parsed = parseRouterRow(row)
    if (!parsed?.receiverPubkey) continue
    const innerEventId = innerEventIdsByRowIndex[index] || innerEventIdsByRowIndex[String(index)] || ''
    rows.push({
      type: 'seed',
      recordType: ROUTER_SEED_RECORD_TYPE,
      router,
      receiverPubkey: parsed.receiverPubkey,
      iykcPubkey: parsed.iykcPubkey,
      innerEventId,
      rowHash: innerEventId ? '' : rowHash(payloadRow, row),
      payloadRow,
      row,
      firstSeenAt: createdAt,
      lastSeenAt: createdAt
    })
  }
  return rows
}

export function routerSeedRowKey (seed = {}) {
  const row = seed.row ? parseRouterRow(seed.row) : null
  const innerEventId = seed.innerEventId || ''
  const fallbackRowHash = seed.rowHash || (seed.payloadRow && seed.row ? rowHash(seed.payloadRow, seed.row) : '')
  return [
    seed.channelPubkey || '',
    seed.receiverPubkey || row?.receiverPubkey || '',
    innerEventId ? `event:${innerEventId}` : `row:${fallbackRowHash}`
  ].join(':')
}

export function compactSeedNymCarriers (carriers = []) {
  return carriers.map(carrier => ({
    id: carrier.id,
    kind: carrier.kind,
    pubkey: carrier.pubkey,
    created_at: carrier.created_at,
    tags: cloneTags(carrier.tags),
    content: carrier.content || '',
    sig: carrier.sig
  }))
}

function routerWithSingleRow (router, payloadRow, row) {
  const compact = compactRouter(router)
  return {
    ...compact,
    tags: compact.tags.concat([['c', '0', '1']]),
    content: encodeJsonlRows(payloadRow, row)
  }
}

function isSeedRowInRange (seed, since, until) {
  const firstSeenAt = seed.firstSeenAt ?? seed.router?.created_at
  const lastSeenAt = seed.lastSeenAt ?? seed.router?.created_at
  if (!Number.isFinite(firstSeenAt) || !Number.isFinite(lastSeenAt)) return isEventInRange(seed.router, since, until)
  if (since != null && lastSeenAt < since) return false
  if (until != null && firstSeenAt > until) return false
  return true
}

function compactRoutersFromSeed (seed, { receiverPubkey, since, until }) {
  if (!seed?.payloadRow || !seed?.row || !seed?.router || !isSeedRowInRange(seed, since, until)) return []
  if (receiverPubkey && seed.receiverPubkey !== receiverPubkey) return []
  return [{
    recordType: ROUTER_SEED_RECORD_TYPE,
    router: routerWithSingleRow(seed.router, seed.payloadRow, seed.row)
  }]
}

function nymCarrierRecordTime (seed) {
  return seed?.carriers?.reduce((max, carrier) => Math.max(max, carrier.created_at || 0), 0) || 0
}

function compactNymCarriersFromSeed (seed, { since, until }) {
  // eslint-disable-next-line camelcase
  const created_at = nymCarrierRecordTime(seed)
  // eslint-disable-next-line camelcase
  if (!seed?.carriers?.length || !isEventInRange({ created_at }, since, until)) return []
  return [{
    recordType: NYM_CARRIER_SEED_RECORD_TYPE,
    carriers: compactSeedNymCarriers(seed.carriers)
  }]
}

function compactRecordsFromSeed (seed, { receiverPubkey, since, until }) {
  if (seed?.recordType === NYM_CARRIER_SEED_RECORD_TYPE) {
    return compactNymCarriersFromSeed(seed, { since, until })
  }
  if (seed?.recordType === ROUTER_SEED_RECORD_TYPE) {
    return compactRoutersFromSeed(seed, { receiverPubkey, since, until })
  }
  return []
}

function backfillRequestRange (question, since, until) {
  const content = parseEventContent({ ...question, kind: ASK_KIND })
  const payload = isPlainObject(content?.payload) ? content.payload : {}
  return {
    since: since ?? payload.since ?? 0,
    until: until ?? payload.until ?? nowSeconds()
  }
}

function eventRecordFromInput (event) {
  return Number.isInteger(event?.kind) ? [event] : []
}

export function createEventReplyPacker ({
  messenger,
  channelPubkey,
  question,
  receiverPubkey = question?.pubkey,
  code,
  payload = {},
  eventsPerChunk = DEFAULT_EVENTS_PER_CHUNK,
  recordsFromInput = eventRecordFromInput,
  sendEmptyReply = false
}) {
  if (!messenger?.reply) throw new ValidationError('MESSENGER_REQUIRED')
  if (!question?.id) throw new ValidationError('QUESTION_REQUIRED')
  if (!receiverPubkey) throw new ValidationError('RECEIVER_PUBKEY_REQUIRED')
  if (!Number.isSafeInteger(eventsPerChunk) || eventsPerChunk < 1) throw new ValidationError('INVALID_EVENTS_PER_CHUNK')

  let chunk = ''
  let chunkEvents = 0
  let index = 0
  let finalized = false
  let published = false

  async function publish (isLast) {
    const jsonl = chunk
    chunk = ''
    chunkEvents = 0
    published = true
    await messenger.reply({
      channelPubkey,
      question,
      receiverPubkey,
      code,
      payload: {
        ...payload,
        index: index++,
        isLast,
        jsonl
      }
    })
  }

  async function appendRecord (record, { flush = true } = {}) {
    chunk += `${JSON.stringify(record)}\n`
    chunkEvents++
    if (flush && chunkEvents >= eventsPerChunk) await publish(false)
  }

  async function appendRecords (records, { final = false } = {}) {
    for (let i = 0; i < records.length; i++) {
      const isLast = final && i === records.length - 1
      await appendRecord(records[i], { flush: !isLast })
    }
  }

  async function update (input) {
    if (finalized) throw new Error('PACKER_FINALIZED')
    await appendRecords(await recordsFromInput(input))
  }

  async function finalize (input) {
    if (finalized) return
    if (input != null) await appendRecords(await recordsFromInput(input), { final: true })
    finalized = true
    if (!chunk && !sendEmptyReply && !published) return
    await publish(true)
  }

  return {
    update,
    finalize
  }
}

export function createMissingMessageReplyPacker ({
  messenger,
  channelPubkey,
  question,
  receiverPubkey = question?.pubkey,
  since,
  until,
  eventsPerChunk = DEFAULT_EVENTS_PER_CHUNK,
  sendEmptyReply = false
}) {
  if (!messenger?.reply) throw new ValidationError('MESSENGER_REQUIRED')
  if (!question?.id) throw new ValidationError('QUESTION_REQUIRED')
  if (!receiverPubkey) throw new ValidationError('RECEIVER_PUBKEY_REQUIRED')
  if (!Number.isSafeInteger(eventsPerChunk) || eventsPerChunk < 1) throw new ValidationError('INVALID_EVENTS_PER_CHUNK')

  const range = backfillRequestRange(question, since, until)
  return createEventReplyPacker({
    messenger,
    channelPubkey,
    question,
    receiverPubkey,
    code: MISSING_MESSAGES_REPLY_CODE,
    payload: { since: range.since, until: range.until },
    eventsPerChunk,
    sendEmptyReply,
    recordsFromInput: seed => compactRecordsFromSeed(seed, { receiverPubkey, since: range.since, until: range.until })
  })
}
