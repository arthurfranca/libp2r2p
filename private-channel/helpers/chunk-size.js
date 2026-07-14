import { MAX_EVENT_BYTES, PRIVATE_BROADCAST_KIND, ROUTER_KIND } from '../constants/index.js'
import { eventByteLength } from './event.js'
import { payloadByteLength as nip44v3PayloadByteLength } from '../../nip44-v3/index.js'

const MAX_TIME_SECONDS = 9999999999
const MAX_CHUNK_TAG_VALUE = '9999999999'
const SAMPLE_PUBKEY = 'f'.repeat(64)
const SAMPLE_SIGNATURE = 'f'.repeat(128)

function base64EncodedByteLength (byteLength) {
  return Math.ceil(byteLength / 3) * 4
}

function routerPlaintextByteLengthForChunk (jsonlByteLength) {
  return eventByteLength({
    kind: ROUTER_KIND,
    pubkey: SAMPLE_PUBKEY,
    created_at: MAX_TIME_SECONDS,
    tags: [['f', SAMPLE_PUBKEY], ['imkc', SAMPLE_PUBKEY, `${MAX_TIME_SECONDS}:${SAMPLE_SIGNATURE}`], ['c', MAX_CHUNK_TAG_VALUE, MAX_CHUNK_TAG_VALUE], ['r', SAMPLE_PUBKEY]],
    content: 'A'.repeat(base64EncodedByteLength(jsonlByteLength)),
    id: SAMPLE_PUBKEY,
    sig: SAMPLE_SIGNATURE
  })
}

function outerEventByteLengthForChunk (jsonlByteLength) {
  const contentByteLength = nip44v3PayloadByteLength(routerPlaintextByteLengthForChunk(jsonlByteLength), 0)
  if (!Number.isFinite(contentByteLength)) return Infinity

  return eventByteLength({
    kind: PRIVATE_BROADCAST_KIND,
    created_at: MAX_TIME_SECONDS,
    // Reserve the deletion capability tag used by high-level private sends.
    tags: [['s', SAMPLE_PUBKEY], ['expiration', String(MAX_TIME_SECONDS)]],
    content: 'A'.repeat(contentByteLength),
    pubkey: SAMPLE_PUBKEY,
    id: SAMPLE_PUBKEY,
    sig: SAMPLE_SIGNATURE
  })
}

function maxJsonlChunkByteSize () {
  let min = 1
  let max = MAX_EVENT_BYTES

  while (min < max) {
    const mid = Math.ceil((min + max) / 2)
    if (outerEventByteLengthForChunk(mid) <= MAX_EVENT_BYTES) min = mid
    else max = mid - 1
  }

  return min
}

// NIP-44 v3 padding has large size jumps, so derive this from the actual signed
// event shapes instead of estimating a flat overhead.
export const JSONL_CHUNK_BYTES = maxJsonlChunkByteSize()
// Nym carrier content is a base64 slice of the inner event JSON. The router
// budget is more conservative because routers carry extra tags, so it is safe
// to reuse it as the maximum carrier content length.
export const NYM_CARRIER_CHUNK_CHARS = JSONL_CHUNK_BYTES
