import { ValidationError } from '../../error/index.js'

export const IDEMPOTENCY_TAG = 'D'
export const MAX_IDEMPOTENCY_KEY_BYTES = 256

const encoder = new TextEncoder()
const MAX_CHUNK_SUFFIX_BYTES = String(Number.MAX_SAFE_INTEGER).length + 1
const MAX_IDEMPOTENCY_KEY_BASE_BYTES = MAX_IDEMPOTENCY_KEY_BYTES - MAX_CHUNK_SUFFIX_BYTES

function keyBytes (value) {
  return encoder.encode(value).byteLength
}

function invalidKey () {
  return new ValidationError('INVALID_IDEMPOTENCY_KEY')
}

export function normalizeIdempotencyKey (value) {
  if (value === undefined) return ''
  if (typeof value !== 'string' || !value || keyBytes(value) > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw invalidKey()
  }
  return value
}

export function normalizeIdempotencyKeyBase (value) {
  if (value === undefined) return ''
  if (typeof value !== 'string' || !value || keyBytes(value) > MAX_IDEMPOTENCY_KEY_BASE_BYTES) {
    throw invalidKey()
  }
  return value
}

export function chunkIdempotencyKey (base, index) {
  const normalizedBase = normalizeIdempotencyKeyBase(base)
  if (!normalizedBase) return undefined
  if (!Number.isSafeInteger(index) || index < 0) throw invalidKey()
  return normalizeIdempotencyKey(`${normalizedBase}:${index}`)
}

export function appendIdempotencyTag (tags, idempotencyKey) {
  const key = normalizeIdempotencyKey(idempotencyKey)
  if (!key) return tags
  return [[IDEMPOTENCY_TAG, key], ...tags]
}

export function readIdempotencyKey (event) {
  const tags = (Array.isArray(event?.tags) ? event.tags : [])
    .filter(tag => Array.isArray(tag) && tag[0] === IDEMPOTENCY_TAG)
  if (!tags.length) return { key: null, reason: 'missing' }
  if (tags.length !== 1 || tags[0].length !== 2) return { key: null, reason: 'malformed' }
  try {
    return { key: normalizeIdempotencyKey(tags[0][1]), reason: '' }
  } catch {
    return { key: null, reason: 'malformed' }
  }
}
