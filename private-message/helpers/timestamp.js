import { ValidationError } from '../../error/index.js'

export const MILLISECONDS_TAG = 'ms'

const CANONICAL_MILLISECONDS = /^(0|[1-9][0-9]{0,2})$/

function normalizeCreatedAt (createdAt) {
  if (!Number.isSafeInteger(createdAt) || createdAt < 0) throw new ValidationError('INVALID_CREATED_AT')
  return createdAt
}

export function timestampFromMilliseconds (createdAtMs) {
  if (!Number.isSafeInteger(createdAtMs) || createdAtMs < 0) {
    throw new ValidationError('INVALID_CREATED_AT_MS')
  }
  return {
    createdAt: Math.floor(createdAtMs / 1000),
    milliseconds: createdAtMs % 1000
  }
}

export function messageTimestamp ({ createdAt, createdAtMs, nowMs = Date.now } = {}) {
  if (createdAtMs != null) {
    const timestamp = timestampFromMilliseconds(createdAtMs)
    if (createdAt != null && normalizeCreatedAt(createdAt) !== timestamp.createdAt) {
      throw new ValidationError('INVALID_CREATED_AT_MS')
    }
    return timestamp
  }
  if (createdAt != null) return { createdAt: normalizeCreatedAt(createdAt), milliseconds: 0 }
  const current = nowMs()
  if (!Number.isSafeInteger(current) || current < 0) throw new ValidationError('INVALID_CREATED_AT_MS')
  return {
    createdAt: Math.floor(current / 1000),
    milliseconds: current % 1000
  }
}

export function setMillisecondsTag (tags, milliseconds) {
  if (!Number.isSafeInteger(milliseconds) || milliseconds < 0 || milliseconds > 999) {
    throw new ValidationError('INVALID_MILLISECONDS')
  }
  const out = (Array.isArray(tags) ? tags : [])
    .filter(tag => !Array.isArray(tag) || tag[0] !== MILLISECONDS_TAG)
    .map(tag => Array.isArray(tag) ? [...tag] : tag)
  out.push([MILLISECONDS_TAG, String(milliseconds)])
  return out
}

export function readMilliseconds (event) {
  const tags = (event?.tags || []).filter(tag => Array.isArray(tag) && tag[0] === MILLISECONDS_TAG)
  if (!tags.length) return { milliseconds: 0, reason: 'missing' }
  if (tags.length !== 1 || tags[0].length !== 2 || !CANONICAL_MILLISECONDS.test(tags[0][1])) {
    return { milliseconds: 0, reason: tags.length === 1 ? 'invalid' : 'duplicate' }
  }
  const milliseconds = Number(tags[0][1])
  if (!Number.isInteger(milliseconds) || milliseconds < 0 || milliseconds > 999) {
    return { milliseconds: 0, reason: 'invalid' }
  }
  return { milliseconds, reason: '' }
}

export function eventCreatedAtMs (event, { readMillisecondsTag = true } = {}) {
  if (!Number.isSafeInteger(event?.created_at) || event.created_at < 0) return 0
  const { milliseconds } = readMillisecondsTag
    ? readMilliseconds(event)
    : { milliseconds: 0 }
  return event.created_at * 1000 + milliseconds
}
