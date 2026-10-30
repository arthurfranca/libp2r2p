import { ValidationError } from '../../error/index.js'

const HEX_32 = /^[0-9a-f]{64}$/

export function serializableEventError (event) {
  if (!event || typeof event !== 'object' || Array.isArray(event)) return 'INVALID_EVENT'
  if (!Number.isSafeInteger(event.kind) || event.kind < 0 || event.kind > 0xffff) return 'INVALID_EVENT_KIND'
  if (!Number.isSafeInteger(event.created_at) || event.created_at < 0) return 'INVALID_EVENT_CREATED_AT'
  if (typeof event.pubkey !== 'string' || !HEX_32.test(event.pubkey)) return 'INVALID_EVENT_PUBKEY'
  if (typeof event.content !== 'string') return 'INVALID_EVENT_CONTENT'
  if (!Array.isArray(event.tags) ||
      !event.tags.every(tag => Array.isArray(tag) && tag.length > 0 && tag.every(value => typeof value === 'string'))) {
    return 'INVALID_EVENT_TAGS'
  }
  return null
}

export function isSerializableEvent (event) {
  return serializableEventError(event) === null
}

export function assertSerializableEvent (event) {
  const code = serializableEventError(event)
  if (code) throw new ValidationError(code)
  return event
}

export function serializeEvent (event) {
  assertSerializableEvent(event)
  return JSON.stringify([0, event.pubkey, event.created_at, event.kind, event.tags, event.content])
}

function isHighSurrogate (value) {
  return value >= 0xd800 && value <= 0xdbff
}

function isLowSurrogate (value) {
  return value >= 0xdc00 && value <= 0xdfff
}

function chunkEnd (value, start, chunkSize) {
  let end = Math.min(start + Math.max(2, chunkSize), value.length)
  if (
    end < value.length &&
    end > start + 1 &&
    isHighSurrogate(value.charCodeAt(end - 1)) &&
    isLowSurrogate(value.charCodeAt(end))
  ) {
    end--
  }
  return end
}

function * serializeJsonString (value, chunkSize) {
  yield '"'
  for (let start = 0; start < value.length;) {
    const end = chunkEnd(value, start, chunkSize)
    const serialized = JSON.stringify(value.slice(start, end))
    yield serialized.slice(1, -1)
    start = end
  }
  yield '"'
}

/**
 * Produces the exact same text as serializeEvent() without constructing the
 * complete serialized event in one JavaScript string.
 */
export function * serializeEventChunks (event, { chunkSize = 4 * 1024, validate = true } = {}) {
  if (validate) assertSerializableEvent(event)
  yield `[0,"${event.pubkey}",${event.created_at},${event.kind},[`

  for (let tagIndex = 0; tagIndex < event.tags.length; tagIndex++) {
    if (tagIndex) yield ','
    yield '['
    const tag = event.tags[tagIndex]
    for (let valueIndex = 0; valueIndex < tag.length; valueIndex++) {
      if (valueIndex) yield ','
      yield * serializeJsonString(tag[valueIndex], chunkSize)
    }
    yield ']'
  }

  yield '],'
  yield * serializeJsonString(event.content, chunkSize)
  yield ']'
}
