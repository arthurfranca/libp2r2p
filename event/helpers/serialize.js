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
