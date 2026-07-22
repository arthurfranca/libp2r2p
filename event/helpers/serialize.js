const HEX_32 = /^[0-9a-f]{64}$/

export function validateEvent (event) {
  if (!event || typeof event !== 'object' || Array.isArray(event)) return false
  if (!Number.isSafeInteger(event.kind) || event.kind < 0 || event.kind > 0xffff) return false
  if (!Number.isSafeInteger(event.created_at) || event.created_at < 0) return false
  if (typeof event.pubkey !== 'string' || !HEX_32.test(event.pubkey)) return false
  if (typeof event.content !== 'string' || !Array.isArray(event.tags)) return false
  return event.tags.every(tag => Array.isArray(tag) && tag.length > 0 && tag.every(value => typeof value === 'string'))
}

export function serializeEvent (event) {
  if (!validateEvent(event)) throw new Error('INVALID_EVENT')
  return JSON.stringify([0, event.pubkey, event.created_at, event.kind, event.tags, event.content])
}
