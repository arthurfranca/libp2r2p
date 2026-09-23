import { getEventHash, isSerializableEvent } from '../../event/index.js'
import { ValidationError } from '../../error/index.js'

// Transport sender and claimed author are distinct. This also validates a
// supplied ID before discarding it from the unsigned wire representation.
export function normalizeRumor (rumor, senderPubkey) {
  if (!rumor || typeof rumor !== 'object' || Array.isArray(rumor) || 'sig' in rumor) throw new ValidationError('INVALID_RUMOR')
  const event = {
    pubkey: rumor.pubkey === undefined ? senderPubkey : rumor.pubkey,
    kind: rumor.kind,
    created_at: rumor.created_at,
    tags: rumor.tags,
    content: rumor.content
  }
  if (!isSerializableEvent(event)) throw new ValidationError('INVALID_RUMOR')
  const id = getEventHash(event)
  if (rumor.id !== undefined && rumor.id !== id) throw new ValidationError('INVALID_RUMOR_ID')
  return { ...event, id }
}

export function deliveryInfo (event, senderPubkey) {
  return {
    senderPubkey,
    provenance: event.sig ? 'signed' : event.pubkey === senderPubkey ? 'direct' : 'hearsay'
  }
}
