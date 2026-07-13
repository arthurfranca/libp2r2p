import { finalizeEvent, verifyEvent } from 'nostr-tools'
import { decrypt, encrypt, getConversationKey } from 'nostr-tools/nip44'
import { NIP46_KIND } from '../constants/index.js'

const PUBKEY = /^[0-9a-f]{64}$/

export function validPubkey (value) {
  return typeof value === 'string' && PUBKEY.test(value)
}

export function hasPTag (event, pubkey) {
  return Array.isArray(event?.tags) && event.tags.some(tag => tag?.[0] === 'p' && tag?.[1] === pubkey)
}

export function isNip46EventFor (event, pubkey) {
  return event?.kind === NIP46_KIND &&
    validPubkey(event.pubkey) &&
    hasPTag(event, pubkey) &&
    verifyEvent(event)
}

export function decodeNip46Frame (event, secretKey) {
  try {
    const plaintext = decrypt(event.content, getConversationKey(secretKey, event.pubkey))
    const frame = JSON.parse(plaintext)
    return frame && typeof frame === 'object' && !Array.isArray(frame) ? frame : null
  } catch {
    return null
  }
}

export function createNip46Event ({ secretKey, recipientPubkey, payload }) {
  return finalizeEvent({
    kind: NIP46_KIND,
    created_at: Math.floor(Date.now() / 1000),
    tags: [['p', recipientPubkey]],
    content: encrypt(JSON.stringify(payload), getConversationKey(secretKey, recipientPubkey))
  }, secretKey)
}

export function validRequestFrame (frame) {
  return typeof frame?.id === 'string' && frame.id &&
    typeof frame.method === 'string' && frame.method &&
    Array.isArray(frame.params) && frame.params.every(param => typeof param === 'string')
}

export function requestError (reason = 'NIP46_REQUEST_REJECTED') {
  return reason instanceof Error
    ? reason
    : new Error(typeof reason === 'string' && reason ? reason : 'NIP46_REQUEST_REJECTED')
}
