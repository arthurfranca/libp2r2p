import { schnorr } from '@noble/curves/secp256k1.js'
import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16, base16ToBytes } from '../base16/index.js'
import {
  isAddressableKind,
  isEphemeralKind,
  isRegularKind,
  isReplaceableKind
} from '../kind/index.js'
import { serializeEvent, validateEvent } from './helpers/serialize.js'

const HEX_32 = /^[0-9a-f]{64}$/
const HEX_64 = /^[0-9a-f]{128}$/
const textEncoder = new TextEncoder()

function copyTags (tags) {
  return Array.isArray(tags) ? tags.map(tag => Array.isArray(tag) ? tag.slice() : tag) : tags
}

export { validateEvent }

export function getEventHash (event) {
  return bytesToBase16(sha256(textEncoder.encode(serializeEvent(event))))
}

export function finalizeEvent (template, secretKey) {
  if (!(secretKey instanceof Uint8Array) || secretKey.length !== 32) throw new Error('INVALID_SECRET_KEY')
  const pubkey = bytesToBase16(schnorr.getPublicKey(secretKey))
  const event = { ...template, tags: copyTags(template?.tags), pubkey }
  const id = getEventHash(event)
  return { ...event, id, sig: bytesToBase16(schnorr.sign(base16ToBytes(id), secretKey)) }
}

export function verifyEvent (event) {
  if (!validateEvent(event) || !HEX_32.test(event.id) || !HEX_64.test(event.sig)) return false
  let id
  try {
    id = getEventHash(event)
    return id === event.id && schnorr.verify(base16ToBytes(event.sig), base16ToBytes(id), base16ToBytes(event.pubkey))
  } catch {
    return false
  }
}

function hasTagDefinedClassification (event, classification) {
  if (!event || typeof event !== 'object' || !Array.isArray(event.tags)) return false

  if (classification === 'ephemeral') {
    if (!Number.isSafeInteger(event.created_at) || event.created_at < 0) return false
    const timestamp = String(event.created_at)
    return event.tags.some(tag => Array.isArray(tag) && tag[0] === 'expiration' && tag[1] === timestamp)
  }

  const dTag = event.tags.find(tag => Array.isArray(tag) && tag[0] === 'd')
  if (typeof dTag?.[1] !== 'string') return false
  if (classification === 'replaceable') return dTag[1] === ''
  if (classification === 'addressable') return dTag[1] !== ''
  return false
}

export function classifyEvent (event, { includeLegacyKindRanges = true } = {}) {
  if (!event || typeof event !== 'object') return []
  const options = { includeLegacyKindRanges }
  return [
    ['regular', isRegularEvent],
    ['replaceable', isReplaceableEvent],
    ['ephemeral', isEphemeralEvent],
    ['addressable', isAddressableEvent]
  ].filter(([, predicate]) => predicate(event, options)).map(([classification]) => classification)
}

export function isRegularEvent (event, options) {
  if (!event || typeof event !== 'object') return false
  return (options?.includeLegacyKindRanges !== false && isRegularKind(event.kind)) ||
    (!isReplaceableEvent(event, options) && !isAddressableEvent(event, options))
}

export function isReplaceableEvent (event, options) {
  return (options?.includeLegacyKindRanges !== false && isReplaceableKind(event?.kind)) ||
    hasTagDefinedClassification(event, 'replaceable')
}

export function isEphemeralEvent (event, options) {
  return (options?.includeLegacyKindRanges !== false && isEphemeralKind(event?.kind)) ||
    hasTagDefinedClassification(event, 'ephemeral')
}

export function isAddressableEvent (event, options) {
  return (options?.includeLegacyKindRanges !== false && isAddressableKind(event?.kind)) ||
    hasTagDefinedClassification(event, 'addressable')
}
