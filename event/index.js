import { schnorr } from '@noble/curves/secp256k1.js'
import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16, base16ToBytes } from '../base16/index.js'
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
