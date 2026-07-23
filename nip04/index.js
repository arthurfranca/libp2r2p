import { cbc } from '@noble/ciphers/aes.js'
import { randomBytes } from '@noble/ciphers/utils.js'

import { base16ToBytes } from '../base16/index.js'
import { base64ToBytes, bytesToBase64 } from '../base64/index.js'
import { sharedXOnlySecret } from '../ecdh/index.js'
import { ValidationError } from '../error/index.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })

function decodeBase64 (value) {
  if (typeof value !== 'string' || value.length % 4 !== 0 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new ValidationError('INVALID_BASE64')
  }
  const bytes = base64ToBytes(value)
  if (bytesToBase64(bytes) !== value) throw new ValidationError('NON_CANONICAL_BASE64')
  return bytes
}

function conversationKey (secretKey, pubkey) {
  if (!(secretKey instanceof Uint8Array) || secretKey.length !== 32) throw new ValidationError('INVALID_SECRET_KEY')
  if (typeof pubkey !== 'string' || !/^[0-9a-f]{64}$/.test(pubkey)) throw new ValidationError('INVALID_PUBLIC_KEY')
  return sharedXOnlySecret(secretKey, pubkey)
}

export function encrypt (secretKey, pubkey, plaintext) {
  if (typeof plaintext !== 'string') throw new ValidationError('PLAINTEXT_SHOULD_BE_A_STRING')
  const iv = randomBytes(16)
  const ciphertext = cbc(conversationKey(secretKey, pubkey), iv).encrypt(encoder.encode(plaintext))
  return `${bytesToBase64(ciphertext)}?iv=${bytesToBase64(iv)}`
}

export function decrypt (secretKey, pubkey, payload) {
  if (typeof payload !== 'string') throw new ValidationError('CIPHERTEXT_SHOULD_BE_A_STRING')
  const parts = payload.split('?iv=')
  if (parts.length !== 2 || !parts[0] || !parts[1]) throw new ValidationError('INVALID_NIP04_ENVELOPE')
  const ciphertext = decodeBase64(parts[0])
  const iv = decodeBase64(parts[1])
  if (iv.length !== 16 || ciphertext.length === 0 || ciphertext.length % 16 !== 0) throw new ValidationError('INVALID_NIP04_ENVELOPE')
  try {
    return decoder.decode(cbc(conversationKey(secretKey, pubkey), iv).decrypt(ciphertext))
  } catch (cause) {
    if (cause instanceof ValidationError && cause.code !== 'INVALID_NIP04_CIPHERTEXT') throw cause
    throw new ValidationError('INVALID_NIP04_CIPHERTEXT', { cause })
  }
}
