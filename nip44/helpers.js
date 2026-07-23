import { chacha20 } from '@noble/ciphers/chacha.js'
import { equalBytes, randomBytes } from '@noble/ciphers/utils.js'
import { expand, extract } from '@noble/hashes/hkdf.js'
import { hmac } from '@noble/hashes/hmac.js'
import { sha256 } from '@noble/hashes/sha2.js'

import { base64ToBytes, bytesToBase64 } from '../base64/index.js'
import { ValidationError } from '../error/index.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })
const minPlaintextSize = 1
const maxPlaintextSize = 0xffffffff
const maxRawPayloadSize = 1 + 32 + 6 + 0x100000000 + 32
const maxEncodedPayloadSize = Math.ceil(maxRawPayloadSize / 3) * 4

function concatBytes (...arrays) {
  const output = new Uint8Array(arrays.reduce((total, value) => total + value.length, 0))
  let offset = 0
  for (const value of arrays) {
    output.set(value, offset)
    offset += value.length
  }
  return output
}

export function getMessageKeys (conversationKey, nonce) {
  if (!(conversationKey instanceof Uint8Array) || conversationKey.length !== 32) throw new ValidationError('INVALID_CONVERSATION_KEY')
  if (!(nonce instanceof Uint8Array) || nonce.length !== 32) throw new ValidationError('INVALID_NONCE')
  const keys = expand(sha256, conversationKey, nonce, 76)
  return { key: keys.subarray(0, 32), nonce: keys.subarray(32, 44), hmacKey: keys.subarray(44) }
}

export function calcPaddedLen (length) {
  if (!Number.isSafeInteger(length) || length < minPlaintextSize || length > maxPlaintextSize) throw new ValidationError('INVALID_PLAINTEXT_SIZE')
  if (length <= 32) return 32
  const nextPower = 2 ** (Math.floor(Math.log2(length - 1)) + 1)
  const chunk = nextPower <= 256 ? 32 : nextPower / 8
  return chunk * (Math.floor((length - 1) / chunk) + 1)
}

export function pad (plaintext) {
  if (typeof plaintext !== 'string') throw new ValidationError('PLAINTEXT_SHOULD_BE_A_STRING')
  const bytes = encoder.encode(plaintext)
  const length = bytes.length
  calcPaddedLen(length)
  const prefixLength = length < 0x10000 ? 2 : 6
  const prefix = new Uint8Array(prefixLength)
  const view = new DataView(prefix.buffer)
  if (prefixLength === 2) view.setUint16(0, length, false)
  else view.setUint32(2, length, false)
  return concatBytes(prefix, bytes, new Uint8Array(calcPaddedLen(length) - length))
}

export function unpad (padded) {
  if (!(padded instanceof Uint8Array) || padded.length < 2) throw new ValidationError('INVALID_PADDING')
  const view = new DataView(padded.buffer, padded.byteOffset, padded.byteLength)
  const shortLength = view.getUint16(0, false)
  const prefixLength = shortLength === 0 ? 6 : 2
  if (padded.length < prefixLength) throw new ValidationError('INVALID_PADDING')
  const length = shortLength === 0 ? view.getUint32(2, false) : shortLength
  if (shortLength === 0 && length < 0x10000) throw new ValidationError('INVALID_PADDING')
  let expected
  try { expected = prefixLength + calcPaddedLen(length) } catch (cause) { throw new ValidationError('INVALID_PADDING', { cause }) }
  if (padded.length !== expected || prefixLength + length > padded.length) throw new ValidationError('INVALID_PADDING')
  try { return decoder.decode(padded.subarray(prefixLength, prefixLength + length)) } catch (cause) { throw new ValidationError('INVALID_UTF8', { cause }) }
}

export function encodePayload (conversationKey, plaintext, nonce = randomBytes(32)) {
  const messageKeys = getMessageKeys(conversationKey, nonce)
  const ciphertext = chacha20(messageKeys.key, messageKeys.nonce, pad(plaintext))
  const mac = hmac(sha256, messageKeys.hmacKey, concatBytes(nonce, ciphertext))
  return bytesToBase64(concatBytes(new Uint8Array([2]), nonce, ciphertext, mac))
}

export function decodePayload (conversationKey, payload) {
  if (typeof payload !== 'string' || payload.length < 132 || payload.length > maxEncodedPayloadSize || payload[0] === '#') throw new ValidationError('INVALID_NIP44_PAYLOAD')
  if (payload.length % 4 !== 0 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(payload)) throw new ValidationError('INVALID_BASE64')
  const data = base64ToBytes(payload)
  if (bytesToBase64(data) !== payload || data.length < 99 || data[0] !== 2) throw new ValidationError('INVALID_NIP44_PAYLOAD')
  const nonce = data.subarray(1, 33)
  const ciphertext = data.subarray(33, -32)
  const mac = data.subarray(-32)
  const messageKeys = getMessageKeys(conversationKey, nonce)
  const calculated = hmac(sha256, messageKeys.hmacKey, concatBytes(nonce, ciphertext))
  if (!equalBytes(mac, calculated)) throw new ValidationError('INVALID_MAC')
  return unpad(chacha20(messageKeys.key, messageKeys.nonce, ciphertext))
}

export function extractConversationKey (sharedSecret, salt = 'nip44-v2') {
  if (typeof salt !== 'string') throw new ValidationError('SALT_SHOULD_BE_A_STRING')
  const saltBytes = encoder.encode(salt)
  if (saltBytes.length === 0 || saltBytes.length > 32) throw new ValidationError('INVALID_SALT')
  return extract(sha256, sharedSecret, saltBytes)
}
