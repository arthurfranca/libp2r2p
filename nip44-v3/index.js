import { extract as hkdfExtract, expand as hkdfExpand } from '@noble/hashes/hkdf.js'
import { hmac } from '@noble/hashes/hmac.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils.js'
import { chacha20 } from '@noble/ciphers/chacha.js'
import { bytesToBase64, base64ToBytes } from '../base64/index.js'
import { sharedXOnlySecret } from '../ecdh/index.js'

// NIP-44 v3 — local implementation (spec.nostr.land/nip44v3)
// Copied from the bunker testbench and verified against the vendored
// upstream test-vectors.json, including non-standard zero-padding cases.

const PAD = { minimum_size: 32, subdivs_small: 4, subdivs_large: 8, large_threshold: 32768 }
const VERSION = 3
const ZERO_NONCE = new Uint8Array(12)
const textDecoder = new TextDecoder('utf-8', { ignoreBOM: true })
const fatalTextDecoder = new TextDecoder('utf-8', { fatal: true })

export function targetSize (len) {
  if (len <= 0) return PAD.minimum_size
  const nextPower = 2 ** Math.ceil(Math.log2(len))
  const subdivs = nextPower >= PAD.large_threshold ? PAD.subdivs_large : PAD.subdivs_small
  const chunkSize = Math.max(PAD.minimum_size, Math.floor(nextPower / subdivs))
  return chunkSize * Math.ceil(len / chunkSize)
}

function u32be (n) {
  const b = new Uint8Array(4)
  new DataView(b.buffer).setUint32(0, n >>> 0, false)
  return b
}

function readU32be (b, off) {
  return new DataView(b.buffer, b.byteOffset, b.byteLength).getUint32(off, false)
}

function equalBytes (a, b) {
  if (a.length !== b.length) return false
  let diff = 0
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i]
  return diff === 0
}

function randomBytes32 () {
  const bytes = new Uint8Array(32)
  crypto.getRandomValues(bytes)
  return bytes
}

export function deriveKeys (seckey, pubkey, nonce) {
  const shared = sharedXOnlySecret(seckey, pubkey)
  const salt = concatBytes(utf8ToBytes('nip44-v3\x00'), nonce)
  const prk = hkdfExtract(sha256, shared, salt)
  return {
    prk,
    encryption_key: hkdfExpand(sha256, prk, utf8ToBytes('encryption_key'), 32),
    mac_key: hkdfExpand(sha256, prk, utf8ToBytes('mac_key'), 32)
  }
}

function chacha (key, data) {
  return chacha20(key, ZERO_NONCE, data)
}

function base64EncodedByteLength (byteLength) {
  return Math.ceil(byteLength / 3) * 4
}

export function payloadByteLength (plaintextByteLength, scopeByteLength = 0) {
  return base64EncodedByteLength(73 + scopeByteLength + targetSize(plaintextByteLength + 4))
}

export function deriveKeysFromConversationKey (conversationKey, nonce) {
  const salt = concatBytes(utf8ToBytes('nip44-v3\x00'), nonce)
  const prk = hkdfExtract(sha256, conversationKey, salt)
  return {
    prk,
    encryption_key: hkdfExpand(sha256, prk, utf8ToBytes('encryption_key'), 32),
    mac_key: hkdfExpand(sha256, prk, utf8ToBytes('mac_key'), 32)
  }
}

// seckey: Uint8Array, pubkey: hex, scope/plaintext: Uint8Array. Returns base64 string.
export function encryptBytes (seckey, pubkey, kind, scope, plaintext, nonce) {
  return encryptWithConversationKeyBytes(deriveSharedConversationKey(seckey, pubkey), kind, scope, plaintext, nonce)
}

export function encryptWithConversationKeyBytes (conversationKey, kind, scope, plaintext, nonce) {
  nonce ??= randomBytes32()
  const { encryption_key: encryptionKey, mac_key: macKey } = deriveKeysFromConversationKey(conversationKey, nonce)
  const prefixed = concatBytes(u32be(plaintext.length), plaintext)
  const padded = new Uint8Array(targetSize(prefixed.length))
  padded.set(prefixed)
  const ct = chacha(encryptionKey, padded)
  const stuffing = concatBytes(u32be(kind), u32be(scope.length), scope, ct)
  const mac = hmac(sha256, macKey, concatBytes(nonce, stuffing))
  return bytesToBase64(concatBytes(new Uint8Array([VERSION]), nonce, mac, stuffing))
}

export function decryptBytes (seckey, pubkey, expectedKind, expectedScope, ciphertext) {
  return decryptWithConversationKeyBytes(deriveSharedConversationKey(seckey, pubkey), expectedKind, expectedScope, ciphertext)
}

export function decryptWithConversationKeyBytes (conversationKey, expectedKind, expectedScope, ciphertext) {
  if (!ciphertext || ciphertext.length === 0) throw new Error('empty ciphertext')
  if (ciphertext[0] === '#') throw new Error('unsupported future version')
  let decoded
  try { decoded = base64ToBytes(ciphertext) } catch { throw new Error('invalid base64') }
  if (decoded.length < 77) throw new Error('ciphertext too short')
  if (decoded[0] !== VERSION) throw new Error(`unsupported version ${decoded[0]}`)
  const nonce = decoded.subarray(1, 33)
  const mac = decoded.subarray(33, 65)
  const kind = readU32be(decoded, 65)
  const scopeLength = readU32be(decoded, 69)
  if (scopeLength > decoded.length - 73) throw new Error('invalid scope length')
  const scope = decoded.subarray(73, 73 + scopeLength)
  try { fatalTextDecoder.decode(scope) } catch { throw new Error('scope is not valid UTF-8') }
  const ct = decoded.subarray(73 + scopeLength)
  if (ct.length < 4) throw new Error('ciphertext too short')
  if (kind !== expectedKind) throw new Error(`kind mismatch: got ${kind}, expected ${expectedKind}`)
  if (!equalBytes(scope, expectedScope)) throw new Error('scope mismatch')
  const { encryption_key: encryptionKey, mac_key: macKey } = deriveKeysFromConversationKey(conversationKey, nonce)
  const authData = concatBytes(nonce, u32be(kind), u32be(scope.length), scope, ct)
  if (!equalBytes(mac, hmac(sha256, macKey, authData))) throw new Error('invalid MAC')
  const padded = chacha(encryptionKey, ct)
  const plaintextLength = readU32be(padded, 0)
  if (plaintextLength + 4 > padded.length) throw new Error('invalid plaintext length')
  if (plaintextLength > 2 ** 31 - 1) throw new Error('plaintext too long')
  // Only verify the padding is all-zeroes. Per spec, implementations MUST NOT do any
  // other check on the padding length — non-standard zero-padding must decrypt.
  const padding = padded.subarray(4 + plaintextLength)
  if (!equalBytes(padding, new Uint8Array(padding.length))) throw new Error('invalid padding')
  return padded.subarray(4, 4 + plaintextLength)
}

function deriveSharedConversationKey (seckey, pubkey) {
  return sharedXOnlySecret(seckey, pubkey)
}

export function normalizeKind (kind) {
  const n = typeof kind === 'string' && kind.trim() !== '' ? Number(kind) : kind
  if (!Number.isInteger(n) || n < 0 || n > 0xffffffff) throw new Error('INVALID_KIND')
  return n
}

// String-oriented helpers for app-facing methods. Plaintext travels as
// base64 on the NIP-07/46 wire so callers can encrypt arbitrary bytes.
export function encrypt (seckey, pubkey, kind, scope, plaintext) {
  return encryptBytes(seckey, pubkey, normalizeKind(kind), utf8ToBytes(scope || ''), utf8ToBytes(plaintext))
}

export function decrypt (seckey, pubkey, kind, scope, ciphertext) {
  return textDecoder.decode(decryptBytes(seckey, pubkey, normalizeKind(kind), utf8ToBytes(scope || ''), ciphertext))
}

export function encryptWithConversationKey (conversationKey, kind, scope, plaintext) {
  return encryptWithConversationKeyBytes(conversationKey, normalizeKind(kind), utf8ToBytes(scope || ''), utf8ToBytes(plaintext))
}

export function decryptWithConversationKey (conversationKey, kind, scope, ciphertext) {
  return textDecoder.decode(decryptWithConversationKeyBytes(conversationKey, normalizeKind(kind), utf8ToBytes(scope || ''), ciphertext))
}

export function nip07Encrypt (seckey, pubkey, kind, scope, plaintextB64) {
  return encryptBytes(seckey, pubkey, normalizeKind(kind), utf8ToBytes(scope || ''), base64ToBytes(plaintextB64))
}

export function nip07Decrypt (seckey, pubkey, kind, scope, ciphertext) {
  return bytesToBase64(decryptBytes(seckey, pubkey, normalizeKind(kind), utf8ToBytes(scope || ''), ciphertext))
}

export const b64encode = bytesToBase64
export const b64decode = base64ToBytes
export const toBytes = utf8ToBytes
export const fromBytes = b => textDecoder.decode(b)
