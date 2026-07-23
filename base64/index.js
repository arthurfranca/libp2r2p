// Plain and URL-safe base64 codecs for binary <-> string conversion.
// Plain base64 is used where a protocol expects standard base64. URL-safe
// base64 is useful for WebAuthn credential IDs and URL/query-string material.

import { ValidationError } from '../error/index.js'

export function bytesToBase64 (bytes) {
  if (!bytes || !Number.isSafeInteger(bytes.length) || typeof bytes[Symbol.iterator] !== 'function') {
    throw new ValidationError('INVALID_BYTE_ARRAY')
  }
  if (typeof Buffer === 'function' && typeof Buffer.from === 'function') {
    return Buffer.from(bytes).toString('base64')
  }
  let s = ''
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i])
  return btoa(s)
}

export function base64ToBytes (b64) {
  if (typeof b64 !== 'string') throw new ValidationError('INVALID_BASE64_TYPE', { message: 'Base64 value should be a string' })
  let bin
  try {
    bin = atob(b64)
  } catch (cause) {
    throw new ValidationError('INVALID_BASE64', { cause })
  }
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

export function bytesToBase64Url (bytes) {
  return bytesToBase64(bytes)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')
}

export function base64UrlToBytes (base64url) {
  if (typeof base64url !== 'string') throw new ValidationError('INVALID_BASE64URL_TYPE', { message: 'Base64URL value should be a string' })
  const value = String(base64url)
  const pad = value.length % 4 === 0 ? '' : '='.repeat(4 - (value.length % 4))
  try {
    return base64ToBytes(value.replace(/-/g, '+').replace(/_/g, '/') + pad)
  } catch (cause) {
    throw new ValidationError('INVALID_BASE64URL', { cause })
  }
}
