// Plain and URL-safe base64 codecs for binary <-> string conversion.
// Plain base64 is used where a protocol expects standard base64. URL-safe
// base64 is useful for WebAuthn credential IDs and URL/query-string material.

export function bytesToBase64 (bytes) {
  if (typeof Buffer === 'function' && typeof Buffer.from === 'function') {
    return Buffer.from(bytes).toString('base64')
  }
  let s = ''
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i])
  return btoa(s)
}

export function base64ToBytes (b64) {
  const bin = atob(b64)
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
  const value = String(base64url)
  const pad = value.length % 4 === 0 ? '' : '='.repeat(4 - (value.length % 4))
  return base64ToBytes(value.replace(/-/g, '+').replace(/_/g, '/') + pad)
}
