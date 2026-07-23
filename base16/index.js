import { ValidationError } from '../error/index.js'

export function bytesToBase16 (bytes) {
  if (!bytes || typeof bytes[Symbol.iterator] !== 'function') throw new ValidationError('INVALID_BYTE_ARRAY')
  let s = ''
  for (const b of bytes) s += b.toString(16).padStart(2, '0')
  return s
}

export function base16ToBytes (base16) {
  if (typeof base16 !== 'string') throw new ValidationError('INVALID_BASE16_TYPE', { message: 'Base16 value should be a string' })
  if (base16.length % 2 !== 0) throw new ValidationError('INVALID_BASE16_LENGTH', { message: 'Invalid Base16 length' })
  if (!/^[0-9a-f]*$/i.test(base16)) throw new ValidationError('INVALID_BASE16_CHARACTER', { message: 'Invalid Base16 character' })
  const out = new Uint8Array(base16.length / 2)
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(base16.slice(i * 2, i * 2 + 2), 16)
  }
  return out
}

export const bytesToHex = bytesToBase16
export const hexToBytes = base16ToBytes
