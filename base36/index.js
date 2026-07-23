import { base16ToBytes, bytesToBase16 } from '../base16/index.js'
import { ValidationError } from '../error/index.js'

export const BASE36_ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz'

const BASE = BigInt(BASE36_ALPHABET.length)
const LEADER = BASE36_ALPHABET[0]
const CHAR_MAP = new Map(
  [...BASE36_ALPHABET].map((character, index) => [character, BigInt(index)])
)
const NSITE_BYTE_LENGTH = 32
const NSITE_TEXT_LENGTH = 50
const MAX_NSITE_VALUE = (1n << 256n) - 1n

function bytesToInteger (bytes) {
  let number = 0n
  for (const byte of bytes) number = (number << 8n) + BigInt(byte)
  return number
}

function integerToBase36 (number) {
  let result = ''
  while (number > 0n) {
    result = BASE36_ALPHABET[Number(number % BASE)] + result
    number /= BASE
  }
  return result || LEADER
}

function parseBase36Integer (value) {
  let number = 0n
  for (const character of value) {
    const digit = CHAR_MAP.get(character)
    if (digit === undefined) throw new ValidationError('INVALID_BASE36_CHARACTER', { message: 'Invalid Base36 character: ' + character })
    number = number * BASE + digit
  }
  return number
}

function integerToMinimalBytes (number) {
  if (number === 0n) return new Uint8Array([0])
  const bytes = []
  while (number > 0n) {
    bytes.unshift(Number(number & 0xffn))
    number >>= 8n
  }
  return Uint8Array.from(bytes)
}

// A binary-safe Base36 codec. Every leading zero byte is represented by a
// leading zero character, making arbitrary byte arrays round-trip exactly.
export function bytesToBase36 (bytes) {
  if (!bytes || !Number.isSafeInteger(bytes.length) || typeof bytes[Symbol.iterator] !== 'function') {
    throw new ValidationError('INVALID_BYTE_ARRAY')
  }
  if (bytes.length === 0) return ''

  const number = bytesToInteger(bytes)
  let result = number === 0n ? '' : integerToBase36(number)
  let leadingZeros = 0
  while (leadingZeros < bytes.length && bytes[leadingZeros] === 0) leadingZeros++
  result = LEADER.repeat(leadingZeros) + result
  return result
}

export function base36ToBytes (value) {
  if (typeof value !== 'string') throw new ValidationError('INVALID_BASE36_TYPE', { message: 'Base36 value should be a string' })
  if (value.length === 0) return new Uint8Array()

  let leadingZeros = 0
  while (value[leadingZeros] === LEADER) leadingZeros++
  const number = parseBase36Integer(value)
  const suffix = number === 0n ? new Uint8Array() : integerToMinimalBytes(number)
  const result = new Uint8Array(leadingZeros + suffix.length)
  result.set(suffix, leadingZeros)
  return result
}

export function base16ToBase36 (value) {
  return bytesToBase36(base16ToBytes(value))
}

export function base36ToBase16 (value) {
  return bytesToBase16(base36ToBytes(value))
}

// NIP-5A treats a raw 32-byte value as one unsigned integer and represents it
// as exactly 50 lowercase Base36 digits. Its leading zeros are numeric width,
// not zero-byte markers as they are in the binary-safe codec above.
export function bytesToBase36Nsite (bytes) {
  if (!bytes || bytes.length !== NSITE_BYTE_LENGTH || typeof bytes[Symbol.iterator] !== 'function') {
    throw new ValidationError('INVALID_NSITE_BASE36_BYTE_LENGTH', { message: 'Nsite Base36 input should be ' + NSITE_BYTE_LENGTH + ' bytes' })
  }
  return integerToBase36(bytesToInteger(bytes)).padStart(NSITE_TEXT_LENGTH, LEADER)
}

export function base36NsiteToBytes (value) {
  if (typeof value !== 'string') throw new ValidationError('INVALID_NSITE_BASE36_TYPE', { message: 'Nsite Base36 value should be a string' })
  if (value.length !== NSITE_TEXT_LENGTH) {
    throw new ValidationError('INVALID_NSITE_BASE36_LENGTH', { message: 'Nsite Base36 value should be ' + NSITE_TEXT_LENGTH + ' characters' })
  }
  if (!/^[0-9a-z]+$/.test(value)) throw new ValidationError('INVALID_NSITE_BASE36_CHARACTER', { message: 'Invalid Nsite Base36 character' })

  const number = parseBase36Integer(value)
  if (number > MAX_NSITE_VALUE) throw new ValidationError('NSITE_BASE36_OVERFLOW', { message: 'Nsite Base36 value exceeds 32 bytes' })
  const bytes = integerToMinimalBytes(number)
  const result = new Uint8Array(NSITE_BYTE_LENGTH)
  if (number !== 0n) result.set(bytes, NSITE_BYTE_LENGTH - bytes.length)
  return result
}

export function base16ToBase36Nsite (value) {
  return bytesToBase36Nsite(base16ToBytes(value))
}

export function base36NsiteToBase16 (value) {
  return bytesToBase16(base36NsiteToBytes(value))
}
