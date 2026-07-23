import { base16ToBytes, bytesToBase16 } from '../base16/index.js'
import { ValidationError } from '../error/index.js'

export const BASE62_ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

const BASE = BigInt(BASE62_ALPHABET.length)
const LEADER = BASE62_ALPHABET[0]
const CHAR_MAP = new Map(
  [...BASE62_ALPHABET].map((character, index) => [character, BigInt(index)])
)

function readOptions (options, allowedKeys) {
  if (options === undefined) return {}
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new ValidationError('INVALID_BASE62_OPTIONS', { message: 'Base62 options should be an object' })
  }
  for (const key of Object.keys(options)) {
    if (!allowedKeys.includes(key)) throw new ValidationError('UNKNOWN_BASE62_OPTION', { message: 'Unknown Base62 option: ' + key })
  }
  return options
}

function readMode (mode = 'bytes') {
  if (mode !== 'bytes' && mode !== 'integer') throw new ValidationError('INVALID_BASE62_MODE', { message: 'Invalid Base62 mode: ' + mode })
  return mode
}

function readLength (value, name, defaultValue, minimum = 0) {
  if (value === undefined) return defaultValue
  if (!Number.isSafeInteger(value) || value < minimum) {
    const qualifier = minimum === 0 ? 'non-negative' : 'positive'
    throw new ValidationError('INVALID_BASE62_LENGTH', { message: 'Base62 ' + name + ' should be a ' + qualifier + ' safe integer' })
  }
  return value
}

function bytesToInteger (bytes) {
  let number = 0n
  for (const byte of bytes) number = (number << 8n) + BigInt(byte)
  return number
}

function integerToBase62 (number) {
  let result = ''
  while (number > 0n) {
    result = BASE62_ALPHABET[Number(number % BASE)] + result
    number /= BASE
  }
  return result || LEADER
}

function parseBase62Integer (value) {
  let number = 0n
  for (const character of value) {
    const digit = CHAR_MAP.get(character)
    if (digit === undefined) throw new ValidationError('INVALID_BASE62_CHARACTER', { message: 'Invalid Base62 character: ' + character })
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

// Byte mode preserves every leading zero byte. Integer mode treats the input
// as an unsigned big-endian value and supports fixed-width textual output.
export function bytesToBase62 (bytes, options) {
  if (!bytes || !Number.isSafeInteger(bytes.length) || typeof bytes[Symbol.iterator] !== 'function') {
    throw new ValidationError('INVALID_BYTE_ARRAY')
  }
  const { mode: rawMode, minLength: rawMinLength } = readOptions(options, ['mode', 'minLength'])
  const mode = readMode(rawMode)
  if (mode === 'bytes' && rawMinLength !== undefined) {
    throw new ValidationError('INVALID_BASE62_MIN_LENGTH_MODE', { message: 'Base62 minLength requires integer mode' })
  }
  if (mode === 'integer') {
    if (bytes.length === 0) throw new ValidationError('EMPTY_BASE62_INTEGER', { message: 'Base62 integer input should not be empty' })
    const minLength = readLength(rawMinLength, 'minLength', 0)
    return integerToBase62(bytesToInteger(bytes)).padStart(minLength, LEADER)
  }
  if (bytes.length === 0) return ''

  const number = bytesToInteger(bytes)
  let result = number === 0n ? '' : integerToBase62(number)
  let leadingZeros = 0
  while (leadingZeros < bytes.length && bytes[leadingZeros] === 0) leadingZeros++
  result = LEADER.repeat(leadingZeros) + result
  return result
}

export function base62ToBytes (value, options) {
  if (typeof value !== 'string') throw new ValidationError('INVALID_BASE62_TYPE', { message: 'Base62 value should be a string' })
  const { mode: rawMode, byteLength: rawByteLength } = readOptions(options, ['mode', 'byteLength'])
  const mode = readMode(rawMode)
  if (mode === 'bytes' && rawByteLength !== undefined) {
    throw new ValidationError('INVALID_BASE62_BYTE_LENGTH_MODE', { message: 'Base62 byteLength requires integer mode' })
  }
  if (mode === 'integer') {
    if (value.length === 0) throw new ValidationError('EMPTY_BASE62_INTEGER', { message: 'Base62 integer value should not be empty' })
    const byteLength = readLength(rawByteLength, 'byteLength', undefined, 1)
    const bytes = integerToMinimalBytes(parseBase62Integer(value))
    if (byteLength === undefined) return bytes
    if (bytes.length > byteLength) throw new ValidationError('BASE62_INTEGER_OVERFLOW', { message: 'Base62 integer exceeds ' + byteLength + ' bytes' })
    const result = new Uint8Array(byteLength)
    result.set(bytes, byteLength - bytes.length)
    return result
  }

  let leadingZeros = 0
  while (value[leadingZeros] === LEADER) leadingZeros++
  const number = parseBase62Integer(value)
  const suffix = number === 0n ? new Uint8Array() : integerToMinimalBytes(number)
  const result = new Uint8Array(leadingZeros + suffix.length)
  result.set(suffix, leadingZeros)
  return result
}

export function base16ToBase62 (value, options) {
  return bytesToBase62(base16ToBytes(value), options)
}

export function base62ToBase16 (value, options) {
  return bytesToBase16(base62ToBytes(value, options))
}
