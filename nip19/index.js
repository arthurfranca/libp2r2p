import { bech32 } from '@scure/base'
import { BASE62_ALPHABET, base62ToBytes, bytesToBase62 } from '../base62/index.js'

const MAX_ENTITY_SIZE = 5000
const MAX_TLV_VALUE_SIZE = 255
const NOSTR_APP_D_TAG_MAX_LENGTH = 260
export const NAPP_ENTITY_REGEX = new RegExp(
  `^\\+{1,3}[${BASE62_ALPHABET}]{48,${MAX_ENTITY_SIZE}}$`
)

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const fatalTextDecoder = new TextDecoder('utf-8', { fatal: true })

const kindByChannel = {
  main: 35128,
  next: 35129,
  draft: 35130
}
const channelByKind = Object.fromEntries(
  Object.entries(kindByChannel).map(([channel, kind]) => [kind, channel])
)
const prefixByChannel = {
  main: '+',
  next: '++',
  draft: '+++'
}
const channelByPrefix = Object.fromEntries(
  Object.entries(prefixByChannel).map(([channel, prefix]) => [prefix, channel])
)

function bytesToHex (bytes) {
  let result = ''
  for (const byte of bytes) result += byte.toString(16).padStart(2, '0')
  return result
}

function hexToBytes (hex, fieldName = 'hex value') {
  if (typeof hex !== 'string' || !/^[0-9a-f]+$/i.test(hex) || hex.length % 2 !== 0) {
    throw new Error(`Invalid ${fieldName}`)
  }
  const result = new Uint8Array(hex.length / 2)
  for (let index = 0; index < result.length; index++) {
    result[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16)
  }
  return result
}

function fixedHexToBytes (hex, byteLength, fieldName) {
  const bytes = hexToBytes(hex, fieldName)
  if (bytes.length !== byteLength) throw new Error(`${fieldName} should be ${byteLength} bytes`)
  return bytes
}

function concatBytes (arrays) {
  const size = arrays.reduce((total, array) => total + array.length, 0)
  const result = new Uint8Array(size)
  let offset = 0
  for (const array of arrays) {
    result.set(array, offset)
    offset += array.length
  }
  return result
}

function encodeText (value, fieldName) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${fieldName} should be a non-empty string`)
  }
  const bytes = textEncoder.encode(value)
  if (bytes.length > MAX_TLV_VALUE_SIZE) throw new Error(`${fieldName} is too big`)
  // TextEncoder replaces lone surrogates. Reject them instead of silently changing
  // the signed/canonical entity value.
  if (fatalTextDecoder.decode(bytes) !== value) throw new Error(`Invalid ${fieldName} UTF-8`)
  return bytes
}

function decodeText (bytes, fieldName) {
  if (bytes.length === 0 || bytes.length > MAX_TLV_VALUE_SIZE) {
    throw new Error(`Invalid ${fieldName} length`)
  }
  try {
    return fatalTextDecoder.decode(bytes)
  } catch {
    throw new Error(`Invalid ${fieldName} UTF-8`)
  }
}

function encodeTlvEntries (entries) {
  return concatBytes(entries.map(([type, value]) => {
    if (!Number.isInteger(type) || type < 0 || type > 255) throw new Error('Invalid TLV type')
    if (!(value instanceof Uint8Array)) throw new TypeError('TLV value should be a Uint8Array')
    if (value.length > MAX_TLV_VALUE_SIZE) throw new Error('TLV value is too big')
    const entry = new Uint8Array(value.length + 2)
    entry[0] = type
    entry[1] = value.length
    entry.set(value, 2)
    return entry
  }))
}

function decodeTlvEntries (bytes) {
  const entries = []
  let offset = 0
  while (offset < bytes.length) {
    if (bytes.length - offset < 2) throw new Error('Truncated TLV header')
    const type = bytes[offset]
    const length = bytes[offset + 1]
    offset += 2
    if (bytes.length - offset < length) throw new Error(`Truncated TLV value for type ${type}`)
    entries.push([type, bytes.slice(offset, offset + length)])
    offset += length
  }
  return entries
}

function onlyValue (values, fieldName) {
  if (values.length > 1) throw new Error(`Duplicate ${fieldName}`)
  return values[0]
}

export function nfileEncode ({ root, relays = [], author, mime, filename }) {
  if (!Array.isArray(relays)) throw new TypeError('relays should be an array')
  const entries = [[0, fixedHexToBytes(root, 32, 'MMR root')]]
  for (const relay of relays) entries.push([1, encodeText(relay, 'relay hint')])
  if (author !== undefined) entries.push([2, fixedHexToBytes(author, 32, 'author hint')])
  if (mime !== undefined) entries.push([3, encodeText(mime, 'MIME')])
  if (filename !== undefined) entries.push([4, encodeText(filename, 'filename')])

  const words = bech32.toWords(encodeTlvEntries(entries))
  return bech32.encode('nfile', words, MAX_ENTITY_SIZE)
}

export function nfileDecode (entity) {
  if (typeof entity !== 'string' || entity !== entity.toLowerCase()) {
    throw new Error('nfile should use canonical lowercase Bech32')
  }
  let decoded
  try {
    decoded = bech32.decode(entity, MAX_ENTITY_SIZE)
  } catch (error) {
    throw new Error(`Invalid nfile: ${error.message}`)
  }
  if (decoded.prefix !== 'nfile') throw new Error('Invalid nfile prefix')

  let bytes
  try {
    bytes = new Uint8Array(bech32.fromWords(decoded.words))
  } catch (error) {
    throw new Error(`Invalid nfile data: ${error.message}`)
  }

  const known = new Map([[0, []], [1, []], [2, []], [3, []], [4, []]])
  for (const [type, value] of decodeTlvEntries(bytes)) known.get(type)?.push(value)

  const rootBytes = onlyValue(known.get(0), 'MMR root')
  if (!rootBytes) throw new Error('Missing MMR root')
  if (rootBytes.length !== 32) throw new Error('MMR root should be 32 bytes')

  const authorBytes = onlyValue(known.get(2), 'author hint')
  if (authorBytes && authorBytes.length !== 32) throw new Error('Author hint should be 32 bytes')
  const mimeBytes = onlyValue(known.get(3), 'MIME')
  const filenameBytes = onlyValue(known.get(4), 'filename')

  const result = {
    root: bytesToHex(rootBytes),
    relays: known.get(1).map(value => decodeText(value, 'relay hint'))
  }
  if (authorBytes) result.author = bytesToHex(authorBytes)
  if (mimeBytes) result.mime = decodeText(mimeBytes, 'MIME')
  if (filenameBytes) result.filename = decodeText(filenameBytes, 'filename')
  return result
}

function isNostrAppDTagSafe (value) {
  return typeof value === 'string' && value.length <= NOSTR_APP_D_TAG_MAX_LENGTH
}

export function appEncode (ref) {
  if (!isNostrAppDTagSafe(ref.dTag)) throw new Error('Invalid deduplication tag')
  const channel = ref.channel ? (prefixByChannel[ref.channel] && ref.channel) : channelByKind[ref.kind]
  if (!channel) throw new Error('Wrong channel')

  // Keep the established app-entity byte ordering exactly for compatibility.
  const groups = [
    [textEncoder.encode(ref.dTag)],
    (ref.relays || []).map(relay => textEncoder.encode(relay)),
    [fixedHexToBytes(ref.pubkey, 32, 'author pubkey')]
  ]
  const entries = []
  groups
    .map((values, type) => [type, values])
    .reverse()
    .forEach(([type, values]) => {
      for (const value of values) entries.push([type, value])
    })
  const entity = `${prefixByChannel[channel]}${bytesToBase62(encodeTlvEntries(entries))}`
  if (entity.length > MAX_ENTITY_SIZE) throw new Error('App entity is too big')
  return entity
}

export function appDecode (entity) {
  if (typeof entity !== 'string' || entity.length > MAX_ENTITY_SIZE) throw new Error('Invalid app entity')
  const prefix = entity.match(/^\+*/)?.[0]
  const channel = channelByPrefix[prefix]
  if (!channel) throw new Error('Invalid channel')

  const values = new Map()
  for (const [type, value] of decodeTlvEntries(base62ToBytes(entity.slice(prefix.length)))) {
    const list = values.get(type) || []
    list.push(value)
    values.set(type, list)
  }
  const dTagBytes = values.get(0)?.[0]
  const pubkeyBytes = values.get(2)?.[0]
  if (!dTagBytes) throw new Error('Missing deduplication tag')
  if (!pubkeyBytes) throw new Error('Missing author pubkey')
  if (pubkeyBytes.length !== 32) throw new Error('Author pubkey should be 32 bytes')
  const dTag = textDecoder.decode(dTagBytes)
  if (!isNostrAppDTagSafe(dTag)) throw new Error('Invalid deduplication tag')

  return {
    dTag,
    pubkey: bytesToHex(pubkeyBytes),
    kind: kindByChannel[channel],
    channel,
    relays: (values.get(1) || []).map(value => textDecoder.decode(value))
  }
}

export function npubEncode (hex) {
  return bech32.encode('npub', bech32.toWords(fixedHexToBytes(hex, 32, 'pubkey')))
}

export function npubDecode (entity) {
  return decodeSimpleEntity(entity, 'npub', 'pubkey')
}

export function nsecEncode (hex) {
  return bech32.encode('nsec', bech32.toWords(fixedHexToBytes(hex, 32, 'secret key')))
}

export function nsecDecode (entity) {
  return decodeSimpleEntity(entity, 'nsec', 'secret key')
}

function decodeSimpleEntity (entity, prefix, fieldName) {
  if (typeof entity !== 'string' || !entity.startsWith(`${prefix}1`)) {
    throw new Error(`Invalid ${prefix} format`)
  }
  try {
    const decoded = bech32.decode(entity)
    if (decoded.prefix !== prefix) throw new Error(`Invalid ${prefix} prefix`)
    const bytes = new Uint8Array(bech32.fromWords(decoded.words))
    if (bytes.length !== 32) throw new Error(`Invalid ${fieldName} length`)
    return bytesToHex(bytes)
  } catch (error) {
    throw new Error(`Failed to decode ${prefix}: ${error.message}`)
  }
}
