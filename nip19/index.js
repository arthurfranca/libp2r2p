import { bech32 } from '@scure/base'
import { BASE62_ALPHABET, base62ToBytes, bytesToBase62 } from '../base62/index.js'
import { ValidationError } from '../error/index.js'

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

function invalid (code, message, cause) {
  return new ValidationError(code, { message, cause })
}

function bytesToHex (bytes) {
  let result = ''
  for (const byte of bytes) result += byte.toString(16).padStart(2, '0')
  return result
}

function hexToBytes (hex, fieldName = 'hex value') {
  if (typeof hex !== 'string' || !/^[0-9a-f]+$/i.test(hex) || hex.length % 2 !== 0) {
    throw invalid('INVALID_NIP19_HEX', `Invalid ${fieldName}`)
  }
  const result = new Uint8Array(hex.length / 2)
  for (let index = 0; index < result.length; index++) {
    result[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16)
  }
  return result
}

function fixedHexToBytes (hex, byteLength, fieldName) {
  const bytes = hexToBytes(hex, fieldName)
  if (bytes.length !== byteLength) throw invalid('INVALID_NIP19_FIELD_LENGTH', `${fieldName} should be ${byteLength} bytes`)
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

function encodeText (value, fieldName, { allowEmpty = false } = {}) {
  if (typeof value !== 'string' || (!allowEmpty && value.length === 0)) {
    throw invalid('INVALID_NIP19_TEXT_FIELD', `${fieldName} should be ${allowEmpty ? 'a string' : 'a non-empty string'}`)
  }
  const bytes = textEncoder.encode(value)
  if (bytes.length > MAX_TLV_VALUE_SIZE) throw invalid('NIP19_FIELD_TOO_LARGE', `${fieldName} is too big`)
  // TextEncoder replaces lone surrogates. Reject them instead of silently changing
  // the signed/canonical entity value.
  if (fatalTextDecoder.decode(bytes) !== value) throw invalid('INVALID_NIP19_UTF8', `Invalid ${fieldName} UTF-8`)
  return bytes
}

function decodeText (bytes, fieldName, { allowEmpty = false } = {}) {
  if ((!allowEmpty && bytes.length === 0) || bytes.length > MAX_TLV_VALUE_SIZE) {
    throw invalid('INVALID_NIP19_FIELD_LENGTH', `Invalid ${fieldName} length`)
  }
  try {
    return fatalTextDecoder.decode(bytes)
  } catch (cause) {
    throw invalid('INVALID_NIP19_UTF8', `Invalid ${fieldName} UTF-8`, cause)
  }
}

function encodeTlvEntries (entries) {
  return concatBytes(entries.map(([type, value]) => {
    if (!Number.isInteger(type) || type < 0 || type > 255) throw invalid('INVALID_TLV_TYPE', 'Invalid TLV type')
    if (!(value instanceof Uint8Array)) throw invalid('INVALID_TLV_VALUE', 'TLV value should be a Uint8Array')
    if (value.length > MAX_TLV_VALUE_SIZE) throw invalid('TLV_VALUE_TOO_LARGE', 'TLV value is too big')
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
    if (bytes.length - offset < 2) throw invalid('TRUNCATED_TLV_HEADER', 'Truncated TLV header')
    const type = bytes[offset]
    const length = bytes[offset + 1]
    offset += 2
    if (bytes.length - offset < length) throw invalid('TRUNCATED_TLV_VALUE', `Truncated TLV value for type ${type}`)
    entries.push([type, bytes.slice(offset, offset + length)])
    offset += length
  }
  return entries
}

function onlyValue (values, fieldName) {
  if (values.length > 1) throw invalid('DUPLICATE_NIP19_FIELD', `Duplicate ${fieldName}`)
  return values[0]
}

function encodeBech32Tlv (prefix, entries) {
  return bech32.encode(prefix, bech32.toWords(encodeTlvEntries(entries)), MAX_ENTITY_SIZE)
}

function encodeStandardPointer (prefix, entries) {
  return encodeBech32Tlv(prefix, entries.slice().sort(([left], [right]) => right - left))
}

function decodeBech32Bytes (entity, prefix) {
  if (typeof entity !== 'string' || entity !== entity.toLowerCase() || !entity.startsWith(`${prefix}1`)) {
    throw invalid('NON_CANONICAL_NIP19_ENTITY', `${prefix} should use canonical lowercase Bech32`)
  }
  let decoded
  try {
    decoded = bech32.decode(entity, MAX_ENTITY_SIZE)
  } catch (error) {
    throw invalid('INVALID_NIP19_ENTITY', `Invalid ${prefix}: ${error.message}`, error)
  }
  if (decoded.prefix !== prefix) throw invalid('INVALID_NIP19_PREFIX', `Invalid ${prefix} prefix`)
  try {
    return new Uint8Array(bech32.fromWords(decoded.words))
  } catch (error) {
    throw invalid('INVALID_NIP19_DATA', `Invalid ${prefix} data: ${error.message}`, error)
  }
}

function decodeKnownTlv (entity, prefix, types) {
  const known = new Map(types.map(type => [type, []]))
  for (const [type, value] of decodeTlvEntries(decodeBech32Bytes(entity, prefix))) known.get(type)?.push(value)
  return known
}

function encodeUint32 (value, fieldName = 'kind') {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) throw invalid('INVALID_NIP19_UINT32', `Invalid ${fieldName}`)
  const bytes = new Uint8Array(4)
  new DataView(bytes.buffer).setUint32(0, value, false)
  return bytes
}

function decodeUint32 (bytes, fieldName = 'kind') {
  if (!bytes || bytes.length !== 4) throw invalid('INVALID_NIP19_UINT32', `${fieldName} should be 4 bytes`)
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, false)
}

function relayEntries (relays) {
  if (!Array.isArray(relays)) throw invalid('INVALID_RELAY_HINTS', 'relays should be an array')
  return relays.map(relay => [1, encodeText(relay, 'relay hint')])
}

function decodeRelayHints (values) {
  return values.map(value => decodeText(value, 'relay hint'))
}

export function nfileEncode ({ root, relays = [], author, mime, filename }) {
  if (!Array.isArray(relays)) throw invalid('INVALID_RELAY_HINTS', 'relays should be an array')
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
    throw invalid('NON_CANONICAL_NFILE', 'nfile should use canonical lowercase Bech32')
  }
  let decoded
  try {
    decoded = bech32.decode(entity, MAX_ENTITY_SIZE)
  } catch (error) {
    throw invalid('INVALID_NFILE', `Invalid nfile: ${error.message}`, error)
  }
  if (decoded.prefix !== 'nfile') throw invalid('INVALID_NFILE_PREFIX', 'Invalid nfile prefix')

  let bytes
  try {
    bytes = new Uint8Array(bech32.fromWords(decoded.words))
  } catch (error) {
    throw invalid('INVALID_NFILE_DATA', `Invalid nfile data: ${error.message}`, error)
  }

  const known = new Map([[0, []], [1, []], [2, []], [3, []], [4, []]])
  for (const [type, value] of decodeTlvEntries(bytes)) known.get(type)?.push(value)

  const rootBytes = onlyValue(known.get(0), 'MMR root')
  if (!rootBytes) throw invalid('MISSING_NFILE_ROOT', 'Missing MMR root')
  if (rootBytes.length !== 32) throw invalid('INVALID_NFILE_ROOT', 'MMR root should be 32 bytes')

  const authorBytes = onlyValue(known.get(2), 'author hint')
  if (authorBytes && authorBytes.length !== 32) throw invalid('INVALID_NFILE_AUTHOR', 'Author hint should be 32 bytes')
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

export function noteEncode (eventId) {
  return bech32.encode('note', bech32.toWords(fixedHexToBytes(eventId, 32, 'event ID')), MAX_ENTITY_SIZE)
}

export function noteDecode (entity) {
  const bytes = decodeBech32Bytes(entity, 'note')
  if (bytes.length !== 32) throw invalid('INVALID_NOTE_EVENT_ID', 'event ID should be 32 bytes')
  return bytesToHex(bytes)
}

export function nprofileEncode ({ pubkey, relays = [] }) {
  return encodeStandardPointer('nprofile', [
    [0, fixedHexToBytes(pubkey, 32, 'profile pubkey')],
    ...relayEntries(relays)
  ])
}

export function nprofileDecode (entity) {
  const known = decodeKnownTlv(entity, 'nprofile', [0, 1])
  const pubkey = onlyValue(known.get(0), 'profile pubkey')
  if (!pubkey || pubkey.length !== 32) throw invalid('INVALID_NPROFILE_PUBKEY', 'profile pubkey should be 32 bytes')
  return { pubkey: bytesToHex(pubkey), relays: decodeRelayHints(known.get(1)) }
}

export function neventEncode ({ id, relays = [], author, kind }) {
  const entries = [[0, fixedHexToBytes(id, 32, 'event ID')], ...relayEntries(relays)]
  if (author !== undefined) entries.push([2, fixedHexToBytes(author, 32, 'event author')])
  if (kind !== undefined) entries.push([3, encodeUint32(kind)])
  return encodeStandardPointer('nevent', entries)
}

export function neventDecode (entity) {
  const known = decodeKnownTlv(entity, 'nevent', [0, 1, 2, 3])
  const id = onlyValue(known.get(0), 'event ID')
  const author = onlyValue(known.get(2), 'event author')
  const kind = onlyValue(known.get(3), 'event kind')
  if (!id || id.length !== 32) throw invalid('INVALID_NEVENT_ID', 'event ID should be 32 bytes')
  if (author && author.length !== 32) throw invalid('INVALID_NEVENT_AUTHOR', 'event author should be 32 bytes')
  const result = { id: bytesToHex(id), relays: decodeRelayHints(known.get(1)) }
  if (author) result.author = bytesToHex(author)
  if (kind) result.kind = decodeUint32(kind, 'event kind')
  return result
}

export function naddrEncode ({ identifier, pubkey, kind, relays = [] }) {
  return encodeStandardPointer('naddr', [
    [0, encodeText(identifier, 'identifier', { allowEmpty: true })],
    ...relayEntries(relays),
    [2, fixedHexToBytes(pubkey, 32, 'address author')],
    [3, encodeUint32(kind)]
  ])
}

export function naddrDecode (entity) {
  const known = decodeKnownTlv(entity, 'naddr', [0, 1, 2, 3])
  const identifier = onlyValue(known.get(0), 'identifier')
  const pubkey = onlyValue(known.get(2), 'address author')
  const kind = onlyValue(known.get(3), 'address kind')
  if (!identifier) throw invalid('MISSING_NADDR_IDENTIFIER', 'Missing identifier')
  if (!pubkey || pubkey.length !== 32) throw invalid('INVALID_NADDR_AUTHOR', 'address author should be 32 bytes')
  return {
    identifier: decodeText(identifier, 'identifier', { allowEmpty: true }),
    pubkey: bytesToHex(pubkey),
    kind: decodeUint32(kind, 'address kind'),
    relays: decodeRelayHints(known.get(1))
  }
}

export function nrelayEncode (relay) {
  return encodeStandardPointer('nrelay', [[0, encodeText(relay, 'relay URL')]])
}

export function nrelayDecode (entity) {
  const known = decodeKnownTlv(entity, 'nrelay', [0])
  const relay = onlyValue(known.get(0), 'relay URL')
  if (!relay) throw invalid('MISSING_NRELAY_URL', 'Missing relay URL')
  return decodeText(relay, 'relay URL')
}

function isNostrAppDTagSafe (value) {
  return typeof value === 'string' && value.length <= NOSTR_APP_D_TAG_MAX_LENGTH
}

export function appEncode (ref) {
  if (!isNostrAppDTagSafe(ref?.dTag)) throw invalid('INVALID_APP_D_TAG', 'Invalid deduplication tag')
  const channel = ref.channel ? (prefixByChannel[ref.channel] && ref.channel) : channelByKind[ref.kind]
  if (!channel) throw invalid('INVALID_APP_CHANNEL', 'Wrong channel')

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
  if (entity.length > MAX_ENTITY_SIZE) throw invalid('APP_ENTITY_TOO_LARGE', 'App entity is too big')
  return entity
}

export function appDecode (entity) {
  if (typeof entity !== 'string' || entity.length > MAX_ENTITY_SIZE) throw invalid('INVALID_APP_ENTITY', 'Invalid app entity')
  const prefix = entity.match(/^\+*/)?.[0]
  const channel = channelByPrefix[prefix]
  if (!channel) throw invalid('INVALID_APP_CHANNEL', 'Invalid channel')

  const values = new Map()
  for (const [type, value] of decodeTlvEntries(base62ToBytes(entity.slice(prefix.length)))) {
    const list = values.get(type) || []
    list.push(value)
    values.set(type, list)
  }
  const dTagBytes = values.get(0)?.[0]
  const pubkeyBytes = values.get(2)?.[0]
  if (!dTagBytes) throw invalid('MISSING_APP_D_TAG', 'Missing deduplication tag')
  if (!pubkeyBytes) throw invalid('MISSING_APP_AUTHOR', 'Missing author pubkey')
  if (pubkeyBytes.length !== 32) throw invalid('INVALID_APP_AUTHOR', 'Author pubkey should be 32 bytes')
  const dTag = textDecoder.decode(dTagBytes)
  if (!isNostrAppDTagSafe(dTag)) throw invalid('INVALID_APP_D_TAG', 'Invalid deduplication tag')

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
  try {
    const bytes = decodeBech32Bytes(entity, prefix)
    if (bytes.length !== 32) throw invalid('INVALID_NIP19_FIELD_LENGTH', `Invalid ${fieldName} length`)
    return bytesToHex(bytes)
  } catch (error) {
    if (error instanceof ValidationError && error.code === 'INVALID_NIP19_FIELD_LENGTH') throw error
    throw invalid('INVALID_NIP19_ENTITY', `Failed to decode ${prefix}: ${error.message}`, error)
  }
}
