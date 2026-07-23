import { schnorr, secp256k1 } from '@noble/curves/secp256k1.js'

import { bytesToHex, hexToBytes } from '../base16/index.js'
import { ValidationError } from '../error/index.js'
import { finalizeEvent } from '../event/index.js'
import { nsecDecode, nsecEncode, npubDecode, npubEncode } from '../nip19/index.js'

const HEX_SECKEY_REGEX = /^[0-9a-f]{64}$/i

export function generateSecretKey () {
  return schnorr.utils.randomSecretKey()
}

export function getPublicKey (secretKey) {
  if (!(secretKey instanceof Uint8Array) || secretKey.length !== 32 || !secp256k1.utils.isValidSecretKey(secretKey)) {
    throw new ValidationError('INVALID_SECRET_KEY')
  }
  return bytesToHex(schnorr.getPublicKey(secretKey))
}

export function generateKeypair () {
  const secretKey = generateSecretKey()
  const pubkey = getPublicKey(secretKey)
  return {
    secretKey,
    seckey: bytesToHex(secretKey),
    pubkey,
    nsec: nsecEncode(bytesToHex(secretKey)),
    npub: npubEncode(pubkey)
  }
}

// Accepts either an `nsec1...` bech32 string or a 64-char hex secret key.
// Returns both encodings so callers can store the hex form and display the
// nsec form without re-decoding.
export function keypairFromSeckey (raw) {
  let secretKey
  if (HEX_SECKEY_REGEX.test(raw)) {
    secretKey = hexToBytes(raw.toLowerCase())
  } else {
    try {
      secretKey = hexToBytes(nsecDecode(raw))
    } catch (cause) {
      throw new ValidationError('NOT_A_SECRET_KEY', { cause })
    }
  }
  const pubkey = getPublicKey(secretKey)
  return {
    secretKey,
    seckey: bytesToHex(secretKey),
    pubkey,
    nsec: nsecEncode(bytesToHex(secretKey)),
    npub: npubEncode(pubkey)
  }
}

// npub-only: hex pubkeys are not accepted for read-only imports because
// bech32 includes a checksum that protects against user-typo imports of a
// pubkey they cannot sign for.
export function pubkeyFromNpub (npub) {
  try {
    return npubDecode(npub)
  } catch (cause) {
    throw new ValidationError('NOT_AN_NPUB', { cause })
  }
}

export function nsecFromHex (hex) {
  return nsecEncode(hex)
}

export function npubFromPubkey (pubkey) {
  return npubEncode(pubkey)
}

function cleanProfileValue (value) {
  return String(value ?? '').trim()
}

function profileContentFromEvent (event) {
  if (!event?.content) return {}
  try {
    const parsed = JSON.parse(event.content)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed
  } catch {}
  return {}
}

export function profileEventTemplate ({ name = '', picture = '', profileEvent = null } = {}) {
  const cleanName = cleanProfileValue(name)
  const cleanPicture = cleanProfileValue(picture)
  const content = profileContentFromEvent(profileEvent)

  if (cleanName) content.name = cleanName
  else delete content.name
  if (cleanPicture) content.picture = cleanPicture
  else delete content.picture

  const tags = Array.isArray(profileEvent?.tags)
    ? profileEvent.tags
      .filter(t => Array.isArray(t) && t[0] !== 'name' && t[0] !== 'picture')
      .map(t => t.slice())
    : []
  if (cleanName) tags.push(['name', cleanName])
  if (cleanPicture) tags.push(['picture', cleanPicture])

  return {
    kind: 0,
    created_at: Math.floor(Date.now() / 1000),
    tags,
    content: JSON.stringify(content)
  }
}

export function signProfileEvent ({ secretKey, name = '', picture, profileEvent = null }) {
  return finalizeEvent(profileEventTemplate({ name, picture, profileEvent }), secretKey)
}

// Signs a NIP-65 kind:10002 relay-list event. Each URL present in both
// `writeRelays` and `readRelays` is emitted as a bare `['r', url]` tag;
// otherwise it gets the explicit `'read'` or `'write'` marker.
export function signRelayListEvent ({ secretKey, writeRelays = [], readRelays = [] }) {
  const write = new Set(writeRelays)
  const read = new Set(readRelays)
  const all = new Set([...write, ...read])
  const tags = []
  for (const url of all) {
    const isWrite = write.has(url)
    const isRead = read.has(url)
    if (isWrite && isRead) tags.push(['r', url])
    else if (isWrite) tags.push(['r', url, 'write'])
    else tags.push(['r', url, 'read'])
  }
  return finalizeEvent({
    kind: 10002,
    created_at: Math.floor(Date.now() / 1000),
    tags,
    content: ''
  }, secretKey)
}

export function parseProfileEvent (event) {
  if (!event || event.kind !== 0) return { name: '', picture: '' }
  let parsed = {}
  try { parsed = JSON.parse(event.content) } catch { parsed = {} }
  const fromTag = name => event.tags.find(t => t[0] === name)?.[1]
  return {
    name: (fromTag('name') || parsed.name || parsed.display_name || '').trim(),
    about: (parsed.about || '').trim(),
    picture: (fromTag('picture') || parsed.picture || '').trim()
  }
}
