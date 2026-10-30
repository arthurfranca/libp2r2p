import { schnorr, secp256k1 } from '@noble/curves/secp256k1.js'

import { bytesToHex, hexToBytes } from '../../base16/index.js'
import { ValidationError } from '../../error/index.js'
import { finalizeEvent } from '../../event/js/index.js'
import { nsecDecode, nsecEncode, npubDecode, npubEncode } from '../../nip19/index.js'

const HEX_SECKEY_REGEX = /^[0-9a-f]{64}$/i

export function validateSecretKey (secretKey) {
  if (!(secretKey instanceof Uint8Array) || secretKey.length !== 32 || !secp256k1.utils.isValidSecretKey(secretKey)) {
    throw new ValidationError('INVALID_SECRET_KEY')
  }
}

export function generateSecretKey () {
  return schnorr.utils.randomSecretKey()
}

export function getPublicKey (secretKey) {
  validateSecretKey(secretKey)
  return bytesToHex(schnorr.getPublicKey(secretKey))
}

export function keypairFromSecretKey (secretKey, pubkey) {
  return {
    secretKey,
    seckey: bytesToHex(secretKey),
    pubkey,
    nsec: nsecEncode(bytesToHex(secretKey)),
    npub: npubEncode(pubkey)
  }
}

export function generateKeypair () {
  const secretKey = generateSecretKey()
  return keypairFromSecretKey(secretKey, getPublicKey(secretKey))
}

export function decodeSecretKey (raw) {
  if (HEX_SECKEY_REGEX.test(raw)) {
    return hexToBytes(raw.toLowerCase())
  }
  try {
    return hexToBytes(nsecDecode(raw))
  } catch (cause) {
    throw new ValidationError('NOT_A_SECRET_KEY', { cause })
  }
}

export function keypairFromSeckey (raw) {
  const secretKey = decodeSecretKey(raw)
  return keypairFromSecretKey(secretKey, getPublicKey(secretKey))
}

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

export function relayListTags ({ writeRelays = [], readRelays = [] }) {
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
  return tags
}

export function signRelayListEvent ({ secretKey, writeRelays = [], readRelays = [] }) {
  return finalizeEvent({
    kind: 10002,
    created_at: Math.floor(Date.now() / 1000),
    tags: relayListTags({ writeRelays, readRelays }),
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
