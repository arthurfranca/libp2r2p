import { ValidationError } from '../../error/index.js'
import { npubDecode, nprofileDecode } from '../../nip19/index.js'
import { compactNip05Raw, decodeNip05Identifier } from '../../nip05/helpers/nip05-identifier.js'

const HEX_PUBKEY = /^[0-9a-f]{64}$/

// Strips the optional mention prefixes accepted by NIP-21 (`nostr:`) and the
// social `@` handle marker. Either prefix may appear first.
function stripReferencePrefix (value) {
  let text = value.trim()
  let changed = true
  while (changed && text) {
    changed = false
    if (/^nostr:/i.test(text)) {
      text = text.slice(6)
      changed = true
    }
    if (text.startsWith('@')) {
      text = text.slice(1)
      changed = true
    }
  }
  return text
}

// Decodes a user reference without performing any network lookup.
// Returns `{ type: 'pubkey', pubkey, relays, raw }` for npub/nprofile/hex or
// `{ type: 'nip05', local, domain, raw }` for NIP-05 (standard or extended),
// where `raw` is always the most compact canonical spelling. Throws
// `ValidationError('INVALID_USER_REFERENCE')` when the value cannot be
// decoded; use `tryDecodeUserReference` when a null result is preferred.
export function decodeUserReference (value) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new ValidationError('INVALID_USER_REFERENCE', { message: 'USER_REFERENCE_SHOULD_BE_A_NON_EMPTY_STRING' })
  }
  const text = stripReferencePrefix(value)
  if (!text) {
    throw new ValidationError('INVALID_USER_REFERENCE', { message: 'EMPTY_USER_REFERENCE' })
  }

  if (HEX_PUBKEY.test(text)) {
    const raw = text.toLowerCase()
    return { type: 'pubkey', pubkey: raw, relays: [], raw }
  }

  if (text.toLowerCase().startsWith('npub1')) {
    try {
      const raw = text.toLowerCase()
      return { type: 'pubkey', pubkey: npubDecode(raw), relays: [], raw }
    } catch (cause) {
      throw new ValidationError('INVALID_USER_REFERENCE', { message: 'INVALID_NPUB', cause })
    }
  }

  if (text.toLowerCase().startsWith('nprofile1')) {
    try {
      const raw = text.toLowerCase()
      const { pubkey, relays } = nprofileDecode(raw)
      return { type: 'pubkey', pubkey, relays, raw }
    } catch (cause) {
      throw new ValidationError('INVALID_USER_REFERENCE', { message: 'INVALID_NPROFILE', cause })
    }
  }

  let nip05
  try {
    nip05 = decodeNip05Identifier(text)
  } catch (cause) {
    throw new ValidationError('INVALID_USER_REFERENCE', { message: 'INVALID_NIP05', cause })
  }
  const raw = compactNip05Raw(nip05.local, nip05.domain)
  return { type: 'nip05', ...nip05, raw }
}

// Non-throwing variant of `decodeUserReference`: returns the decoded
// reference or `null` when the value is not a valid user reference.
export function tryDecodeUserReference (value) {
  try {
    return decodeUserReference(value)
  } catch (error) {
    if (error instanceof ValidationError) return null
    throw error
  }
}

// Returns the canonical compact spelling for a user reference, either as a
// string or as a decoded reference object.
export function encodeUserReference (value) {
  const ref = typeof value === 'string'
    ? decodeUserReference(value)
    : value && typeof value === 'object' &&
        (value.type === 'pubkey' || value.type === 'nip05')
      ? value
      : null
  if (!ref) {
    throw new ValidationError('INVALID_USER_REFERENCE', { message: 'Invalid user reference' })
  }
  if (ref.type === 'pubkey') return ref.raw ?? ref.pubkey
  return ref.raw ?? compactNip05Raw(ref.local, ref.domain)
}
