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
// Returns `{ kind: 'pubkey', pubkey, relays, raw }` for npub/nprofile/hex or
// `{ kind: 'nip05', local, domain, raw }` for NIP-05 (standard or extended),
// where `raw` is always the most compact canonical spelling.
export function decodeUserReference (value) {
  if (typeof value !== 'string') return null
  const text = stripReferencePrefix(value)
  if (!text) return null

  if (HEX_PUBKEY.test(text)) {
    const raw = text.toLowerCase()
    return { kind: 'pubkey', pubkey: raw, relays: [], raw }
  }

  if (text.toLowerCase().startsWith('npub1')) {
    try {
      const raw = text.toLowerCase()
      return { kind: 'pubkey', pubkey: npubDecode(raw), relays: [], raw }
    } catch {
      return null
    }
  }

  if (text.toLowerCase().startsWith('nprofile1')) {
    try {
      const raw = text.toLowerCase()
      const { pubkey, relays } = nprofileDecode(raw)
      return { kind: 'pubkey', pubkey, relays, raw }
    } catch {
      return null
    }
  }

  const nip05 = decodeNip05Identifier(text)
  if (!nip05) return null
  const raw = compactNip05Raw(nip05.local, nip05.domain)
  return { kind: 'nip05', ...nip05, raw }
}

// Returns the canonical compact spelling for a user reference, either as a
// string or as a decoded reference object.
export function encodeUserReference (value) {
  const ref = typeof value === 'string'
    ? decodeUserReference(value)
    : value && typeof value === 'object' &&
        (value.kind === 'pubkey' || value.kind === 'nip05')
      ? value
      : null
  if (!ref) {
    throw new ValidationError('INVALID_USER_REFERENCE', { message: 'Invalid user reference' })
  }
  if (ref.kind === 'pubkey') return ref.raw ?? ref.pubkey
  return ref.raw ?? compactNip05Raw(ref.local, ref.domain)
}
