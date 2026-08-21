import { ValidationError } from '../error/index.js'
import {
  compactNip05Raw,
  nip05FromLocalDomain
} from '../nip05/helpers/nip05-identifier.js'
import {
  decodeUserReference,
  encodeUserReference
} from '../nip27/helpers/user-reference.js'
import {
  NAPP_ENTITY_REGEX,
  appDecode
} from '../nip19/index.js'

export const APP_URL_MIN_ENTITY_BODY_LENGTH = 48

const APP_NAME_MAX_LENGTH = 260
const CHANNEL_BY_PREFIX = { '+': 'main', '++': 'next', '+++': 'draft' }
const PREFIX_BY_CHANNEL = { main: '+', next: '++', draft: '+++' }
const KIND_BY_CHANNEL = { main: 35128, next: 35129, draft: 35130 }
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/

function safeDecode (value) {
  try {
    return decodeURIComponent(value)
  } catch {
    return null
  }
}

function isValidAppName (appName) {
  return typeof appName === 'string' &&
    appName.length > 0 &&
    appName.length <= APP_NAME_MAX_LENGTH &&
    !appName.startsWith('+') &&
    !appName.includes('/') &&
    !CONTROL_CHARS.test(appName)
}

// Decodes a raw (still percent-encoded) first path segment such as
// `+apps` or `+caf%C3%A9@bob@example.com` or `+3swFhu...`.
// Returns:
// - `{ type: 'entity', entity }` for NIP-19 app entities;
// - `{ type: 'named', prefix, channel, appName, user }` for named URLs
//   (`user` is null when no user part is present or it is invalid);
// - `null` when the segment is not a valid app URL.
export function decodeAppUrl (segment) {
  if (typeof segment !== 'string' || !segment) return null
  const prefixMatch = segment.match(/^\+{1,3}/)
  if (!prefixMatch) return null
  const prefix = prefixMatch[0]

  if (NAPP_ENTITY_REGEX.test(segment)) {
    try {
      appDecode(segment)
    } catch {
      return null
    }
    return { type: 'entity', entity: segment }
  }

  const remainder = segment.slice(prefix.length)
  if (!remainder) return null
  const parts = remainder.split('@')
  let appName
  let user = null

  if (parts.length === 1) {
    appName = safeDecode(parts[0])
  } else if (parts.length === 2) {
    const tail = safeDecode(parts[1])
    appName = safeDecode(parts[0])
    user = tail === null ? null : decodeUserReference(tail)
  } else {
    const local = safeDecode(parts[parts.length - 2])
    const domain = safeDecode(parts[parts.length - 1])
    const nip05 = nip05FromLocalDomain(local, domain)
    if (nip05) {
      user = { kind: 'nip05', ...nip05, raw: compactNip05Raw(local, domain) }
      appName = parts.slice(0, -2).map(safeDecode).join('@')
    } else {
      appName = safeDecode(remainder)
    }
  }

  if (appName === null || !isValidAppName(appName)) return null
  if (!user && appName.length >= APP_URL_MIN_ENTITY_BODY_LENGTH) return null

  return {
    type: 'named',
    prefix,
    channel: CHANNEL_BY_PREFIX[prefix],
    appName,
    user
  }
}

// Encodes a named app URL segment. `user` is a decoded user reference:
// NIP-05 (`bob@example.com`, `_@example.com`, `example.com` or the custom
// `bob.xyz.example.com` form), npub, nprofile or hex pubkey.
export function encodeAppUrl ({ appName, channel = 'main', user }) {
  if (!isValidAppName(appName)) {
    throw new ValidationError('INVALID_APP_URL_NAME', { message: 'Invalid app URL name' })
  }
  const prefix = PREFIX_BY_CHANNEL[channel]
  if (!prefix) {
    throw new ValidationError('INVALID_APP_URL_CHANNEL', { message: 'Invalid app URL channel' })
  }
  const userRef = decodeUserReference(user)
  if (!userRef) {
    throw new ValidationError('INVALID_APP_URL_USER', { message: 'Invalid app URL user' })
  }

  let encodedAppName = encodeURIComponent(appName)
  let userText
  if (userRef.kind === 'nip05') {
    // Keep `@` raw inside the app name so the verbose NIP-05 form stays
    // readable (`+my@app@bob@example.com`).
    encodedAppName = encodedAppName.replace(/%40/g, '@')
    const appNameHasAt = appName.includes('@')
    if (userRef.local === '_') {
      userText = userRef.domain.split('.').length === 2 && !appNameHasAt
        ? userRef.domain
        : `_@${userRef.domain}`
    } else {
      // Prefer the shorter single-`@` custom NIP-05 form (`bob.example.com`)
      // unless the app name itself contains `@`, which forces the verbose form.
      userText = appNameHasAt
        ? `${userRef.local}@${userRef.domain}`
        : `${userRef.local}.${userRef.domain}`
    }
  } else {
    userText = encodeUserReference(userRef)
  }

  return `${prefix}${encodedAppName}@${userText}`
}

// Convenience for callers that need the manifest kind for a decoded channel.
export function appUrlKindByChannel (channel) {
  return KIND_BY_CHANNEL[channel] || null
}
