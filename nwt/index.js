import { base64UrlToBytes, bytesToBase64Url } from '../base64/index.js'
import { verifyEvent } from '../event/index.js'
import { NWT } from '../kind/index.js'

const textDecoder = new TextDecoder('utf-8', { fatal: true })
const textEncoder = new TextEncoder()
const BASE64URL = /^[A-Za-z0-9_-]+$/
const REGISTERED_CLAIMS = new Set(['iss', 'sub', 'aud', 'iat', 'exp', 'nbf'])
const SINGLE_CLAIMS = new Set(['iss', 'sub', 'iat', 'exp', 'nbf'])
const TIMESTAMP_CLAIMS = new Set(['iat', 'exp', 'nbf'])
const MAX_CLAIMS = 512

function fail (code) {
  throw new Error(code)
}

function cloneTags (tags) {
  return tags.map(tag => tag.slice())
}

function equalTags (left, right) {
  return left.length === right.length && left.every((tag, index) => {
    const other = right[index]
    return tag.length === other.length && tag.every((value, valueIndex) => value === other[valueIndex])
  })
}

function normalizeTimestamp (value, code) {
  if (!Number.isSafeInteger(value) || value < 0) fail(code)
  return value
}

function parseTimestamp (value, code) {
  if (!/^(?:0|[1-9][0-9]*)$/.test(value)) fail(code)
  return normalizeTimestamp(Number(value), code)
}

function normalizeStringClaim (value, code) {
  if (typeof value !== 'string' || value.length === 0) fail(code)
  return value
}

function normalizeAudience (audience, { required = false } = {}) {
  if (audience === undefined) {
    if (required) fail('INVALID_AUDIENCE')
    return []
  }
  const values = typeof audience === 'string' ? [audience] : audience
  if (!Array.isArray(values) || (required && values.length === 0)) fail('INVALID_AUDIENCE')
  return values.map(value => normalizeStringClaim(value, 'INVALID_AUDIENCE'))
}

function normalizeExtraClaims (claims) {
  if (claims === undefined) return []
  if (!Array.isArray(claims)) fail('INVALID_CLAIMS')
  return claims.map(tag => {
    if (!Array.isArray(tag) || tag.length < 2 || tag.some(value => typeof value !== 'string')) fail('INVALID_CLAIM')
    if (tag[0].length === 0 || REGISTERED_CLAIMS.has(tag[0])) fail('INVALID_CUSTOM_CLAIM')
    return tag.slice()
  })
}

function parseClaims (event) {
  if (event.tags.length > MAX_CLAIMS) fail('TOO_MANY_CLAIMS')

  const single = new Map()
  const audience = []
  const claims = []

  for (const tag of event.tags) {
    if (tag.length < 2 || tag[0].length === 0) fail('INVALID_CLAIM')
    const name = tag[0]

    if (!REGISTERED_CLAIMS.has(name)) {
      claims.push(tag.slice())
      continue
    }
    if (tag.length !== 2 || tag[1].length === 0) fail('INVALID_REGISTERED_CLAIM')
    if (name === 'aud') {
      audience.push(tag[1])
      continue
    }
    if (SINGLE_CLAIMS.has(name) && single.has(name)) fail('DUPLICATE_SINGLE_CLAIM')
    single.set(name, TIMESTAMP_CLAIMS.has(name)
      ? parseTimestamp(tag[1], `INVALID_${name.toUpperCase()}_CLAIM`)
      : tag[1])
  }

  const expiration = single.get('exp') ?? null
  const notBefore = single.get('nbf') ?? null
  if (expiration !== null && notBefore !== null && notBefore > expiration) fail('INVALID_TIME_WINDOW')

  return {
    event,
    id: event.id,
    signer: event.pubkey,
    issuer: single.get('iss') ?? event.pubkey,
    subject: single.get('sub') ?? event.pubkey,
    audience,
    issuedAt: single.get('iat') ?? event.created_at,
    expiration,
    notBefore,
    claims,
    content: event.content
  }
}

function parseVerifiedEvent (event) {
  if (!verifyEvent(event)) fail('INVALID_NWT_EVENT')
  if (event.kind !== NWT) fail('INVALID_NWT_KIND')
  return parseClaims(event)
}

function wireEvent (event) {
  return {
    id: event.id,
    pubkey: event.pubkey,
    created_at: event.created_at,
    kind: event.kind,
    tags: cloneTags(event.tags),
    content: event.content,
    sig: event.sig
  }
}

export async function createToken ({
  signEvent,
  issuer,
  subject,
  audience,
  issuedAt,
  expiration,
  notBefore,
  claims,
  content = '',
  createdAt = Math.floor(Date.now() / 1000)
} = {}) {
  if (typeof signEvent !== 'function') throw new TypeError('SIGN_EVENT_SHOULD_BE_A_FUNCTION')
  if (typeof content !== 'string') fail('INVALID_CONTENT')

  const tags = []
  if (issuer !== undefined) tags.push(['iss', normalizeStringClaim(issuer, 'INVALID_ISSUER')])
  if (subject !== undefined) tags.push(['sub', normalizeStringClaim(subject, 'INVALID_SUBJECT')])
  for (const value of normalizeAudience(audience)) tags.push(['aud', value])
  if (issuedAt !== undefined) tags.push(['iat', String(normalizeTimestamp(issuedAt, 'INVALID_ISSUED_AT'))])
  if (expiration !== undefined) tags.push(['exp', String(normalizeTimestamp(expiration, 'INVALID_EXPIRATION'))])
  if (notBefore !== undefined) tags.push(['nbf', String(normalizeTimestamp(notBefore, 'INVALID_NOT_BEFORE'))])
  tags.push(...normalizeExtraClaims(claims))
  if (tags.length > MAX_CLAIMS) fail('TOO_MANY_CLAIMS')

  const expected = {
    kind: NWT,
    created_at: normalizeTimestamp(createdAt, 'INVALID_CREATED_AT'),
    tags,
    content
  }
  if (expiration !== undefined && notBefore !== undefined && notBefore > expiration) fail('INVALID_TIME_WINDOW')

  const event = await signEvent({ ...expected, tags: cloneTags(expected.tags) })
  parseVerifiedEvent(event)
  if (
    event.kind !== expected.kind ||
    event.created_at !== expected.created_at ||
    event.content !== expected.content ||
    !equalTags(event.tags, expected.tags)
  ) fail('SIGNED_NWT_EVENT_WAS_CHANGED')
  return event
}

export function encodeToken (event, { includeAuthorizationScheme = false } = {}) {
  parseVerifiedEvent(event)
  const token = bytesToBase64Url(textEncoder.encode(JSON.stringify(wireEvent(event))))
  return includeAuthorizationScheme ? `Nostr ${token}` : token
}

export function decodeToken (value) {
  if (typeof value !== 'string' || value.length === 0) fail('INVALID_NWT_TOKEN')
  let token = value
  if (value.startsWith('Nostr ')) token = value.slice(6)
  else if (/\s/.test(value)) fail('INVALID_AUTHORIZATION_HEADER')
  if (!BASE64URL.test(token) || token.length % 4 === 1) fail('INVALID_NWT_ENCODING')

  let bytes
  try {
    bytes = base64UrlToBytes(token)
    if (bytesToBase64Url(bytes) !== token) fail('INVALID_NWT_ENCODING')
  } catch {
    fail('INVALID_NWT_ENCODING')
  }

  try {
    const event = JSON.parse(textDecoder.decode(bytes))
    if (!event || typeof event !== 'object' || Array.isArray(event)) fail('INVALID_NWT_EVENT_JSON')
    return event
  } catch (error) {
    if (error?.message === 'INVALID_NWT_EVENT_JSON') throw error
    fail('INVALID_NWT_EVENT_JSON')
  }
}

export function validateToken (value, {
  audience,
  signer,
  issuer,
  subject,
  now = Math.floor(Date.now() / 1000),
  clockSkewSeconds = 60,
  requireAudience = false,
  requireExpiration = false
} = {}) {
  const event = typeof value === 'string' ? decodeToken(value) : value
  const token = parseVerifiedEvent(event)
  normalizeTimestamp(now, 'INVALID_NOW')
  normalizeTimestamp(clockSkewSeconds, 'INVALID_CLOCK_SKEW')
  if (!Number.isSafeInteger(now + clockSkewSeconds) || !Number.isSafeInteger(now - clockSkewSeconds)) {
    fail('INVALID_CLOCK_SKEW')
  }

  if (token.notBefore !== null && now + clockSkewSeconds < token.notBefore) fail('NWT_NOT_YET_VALID')
  if (token.expiration !== null && now - clockSkewSeconds >= token.expiration) fail('NWT_EXPIRED')
  if (requireExpiration && token.expiration === null) fail('NWT_EXPIRATION_REQUIRED')

  if (token.audience.length === 0) {
    if (requireAudience) fail('NWT_AUDIENCE_REQUIRED')
  } else {
    if (audience === undefined) fail('NWT_AUDIENCE_REQUIRED')
    const expectedAudience = normalizeAudience(audience, { required: true })
    if (!expectedAudience.some(value => token.audience.includes(value))) fail('NWT_AUDIENCE_MISMATCH')
  }

  for (const [expected, actual, code] of [
    [signer, token.signer, 'NWT_SIGNER_MISMATCH'],
    [issuer, token.issuer, 'NWT_ISSUER_MISMATCH'],
    [subject, token.subject, 'NWT_SUBJECT_MISMATCH']
  ]) {
    if (expected !== undefined && normalizeStringClaim(expected, code) !== actual) fail(code)
  }

  return token
}
