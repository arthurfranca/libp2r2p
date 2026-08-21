import {
  naddrDecode,
  neventDecode,
  noteDecode,
  nrelayDecode
} from '../nip19/index.js'
import { queryProfile } from '../nip05/index.js'
import { normalizeRelayUrl } from '../url/index.js'
import {
  decodeUserReference,
  encodeUserReference
} from './helpers/user-reference.js'

export { decodeUserReference, encodeUserReference }

const BECH32_BODY = '[ac-hj-np-z02-9]'
const BOUNDARY_PREFIX = /(?<=^|[\s"«„「¡¿:{([])/.source
const BOUNDARY_SUFFIX = /(?=\.?$|[.,]?\s|\.(?=\.)|\.?["»”」'!?;\])}])/.source

const URL_SOURCE =
  '(?<url>' +
  /(?<protocol>https:\/\/)?(?:[-_A-Za-z0-9]{1,30}\.){1,4}[a-z]{2,63}/.source +
  /(?:(?:\/[-._A-Za-z0-9%@#]{1,300}){0,11}(?<ext>\.[a-z-0-9]{3,4})|(?:\/[-._A-Za-z0-9%@#]{1,300}){0,11})\/?/.source +
  /(?:\??&?(?:[-_+.A-Za-z0-9%*]{1,30}=[-_+.A-Za-z0-9%*]{1,300}&?){1,40}|\?)?/.source +
  /(?:(?:#|(?<=#))[-_+.A-Za-z0-9%=&*:~,]{1,4000})?/.source +
  /(?:\/|(?<![.,]))/.source +
  ')'

const NIP05_LOCAL = '[a-z0-9._-]{1,64}'
const NIP05_DOMAIN = '(?:[a-z0-9-]+\\.)+[a-z]{2,63}'
const NIP05_STANDARD = `(?:@)?(?<nip05>${NIP05_LOCAL}@${NIP05_DOMAIN})`
const NIP05_AT_ROOT = '@(?<nip05AtRoot>[a-z0-9-]+\\.[a-z]{2,63})'
const NIP05_AT_CUSTOM = `@(?<nip05AtCustom>${NIP05_LOCAL}\\.${NIP05_DOMAIN})`
const NIP05_BARE_ROOT = '(?<nip05BareRoot>[a-z0-9-]+\\.[a-z]{2,63})'
const NIP05_BARE_CUSTOM = `(?<nip05BareCustom>${NIP05_LOCAL}\\.${NIP05_DOMAIN})`

function entitySource (name) {
  const bodyLength = name === 'npub' ? '58' : (name === 'nrelay' ? '10,5000' : '58,5000')
  return `(?:@|nostr:)?(?<${name}>${name}1${BECH32_BODY}{${bodyLength}})`
}

const ENTITY_SOURCES = [
  entitySource('nrelay'),
  entitySource('npub'),
  entitySource('nprofile'),
  entitySource('note'),
  entitySource('nevent'),
  entitySource('naddr')
]

const HASHTAG_SOURCE = /(?:#(?<hashtag>[^\s.!¡?¿@#$%^&*()=+/,[{\]};:'"><]+))/.source

let regexCache
function getReferencesRegex (bareNip05) {
  const key = bareNip05 ? 'bare' : 'handle'
  regexCache ??= {}
  if (regexCache[key]) return regexCache[key]

  const nip05Compact = [
    NIP05_AT_ROOT,
    NIP05_AT_CUSTOM,
    ...(bareNip05 ? [NIP05_BARE_ROOT, NIP05_BARE_CUSTOM] : [])
  ]
  const alternatives = [
    URL_SOURCE,
    NIP05_STANDARD,
    ...nip05Compact,
    ...ENTITY_SOURCES,
    HASHTAG_SOURCE
  ]
  // Bare compact NIP-05 forms are ambiguous with plain hostnames/URLs, so
  // they are placed first only when explicitly requested.
  if (bareNip05) {
    alternatives.splice(0, 0, NIP05_BARE_ROOT, NIP05_BARE_CUSTOM)
  }
  const source = BOUNDARY_PREFIX + '(?:' + alternatives.join('|') + ')' + BOUNDARY_SUFFIX
  return (regexCache[key] = new RegExp(source, 'gu'))
}

// Strips the optional NIP-21 (`nostr:`) and `@` mention prefixes.
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

function normalizeRelays (relays) {
  return (relays || [])
    .map(relay => {
      try {
        return normalizeRelayUrl(relay)
      } catch {
        return null
      }
    })
    .filter(Boolean)
}

// Parses a single NIP-27-style reference (with optional `@`/`nostr:`
// prefixes): NIP-05 (including the custom compact forms), npub/nprofile/hex
// accounts, note/nevent/naddr events and nrelay relays.
export function decodeReference (value) {
  if (typeof value !== 'string') return null
  const original = value.trim()
  if (!original) return null
  const text = stripReferencePrefix(original)
  if (!text) return null

  const account = decodeUserReference(text)
  if (account) {
    return account.kind === 'pubkey'
      ? {
          type: 'pubkey',
          original,
          value: account.raw,
          pubkey: account.pubkey,
          relays: account.relays
        }
      : {
          type: 'nip05',
          original,
          value: account.raw,
          local: account.local,
          domain: account.domain
        }
  }

  if (text.startsWith('note1')) {
    try {
      return { type: 'note', original, value: text, id: noteDecode(text) }
    } catch {
      return null
    }
  }
  if (text.startsWith('nevent1')) {
    try {
      return { type: 'nevent', original, value: text, ...neventDecode(text) }
    } catch {
      return null
    }
  }
  if (text.startsWith('naddr1')) {
    try {
      return { type: 'naddr', original, value: text, ...naddrDecode(text) }
    } catch {
      return null
    }
  }
  if (text.startsWith('nrelay1')) {
    try {
      return { type: 'nrelay', original, value: text, relay: nrelayDecode(text) }
    } catch {
      return null
    }
  }
  return null
}

const NIP94_TAGS = {
  url: [{ key: 'url', type: 'array' }],
  'aes-256-gcm': ['key', 'iv'],
  m: ['m'],
  x: [{ key: 'x', type: 'array' }],
  ox: ['ox'],
  size: ['size'],
  dim: ['dim'],
  magnet: ['magnet'],
  i: ['i'],
  blurhash: ['blurhash'],
  thumb: ['thumb'],
  image: ['image'],
  summary: ['summary'],
  alt: ['alt'],
  caption: ['caption']
}

// Decodes the file/media metadata carried in a URL fragment
// (`#m=image/png&dim=640x480&alt=...`). Kept generic on purpose: the old
// draft number (54) was taken by an unrelated NIP.
export function decodeMediaMetadata (url, { extraTags } = {}) {
  if (typeof url !== 'string' || !url) return {}

  const tags = extraTags ? { ...NIP94_TAGS, ...extraTags } : NIP94_TAGS
  const tagIndexes = {}
  const obj = (url.match(/(?<=#)[-_+.A-Za-z0-9%=&*]{1,4000}/)?.[0] || '')
    .split('&')
    .filter(Boolean)
    .reduce((memo, item) => {
      let [key, value = ''] = item.split('=')
      key = decodeFragmentValue(key)
      value = decodeFragmentValue(value)
      const config = tags[key]
      if (!config || (tagIndexes[key] ??= 0) === config.length) return memo

      const name = config[tagIndexes[key]]?.key ?? config[tagIndexes[key]]
      const type = config[tagIndexes[key]]?.type ?? 'string'
      switch (type) {
        case 'array': memo[name] ??= []; memo[name].push(value); break
        case 'string': memo[name] = value; tagIndexes[key]++; break
        default: break
      }
      return memo
    }, {})

  if ('dim' in obj) {
    const { width, height } = obj.dim.match(
      /(?<width>[1-9]{1}[0-9]{0,10})(?:\s*[xX]\s*)(?<height>[1-9]{1}[0-9]{0,10})/
    )?.groups ?? {}
    if (width !== undefined) obj.width = width
    if (height !== undefined) obj.height = height
  }
  return obj
}

function decodeFragmentValue (value) {
  try {
    return decodeURIComponent(value.replace(/\+/g, '%20'))
  } catch {
    return value
  }
}

function getReferenceItem (original, groups, { getMimeType }) {
  if (groups.url) {
    const url = `${groups.protocol ? '' : 'https://'}${groups.url}`
    const urlItem = { value: url, ...(groups.ext && { ext: groups.ext }), ...decodeMediaMetadata(url) }
    if (!urlItem.m && typeof getMimeType === 'function') {
      const mime = getMimeType({ url, ext: groups.ext })
      if (mime) urlItem.m = mime
    }
    return { key: 'url', url: urlItem }
  }

  if (
    groups.nip05 ||
    groups.nip05AtRoot ||
    groups.nip05AtCustom ||
    groups.nip05BareRoot ||
    groups.nip05BareCustom
  ) {
    const account = decodeUserReference(original)
    if (!account) return null
    return {
      key: 'nip05',
      nip05: {
        original,
        value: account.raw,
        local: account.local,
        domain: account.domain
      }
    }
  }

  if (groups.hashtag) {
    return { key: 'hashtag', hashtag: { value: groups.hashtag } }
  }

  const ref = decodeReference(original)
  if (!ref) return null
  switch (ref.type) {
    case 'pubkey': {
      const isNprofile = ref.value.startsWith('nprofile1')
      const profile = {
        pubkey: ref.pubkey,
        relays: normalizeRelays(ref.relays),
        original,
        nip19Type: isNprofile ? 'nprofile' : 'npub'
      }
      if (!isNprofile) profile.npub = ref.value
      return { key: 'profile', profile }
    }
    case 'note':
      return { key: 'event', event: { id: ref.id, relays: [], original, nip19Type: 'note' } }
    case 'nevent':
      return {
        key: 'event',
        event: {
          id: ref.id,
          relays: normalizeRelays(ref.relays),
          original,
          nip19Type: 'nevent',
          ...(ref.author !== undefined && { author: ref.author }),
          ...(ref.kind !== undefined && { kind: ref.kind })
        }
      }
    case 'naddr':
      return {
        key: 'event',
        event: {
          identifier: ref.identifier,
          pubkey: ref.pubkey,
          kind: ref.kind,
          relays: normalizeRelays(ref.relays),
          original,
          nip19Type: 'naddr'
        }
      }
    case 'nrelay':
      return { key: 'relay', relay: { relay: ref.relay, original, value: ref.value } }
    default:
      return null
  }
}

// Modernized NIP-27 text-reference extractor. Accepts optional `@` and
// `nostr:` mention prefixes, NIP-05 (standard, root and custom compact) and
// keeps the same item shape as the previous text extractor for URLs/entities.
// Bare compact NIP-05 spellings (`bob.example.com`) are only recognized when
// `{ bareNip05: true }` is passed, since they are otherwise indistinguishable
// from plain hostnames; prefixed forms (`@bob.example.com`) always work.
export function extractMedia (content, { bareNip05 = false, getMimeType } = {}) {
  if (typeof content !== 'string') return []
  const regex = getReferencesRegex(bareNip05)
  const items = []
  let end = 0

  for (const match of content.matchAll(regex)) {
    const start = match.index
    if (start > end) {
      items.push({ key: 'text', text: { value: content.slice(end, start) } })
    }
    const original = match[0]
    end = start + original.length
    const item = getReferenceItem(original, match.groups, { getMimeType })
    if (item) items.push(item)
  }

  if (items.length === 0) {
    items.push({ key: 'text', text: { value: content } })
  } else if (end < content.length) {
    items.push({ key: 'text', text: { value: content.slice(end) } })
  }
  return items
}

// Resolves a user reference to a pubkey and relay hints. NIP-05 lookups go
// through `queryProfile` (accepting standard, root and compact custom
// spellings); npub/nprofile/hex are resolved locally.
export async function resolveUserReference (value, options = {}) {
  const account = decodeUserReference(value)
  if (!account) return null
  if (account.kind === 'pubkey') {
    return { pubkey: account.pubkey, relays: account.relays, label: account.raw }
  }

  const result = await queryProfile(`${account.local}@${account.domain}`, options)
  if (!result) return null
  return { pubkey: result.pubkey, relays: result.relays, label: account.raw }
}
