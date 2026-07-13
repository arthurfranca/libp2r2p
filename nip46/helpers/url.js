const PUBKEY = /^[0-9a-f]{64}$/

function uniqueStrings (values) {
  if (!Array.isArray(values)) return []
  return [...new Set(values.filter(value => typeof value === 'string' && value))]
}

function validPubkey (value) {
  return typeof value === 'string' && PUBKEY.test(value)
}

export function normalizeBunkerPointer (pointer) {
  if (!pointer || typeof pointer !== 'object') return null
  const remoteSignerPubkey = String(pointer.remoteSignerPubkey || '').toLowerCase()
  const relays = uniqueStrings(pointer.relays)
  if (!validPubkey(remoteSignerPubkey) || !relays.length) return null
  return {
    remoteSignerPubkey,
    relays,
    secret: typeof pointer.secret === 'string' && pointer.secret ? pointer.secret : null
  }
}

/** Parses a direct NIP-46 bunker URL without performing a network lookup. */
export function parseBunkerUrl (input) {
  try {
    const url = new URL(input)
    if (url.protocol !== 'bunker:') return null
    return normalizeBunkerPointer({
      remoteSignerPubkey: url.hostname,
      relays: url.searchParams.getAll('relay'),
      secret: url.searchParams.get('secret')
    })
  } catch {
    return null
  }
}

/** Serializes a direct NIP-46 bunker pointer without any network lookup. */
export function toBunkerUrl (pointer) {
  const normalized = normalizeBunkerPointer(pointer)
  if (!normalized) throw new Error('INVALID_BUNKER_POINTER')
  const url = new URL(`bunker://${normalized.remoteSignerPubkey}`)
  for (const relay of normalized.relays) url.searchParams.append('relay', relay)
  if (normalized.secret) url.searchParams.set('secret', normalized.secret)
  return url.toString()
}

/** Builds a NIP-46 client-initiated connection URI. */
export function createNostrConnectURI ({
  clientPubkey,
  relays,
  secret,
  perms = [],
  name,
  url,
  image
} = {}) {
  const normalizedPubkey = String(clientPubkey || '').toLowerCase()
  const normalizedRelays = uniqueStrings(relays)
  if (!validPubkey(normalizedPubkey) || !normalizedRelays.length || typeof secret !== 'string' || !secret) {
    throw new Error('INVALID_NOSTRCONNECT_URI')
  }

  const uri = new URL(`nostrconnect://${normalizedPubkey}`)
  for (const relay of normalizedRelays) uri.searchParams.append('relay', relay)
  uri.searchParams.set('secret', secret)
  if (Array.isArray(perms) && perms.length) uri.searchParams.set('perms', perms.join(','))
  if (name) uri.searchParams.set('name', name)
  if (url) uri.searchParams.set('url', url)
  if (image) uri.searchParams.set('image', image)
  return uri.toString()
}

export function parseNostrConnectURI (input) {
  try {
    const url = new URL(input)
    const clientPubkey = url.hostname.toLowerCase()
    const relays = uniqueStrings(url.searchParams.getAll('relay'))
    const secret = url.searchParams.get('secret') || ''
    if (url.protocol !== 'nostrconnect:' || !validPubkey(clientPubkey) || !relays.length || !secret) return null
    return {
      clientPubkey,
      relays,
      secret,
      perms: uniqueStrings((url.searchParams.get('perms') || '').split(',')),
      clientMetadata: {
        ...(url.searchParams.get('name') ? { name: url.searchParams.get('name') } : {}),
        ...(url.searchParams.get('url') ? { url: url.searchParams.get('url') } : {}),
        ...(url.searchParams.get('image') ? { image: url.searchParams.get('image') } : {})
      }
    }
  } catch {
    return null
  }
}
