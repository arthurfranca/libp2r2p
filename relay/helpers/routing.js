import { freeRelays } from '../constants/index.js'

const DEFAULT_RELAYS_PER_PUBKEY = 2

function excludedRelaysFor (excludeRelaysByPubkey, pubkey) {
  if (!excludeRelaysByPubkey) return []
  return excludeRelaysByPubkey instanceof Map
    ? excludeRelaysByPubkey.get(pubkey) || []
    : excludeRelaysByPubkey[pubkey] || []
}

// Given pubkeys and their relay mappings, picks the minimum set of relays
// that covers all pubkeys (up to maxPerPubkey relays each), preferring
// relays shared by more pubkeys. Returns Map<relayUrl, pubkey[]>.
export function pickRelaysForPubkeys (pubkeys, relaysByPubkey, {
  maxPerPubkey = DEFAULT_RELAYS_PER_PUBKEY,
  relayType = 'write',
  excludeRelaysByPubkey,
  emptyRelaysFallback = freeRelays.slice(0, DEFAULT_RELAYS_PER_PUBKEY)
} = {}) {
  const type = relayType === 'read' ? 'read' : 'write'
  const pkToPossibleRelays = new Map()
  for (const pk of pubkeys) {
    const relays = relaysByPubkey[pk]?.[type] || []
    const excluded = new Set(excludedRelaysFor(excludeRelaysByPubkey, pk))
    const candidates = (relays.length ? relays : emptyRelaysFallback)
      .filter(relay => !excluded.has(relay))
    pkToPossibleRelays.set(pk, new Set(candidates))
  }

  const relayCounts = new Map()
  for (const relays of pkToPossibleRelays.values()) {
    for (const relay of relays) relayCounts.set(relay, (relayCounts.get(relay) || 0) + 1)
  }
  const rankedRelays = [...relayCounts.keys()].sort((a, b) => relayCounts.get(b) - relayCounts.get(a))

  const relayToAuthors = new Map()
  for (const pk of pubkeys) {
    const possibleRelays = pkToPossibleRelays.get(pk)
    let assigned = 0
    for (const relay of rankedRelays) {
      if (assigned >= maxPerPubkey) break
      if (!possibleRelays.has(relay)) continue
      if (!relayToAuthors.has(relay)) relayToAuthors.set(relay, [])
      relayToAuthors.get(relay).push(pk)
      assigned++
    }
  }

  return relayToAuthors
}
