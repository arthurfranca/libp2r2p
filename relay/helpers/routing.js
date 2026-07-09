import { freeRelays } from '../constants/index.js'

const DEFAULT_RELAYS_PER_PUBKEY = 2

// Given pubkeys and their relay mappings, picks the minimum set of relays
// that covers all pubkeys (up to maxPerPubkey relays each), preferring
// relays shared by more pubkeys. Returns Map<relayUrl, pubkey[]>.
export function pickRelaysForPubkeys (pubkeys, relaysByPubkey, { maxPerPubkey = DEFAULT_RELAYS_PER_PUBKEY, relayType = 'write' } = {}) {
  const type = relayType === 'read' ? 'read' : 'write'
  const pkToPossibleRelays = new Map()
  for (const pk of pubkeys) {
    const relays = relaysByPubkey[pk]?.[type] || []
    pkToPossibleRelays.set(pk, new Set(relays.length ? relays : freeRelays.slice(0, DEFAULT_RELAYS_PER_PUBKEY)))
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
