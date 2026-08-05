// Used only to discover users' NIP-65 relay lists (kind:10002).
export const seedRelays = [
  'wss://relay.44billion.net',
  'wss://purplepag.es',
  'wss://user.kindpag.es',
  'wss://relay.nos.social',
  // Disabled 2026-08-05: accepted kind:10002 with OK but did not broadcast it
  // to a live subscription within 15 seconds. Keep for future retesting.
  // 'wss://nostr.land',
  'wss://indexer.coracle.social'
]

// Fallback write-accepting relays. Used as the initial write/read-relay set for
// new accounts and as a fallback when we cannot resolve a user's own relays.
export const freeRelays = [
  'wss://relay.44billion.net',
  'wss://nos.lol',
  'wss://relay.primal.net'
]

// Shared app-discovery relays used by Nostr app launchers and uploaders.
export const nappRelays = [
  'wss://relay.44billion.net',
  'wss://relay.ditto.pub',
  'wss://relay.dreamith.to'
]
