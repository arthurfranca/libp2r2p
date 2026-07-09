// Used only to discover users' NIP-65 relay lists (kind:10002).
export const seedRelays = [
  'wss://relay.44billion.net',
  'wss://purplepag.es',
  'wss://user.kindpag.es',
  'wss://relay.nos.social',
  'wss://nostr.land',
  'wss://indexer.coracle.social'
]

// Fallback write-accepting relays. Used as the initial write/read-relay set for
// new accounts and as a fallback when we cannot resolve a user's own relays.
export const freeRelays = [
  'wss://relay.44billion.net',
  'wss://nos.lol',
  'wss://relay.primal.net',
  'wss://relay.damus.io'
]
