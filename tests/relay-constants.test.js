import assert from 'node:assert/strict'
import { test } from 'node:test'

import { freeRelays, nappRelays, seedRelays } from 'libp2r2p/relay'

test('exports the canonical seed relays', () => {
  assert.deepEqual(seedRelays, [
    'wss://relay.44billion.net',
    'wss://purplepag.es',
    'wss://user.kindpag.es',
    'wss://relay.nos.social',
    'wss://indexer.coracle.social'
  ])
  assert.equal(seedRelays.includes('wss://nostr.land'), false)
})

test('exports active fallback relays without the retired Damus relay', () => {
  assert.deepEqual(freeRelays, [
    'wss://relay.44billion.net',
    'wss://nos.lol',
    'wss://relay.primal.net'
  ])
  assert.equal(freeRelays.includes('wss://relay.damus.io'), false)
})

test('exports the shared app-discovery relays', () => {
  assert.deepEqual(nappRelays, [
    'wss://relay.44billion.net',
    'wss://relay.ditto.pub',
    'wss://relay.dreamith.to'
  ])
})
