import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'

import { clearRelayQueryCache } from '../relay/services/query.js'
import { getLatestEventsByPubkey } from '../relay/services/events.js'

afterEach(() => {
  clearRelayQueryCache()
})

function kind0Event (pubkey, index, createdAt = 1) {
  return {
    id: String(index).repeat(64),
    kind: 0,
    pubkey,
    created_at: createdAt,
    tags: [],
    content: ''
  }
}

test('getLatestEventsByPubkey runs two passes and batches missing authors on remaining and fallback relays', async () => {
  const pubkeys = ['7'.repeat(64), '8'.repeat(64)]
  const relays = [
    'wss://primary-one.test',
    'wss://primary-two.test',
    'wss://remaining.test'
  ]
  const fallbackRelays = [
    'wss://free-one.test',
    'wss://free-two.test',
    'wss://free-three.test',
    'wss://free-four.test'
  ]
  const calls = []

  const result = await getLatestEventsByPubkey(pubkeys, {
    kinds: [0],
    fallbackRelays: fallbackRelays.slice(0, 3),
    _getRelaysByPubkey: async () =>
      Object.fromEntries(pubkeys.map(pubkey => [pubkey, { write: relays }])),
    _getEvents: async (filter, selectedRelays) => {
      calls.push({ authors: filter.authors, relay: selectedRelays[0] })
      if (selectedRelays[0] !== 'wss://remaining.test') return { result: [] }
      return {
        result: pubkeys.map((pubkey, index) => kind0Event(pubkey, index + 1))
      }
    }
  })

  assert.deepEqual(calls.map(call => call.relay), [
    ...relays,
    ...fallbackRelays.slice(0, 3)
  ])
  calls.forEach(call => assert.deepEqual(call.authors, pubkeys))
  assert.deepEqual(result.events.map(event => event.pubkey), pubkeys)
  assert.deepEqual(Object.keys(result.byPubkey), pubkeys)
  assert.deepEqual(Object.keys(result.relaysByPubkey), pubkeys)
})

test('getLatestEventsByPubkey falls back to free relays when discovery fails', async () => {
  const pubkey = '9'.repeat(64)
  const calls = []
  const result = await getLatestEventsByPubkey([pubkey], {
    kinds: [0],
    fallbackRelays: [
      'wss://free-one.test',
      'wss://free-two.test',
      'wss://free-three.test'
    ],
    _getRelaysByPubkey: async () => {
      throw new Error('relay discovery unavailable')
    },
    _getEvents: async (filter, selectedRelays) => {
      calls.push({ authors: filter.authors, relay: selectedRelays[0] })
      return { result: [] }
    }
  })

  assert.deepEqual(calls.map(call => call.relay), [
    'wss://free-one.test',
    'wss://free-two.test',
    'wss://free-three.test'
  ])
  calls.forEach(call => assert.deepEqual(call.authors, [pubkey]))
  assert.deepEqual(result.events, [])
  assert.deepEqual(result.byPubkey, {})
})

test('getLatestEventsByPubkey does not re-query used relays when no fallback remains', async () => {
  const pubkey = 'c'.repeat(64)
  const calls = []
  const result = await getLatestEventsByPubkey([pubkey], {
    kinds: [0],
    fallbackRelays: [],
    _getRelaysByPubkey: async () => ({ [pubkey]: { write: ['wss://relay.test'] } }),
    _getEvents: async (_filter, selectedRelays) => {
      calls.push(selectedRelays[0])
      return { result: [] }
    }
  })

  assert.deepEqual(calls, ['wss://relay.test'])
  assert.deepEqual(result.events, [])
})

test('getLatestEventsByPubkey reuses provided relays and discovers only missing authors', async () => {
  const alice = 'a'.repeat(64)
  const bob = 'b'.repeat(64)
  let discoveryCalls = 0
  const result = await getLatestEventsByPubkey([alice, bob], {
    kinds: [0],
    relaysByPubkey: { [alice]: { write: ['wss://alice.test'] } },
    _getRelaysByPubkey: async missing => {
      discoveryCalls++
      assert.deepEqual(missing, [bob])
      return { [bob]: { write: ['wss://bob.test'] } }
    },
    _getEvents: async (_filter, selectedRelays) => ({
      result: selectedRelays[0] === 'wss://alice.test'
        ? [kind0Event(alice, 1)]
        : [kind0Event(bob, 2)]
    })
  })

  assert.equal(discoveryCalls, 1)
  assert.deepEqual(Object.keys(result.relaysByPubkey), [alice, bob])
  assert.deepEqual(Object.keys(result.byPubkey), [alice, bob])
})

test('getLatestEventsByPubkey fetches addressable events with d tags per pubkey', async () => {
  const alice = 'a'.repeat(64)
  const bob = 'b'.repeat(64)
  const calls = []
  const result = await getLatestEventsByPubkey([alice, bob], {
    kinds: [30023],
    dTagsByPubkey: { [alice]: 'draft', [bob]: 'draft' },
    fallbackRelays: [],
    _getRelaysByPubkey: async () => ({
      [alice]: { write: ['wss://one.test'] },
      [bob]: { write: ['wss://one.test'] }
    }),
    _getEvents: async filter => {
      calls.push(filter)
      return {
        result: [{
          id: 'a'.repeat(64),
          kind: 30023,
          pubkey: alice,
          created_at: 1,
          tags: [['d', 'draft']],
          content: ''
        }]
      }
    }
  })

  assert.deepEqual(calls, [{ kinds: [30023], authors: [alice, bob], '#d': ['draft'] }])
  assert.equal(result.events.length, 1)
  assert.equal(result.byPubkey[alice].tags[0][1], 'draft')
  assert.equal(result.byPubkey[bob], undefined)
})

test('getLatestEventsByPubkey keeps the newest event per address', async () => {
  const alice = 'a'.repeat(64)
  const older = { ...kind0Event(alice, 1, 5), content: 'older' }
  const newer = { ...kind0Event(alice, 2, 6), content: 'newer' }
  const result = await getLatestEventsByPubkey([alice], {
    kinds: [0],
    fallbackRelays: [],
    _getRelaysByPubkey: async () => ({ [alice]: { write: ['wss://one.test', 'wss://two.test'] } }),
    _getEvents: async (_filter, selectedRelays) => ({
      result: selectedRelays[0] === 'wss://one.test' ? [older] : [newer]
    })
  })

  assert.equal(result.byPubkey[alice].content, 'newer')
})
