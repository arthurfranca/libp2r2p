import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { generateSecretKey } from '../key/index.js'

import TestSigner from './helpers/test-signer.js'
import { bytesToHex } from '../base16/index.js'
import { makeContentKeyEvent } from '../content-key/event/index.js'
import { getIykcProofs, clearContentKeyCache } from '../content-key/services/iykc-proof.js'
import {
  cacheRelayListEvent,
  clearRelayQueryCache,
  getRelaysByPubkey,
  parseRelayListEvent,
  subscribeRelayListUpdates
} from '../relay/services/query.js'
import { pickRelaysForPubkeys } from '../relay/helpers/routing.js'

afterEach(() => {
  clearRelayQueryCache()
  clearContentKeyCache()
  TestSigner.releaseAll()
})

function signer () {
  return TestSigner.getOrCreate(bytesToHex(generateSecretKey()))
}

function relayListEvent (pubkey, createdAt, tags) {
  return { kind: 10002, pubkey, created_at: createdAt, tags, content: '' }
}

test('cacheRelayListEvent tracks newer relay-list events and exposes changed sets', () => {
  const first = cacheRelayListEvent(relayListEvent('alice', 1, [['r', 'wss://one.example']]))
  const older = cacheRelayListEvent(relayListEvent('alice', 1, [['r', 'wss://two.example']]))
  const newer = cacheRelayListEvent(relayListEvent('alice', 2, [['r', 'wss://two.example', 'write']]))

  assert.deepEqual(first.relays, { read: ['wss://one.example'], write: ['wss://one.example'] })
  assert.equal(older, null)
  assert.deepEqual(newer.previousRelays, { read: ['wss://one.example'], write: ['wss://one.example'] })
  assert.deepEqual(newer.relays, { read: [], write: ['wss://two.example'] })
  assert.deepEqual(newer.changes, { read: true, write: true, both: true })
})

test('cacheRelayListEvent normalizes public relays and applies the NIP-01 tie break', () => {
  const first = cacheRelayListEvent({
    ...relayListEvent('alice', 5, [
      ['r', 'HTTPS://Relay.Example/'],
      ['r', 'ws://localhost:8080'],
      ['r', 'not a relay://value']
    ]),
    id: 'b'.repeat(64)
  })
  const tieWinner = cacheRelayListEvent({
    ...relayListEvent('alice', 5, [['r', 'relay-two.example', 'write']]),
    id: 'a'.repeat(64)
  })

  assert.deepEqual(first.relays, {
    read: ['wss://relay.example'],
    write: ['wss://relay.example']
  })
  assert.deepEqual(tieWinner.relays, {
    read: [],
    write: ['wss://relay-two.example']
  })
})

test('pickRelaysForPubkeys covers pubkeys with shared relay preference', () => {
  const picked = pickRelaysForPubkeys(['alice', 'bob'], {
    alice: { write: ['wss://shared.example', 'wss://alice.example'] },
    bob: { write: ['wss://shared.example', 'wss://bob.example'] }
  }, { maxPerPubkey: 1 })

  assert.deepEqual([...picked.entries()], [['wss://shared.example', ['alice', 'bob']]])
})

test('pickRelaysForPubkeys excludes already used relays per pubkey', () => {
  const picked = pickRelaysForPubkeys(['alice', 'bob'], {
    alice: { write: ['wss://shared.example', 'wss://alice.example'] },
    bob: { write: ['wss://shared.example', 'wss://bob.example'] }
  }, {
    maxPerPubkey: 1,
    excludeRelaysByPubkey: { alice: ['wss://shared.example'] }
  })

  assert.deepEqual([...picked.entries()], [
    ['wss://alice.example', ['alice']],
    ['wss://shared.example', ['bob']]
  ])
})

test('pickRelaysForPubkeys supports empty fallback so excluded authors stay unrouted', () => {
  const picked = pickRelaysForPubkeys(['alice'], {
    alice: { write: ['wss://one.example'] }
  }, {
    maxPerPubkey: Infinity,
    excludeRelaysByPubkey: new Map([['alice', ['wss://one.example']]]),
    emptyRelaysFallback: []
  })

  assert.equal(picked.size, 0)
})

test('getRelaysByPubkey fetches latest relay lists and falls back when absent', async () => {
  const calls = []
  const relays = await getRelaysByPubkey(['alice', 'bob'], {
    _getEvents: async (filter, relayUrls, options) => {
      calls.push({ filter, relayUrls })
      assert.deepEqual(options, { timeout: 5000, timeoutAfterFirstEose: 500 })
      return { result: [relayListEvent('alice', 9, [['r', 'wss://alice.example', 'write']])] }
    }
  })

  assert.equal(calls.length, 1)
  assert.deepEqual(calls[0].filter.authors, ['alice', 'bob'])
  assert.deepEqual(relays.alice, { read: [], write: ['wss://alice.example'] })
  assert.deepEqual(relays.bob.write.slice(0, 2), ['wss://relay.44billion.net', 'wss://nos.lol'])
})

test('getRelaysByPubkey threads configurable relay-list query timing', async () => {
  const calls = []
  await getRelaysByPubkey(['alice'], {
    timeout: 2000,
    timeoutAfterFirstEose: 250,
    _getEvents: async (_filter, _relays, options) => {
      calls.push(options)
      return { result: [] }
    }
  })
  assert.deepEqual(calls, [{ timeout: 2000, timeoutAfterFirstEose: 250 }])
})

test('getRelaysByPubkey returns the latest relay-list event with includeEvents', async () => {
  const event = {
    ...relayListEvent('alice', 9, [['r', 'wss://alice.example', 'write']]),
    id: 'c'.repeat(64)
  }
  const relays = await getRelaysByPubkey(['alice', 'bob'], {
    includeEvents: true,
    _getEvents: async () => ({ result: [event] })
  })

  assert.deepEqual(relays.alice, {
    read: [],
    write: ['wss://alice.example'],
    event: { ...event, tags: [...event.tags] }
  })
  assert.equal(relays.bob.event, null)
  assert.deepEqual(relays.bob.write.slice(0, 2), ['wss://relay.44billion.net', 'wss://nos.lol'])
})

test('getRelaysByPubkey can disable the free-relay fallback for missing relay lists', async () => {
  const relays = await getRelaysByPubkey(['alice'], {
    includeEvents: true,
    emptyRelaysFallback: [],
    _getEvents: async () => ({ result: [] })
  })

  assert.deepEqual(relays.alice, { read: [], write: [], event: null })
})

test('getRelaysByPubkey uses the configured empty-relays fallback', async () => {
  const relays = await getRelaysByPubkey(['alice'], {
    emptyRelaysFallback: ['wss://fallback.example'],
    _getEvents: async () => ({ result: [] })
  })

  assert.deepEqual(relays.alice, {
    read: ['wss://fallback.example'],
    write: ['wss://fallback.example']
  })
})

test('getRelaysByPubkey forceRefresh re-queries cached pubkeys without regressing newer events', async () => {
  let calls = 0
  const event = (createdAt, id) => ({
    ...relayListEvent('alice', createdAt, [['r', 'wss://alice.example', 'write']]),
    id
  })

  const first = await getRelaysByPubkey(['alice'], {
    _getEvents: async () => {
      calls++
      return { result: [event(9, 'b'.repeat(64))] }
    }
  })
  assert.equal(calls, 1)
  assert.deepEqual(first.alice, { read: [], write: ['wss://alice.example'] })

  const second = await getRelaysByPubkey(['alice'], {
    forceRefresh: true,
    includeEvents: true,
    _getEvents: async () => {
      calls++
      return { result: [event(8, 'a'.repeat(64))] }
    }
  })
  assert.equal(calls, 2)
  assert.equal(second.alice.event.created_at, 9)

  const third = await getRelaysByPubkey(['alice'], {
    forceRefresh: true,
    includeEvents: true,
    _getEvents: async () => {
      calls++
      return { result: [] }
    }
  })
  assert.equal(calls, 3)
  assert.equal(third.alice.event.created_at, 9)
})

test('getRelaysByPubkey forceRefresh still shares concurrent requests', async () => {
  let calls = 0
  let release
  const gate = new Promise(resolve => { release = resolve })
  const options = {
    forceRefresh: true,
    _getEvents: async () => {
      calls++
      return await gate
    }
  }

  const firstRequest = getRelaysByPubkey(['alice'], options)
  const secondRequest = getRelaysByPubkey(['alice'], options)
  release({ result: [relayListEvent('alice', 1, [['r', 'wss://alice.example']])] })
  await Promise.all([firstRequest, secondRequest])

  assert.equal(calls, 1)
})

test('getRelaysByPubkey applies the relay URL policy to parsed lists', async () => {
  const policy = { onion: true, localRelay: true, nostrEntityUrls: true }
  const onion = 'ws://oxtrdevav64z64yb7x6rjg4ntzqjhedm5b5zjqulugknhzr46ny2qbad.onion'
  const relays = await getRelaysByPubkey(['alice'], {
    relayUrlPolicy: policy,
    _getEvents: async () => ({
      result: [relayListEvent('alice', 1, [
        ['r', onion, 'write'],
        ['r', 'ws://localhost:4869'],
        ['r', 'wss://npub1example.com', 'read'],
        ['r', 'ws://localhost:8080', 'write']
      ])]
    })
  })

  assert.deepEqual(relays.alice, {
    read: ['ws://localhost:4869', 'wss://npub1example.com'],
    write: [onion, 'ws://localhost:4869']
  })
})

test('parseRelayListEvent applies the relay URL policy', () => {
  const event = relayListEvent('alice', 1, [['r', 'ws://localhost:4869']])
  assert.deepEqual(parseRelayListEvent(event), { read: [], write: [] })
  assert.deepEqual(parseRelayListEvent(event, { localRelay: true }), {
    read: ['ws://localhost:4869'],
    write: ['ws://localhost:4869']
  })
})

test('getRelaysByPubkey shares concurrent queries for the same pubkey', async () => {
  let calls = 0
  let release
  const gate = new Promise(resolve => { release = resolve })
  const options = {
    _getEvents: async () => {
      calls++
      return await gate
    }
  }

  const firstRequest = getRelaysByPubkey(['alice'], options)
  const secondRequest = getRelaysByPubkey(['alice'], options)
  release({ result: [relayListEvent('alice', 1, [['r', 'wss://alice.example']])] })
  const [first, second] = await Promise.all([firstRequest, secondRequest])

  assert.equal(calls, 1)
  assert.deepEqual(first, second)
  assert.notEqual(first.alice, second.alice)
})

test('getIykcProofs finds latest content-key events through relay routing', async () => {
  const user = signer()
  const older = signer()
  const newer = signer()
  const userPubkey = await user.getPublicKey()
  const olderEvent = await makeContentKeyEvent({ userSigner: user, contentKeySigner: older, createdAt: 7 })
  const newerEvent = await makeContentKeyEvent({ userSigner: user, contentKeySigner: newer, createdAt: 8 })

  const found = await getIykcProofs([userPubkey], {
    _getRelaysByPubkey: async () => ({ [userPubkey]: { write: ['wss://one.example'] } }),
    _getEvents: async () => ({ result: [olderEvent, newerEvent] })
  })

  assert.equal(found[userPubkey].iykcPubkey, await newer.getPublicKey())
})

test('subscribeRelayListUpdates only reports watched relay-type changes', async () => {
  const changes = []
  let aborted = false
  async function * events (_filter, _relays, { signal }) {
    signal.addEventListener('abort', () => { aborted = true }, { once: true })
    yield relayListEvent('alice', 1, [['r', 'wss://read.example', 'read']])
    yield relayListEvent('alice', 2, [['r', 'wss://write.example', 'write']])
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
  }

  const stop = subscribeRelayListUpdates(['alice'], {
    relayType: 'write',
    onChange: change => changes.push(change),
    _eventsFeedGenerator: events
  })
  await new Promise(resolve => setImmediate(resolve))
  stop()
  await new Promise(resolve => setImmediate(resolve))

  assert.equal(changes.length, 1)
  assert.deepEqual(changes[0].relays, { read: [], write: ['wss://write.example'] })
  assert.ok(aborted)
})
