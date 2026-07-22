import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { generateSecretKey } from '../key/index.js'

import TestSigner from './helpers/test-signer.js'
import { bytesToHex } from '../base16/index.js'
import { makeContentKeyEvent } from '../content-key/event/index.js'
import { getIykcProofs, clearContentKeyCache } from '../content-key/services/iykc-proof.js'
import { cacheRelayListEvent, clearRelayQueryCache, getRelaysByPubkey, subscribeRelayListUpdates } from '../relay/services/query.js'
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

test('pickRelaysForPubkeys covers pubkeys with shared relay preference', () => {
  const picked = pickRelaysForPubkeys(['alice', 'bob'], {
    alice: { write: ['wss://shared.example', 'wss://alice.example'] },
    bob: { write: ['wss://shared.example', 'wss://bob.example'] }
  }, { maxPerPubkey: 1 })

  assert.deepEqual([...picked.entries()], [['wss://shared.example', ['alice', 'bob']]])
})

test('getRelaysByPubkey fetches latest relay lists and falls back when absent', async () => {
  const calls = []
  const relays = await getRelaysByPubkey(['alice', 'bob'], {
    _getEvents: async (filter, relayUrls, options) => {
      calls.push({ filter, relayUrls })
      assert.deepEqual(options, { timeout: 5000, timeoutAfterFirstEose: null })
      return { result: [relayListEvent('alice', 9, [['r', 'wss://alice.example', 'write']])] }
    }
  })

  assert.equal(calls.length, 1)
  assert.deepEqual(calls[0].filter.authors, ['alice', 'bob'])
  assert.deepEqual(relays.alice, { read: [], write: ['wss://alice.example'] })
  assert.deepEqual(relays.bob.write.slice(0, 2), ['wss://relay.44billion.net', 'wss://nos.lol'])
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
