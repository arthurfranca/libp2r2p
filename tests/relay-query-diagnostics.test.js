import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { getLatestEventsByPubkey, getRelaysByPubkey } from '../relay/index.js'
import { clearRelayQueryCache } from '../relay/services/query.js'

afterEach(clearRelayQueryCache)
const pubkey = 'a'.repeat(64)
const rate = Object.assign(new Error('rate-limited: busy'), { category: 'relay', retryAt: 30000, retryAfterMs: 20000 })
const failure = { result: [], relays: [{ relay: 'wss://seed.test', status: 'error', error: rate }], errors: [{ relay: 'wss://seed.test', reason: rate }] }

test('diagnostics preserve discovery and per-relay failures without losing partial events', async () => {
  const event = { kind: 0, pubkey, id: 'b'.repeat(64), created_at: 1, tags: [] }
  const result = await getLatestEventsByPubkey([pubkey], {
    kinds: [0], fallbackRelays: ['wss://good.test'],
    relayListOptions: { _getEvents: async () => failure },
    _getEvents: async (_filter, relays) => relays[0] === 'wss://good.test'
      ? { result: [{ event }], relays: [{ relay: relays[0], status: 'eose' }] }
      : { ...failure, relays: [{ relay: relays[0], status: 'error', error: rate }] }
  })
  assert.equal(result.byPubkey[pubkey], event)
  assert.equal(result.requests[0].phase, 'discovery')
  assert.equal(result.requests[0].relays[0].error, rate)
  assert.ok(result.requests.some(request => request.phase === 'fallback'))
  assert.equal(result.requests[0].relays[0].error.retryAt, 30000)
})

test('a failed discovery is not negatively cached and all shared callers receive its report', async () => {
  let calls = 0
  const gate = Promise.withResolvers()
  const reports = []
  const options = { _getEvents: async () => { calls++; return gate.promise }, onQueryResult: report => reports.push(report) }
  const a = getRelaysByPubkey([pubkey], options)
  const b = getRelaysByPubkey([pubkey], options)
  gate.resolve(failure)
  await Promise.all([a, b])
  assert.equal(calls, 1)
  assert.equal(reports.length, 2)
  assert.equal(reports[0], reports[1])
  await getRelaysByPubkey([pubkey], options)
  assert.equal(calls, 2)
})

test('one cancelled discovery consumer cannot abort another consumer', async () => {
  const gate = Promise.withResolvers()
  const controller = new AbortController()
  let sharedSignal
  const options = { _getEvents: async (_filter, _relays, { signal }) => { sharedSignal = signal; return gate.promise } }
  const a = getRelaysByPubkey([pubkey], { ...options, signal: controller.signal })
  const b = getRelaysByPubkey([pubkey], options)
  controller.abort()
  await assert.rejects(a, { name: 'AbortError' })
  assert.equal(sharedSignal.aborted, false)
  gate.resolve(failure)
  await b
})

test('exclusions apply to discovery and both event passes', async () => {
  const calls = []
  const blocked = 'wss://blocked.test'
  await getLatestEventsByPubkey([pubkey], {
    kinds: [0], relaysByPubkey: { [pubkey]: { write: [blocked, 'wss://other.test'] } },
    excludeRelaysByPubkey: new Map([[pubkey, [blocked]]]), fallbackRelays: [blocked, 'wss://fallback.test'],
    _getEvents: async (_filter, relays) => { calls.push(...relays); return { result: [] } }
  })
  assert.deepEqual(calls, ['wss://other.test', 'wss://fallback.test'])
  await getRelaysByPubkey([pubkey], {
    excludeRelays: ['wss://relay.44billion.net'], _getEvents: async (_filter, relays) => {
      assert.ok(!relays.includes('wss://relay.44billion.net'))
      return failure
    }
  })
})

test('thrown errors remain available and abort does not start a fallback pass', async () => {
  const controller = new AbortController()
  const reports = []
  const result = await getLatestEventsByPubkey([pubkey], {
    kinds: [0], relaysByPubkey: { [pubkey]: { write: ['wss://one.test'] } }, fallbackRelays: [],
    _getEvents: async () => { throw rate }
  })
  assert.equal(result.requests[0].error, rate)
  await assert.rejects(getLatestEventsByPubkey([pubkey], {
    kinds: [0], signal: controller.signal, relaysByPubkey: { [pubkey]: { write: ['wss://one.test'] } },
    onQueryResult: report => reports.push(report),
    _getEvents: async () => { controller.abort(); throw controller.signal.reason }
  }), { name: 'AbortError' })
  assert.equal(reports.length, 1)
})
