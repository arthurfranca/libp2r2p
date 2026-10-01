import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { createPrivateMessageSession } from '../private-message/index.js'
import { subscribe } from '../private-channel/index.js'
import { RelayPool } from '../relay/index.js'

globalThis.IDBKeyRange = IDBKeyRange
const tick = () => new Promise(resolve => setImmediate(resolve))
async function until (predicate) {
  for (let i = 0; i < 100; i++) { if (await predicate()) return; await tick() }
  assert.fail('condition not reached')
}
const signer = { getPublicKey: () => '1'.repeat(64), withSharedKey () { return this }, nip44v3Decrypt () { throw new Error('unexpected decrypt') } }
const overflow = since => Object.assign(new Error('RELAY_LIVE_BUFFER_FULL'), { code: 'RELAY_LIVE_BUFFER_FULL', recoverySince: since })
function fixture (t) {
  const calls = []; const timers = []; const errors = []
  let now = Date.now()
  const session = createPrivateMessageSession({
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true }, _random: () => 0.5, _now: () => now
  })
  const subscribe = options => {
    const done = Promise.withResolvers(); const ready = Promise.withResolvers()
    const sub = { options, done, ready, closed: false }
    calls.push(sub)
    return { done: done.promise, ready: ready.promise, close () { sub.closed = true; done.resolve({ status: 'closed' }) } }
  }
  const watch = (channel, relays = ['wss://a.example'], onSubscriptionState) => session.watch({
    channels: [channel], relays, receiverSigner: signer, privateChannelSigner: signer,
    onSubscriptionState, onError: error => errors.push(error), _subscribe: subscribe
  })
  t.after(() => session.unwatch())
  return { session, watch, calls, timers, errors, advance: ms => { now += ms }, async retry () { const timer = timers.find(timer => !timer.cancelled && !timer.used); assert.ok(timer); timer.used = true; await timer.fn(); await tick(); return timer.delay } }
}

test('shared relay waits for every gap commit, preserves healthy relays and repairs equal watches', async t => {
  const f = fixture(t)
  const persisted = Promise.withResolvers()
  const notifications = []
  const forChannel = channel => async state => { notifications.push({ channel, ...state }); if (channel === 'b' && state.state === 'interrupted') await persisted.promise }
  await f.watch('a', ['wss://a.example', 'wss://healthy.example'], forChannel('a'))
  await f.watch('b', ['wss://a.example'], forChannel('b'))
  const failed = f.calls.at(-1)
  const healthy = f.calls[1]
  failed.done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => notifications.length === 2)
  await f.watch('a', ['wss://a.example', 'wss://healthy.example'], forChannel('a'))
  assert.equal(f.calls.length, 3)
  assert.equal(f.timers.length, 0)
  assert.equal(healthy.closed, false)
  persisted.resolve()
  await until(() => f.timers.length === 1)
  assert.equal(await f.retry(), 1000)
  const replacement = f.calls.at(-1)
  assert.equal(f.calls.length, 4)
  replacement.ready.resolve({ relays: ['wss://a.example'] })
  await until(() => notifications.filter(value => value.state === 'ready').length === 2)
  assert.ok(notifications.every(value => value.since === 100))
  assert.equal(healthy.closed, false)
  // Completion of an older generation must not retire its successor.
  f.calls[0].done.resolve({ status: 'failed', error: overflow(0) })
  await tick()
  await f.watch('a', ['wss://a.example', 'wss://healthy.example'], forChannel('a'))
  assert.equal(f.calls.length, 4)
})

test('gap persistence failure retries before reopen, and cancellation fences pending callbacks', async t => {
  const f = fixture(t)
  let attempts = 0
  await f.watch('a', undefined, () => { if (++attempts < 3) throw new Error('IDB unavailable') })
  f.calls[0].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 1)
  assert.equal(await f.retry(), 1000)
  assert.equal(f.calls.length, 1)
  assert.equal(await f.retry(), 2000)
  assert.equal(f.calls.length, 2)
  f.calls[1].done.resolve({ status: 'failed', error: overflow(90) })
  await until(() => f.timers.length === 3)
  await f.session.unwatch()
  await f.timers[2].fn()
  assert.equal(f.calls.length, 2)
})

test('retry delay survives short attempts and resets only after a stable minute', async t => {
  const f = fixture(t)
  await f.watch('a')
  f.calls[0].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 1)
  assert.equal(await f.retry(), 1000)
  f.calls[1].ready.resolve({ relays: ['wss://a.example'] })
  await tick()
  f.advance(1000)
  f.calls[1].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 2)
  assert.equal(await f.retry(), 2000)
  f.calls[2].ready.resolve({ relays: ['wss://a.example'] })
  await tick()
  f.advance(60000)
  f.calls[2].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 3)
  assert.equal(await f.retry(), 1000)
})

for (const limits of [{ maxBufferedLiveEvents: 2 }, { maxBufferedLiveBytes: 500 }]) {
  test(`real pool overflow restarts private subscription (${Object.keys(limits)[0]})`, async t => {
    const connections = []; const subscriptions = []; const errors = []; const states = []; const timers = []
    const pool = new RelayPool({
      _createRelay: () => {
        const connection = {
          ws: { readyState: 1 }, connect: async () => {}, close () {}, subscribe (filters, handlers) {
            const sub = { filters, handlers, closed: false, close () { this.closed = true; handlers.onclose?.() } }
            subscriptions.push(sub)
            queueMicrotask(() => handlers.oneose())
            return sub
          }
        }
        connections.push(connection)
        return connection
      }
    })
    const session = createPrivateMessageSession({ _setTimeout: fn => { const timer = { fn }; timers.push(timer); return timer }, _clearTimeout: timer => { timer.cancelled = true } })
    t.after(async () => { await session.unwatch(); await pool.disconnectAll() })
    await session.watch({
      channels: ['2'.repeat(64)], relays: ['wss://a.example'], receiverSigner: signer, privateChannelSigner: signer,
      onError: error => errors.push(error), onSubscriptionState: state => states.push(state),
      _subscribe: options => subscribe({
        ...options, receivedChunkIndexedDB: new IDBFactory(),
        _liveEventsGenerator: (filter, relays, options) => pool.getLiveEventsGenerator(filter, relays, { ...options, ...limits })
      })
    })
    await until(() => subscriptions.length === 1)
    await tick()
    for (let i = 0; i < 3; i++) subscriptions[0].handlers.onevent({ id: String(i), kind: 26400, created_at: 100 + i, content: 'x'.repeat(200) })
    await until(() => timers.length === 1)
    assert.equal(errors[0].code, 'RELAY_LIVE_BUFFER_FULL')
    assert.equal(errors[0].buffer.stage, 'delivery')
    assert.equal(errors[0].buffer.limits[limits.maxBufferedLiveEvents ? 'events' : 'bytes'], Object.values(limits)[0])
    assert.ok(errors[0].recoverySince <= 100)
    assert.equal(subscriptions[0].closed, true)
    await timers[0].fn()
    await until(() => states.some(state => state.state === 'ready'))
    assert.equal(subscriptions.length, 2)
    assert.equal(connections.length, 1)
    assert.equal(subscriptions[1].closed, false)
  })
}

test('URL aliases share one recovery and match canonical relay readiness', async t => {
  const f = fixture(t)
  await f.watch('a', ['wss://A.example/', 'wss://a.example'])
  assert.equal(f.calls.length, 1)
  assert.deepEqual(f.calls[0].options.relays, ['wss://a.example'])
  f.calls[0].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 1)
  await f.retry()
  f.calls[1].ready.resolve({ relays: ['wss://a.example'] })
  await tick()
  await f.watch('a', ['wss://a.example/'])
  assert.equal(f.calls.length, 2)
})

test('equal watches repair a terminated subscription and permanent refusals stop automatic retries', async t => {
  const f = fixture(t)
  await f.watch('a')
  f.calls[0].done.resolve({ status: 'ended' })
  await tick()
  await f.watch('a')
  assert.equal(f.calls.length, 2)
  f.calls[1].done.resolve({ status: 'failed', error: overflow(100) })
  await until(() => f.timers.length === 1)
  await f.retry()
  f.calls[2].ready.resolve({ relays: [], errors: [{ relay: 'wss://a.example', reason: new Error('blocked: denied') }] })
  await tick()
  assert.equal(f.calls[2].closed, true)
  assert.equal(f.timers.length, 1)
})
