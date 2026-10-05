import assert from 'node:assert/strict'
import { test } from 'node:test'
import { RelayPool } from '../relay/index.js'
import { ReadRetry } from '../relay/helpers/read-retry.js'

const A = 'wss://a.example'
const B = 'wss://b.example'
const flush = async () => { for (let n = 0; n < 3; n++) await new Promise(resolve => setImmediate(resolve)) }
const refusal = message => Object.assign(new Error(message), { category: 'relay' })
const disconnected = () => Object.assign(new Error('socket disconnected'), { category: 'transport' })
const event = id => ({ id, kind: 1, created_at: Math.floor(Date.now() / 1000), tags: [], content: '' })

function fixture (t, { check, connect, capacity = {} } = {}) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1000000 })
  const state = { online: true, checks: 0, monitors: 0, stopped: 0, subscriptions: [], connects: [] }
  let wake
  const pool = new RelayPool({
    ...capacity,
    _isOnline: async options => { state.checks++; return check ? check(options, state) : state.online },
    _onOnline: handler => { state.monitors++; wake = handler; return () => { state.stopped++; wake = null } },
    _createRelay: url => ({
      ws: { readyState: 1 },
      async connect () { state.connects.push(url); await connect?.(url, state) },
      async close () { this.ws.readyState = 3 },
      subscribe (filters, handlers) {
        const sub = {
          url, filter: filters[0], handlers, closed: false, close (error) {
            if (this.closed) return
            this.closed = true
            handlers.onclose?.(error)
          }
        }
        state.subscriptions.push(sub)
        return sub
      }
    })
  })
  const readers = []
  const read = (urls = [A], options = {}, method = 'getLiveEventsGenerator') => {
    const stream = pool[method]({ kinds: [1] }, urls, { timeout: null, timeoutAfterFirstEose: null, timeoutForReconnectGap: null, ...options })
    const items = []
    const done = (async () => { for await (const item of stream) items.push(item) })()
    done.catch(() => {})
    const reader = { stream, items, done }
    readers.push(reader)
    return reader
  }
  t.after(async () => {
    for (const reader of readers) { await reader.stream.return(); await reader.done }
    await pool.disconnectAll()
  })
  return {
    state, pool, read,
    live: (url = A) => state.subscriptions.findLast(sub => sub.url === url && sub.filter.limit === 0),
    history: (url = A) => state.subscriptions.findLast(sub => sub.url === url && sub.filter.limit !== 0),
    async advance (ms) { await flush(); t.mock.timers.tick(ms); await flush() },
    online () { state.online = true; wake?.() }
  }
}

for (const message of ['blocked: denied', 'restricted: private', 'auth-required: sign in', 'pow: 20', 'invalid: filter', 'unknown rejection']) {
  test(`live ${message} ends the route, preserves its error and never starts reconnect history`, async t => {
    const f = fixture(t)
    const reader = f.read()
    await flush()
    const error = refusal(message)
    f.live().close(error)
    await reader.done
    assert.equal(reader.items.find(item => item.type === 'error').error, error)
    assert.equal((await reader.stream.ready).errors[0].reason, error)
    assert.equal(reader.items.find(item => item.type === 'eose').relays[0].status, 'error')
    await f.advance(600000)
    f.online(); await flush()
    assert.equal(f.state.subscriptions.length, 1)
    assert.equal(f.state.checks, 0)
    assert.equal(f.history(), undefined)
    const explicit = f.read()
    await flush()
    assert.equal(f.state.subscriptions.length, 2, 'a new explicit reader is a new operation')
    await explicit.stream.return()
  })
}

test('local authentication and validation failures are terminal even with a transport category', async t => {
  const f = fixture(t)
  for (const name of ['Nip42AuthenticationError', 'ValidationError']) {
    const reader = f.read()
    await flush()
    const error = Object.assign(new Error('local failure'), { name, category: 'connection' })
    f.live().close(error)
    await reader.done
    assert.equal(reader.items.find(item => item.type === 'error').error, error)
  }
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 2)
})

test('one refused relay leaves another ready relay live and preserves accepted events', async t => {
  const f = fixture(t)
  const reader = f.read([A, B])
  await flush()
  f.live(A).handlers.oneose(); f.live(B).handlers.oneose()
  f.live(A).handlers.onevent(event('accepted-a'))
  f.live(B).handlers.onevent(event('accepted-b'))
  f.live(A).close(refusal('blocked: denied'))
  await f.advance(60000)
  assert.deepEqual(reader.stream.readyRelays, [B])
  assert.equal(f.live(B).closed, false)
  assert.deepEqual(reader.items.filter(item => item.type === 'event').map(item => item.event.id), ['accepted-a', 'accepted-b'])
  assert.ok(reader.items.some(item => item.type === 'live-progress' && item.relay === B))
  assert.ok(!reader.items.some(item => item.type === 'live-progress' && item.relay === A))
  f.live(B).close(refusal('pow: 20'))
  await reader.done
  assert.equal(reader.stream.readyRelays.length, 0)
  assert.equal(f.state.subscriptions.length, 2)
})

test('rate limits use max(backoff, retryAt), avoid probes and retain their absolute deadline', async t => {
  const f = fixture(t)
  f.read()
  await flush()
  const first = Object.assign(refusal('rate-limited: busy'), { retryAt: 1005000, retryAfterMs: 5000 })
  f.live().close(first)
  await f.advance(1000)
  f.online(); await flush()
  await f.advance(3999)
  assert.equal(f.state.subscriptions.length, 1)
  await f.advance(1)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 2)
  const second = Object.assign(refusal('rate-limited: busy'), { retryAt: 1005500, retryAfterMs: 500 })
  f.live().close(second)
  await f.advance(1999)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 2)
  await f.advance(1)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 3, 'the two-second local backoff wins')
  assert.equal(f.state.checks, 0)
})

test('online failure backoff doubles and resets only after live and gap recovery succeed', async t => {
  const f = fixture(t)
  f.read()
  await flush()
  f.live().close(disconnected())
  await f.advance(1000)
  f.live().close(disconnected())
  await f.advance(1999)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 2)
  await f.advance(1)
  f.live().handlers.oneose()
  f.history().handlers.oneose()
  await flush()
  f.live().close(disconnected())
  await f.advance(999)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 3)
  await f.advance(1)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 4)
})

test('offline failures spend no backoff, no admission slot and share one online monitor', async t => {
  const f = fixture(t)
  const first = f.read([A])
  const second = f.read([B])
  await flush()
  f.state.online = false
  f.live(A).close(disconnected()); f.live(B).close(disconnected())
  await flush()
  assert.equal(f.state.monitors, 1)
  assert.equal(f.state.checks, 1, 'concurrent failures share their connectivity check')
  await first.stream.return(); await first.done
  assert.equal(f.state.stopped, 0, 'another reader still waits')
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 2)
  const localHistory = f.pool.getEvents({}, [B], { timeout: null })
  await flush()
  f.history(B).handlers.oneose()
  await localHistory
  f.online(); await flush()
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 3)
  f.live(B).close(disconnected())
  await f.advance(999)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 3)
  await f.advance(1)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 4)
  await second.stream.return()
  assert.equal(f.state.stopped, 1)
})

test('going offline during backoff delays the retry without advancing it again', async t => {
  const f = fixture(t)
  f.read()
  await flush()
  f.live().close(disconnected())
  await flush()
  f.state.online = false
  await f.advance(1000)
  assert.equal(f.state.subscriptions.length, 1)
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 1)
  f.online(); await flush()
  f.live().close(disconnected())
  await f.advance(1999)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 2)
  await f.advance(1)
  assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, 3)
})

test('permanent reconnect history failure drains original history and buffered live without certifying the gap', async t => {
  const f = fixture(t)
  const reader = f.read()
  await flush()
  f.live().handlers.oneose()
  f.live().close(disconnected())
  await f.advance(1000)
  f.live().handlers.oneose()
  f.live().handlers.onevent(event('buffered-live'))
  f.history().handlers.onevent(event('received-history'))
  const error = refusal('blocked: history denied')
  f.history().close(error)
  await reader.done
  assert.deepEqual(reader.items.filter(item => item.type === 'event').map(item => item.event.id), ['received-history', 'buffered-live'])
  assert.ok(reader.items.some(item => item.error === error))
  assert.ok(!reader.items.some(item => item.type === 'live-progress'))
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 3)
})

test('local admission failures have their own retry policy', async t => {
  const f = fixture(t, { capacity: { maxSubscriptionsPerRelay: 1, maxConcurrentHistoryPerRelay: 1, maxQueuedReadsPerRelay: 1 } })
  const holder = f.read()
  await flush()
  const waiting = f.read([A], { queueTimeout: 100 })
  await flush()
  const full = f.read([A], { queueTimeout: 100 })
  await flush()
  assert.ok(full.items.some(item => item.error?.code === 'RELAY_READ_QUEUE_FULL'))
  await f.advance(100)
  assert.ok(waiting.items.some(item => item.error?.code === 'RELAY_READ_QUEUE_TIMEOUT'))
  assert.equal(waiting.items.find(item => item.error?.code === 'RELAY_READ_QUEUE_TIMEOUT').error.category, 'timeout')
  await holder.stream.return()
  await f.advance(1000)
  assert.ok(f.state.subscriptions.length > 1)
  assert.equal(f.state.checks, 0, 'local queue pressure does not probe internet')
})

test('impossible atomic reconnect capacity ends the route without retry', async t => {
  const f = fixture(t, { capacity: { maxSubscriptionsPerRelay: 1 } })
  const reader = f.read()
  await flush()
  f.live().handlers.oneose(); f.live().close(disconnected())
  await f.advance(1000)
  await reader.done
  assert.ok(reader.items.some(item => item.error?.code === 'RELAY_READ_CAPACITY'))
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 1)
})

test('a consumer can cancel a shared connectivity probe without cancelling another consumer', async () => {
  const pending = Promise.withResolvers()
  let probeSignal
  const recovery = new ReadRetry({ checkOnline: ({ signal }) => { probeSignal = signal; return pending.promise } })
  const first = new AbortController()
  const second = new AbortController()
  const a = recovery.confirmOnline(first.signal)
  const b = recovery.confirmOnline(second.signal)
  await flush()
  first.abort()
  await assert.rejects(a, { name: 'AbortError' })
  assert.equal(probeSignal.aborted, false)
  pending.resolve(true)
  assert.equal(await b, true)
  assert.equal(probeSignal.aborted, true)
})

test('connection authentication failure resolves readiness and releases the route without retry', async t => {
  const error = Object.assign(new Error('local signing failed'), { name: 'Nip42AuthenticationError' })
  const f = fixture(t, { connect: () => { throw error } })
  const reader = f.read()
  await reader.done
  assert.equal((await reader.stream.ready).errors[0].reason, error)
  await f.advance(600000)
  assert.equal(f.state.connects.length, 1)
  assert.equal(f.state.subscriptions.length, 0)
  assert.equal(f.state.checks, 0)
})

test('cancelling all readers cancels the shared probe and ignores its late response', async t => {
  const pending = Promise.withResolvers()
  let probeSignal
  const f = fixture(t, { check: ({ signal }) => { probeSignal = signal; return pending.promise } })
  const first = f.read([A])
  const second = f.read([B])
  await flush()
  f.live(A).close(disconnected()); f.live(B).close(disconnected())
  await flush()
  await first.stream.return()
  assert.equal(probeSignal.aborted, false)
  await second.stream.return()
  await flush()
  assert.equal(probeSignal.aborted, true)
  pending.resolve(true)
  await f.advance(600000)
  assert.equal(f.state.subscriptions.length, 2)
  assert.equal(f.state.monitors, 0)
})

test('live retries remain exponential and capped at five minutes without an attempt limit', async t => {
  const f = fixture(t)
  f.read()
  await flush()
  let delay = 1000
  for (let step = 0; step < 12; step++) {
    const count = f.state.subscriptions.filter(sub => sub.filter.limit === 0).length
    f.live().close(refusal('error: transient relay fault'))
    await f.advance(delay - 1)
    assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, count)
    await f.advance(1)
    assert.equal(f.state.subscriptions.filter(sub => sub.filter.limit === 0).length, count + 1)
    delay = Math.min(delay * 2, 300000)
  }
  assert.equal(delay, 300000)
  assert.equal(f.state.checks, 0)
})
