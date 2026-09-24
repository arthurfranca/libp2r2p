import assert from 'node:assert/strict'
import { test } from 'node:test'
import { RelayPool } from '../relay/index.js'

const A = 'wss://a.example'
const B = 'wss://b.example'
const tick = () => new Promise(resolve => setImmediate(resolve))
const event = Object.freeze({ id: 'immutable', kind: 1, created_at: 1, tags: [], content: '' })

function fixture (t) {
  const relays = new Map()
  const pool = new RelayPool({
    _createRelay: url => {
      const relay = {
        ws: { readyState: 1 }, subs: [],
        async connect () {},
        async close () { this.ws.readyState = 3 },
        subscribe (filters, handlers) {
          const sub = {
            filters, handlers, closed: false, close () {
              if (this.closed) return
              this.closed = true
              handlers.onclose?.()
            }
          }
          this.subs.push(sub)
          return sub
        }
      }
      relays.set(url.replace(/\/$/, ''), relay)
      return relay
    }
  })
  t.after(() => pool.disconnectAll())
  return { pool, sub: (url = A) => relays.get(url).subs.at(-1), relays }
}

async function collect (stream) {
  const items = []
  for await (const item of stream) items.push(item)
  return items
}

for (const mode of ['eose', 'satisfied', 'closed', 'error', 'timeout', 'cutoff']) {
  test(`one-shot reports ${mode} without mutating events`, { timeout: 2000 }, async t => {
    const { pool, sub } = fixture(t)
    const items = []
    const filter = mode === 'satisfied' ? { limit: 1 } : {}
    const query = pool.getEvents(filter, mode === 'cutoff' ? [A, B] : [A, `${A}/`], {
      timeout: 25, timeoutAfterFirstEose: mode === 'cutoff' ? 5 : null,
      callback: item => items.push(item)
    })
    await tick()
    sub().handlers.onevent(event)
    if (mode === 'eose' || mode === 'cutoff') sub().handlers.oneose()
    if (mode === 'closed') sub().close()
    if (mode === 'error') sub().handlers.onclose(new Error('denied'))
    const result = await query
    assert.equal(result.relays.at(-1).status, mode)
    assert.equal(items.at(-1).type, 'eose')
    assert.deepEqual(items.at(-1).relays, result.relays)
    assert.equal(items.filter(item => item.type === 'eose').length, 1)
    assert.deepEqual(result.result, [{ event, relay: A }])
    assert.equal(result.result[0].event, event)
    assert.equal('meta' in event, false)
    assert.equal(items.filter(item => item.type === 'error').length, ['error', 'timeout'].includes(mode) ? 1 : 0)
    sub().handlers.onevent(event)
    assert.equal(result.result.length, 1)
  })
}

for (const method of ['getEventsGenerator', 'getLiveEventsGenerator', 'getEventsFeedGenerator']) {
  test(`${method} emits an empty initial completion`, async t => {
    const { pool } = fixture(t)
    assert.deepEqual(await collect(pool[method]({}, [])), [{ type: 'eose', relays: [] }])
  })
  test(`${method} does not emit completion after caller abort`, async t => {
    const { pool } = fixture(t)
    const controller = new AbortController()
    const items = collect(pool[method]({}, [A], { signal: controller.signal }))
    await tick()
    controller.abort()
    assert.deepEqual(await items, [])
  })
}

test('a callback failure rejects the operation and closes subscriptions', async t => {
  const { pool, sub } = fixture(t)
  const failed = pool.getEvents({}, [A], { callback: () => { throw new Error('callback failure') } })
  await tick()
  sub().handlers.onevent(event)
  await assert.rejects(failed, /callback failure/)
  assert.ok(sub().closed)
})

test('generator propagates invalid arguments and callback failures', async t => {
  const { pool, sub } = fixture(t)
  await assert.rejects(pool.getEventsGenerator(null, []).next(), { code: 'INVALID_FILTER' })
  await assert.rejects(pool.getEventsGenerator({}, [], { timeout: -1 }).next(), { code: 'INVALID_RELAY_TIMEOUT' })
  const stream = pool.getEventsGenerator({}, [A], { callback: () => { throw new Error('callback failure') } })
  const first = stream.next()
  await tick()
  sub().handlers.onevent(event)
  assert.equal((await first).value.type, 'event')
  await assert.rejects(stream.next(), /callback failure/)
  assert.ok(sub().closed)
})

test('live initial timeout reports an error and keeps the subscription recoverable', { timeout: 2000 }, async t => {
  const { pool, sub } = fixture(t)
  const stream = pool.getLiveEventsGenerator({}, [A], { timeout: 20 })
  t.after(() => stream.return())
  const first = await stream.next()
  assert.equal(first.value.type, 'error')
  assert.equal(first.value.error.message, 'GET_EVENTS_TIMEOUT')
  const marker = (await stream.next()).value
  assert.equal(marker.type, 'eose')
  assert.equal(marker.relays[0].status, 'timeout')
  const ready = await stream.ready
  assert.equal(ready.errors[0].reason, first.value.error)
  assert.equal(sub().closed, false)
  sub().handlers.oneose()
  assert.deepEqual(stream.readyRelays, [A])
  sub().handlers.onevent(event)
  assert.deepEqual((await stream.next()).value, { type: 'event', event, relay: A })
})

test('live events can precede aggregate completion and cutoff does not claim EOSE', { timeout: 2000 }, async t => {
  const { pool, sub } = fixture(t)
  const stream = pool.getLiveEventsGenerator({}, [A, B], { timeoutAfterFirstEose: 10 })
  t.after(() => stream.return())
  const first = stream.next()
  await tick()
  sub(A).handlers.oneose()
  sub(A).handlers.onevent(event)
  assert.equal((await first).value.type, 'event')
  assert.deepEqual((await stream.next()).value, {
    type: 'eose', relays: [{ relay: A, status: 'eose' }, { relay: B, status: 'cutoff' }]
  })
  assert.deepEqual((await stream.ready).relays, [A])
  sub(B).handlers.oneose()
  assert.deepEqual(stream.readyRelays, [A, B])
  sub(B).handlers.onevent({ ...event, id: 'second' })
  assert.equal((await stream.next()).value.type, 'event')
})

test('live errors remain observable after the initial readiness window', async t => {
  const { pool, sub } = fixture(t)
  const stream = pool.getLiveEventsGenerator({}, [A])
  t.after(() => stream.return())
  const first = stream.next()
  await tick()
  sub().handlers.oneose()
  assert.equal((await first).value.type, 'eose')
  const error = new Error('disconnected')
  sub().handlers.onclose(error)
  assert.deepEqual((await stream.next()).value, { type: 'error', relay: A, error })
})

test('live normal close emits an interruption and retains closed readiness status', async t => {
  const { pool, sub } = fixture(t)
  const stream = pool.getLiveEventsGenerator({}, [A])
  t.after(() => stream.return())
  const first = stream.next()
  await tick()
  sub().close()
  const interruption = (await first).value
  assert.equal(interruption.type, 'error')
  assert.equal(interruption.error.code, 'RELAY_LIVE_INTERRUPTED')
  assert.deepEqual((await stream.next()).value, { type: 'eose', relays: [{ relay: A, status: 'closed', error: interruption.error }] })
})

test('feed delivers history, its single completion, then buffered live events', async t => {
  const { pool, relays } = fixture(t)
  const controller = new AbortController()
  const received = []
  const stream = pool.getEventsFeedGenerator({}, [A], { signal: controller.signal })
  const consumption = (async () => { for await (const item of stream) received.push(item) })()
  await tick()
  const [live, history] = relays.get(A).subs
  live.handlers.oneose()
  live.handlers.onevent({ ...event, id: 'live' })
  history.handlers.onevent(event)
  history.handlers.oneose()
  await tick()
  controller.abort()
  await consumption
  assert.deepEqual(received.map(item => item.type), ['event', 'eose', 'event'])
  assert.equal(received[0].event.id, 'immutable')
  assert.equal(received[2].event.id, 'live')
})

test('feed forwards initial errors and propagates general live generator failures', async t => {
  const { pool } = fixture(t)
  const error = new Error('relay rejected')
  const history = async function * () { yield { type: 'error', relay: A, error }; yield { type: 'eose', relays: [{ relay: A, status: 'error', error }] } }
  const live = async function * () { throw new Error('live failed') }
  const stream = pool.getEventsFeedGenerator({}, [A], { _eventsGenerator: history, _liveGenerator: live })
  assert.equal((await stream.next()).value.type, 'error')
  assert.equal((await stream.next()).value.type, 'eose')
  await assert.rejects(stream.next(), /live failed/)
})

test('reconnection updates readiness without repeating initial completion', { timeout: 3000 }, async t => {
  const { pool, sub, relays } = fixture(t)
  const controller = new AbortController()
  const stream = pool.getLiveEventsGenerator({}, [A], { signal: controller.signal })
  const items = []
  const reading = (async () => { for await (const item of stream) items.push(item) })()
  t.after(async () => { controller.abort(); await reading })
  await tick()
  sub().handlers.oneose()
  await stream.ready
  sub().close()
  assert.deepEqual(stream.readyRelays, [])
  await new Promise(resolve => setTimeout(resolve, 1100))
  const resumed = relays.get(A).subs.findLast(sub => sub.filters[0].limit === 0)
  resumed.handlers.oneose()
  sub().handlers.oneose() // recovered history precedes the queued live event
  resumed.handlers.onevent(event)
  await tick()
  assert.deepEqual(stream.readyRelays, [A])
  assert.equal(items.filter(item => item.type === 'eose').length, 1)
  assert.equal(items.filter(item => item.type === 'event').length, 1)
})

test('stopping a live initial attempt resolves readiness without fabricating EOSE', async t => {
  const { pool } = fixture(t)
  const stream = pool.getLiveEventsGenerator({}, [A], { timeout: null })
  const items = collect(stream)
  await tick()
  stream.stopAndDrain()
  assert.deepEqual(await items, [])
  assert.deepEqual(await stream.ready, { relays: [], errors: [] })
})
