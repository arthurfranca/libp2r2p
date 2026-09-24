import assert from 'node:assert/strict'
import { test } from 'node:test'
import { RelayPool } from '../relay/index.js'

const A = 'wss://a.example'
const B = 'wss://b.example'
const NOW = 1700000000
const tick = () => new Promise(resolve => setImmediate(resolve))
const event = (id, createdAt = Math.floor(Date.now() / 1000)) => Object.freeze({ id, created_at: createdAt, kind: 1, tags: [], content: '' })

function fixture (t) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: NOW * 1000 })
  const relays = new Map()
  const pool = new RelayPool({
    _createRelay: url => {
      const relay = {
        ws: { readyState: 1 }, subs: [],
        async connect () {},
        async close () { this.ws.readyState = 3 },
        subscribe (filters, handlers) {
          const sub = {
            filters, handlers, closed: false, close (error) {
              if (this.closed) return
              this.closed = true
              handlers.onclose?.(error)
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
  const streams = []
  t.after(async () => {
    for (const { stream, done } of streams) { await stream.return(); await done }
    await pool.disconnectAll()
  })
  function read (method = 'getLiveEventsGenerator', filter = {}, urls = [A], options = {}) {
    const stream = pool[method](filter, urls, { timeout: null, timeoutAfterFirstEose: null, timeoutForReconnectGap: null, ...options })
    const items = []
    const done = (async () => { for await (const item of stream) items.push(item) })()
    done.catch(() => {})
    streams.push({ stream, done })
    return { stream, items, done }
  }
  const live = (url = A) => relays.get(url).subs.findLast(sub => sub.filters[0].limit === 0)
  const history = (url = A) => relays.get(url).subs.findLast(sub => sub.filters[0].limit !== 0)
  const advance = async ms => { t.mock.timers.tick(ms); await tick() }
  return { pool, read, live, history, advance, relays }
}

const progress = items => items.filter(item => item.type === 'live-progress')

test('initial live accepts a late arrival with a timestamp before connection opening', async t => {
  const { read, live } = fixture(t)
  const { items } = read()
  await tick()
  assert.deepEqual(live().filters, [{ since: NOW - 600, limit: 0 }])
  live().handlers.oneose()
  const late = event('late-arrival', NOW - 60)
  assert.ok(late.created_at >= live().filters[0].since)
  live().handlers.onevent(late)
  await tick()
  assert.equal(items.find(item => item.type === 'event').event, late)
  assert.equal('meta' in late, false)
})

test('progress follows queued events, uses receipt time, and advances while empty', async t => {
  const { read, live, advance } = fixture(t)
  const { items } = read()
  await tick()
  await advance(60000)
  assert.equal(progress(items).length, 0, 'no EOSE yet')
  live().handlers.oneose()
  live().handlers.onevent(event('future', NOW + 100000))
  await advance(59999)
  assert.equal(progress(items).length, 0)
  await advance(1)
  assert.deepEqual(items.map(item => item.type), ['eose', 'event', 'live-progress'])
  const first = progress(items)[0]
  assert.deepEqual(first, { type: 'live-progress', relay: A, epoch: 1, since: NOW + 60, until: NOW + 120 })
  await advance(60000)
  assert.deepEqual(progress(items)[1], { ...first, until: NOW + 180 })
})

test('a slow relay does not prevent progress for a ready relay', async t => {
  const { read, live, advance } = fixture(t)
  const { items } = read('getLiveEventsGenerator', {}, [A, B])
  await tick()
  live(A).handlers.oneose()
  await advance(60000)
  assert.deepEqual(progress(items).map(item => item.relay), [A])
  assert.equal(items.some(item => item.type === 'eose'), false)
  live(B).handlers.oneose()
  await advance(60000)
  assert.deepEqual(progress(items).map(item => item.relay), [A, A, B])
})

test('feed holds progress behind history and earlier live events', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items } = read('getEventsFeedGenerator', {}, [A], { snapshot: true })
  await tick()
  live().handlers.oneose()
  await tick()
  live().handlers.onevent(event('buffered'))
  await advance(60000)
  assert.deepEqual(items, [])
  history().handlers.onevent(event('stored'))
  history().handlers.oneose()
  await tick()
  assert.deepEqual(items.map(item => item.type), ['event', 'eose', 'event', 'live-progress'])
  assert.equal(items[2].event.id, 'buffered')
  assert.equal(items[3].until, NOW + 60)
})

test('limit-zero feed forwards automatic progress', async t => {
  const { read, live, advance } = fixture(t)
  const { items } = read('getEventsFeedGenerator', { limit: 0 })
  await tick()
  live().handlers.oneose()
  await advance(60000)
  assert.deepEqual(items.map(item => item.type), ['eose', 'live-progress'])
})

test('empty relay lists and history-only streams never emit progress', async t => {
  const { read, history, advance } = fixture(t)
  const empty = read('getLiveEventsGenerator', {}, [])
  await empty.done
  const historical = read('getEventsFeedGenerator', {}, [A], { live: false })
  await tick()
  history().handlers.oneose()
  await historical.done
  await advance(120000)
  assert.equal(progress([...empty.items, ...historical.items]).length, 0)
})

for (const action of ['return', 'abort', 'drain']) {
  test(`no progress or interruption error after caller ${action}`, async t => {
    const { read, live, advance } = fixture(t)
    const controller = new AbortController()
    const { items, stream, done } = read('getLiveEventsGenerator', {}, [A], { signal: controller.signal })
    await tick()
    live().handlers.oneose()
    await advance(60000)
    assert.equal(progress(items).length, 1)
    if (action === 'return') await stream.return()
    else if (action === 'abort') controller.abort()
    else stream.stopAndDrain()
    await done
    await advance(180000)
    assert.equal(progress(items).length, 1)
    assert.equal(items.some(item => item.type === 'error'), false)
    assert.equal(live().closed, true)
  })
}

test('empty relay reconnect recovers overlap and starts a separate epoch after recovery', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items } = read()
  await tick()
  const original = live()
  original.handlers.oneose()
  await advance(60000)
  original.close()
  await tick()
  assert.equal(items.at(-1).error.code, 'RELAY_LIVE_INTERRUPTED')
  await advance(1000)
  assert.equal(history().filters[0].since, NOW - 600)
  assert.equal(live().filters[0].since, NOW + 61 - 600)
  live().handlers.oneose()
  await advance(60000)
  assert.equal(progress(items).length, 1, 'recovery has not completed')
  history().handlers.oneose()
  await tick()
  await advance(60000)
  assert.deepEqual(progress(items)[1], { type: 'live-progress', relay: A, epoch: 2, since: NOW + 61, until: NOW + 181 })
  assert.equal(items.filter(item => item.type === 'eose').length, 1)
  original.handlers.oneose()
  original.handlers.onevent(event('obsolete'))
  await tick()
  assert.equal(items.some(item => item.event?.id === 'obsolete'), false)
})

test('recovery cursors are per relay and include cross-relay duplicates', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items } = read('getLiveEventsGenerator', {}, [A, B])
  await tick()
  for (const url of [A, B]) live(url).handlers.oneose()
  const duplicate = event('both', NOW - 300)
  for (const url of [A, B]) live(url).handlers.onevent(duplicate)
  live(A).handlers.onevent(event('only-a', NOW))
  await tick()
  assert.equal(items.filter(item => item.event?.id === 'both').length, 1)
  live(B).close()
  await advance(1000)
  assert.equal(history(B).filters[0].since, NOW - 900)
  live(A).close()
  await advance(1000)
  assert.equal(history(A).filters[0].since, NOW - 600)
})

test('future timestamps cannot move a recovery cursor past their local receipt time', async t => {
  const { read, live, history, advance } = fixture(t)
  read()
  await tick()
  live().handlers.oneose()
  live().handlers.onevent(event('future', NOW + 86400))
  await advance(10000)
  live().close()
  await advance(1000)
  assert.equal(history().filters[0].since, NOW - 600)
})

test('overlap respects explicit since and until and stops progress at until', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items, done } = read('getLiveEventsGenerator', { since: NOW - 10, until: NOW + 90 })
  await tick()
  assert.equal(live().filters[0].since, NOW - 10)
  assert.equal(live().filters[0].until, NOW + 90)
  live().handlers.oneose()
  live().close()
  await advance(1000)
  assert.equal(history().filters[0].since, NOW - 10)
  assert.ok(history().filters[0].until <= NOW + 90)
  live().handlers.oneose()
  history().handlers.oneose()
  await tick()
  await advance(60000)
  assert.equal(progress(items).length, 1)
  await advance(30000)
  await done
  const count = progress(items).length
  await advance(60000)
  assert.equal(progress(items).length, count)
})

test('failed recovery never certifies progress on that connection', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items } = read()
  await tick()
  live().handlers.oneose()
  live().close()
  await advance(1000)
  live().handlers.oneose()
  history().close(new Error('recovery rejected'))
  await advance(60000)
  assert.equal(progress(items).length, 0)
  assert.ok(items.some(item => item.error?.message === 'recovery rejected'))
})

test('a failed gap retains its baseline even after newer live deliveries', async t => {
  const { read, live, history, advance } = fixture(t)
  read()
  await tick()
  live().handlers.oneose()
  live().handlers.onevent(event('before', NOW - 100))
  live().close()
  await advance(1000)
  const firstSince = history().filters[0].since
  live().handlers.oneose()
  live().handlers.onevent(event('during-recovery', NOW + 1))
  history().close(new Error('recovery rejected'))
  await tick()
  live().close()
  await advance(2000)
  assert.equal(history().filters[0].since, firstSince)
})

test('recovery completion waits for live EOSE before emitting progress', async t => {
  const { read, live, history, advance } = fixture(t)
  const { items } = read()
  await tick()
  live().handlers.oneose()
  live().close()
  await advance(1000)
  history().handlers.oneose()
  await advance(60000)
  assert.equal(progress(items).length, 0)
  live().handlers.oneose()
  await advance(60000)
  assert.equal(progress(items).length, 1)
  assert.equal(progress(items)[0].since, NOW + 61)
})

for (const method of ['getLiveEventsGenerator', 'getEventsFeedGenerator']) {
  test(`${method} keeps automatic progress subject to buffer limits`, async t => {
    const { pool, live, advance } = fixture(t)
    const stream = pool[method]({ limit: 0 }, [A], { maxBufferedLiveEvents: 1 })
    t.after(() => stream.return())
    const first = stream.next()
    await tick()
    live().handlers.oneose()
    assert.equal((await first).value.type, 'eose')
    await advance(60000)
    await advance(60000)
    await assert.rejects(stream.next(), { code: 'RELAY_LIVE_BUFFER_FULL' })
    assert.equal(live().closed, true)
  })
}

test('duplicate EOSE and a backward clock never restart or regress progress', async t => {
  const { read, live, advance } = fixture(t)
  const { items } = read()
  await tick()
  live().handlers.oneose()
  await advance(30000)
  live().handlers.oneose()
  await advance(30000)
  assert.equal(progress(items).length, 1)
  assert.equal(progress(items)[0].since, NOW)
  t.mock.timers.setTime((NOW - 120) * 1000)
  await advance(60000)
  assert.equal(progress(items).length, 1)
})
