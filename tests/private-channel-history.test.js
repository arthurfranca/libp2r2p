import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readHistory } from '../private-channel/helpers/history.js'
import { createAbortableSemaphore } from '../helpers/abortable-semaphore.js'
import { RelayPool } from '../relay/index.js'
import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'

const relay = 'wss://history.example'
const event = (time, index = time) => ({ id: index.toString(16).padStart(64, '0'), created_at: time, kind: 3560, content: 'ciphertext' })
const reader = (events, requests = []) => async (filter, relays, options) => {
  requests.push({ ...filter })
  const result = events.filter(event => event.created_at >= filter.since && event.created_at <= filter.until)
    .sort((a, b) => b.created_at - a.created_at || a.id.localeCompare(b.id)).slice(0, filter.limit).map(event => ({ event, relay: relays[0] }))
  result.forEach(({ event }) => options.callback?.({ type: 'event', event }))
  return { result, errors: [], relays: [{ relay: relays[0], status: result.length === filter.limit ? 'satisfied' : 'eose' }] }
}
const run = options => readHistory({ filter: { kinds: [3560], since: 0, until: 100 }, relays: [relay], processEvent: async () => {}, ...options })
const tick = () => new Promise(resolve => setImmediate(resolve))

test('bounded history subdivides saturated intervals and processes each leaf before reading again', async () => {
  const events = Array.from({ length: 65 }, (_, index) => event(index))
  const processed = []
  const requests = []
  const summary = await run({ getEvents: reader(events, requests), processEvent: async event => { await tick(); processed.push(event.created_at) } })
  assert.deepEqual(processed, events.map(event => event.created_at))
  assert.equal(summary.receivedEventCount, 65)
  assert.equal(summary.oldestCreatedAt, 0)
  assert.ok(requests.length > 1)
  assert.ok(requests.every(request => request.limit === 16))
})

test('a saturated second expands safely without skipping equal timestamps', async () => {
  const events = Array.from({ length: 40 }, (_, index) => event(7, index))
  const requests = []
  const processed = []
  await run({ filter: { since: 7, until: 7 }, getEvents: reader(events, requests), processEvent: event => processed.push(event.id) })
  assert.deepEqual(requests.map(request => request.limit), [16, 32, 64])
  assert.deepEqual(processed, events.map(event => event.id))
})

test('a saturated second at the safe ceiling remains explicitly incomplete', async () => {
  const requests = []
  await assert.rejects(run({ filter: { since: 1, until: 1 }, getEvents: reader(Array.from({ length: 300 }, (_, index) => event(1, index)), requests) }), error => {
    assert.equal(error.code, 'PRIVATE_CHANNEL_FETCH_INCOMPLETE')
    assert.equal(error.operation, 'private-channel.fetchHistory')
    assert.equal(error.errors[0].code, 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT')
    return true
  })
  assert.deepEqual(requests.map(request => request.limit), [16, 32, 64, 128, 256])
})

test('satisfied with fewer valid events still cannot prove a complete interval', async () => {
  let calls = 0
  await assert.rejects(run({
    filter: { since: 1, until: 1 }, getEvents: async () => {
      calls++
      return { result: [], relays: [{ relay, status: 'satisfied' }] }
    }
  }), error => error.errors[0].code === 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT')
  assert.equal(calls, 5)
})

test('bounds page bytes during callbacks and also for readers without callbacks', async () => {
  for (const callbacks of [true, false]) {
    await assert.rejects(run({
      getEvents: async (filter, relays, options) => {
        const huge = { ...event(1), content: 'x'.repeat(4 * 1024 * 1024) }
        if (callbacks) options.callback({ type: 'event', event: huge })
        return { result: [{ event: huge }], relays: [{ relay, status: 'eose' }] }
      }
    }), error => error.errors[0].code === 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT')
  }
})

test('preserves partial deliveries and per-relay status-only failures', async () => {
  const processed = []
  await assert.rejects(run({ relays: [relay, 'wss://failed.example'], getEvents: async (filter, [url]) => ({ result: [{ event: event(url === relay ? 1 : 2), relay: url }], relays: [{ relay: url, status: url === relay ? 'eose' : 'timeout' }] }), processEvent: event => processed.push(event.created_at) }), error => {
    assert.deepEqual(error.relays.map(item => item.status), ['eose', 'timeout'])
    assert.deepEqual(error.errors, [])
    assert.equal(error.receivedEventCount, 2)
    return true
  })
  assert.deepEqual(processed, [1, 2])
})

test('partial history reports per-relay coverage instead of failing', async () => {
  const summary = await run({
    relays: [relay, 'wss://failed.example'],
    partial: true,
    getEvents: async (filter, [url]) => ({
      result: url === relay ? [{ event: event(1), relay: url }] : [],
      relays: [{ relay: url, status: url === relay ? 'eose' : 'timeout' }]
    })
  })
  const byRelay = Object.fromEntries(summary.relays.map(entry => [entry.relay, entry]))

  assert.equal(summary.anyEose, true)
  assert.equal(summary.anyEoseWithEvents, true)
  assert.equal(summary.allFailed, false)
  assert.ok(Number.isInteger(summary.relays[0].elapsedMs))
  assert.deepEqual(byRelay[relay].covered, [{ start: 0, end: 100 }])
  assert.deepEqual(byRelay[relay].pending, [])
  assert.deepEqual(byRelay['wss://failed.example'].pending, [{ start: 0, end: 100 }])
  assert.deepEqual(summary.pendingByRelay, { 'wss://failed.example': [{ start: 0, end: 100 }] })
})

test('partial history retries only resumed subranges and merges coverage', async () => {
  const requests = []
  const summary = await run({
    partial: true,
    resume: { [relay]: [{ start: 40, end: 60 }] },
    getEvents: async filter => {
      requests.push({ since: filter.since, until: filter.until })
      return { result: [{ event: event(50), relay }], relays: [{ relay, status: 'eose' }] }
    }
  })

  assert.deepEqual(requests, [{ since: 40, until: 60 }])
  assert.deepEqual(summary.relays[0].covered, [{ start: 40, end: 60 }])
  assert.deepEqual(summary.relays[0].pending, [])
})

test('partial history marks all-failed attempts for the seeder handoff', async () => {
  const summary = await run({
    relays: [relay, 'wss://other.example'],
    partial: true,
    getEvents: async (filter, [url]) => ({ result: [], relays: [{ relay: url, status: 'timeout' }] })
  })

  assert.equal(summary.anyEose, false)
  assert.equal(summary.anyEoseWithEvents, false)
  assert.equal(summary.allFailed, true)
  assert.deepEqual(summary.pendingByRelay, {
    [relay]: [{ start: 0, end: 100 }],
    'wss://other.example': [{ start: 0, end: 100 }]
  })
})

test('one relay gate spans processing and queued cancellation releases the waiter', async () => {
  const entered = Promise.withResolvers()
  const finish = Promise.withResolvers()
  let reads = 0
  const getEvents = (...args) => { reads++; return reader([event(1)])(...args) }
  const first = run({ getEvents, processEvent: async () => { entered.resolve(); await finish.promise } })
  await entered.promise
  const controller = new AbortController()
  const second = run({ signal: controller.signal, getEvents })
  await tick()
  assert.equal(reads, 1)
  controller.abort()
  await assert.rejects(second, { name: 'AbortError' })
  finish.resolve()
  await first
  await run({ getEvents })
  assert.equal(reads, 2)
})

test('a messenger gate admits at most two pages, including persistence work', async () => {
  const gate = createAbortableSemaphore(2)
  const finish = Promise.withResolvers()
  let active = 0
  let peak = 0
  const pending = Array.from({ length: 5 }, (_, index) => run({ relays: [`wss://history-${index}.example`], getEvents: reader([event(1)]), acquirePage: signal => gate.acquire(signal), processEvent: async () => { active++; peak = Math.max(peak, active); await finish.promise; active-- } }))
  await tick()
  assert.equal(active, 2)
  finish.resolve()
  await Promise.all(pending)
  assert.equal(peak, 2)
})

test('storage failures and cancellation release both admission gates for retries', async () => {
  const gate = createAbortableSemaphore(2)
  const options = { getEvents: reader([event(1)]), acquirePage: signal => gate.acquire(signal) }
  await assert.rejects(run({ ...options, processEvent: () => { throw new Error('SAVE_FAILED') } }), /SAVE_FAILED/)
  const controller = new AbortController()
  await assert.rejects(run({ ...options, signal: controller.signal, processEvent: () => controller.abort() }), { name: 'AbortError' })
  assert.equal((await run(options)).receivedEventCount, 1)
})

test('pagination exercises the real relay reader with signed network-boundary fixtures', async t => {
  const secret = generateSecretKey()
  const events = Array.from({ length: 35 }, (_, index) => finalizeEvent({ kind: 3560, created_at: index, tags: [], content: 'history' }, secret))
  const requests = []
  class Socket {
    constructor () { this.readyState = 0; queueMicrotask(() => { this.readyState = 1; this.onopen?.() }) }
    send (raw) {
      const frame = JSON.parse(raw)
      if (frame[0] !== 'REQ') return
      requests.push(frame[2])
      reader(events)(frame[2], [relay], {}).then(page => {
        for (const { event } of page.result) this.onmessage?.({ data: JSON.stringify(['EVENT', frame[1], event]) })
        this.onmessage?.({ data: JSON.stringify(['EOSE', frame[1]]) })
      })
    }
    close () { this.readyState = 3; this.onclose?.({ code: 1000, reason: '', wasClean: true }) }
  }
  const pool = new RelayPool({ WebSocket: Socket })
  t.after(() => pool.disconnectAll())
  const received = []
  const result = await run({ getEvents: (...args) => pool.getEvents(...args), processEvent: event => received.push(event.id) })
  assert.equal(result.receivedEventCount, events.length)
  assert.deepEqual(received, events.map(event => event.id))
  assert.ok(requests.length > 1)
})

test('admission failure preserves relay diagnostics and releases acquired capacity', async () => {
  const reason = Object.assign(new Error('PRIVATE_CHANNEL_HISTORY_QUEUE_FULL'), { code: 'PRIVATE_CHANNEL_HISTORY_QUEUE_FULL' })
  await assert.rejects(run({ getEvents: reader([]), acquirePage: () => { throw reason } }), error => {
    assert.equal(error.code, 'PRIVATE_CHANNEL_FETCH_INCOMPLETE')
    assert.equal(error.relays[0].relay, relay)
    assert.equal(error.errors[0], reason)
    return true
  })
  assert.equal((await run({ getEvents: reader([]) })).receivedEventCount, 0)
})
