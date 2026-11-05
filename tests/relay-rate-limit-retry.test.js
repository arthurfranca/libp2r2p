import { test } from 'node:test'
import assert from 'node:assert/strict'

import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'
import { RelayConnection } from '../relay/services/relay-connection.js'
import { RelayPool } from '../relay/services/relay-pool.js'

class FakeWebSocket {
  static instances = []

  constructor (url) {
    this.url = url
    this.readyState = 0
    this.sent = []
    FakeWebSocket.instances.push(this)
  }

  open () { this.readyState = 1; this.onopen?.() }
  receive (message) { this.onmessage?.({ data: JSON.stringify(message) }) }
  send (message) { this.sent.push(JSON.parse(message)) }
  close () { this.readyState = 3; queueMicrotask(() => this.onclose?.({ reason: 'closed' })) }
}

const tick = () => new Promise(resolve => setImmediate(resolve))

function event (kind = 1) {
  return finalizeEvent({ kind, created_at: 1, tags: [], content: '' }, generateSecretKey())
}

function hll (entries = {}) {
  const registers = new Uint8Array(256)
  for (const [index, value] of Object.entries(entries)) registers[Number(index)] = value
  return [...registers].map(value => value.toString(16).padStart(2, '0')).join('')
}

test('sendEvent retries one rate limit by default without exposing the intermediate error', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const debug = t.mock.method(console, 'debug', () => {})
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const callbacks = []
  const published = event()
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    timeout: 1000,
    onRelayResult: result => callbacks.push(result)
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '200'])
  await tick()
  assert.deepEqual(callbacks, [])
  t.mock.timers.tick(199)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  const result = await work
  assert.equal(result.success, true)
  assert.deepEqual(callbacks, [{ relay: 'wss://relay.example', success: true, outcome: 'published' }])
  assert.equal(debug.mock.callCount(), 1)
  assert.deepEqual(debug.mock.calls[0].arguments, [
    'relay rate-limit retry',
    {
      relay: 'wss://relay.example',
      operation: 'EVENT',
      retryAttempt: 1,
      maxRetries: 1,
      retryAt: 10200,
      delayMs: 200,
      effectiveWaitMs: 200,
      maxWaitMs: 1500
    }
  ])
})

test('sendEvent exposes only the final error after exhausting the configured retry', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const callbacks = []
  const published = event()
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    timeout: 1000,
    onRelayResult: result => callbacks.push(result)
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', published.id, false, 'rate-limited: first', '100'])
  await tick()
  t.mock.timers.tick(100)
  await tick()
  socket.receive(['OK', published.id, false, 'rate-limited: final', '100'])
  const result = await work
  const report = await result.promise
  assert.equal(result.success, false)
  assert.equal(report.errors.length, 1)
  assert.equal(report.errors[0].reason.message, 'rate-limited: final')
  assert.equal(callbacks.length, 1)
  assert.equal(callbacks[0].success, false)
})

test('per-call retry zero and a deadline before retryAt disable automatic retry', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket, rateLimitRetries: 2 })
  t.after(() => pool.disconnectAll())

  const noRetry = event()
  const first = pool.sendEvent(noRetry, ['wss://no-retry.example'], { rateLimitRetries: 0, timeout: 1000 })
  const noRetrySocket = FakeWebSocket.instances.at(-1)
  noRetrySocket.open()
  await tick()
  noRetrySocket.receive(['OK', noRetry.id, false, 'rate-limited: wait', '100'])
  assert.equal((await first).success, false)
  assert.equal(noRetrySocket.sent.filter(frame => frame[0] === 'EVENT').length, 1)

  const tooLate = event()
  const second = pool.sendEvent(tooLate, ['wss://too-late.example'], { timeout: 50 })
  const tooLateSocket = FakeWebSocket.instances.at(-1)
  tooLateSocket.open()
  await tick()
  tooLateSocket.receive(['OK', tooLate.id, false, 'rate-limited: wait', '200'])
  assert.equal((await second).success, false)
  t.mock.timers.tick(200)
  await tick()
  assert.equal(tooLateSocket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('retry wait gates produce an immediate final failure by default', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const published = event()
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    timeout: 5000,
    onRelayResult: () => {}
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '2000'])
  const result = await work
  const report = await result.promise
  assert.equal(result.success, false)
  assert.equal(report.errors[0].reason.retryAfterMs, 2000)
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('per-call max wait and null can override the default gate', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())

  const raised = event()
  const first = pool.sendEvent(raised, ['wss://raised.example'], {
    maxRateLimitRetryWaitMs: 2500,
    timeout: 5000
  })
  const firstSocket = FakeWebSocket.instances.at(-1)
  firstSocket.open()
  await tick()
  firstSocket.receive(['OK', raised.id, false, 'rate-limited: wait', '2000'])
  await tick()
  t.mock.timers.tick(1999)
  await tick()
  assert.equal(firstSocket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(firstSocket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  firstSocket.receive(['OK', raised.id, true, 'saved'])
  assert.equal((await first).success, true)

  const unlimited = event()
  const second = pool.sendEvent(unlimited, ['wss://unlimited.example'], {
    maxRateLimitRetryWaitMs: null,
    timeout: 5000
  })
  const secondSocket = FakeWebSocket.instances.at(-1)
  secondSocket.open()
  await tick()
  secondSocket.receive(['OK', unlimited.id, false, 'rate-limited: wait', '4000'])
  await tick()
  t.mock.timers.tick(4000)
  await tick()
  assert.equal(secondSocket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  secondSocket.receive(['OK', unlimited.id, true, 'saved'])
  assert.equal((await second).success, true)
})

test('the retry gate uses the maximum active cooldown, not only the latest hint', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const relay = new RelayConnection('wss://relay.example/', { WebSocket: FakeWebSocket })
  const connecting = relay.connect()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await connecting
  t.after(() => relay.close())

  const firstEvent = event()
  const secondEvent = event()
  const first = assert.rejects(relay.publish(firstEvent), /rate-limited/)
  const second = assert.rejects(
    relay.publish(secondEvent, { rateLimitRetries: 1 }),
    error => error.retryAt === 10100 && error.effectiveRetryAt === 15000
  )
  socket.receive(['OK', firstEvent.id, false, 'rate-limited: long', '5000'])
  await first

  const startedAt = Date.now()
  socket.receive(['OK', secondEvent.id, false, 'rate-limited: short', '100'])
  await second
  assert.equal(Date.now() - startedAt, 0)
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
})

test('a first ACK aborts only scheduled retries from other relays', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const published = event()
  const work = pool.sendEvent(published, ['wss://limited.example', 'wss://healthy.example'], { timeout: 1000 })
  const [limitedSocket, healthySocket] = FakeWebSocket.instances
  limitedSocket.open()
  healthySocket.open()
  await tick()
  limitedSocket.receive(['OK', published.id, false, 'rate-limited: wait', '500'])
  healthySocket.receive(['OK', published.id, true, 'saved'])
  const result = await work
  assert.equal(result.success, true)
  const report = await result.promise
  assert.equal(report.errors.length, 1)
  assert.equal(report.errors[0].relay, 'wss://limited.example')
  assert.equal(report.errors[0].reason.message, 'rate-limited: wait')
  t.mock.timers.tick(500)
  await tick()
  assert.equal(limitedSocket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('per-call retries can enable retry above a zero pool default', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket, rateLimitRetries: 0 })
  t.after(() => pool.disconnectAll())
  const published = event()
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    rateLimitRetries: 1,
    timeout: 1000
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  await tick()
  t.mock.timers.tick(100)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal((await work).success, true)
})

test('bulk retry preserves its queue position ahead of later work', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const firstEvent = event()
  const secondEvent = event()
  const firstWork = pool.sendEvent(firstEvent, ['wss://relay.example'], { useBulkPacing: true, timeout: 2000 })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', firstEvent.id, false, 'rate-limited: wait', '100'])
  await tick()
  const secondWork = pool.sendEvent(secondEvent, ['wss://relay.example'], { useBulkPacing: true, timeout: 2000 })
  t.mock.timers.tick(100)
  await tick()
  const events = socket.sent.filter(frame => frame[0] === 'EVENT')
  assert.equal(events.length, 2)
  assert.equal(events[1][1].id, firstEvent.id)
  socket.receive(['OK', firstEvent.id, true, 'saved'])
  assert.equal((await firstWork).success, true)
  t.mock.timers.tick(1000)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT')[2][1].id, secondEvent.id)
  socket.receive(['OK', secondEvent.id, true, 'saved'])
  assert.equal((await secondWork).success, true)
})

test('rate-limited AUTH has an independent retry and renews its signed event', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const published = event()
  let authCalls = 0
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    timeout: 1000,
    getAuthEvent: async () => { authCalls++; return event(22242) }
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  socket.receive(['AUTH', 'challenge'])
  await tick()
  socket.receive(['OK', published.id, false, 'auth-required: authenticate'])
  await tick()
  const firstAuth = socket.sent.find(frame => frame[0] === 'AUTH')
  socket.receive(['OK', firstAuth[1].id, false, 'rate-limited: auth', '200'])
  await tick()
  t.mock.timers.tick(200)
  await tick()
  await tick()
  const authFrames = socket.sent.filter(frame => frame[0] === 'AUTH')
  assert.equal(authFrames.length, 2)
  socket.receive(['OK', authFrames[1][1].id, true, 'authenticated'])
  await tick()
  const eventFrames = socket.sent.filter(frame => frame[0] === 'EVENT')
  assert.equal(eventFrames.length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal((await work).success, true)
  assert.equal(authCalls, 2)
})

test('AUTH retry wait gate fails without signing another event', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const published = event()
  let authCalls = 0
  const work = pool.sendEvent(published, ['wss://relay.example'], {
    maxRateLimitRetryWaitMs: 100,
    timeout: 1000,
    getAuthEvent: async () => {
      authCalls++
      return event(22242)
    }
  })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  socket.receive(['AUTH', 'challenge'])
  await tick()
  socket.receive(['OK', published.id, false, 'auth-required: authenticate'])
  await tick()
  const firstAuth = socket.sent.find(frame => frame[0] === 'AUTH')
  socket.receive(['OK', firstAuth[1].id, false, 'rate-limited: auth', '200'])
  const result = await work
  assert.equal(result.success, false)
  assert.equal(authCalls, 1)
  assert.equal(socket.sent.filter(frame => frame[0] === 'AUTH').length, 1)
})

test('first ACK cancels a waiting AUTH retry with its original rate-limit error', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const published = event()
  const work = pool.sendEvent(published, ['wss://limited.example', 'wss://healthy.example'], {
    timeout: 2000,
    getAuthEvent: async () => event(22242)
  })
  const [limitedSocket, healthySocket] = FakeWebSocket.instances
  limitedSocket.open()
  healthySocket.open()
  limitedSocket.receive(['AUTH', 'challenge'])
  await tick()
  limitedSocket.receive(['OK', published.id, false, 'auth-required: authenticate'])
  await tick()
  const authFrame = limitedSocket.sent.find(frame => frame[0] === 'AUTH')
  limitedSocket.receive(['OK', authFrame[1].id, false, 'rate-limited: auth wait', '500'])
  await tick()
  healthySocket.receive(['OK', published.id, true, 'saved'])
  const result = await work
  assert.equal(result.success, true)
  const report = await result.promise
  const limited = report.errors.find(error => error.relay === 'wss://limited.example')
  assert.equal(limited.reason.message, 'rate-limited: auth wait')
  assert.equal(limited.reason.name, 'Error')
  t.mock.timers.tick(500)
  await tick()
  assert.equal(limitedSocket.sent.filter(frame => frame[0] === 'AUTH').length, 1)
})

test('getEvents retries a rate-limited relay without exposing the intermediate error', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const callbacks = []
  const work = pool.getEvents({ kinds: [1] }, ['wss://relay.example'], {
    timeout: 1000,
    callback: item => callbacks.push(item)
  })
  await tick()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  const firstId = socket.sent.find(frame => frame[0] === 'REQ')[1]
  socket.receive(['CLOSED', firstId, 'rate-limited: wait', '200'])
  await tick()
  assert.equal(callbacks.some(item => item.type === 'error'), false)
  t.mock.timers.tick(200)
  await tick()
  const retryId = socket.sent.filter(frame => frame[0] === 'REQ')[1][1]
  socket.receive(['EVENT', retryId, event()])
  socket.receive(['EOSE', retryId])
  const result = await work
  assert.equal(result.success, true)
  assert.deepEqual(result.errors, [])
  assert.equal(callbacks.some(item => item.type === 'error'), false)
})

test('getEvents and countEvents honor the per-call retry wait gate', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())

  const read = pool.getEvents({ kinds: [1] }, ['wss://read.example'], {
    maxRateLimitRetryWaitMs: 50,
    timeout: 1000
  })
  await tick()
  const readSocket = FakeWebSocket.instances.at(-1)
  readSocket.open()
  await tick()
  const readId = readSocket.sent.find(frame => frame[0] === 'REQ')[1]
  readSocket.receive(['CLOSED', readId, 'rate-limited: slow', '100'])
  const readResult = await read
  assert.equal(readResult.errors[0].reason.retryAfterMs, 100)
  assert.equal(readSocket.sent.filter(frame => frame[0] === 'REQ').length, 1)

  const count = pool.countEvents({ kinds: [1] }, ['wss://count.example'], {
    maxRateLimitRetryWaitMs: 50,
    timeout: 1000
  })
  await tick()
  const countSocket = FakeWebSocket.instances.at(-1)
  countSocket.open()
  await tick()
  const countId = countSocket.sent.find(frame => frame[0] === 'COUNT')[1]
  countSocket.receive(['CLOSED', countId, 'rate-limited: slow', '100'])
  const countResult = await count
  assert.equal(countResult.errors[0].reason.retryAfterMs, 100)
  assert.equal(countSocket.sent.filter(frame => frame[0] === 'COUNT').length, 1)
})

test('getEventsGenerator suppresses an intermediate retry error and returns the successful report', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const callbacks = []
  const stream = pool.getEventsGenerator({ kinds: [1] }, ['wss://relay.example'], {
    timeout: 1000,
    callback: item => callbacks.push(item)
  })
  const first = stream.next()
  await tick()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  const firstId = socket.sent.find(frame => frame[0] === 'REQ')[1]
  socket.receive(['CLOSED', firstId, 'rate-limited: wait', '200'])
  await tick()
  assert.equal(callbacks.some(item => item.type === 'error'), false)
  t.mock.timers.tick(200)
  await tick()
  const retryId = socket.sent.filter(frame => frame[0] === 'REQ')[1][1]
  const received = event()
  socket.receive(['EVENT', retryId, received])
  socket.receive(['EOSE', retryId])
  assert.deepEqual((await first).value, { type: 'event', event: received, relay: 'wss://relay.example' })
  assert.equal((await stream.next()).value.type, 'eose')
  const completion = await stream.next()
  assert.equal(completion.done, true)
  assert.deepEqual(completion.value.errors, [])
  assert.equal(completion.value.result.length, 1)
  assert.equal(callbacks.some(item => item.type === 'error'), false)
})

test('feed live:false disables the generic one-shot retry', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const items = []
  const done = (async () => {
    for await (const item of pool.getEventsFeedGenerator({ kinds: [1] }, ['wss://relay.example'], {
      live: false,
      timeout: 1000
    })) items.push(item)
  })()
  await tick()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  const request = socket.sent.find(frame => frame[0] === 'REQ')
  socket.receive(['CLOSED', request[1], 'rate-limited: no generic retry', '100'])
  await done
  assert.deepEqual(items.map(item => item.type), ['error', 'eose'])
  assert.equal(items[0].error.message, 'rate-limited: no generic retry')
  t.mock.timers.tick(100)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'REQ').length, 1)
})

test('countEvents retries rate limits and preserves final aggregation', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const work = pool.countEvents({ kinds: [1] }, ['wss://relay.example'], { timeout: 1000 })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  const firstId = socket.sent.find(frame => frame[0] === 'COUNT')[1]
  socket.receive(['CLOSED', firstId, 'rate-limited: wait', '100'])
  await tick()
  t.mock.timers.tick(100)
  await tick()
  const retryId = socket.sent.filter(frame => frame[0] === 'COUNT')[1][1]
  socket.receive(['COUNT', retryId, { count: 7 }])
  const result = await work
  assert.equal(result.success, true)
  assert.equal(result.count, 7)
  assert.deepEqual(result.errors, [])
})

test('countEvents merges HLL and exact counts after an individual relay retry', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const work = pool.countEvents({ kinds: [1] }, ['wss://limited.example', 'wss://healthy.example'], {
    timeout: 1000,
    timeoutAfterFirstCount: 1000
  })
  const limitedSocket = FakeWebSocket.instances[0]
  const healthySocket = FakeWebSocket.instances[1]
  limitedSocket.open()
  healthySocket.open()
  await tick()
  const limitedId = limitedSocket.sent.find(frame => frame[0] === 'COUNT')[1]
  limitedSocket.receive(['CLOSED', limitedId, 'rate-limited: wait', '100'])
  const healthyId = healthySocket.sent.find(frame => frame[0] === 'COUNT')[1]
  healthySocket.receive(['COUNT', healthyId, { count: 5, hll: hll({ 0: 1, 1: 1 }) }])
  await tick()
  t.mock.timers.tick(100)
  await tick()
  const retryId = limitedSocket.sent.filter(frame => frame[0] === 'COUNT')[1][1]
  limitedSocket.receive(['COUNT', retryId, { count: 7, hll: hll({ 0: 2, 2: 1 }) }])
  const result = await work
  assert.equal(result.success, true)
  assert.equal(result.count, 7)
  assert.equal(result.hllCount, 3)
  assert.deepEqual(result.errors, [])
})

test('connection-local retry advice grows from 1s to 1.5s then 2s and resets after success', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  FakeWebSocket.instances.length = 0
  const relay = new RelayConnection('wss://relay.example/', { WebSocket: FakeWebSocket })
  const connecting = relay.connect()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await connecting
  t.after(() => relay.close())

  const firstEvent = event()
  const first = relay.publish(firstEvent, { rateLimitRetries: 2, deadlineAt: 100000 })
  socket.receive(['OK', firstEvent.id, false, 'rate-limited: no advice'])
  await tick()
  t.mock.timers.tick(999)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)

  socket.receive(['OK', firstEvent.id, false, 'rate-limited: no advice'])
  await tick()
  t.mock.timers.tick(1499)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 3)
  socket.receive(['OK', firstEvent.id, true, 'saved'])
  await first

  const secondEvent = event()
  const rejected = assert.rejects(relay.publish(secondEvent), error => error.retryAfterMs === 1000)
  socket.receive(['OK', secondEvent.id, false, 'rate-limited: no advice'])
  await rejected
})
