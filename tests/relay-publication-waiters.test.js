import { test } from 'node:test'
import assert from 'node:assert/strict'

import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'
import { RelayConnection } from '../relay/services/relay-connection.js'

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

async function fixture (t) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  t.mock.method(console, 'debug', () => {})
  FakeWebSocket.instances.length = 0
  const relay = new RelayConnection('wss://relay.example/', { WebSocket: FakeWebSocket })
  const connecting = relay.connect()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await connecting
  t.after(() => relay.close())
  return { relay, socket }
}

function eventFrames (socket, id) {
  return socket.sent.filter(frame => frame[0] === 'EVENT' && (!id || frame[1].id === id))
}

test('interactive waiters for one event share a single physical publication', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const first = relay.publish(published)
  const second = relay.publish(published)
  assert.equal(eventFrames(socket, published.id).length, 1)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await first, 'saved')
  assert.equal(await second, 'saved')
})

test('bulk waiters for one event share one queue position and one send', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const first = relay.publish(published, { useBulkPacing: true })
  const second = relay.publish(published, { useBulkPacing: true })
  assert.equal(eventFrames(socket, published.id).length, 1)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await first, 'saved')
  assert.equal(await second, 'saved')
})

test('one waiter timeout does not cancel another waiter for the same job', async t => {
  const { relay, socket } = await fixture(t)
  relay.publishTimeout = 1000
  const published = event()
  const first = assert.rejects(relay.publish(published), /PUBLISH_TIMEOUT/)
  t.mock.timers.tick(500)
  const second = relay.publish(published)
  t.mock.timers.tick(500)
  await first
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await second, 'saved')
})

test('interactive joins an active bulk publication without a second send', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const bulk = relay.publish(published, { useBulkPacing: true })
  const interactive = relay.publish(published)
  assert.equal(eventFrames(socket, published.id).length, 1)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await bulk, 'saved')
  assert.equal(await interactive, 'saved')
})

test('interactive joins an active bulk publication already waiting for retry', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const bulk = relay.publish(published, { useBulkPacing: true, rateLimitRetries: 1 })
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  await tick()
  const interactive = relay.publish(published, { rateLimitRetries: 0 })
  t.mock.timers.tick(100)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await bulk, 'saved')
  assert.equal(await interactive, 'saved')
})

test('interactive does not join a queued bulk publication', async t => {
  const { relay, socket } = await fixture(t)
  const blocker = event()
  const published = event()
  const blocking = relay.publish(blocker, { useBulkPacing: true })
  const bulk = relay.publish(published, { useBulkPacing: true })
  const interactive = relay.publish(published)
  assert.equal(eventFrames(socket, published.id).length, 1)

  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await interactive, 'saved')
  assert.equal(await bulk, 'saved')
  assert.equal(eventFrames(socket, published.id).length, 1)

  socket.receive(['OK', blocker.id, true, 'saved'])
  assert.equal(await blocking, 'saved')
  t.mock.timers.tick(1000)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 1)
})

test('bulk joins an existing interactive publication without entering the queue', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const interactive = relay.publish(published)
  const bulk = relay.publish(published, { useBulkPacing: true })
  assert.equal(eventFrames(socket, published.id).length, 1)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await interactive, 'saved')
  assert.equal(await bulk, 'saved')
})

test('waiters apply independent rate-limit retry budgets', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const noRetry = assert.rejects(
    relay.publish(published, { rateLimitRetries: 0 }),
    /rate-limited/
  )
  const retrying = relay.publish(published, { rateLimitRetries: 1 })
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  await noRetry
  t.mock.timers.tick(100)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await retrying, 'saved')
})

test('a waiter deadline can fail independently while another retries', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const expired = assert.rejects(
    relay.publish(published, { rateLimitRetries: 1, deadlineAt: 10050 }),
    /rate-limited/
  )
  const retrying = relay.publish(published, { rateLimitRetries: 1, deadlineAt: 10200 })
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  await expired
  t.mock.timers.tick(100)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await retrying, 'saved')
})

test('a late zero-retry waiter can share an already scheduled retry', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const first = relay.publish(published, { rateLimitRetries: 1 })
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  await tick()
  const late = relay.publish(published, { rateLimitRetries: 0 })
  t.mock.timers.tick(100)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await first, 'saved')
  assert.equal(await late, 'saved')
})

test('aborting one retry signal leaves another waiter retrying', async t => {
  const { relay, socket } = await fixture(t)
  const published = event()
  const firstRetry = new AbortController()
  const secondRetry = new AbortController()
  const first = relay.publish(published, {
    rateLimitRetries: 1,
    retrySignal: firstRetry.signal
  })
  const second = relay.publish(published, {
    rateLimitRetries: 1,
    retrySignal: secondRetry.signal
  })
  socket.receive(['OK', published.id, false, 'rate-limited: wait', '100'])
  const rejected = assert.rejects(first, /rate-limited/)
  firstRetry.abort(new Error('first call got another ACK'))
  await rejected
  t.mock.timers.tick(100)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await second, 'saved')
})

test('interactive rejection does not fail a queued bulk waiter', async t => {
  const { relay, socket } = await fixture(t)
  const blocker = event()
  const published = event()
  const blocking = relay.publish(blocker, { useBulkPacing: true })
  const bulk = relay.publish(published, { useBulkPacing: true })
  const interactive = relay.publish(published)
  const rejected = assert.rejects(interactive, /invalid/)
  socket.receive(['OK', published.id, false, 'invalid: rejected once'])
  await rejected

  socket.receive(['OK', blocker.id, true, 'saved'])
  await blocking
  t.mock.timers.tick(1000)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await bulk, 'saved')
})

test('success settles two already transmitted jobs and ignores a late rejection', async t => {
  const { relay, socket } = await fixture(t)
  const blocker = event()
  const published = event()
  const blocking = relay.publish(blocker, { useBulkPacing: true })
  const bulk = relay.publish(published, { useBulkPacing: true })
  const interactive = relay.publish(published)
  socket.receive(['OK', blocker.id, true, 'saved'])
  await blocking
  t.mock.timers.tick(1000)
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)

  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await bulk, 'saved')
  assert.equal(await interactive, 'saved')
  socket.receive(['OK', published.id, false, 'invalid: late'])
  await tick()
  assert.equal(eventFrames(socket, published.id).length, 2)
})

test('identical AUTH events share one physical send', async t => {
  const { relay, socket } = await fixture(t)
  socket.receive(['AUTH', 'challenge'])
  await tick()
  const authEvent = event(22242)
  const first = relay.authenticate(async () => authEvent, { rateLimitRetries: 0 })
  const second = relay.authenticate(async () => authEvent, { rateLimitRetries: 0 })
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'AUTH').length, 1)
  socket.receive(['OK', authEvent.id, true, 'authenticated'])
  assert.equal(await first, 'authenticated')
  assert.equal(await second, 'authenticated')
})

test('AUTH retry budgets remain independent after a shared physical attempt', async t => {
  const { relay, socket } = await fixture(t)
  socket.receive(['AUTH', 'challenge'])
  await tick()
  const authEvent = event(22242)
  const noRetry = assert.rejects(
    relay.authenticate(async () => authEvent, { rateLimitRetries: 0 }),
    /rate-limited/
  )
  let secondAuthCalls = 0
  const retrying = relay.authenticate(async () => {
    secondAuthCalls++
    return authEvent
  }, { rateLimitRetries: 1 })
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'AUTH').length, 1)
  socket.receive(['OK', authEvent.id, false, 'rate-limited: auth', '100'])
  await noRetry
  t.mock.timers.tick(100)
  await tick()
  assert.equal(secondAuthCalls, 2)
  assert.equal(socket.sent.filter(frame => frame[0] === 'AUTH').length, 2)
  socket.receive(['OK', authEvent.id, true, 'authenticated'])
  assert.equal(await retrying, 'authenticated')
})
