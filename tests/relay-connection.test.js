import { test } from 'node:test'
import assert from 'node:assert/strict'

import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'
import { RelayPool } from '../relay/services/relay-pool.js'
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

function signedEvent (properties = {}) {
  return finalizeEvent({ kind: 1, created_at: 1, tags: [['t', 'x']], content: '', ...properties }, generateSecretKey())
}

async function connected () {
  const relay = new RelayConnection('wss://relay.example/', { WebSocket: FakeWebSocket })
  const promise = relay.connect()
  FakeWebSocket.instances.at(-1).open()
  await promise
  return { relay, socket: FakeWebSocket.instances.at(-1) }
}

test('RelayConnection validates and filters subscription events', async () => {
  const { relay, socket } = await connected()
  const valid = []
  const invalid = []
  const subscription = relay.subscribe([{ kinds: [1], '#t': ['x'] }], {
    onevent: event => valid.push(event),
    oninvalidevent: event => invalid.push(event)
  })
  socket.receive(['EVENT', subscription.id, signedEvent()])
  socket.receive(['EVENT', subscription.id, { ...signedEvent(), content: 'mutated' }])
  socket.receive(['EVENT', subscription.id, signedEvent({ kind: 2 })])
  await tick()
  assert.equal(valid.length, 1)
  assert.equal(invalid.length, 2)
  subscription.close()
  assert.equal(socket.sent.at(-1)[0], 'CLOSE')
})

test('RelayConnection keeps publish, AUTH and COUNT operations separate', async () => {
  const { relay, socket } = await connected()
  const published = signedEvent()
  const publish = relay.publish(published)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await publish, 'saved')

  socket.receive(['AUTH', 'challenge'])
  await tick()
  const authEvent = signedEvent({ kind: 22242 })
  const auth = relay.authenticate(async ({ challenge }) => {
    assert.equal(challenge, 'challenge')
    return authEvent
  })
  await tick()
  socket.receive(['OK', authEvent.id, true, 'authenticated'])
  assert.equal(await auth, 'authenticated')

  const count = relay.countWithHll([{ kinds: [1] }])
  const id = socket.sent.find(message => message[0] === 'COUNT')[1]
  socket.receive(['COUNT', id, { count: 3, hll: '00' }])
  assert.deepEqual(await count, { count: 3, hll: '00' })
})

test('RelayConnection rejects pending work on close and can reopen', async () => {
  const { relay, socket } = await connected()
  const pending = relay.publish(signedEvent())
  socket.close()
  await assert.rejects(pending, /closed/i)
  const reconnect = relay.connect()
  const replacement = FakeWebSocket.instances.at(-1)
  assert.notEqual(replacement, socket)
  replacement.open()
  await reconnect
  await relay.close()
})

test('rate-limit metadata delays new work but never CLOSE, and cancellation removes delayed REQs', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const active = relay.subscribe([{ kinds: [1] }])
  let rejection
  const limited = relay.subscribe([{ kinds: [2] }], { onclose: error => { rejection = error } })
  socket.receive(['CLOSED', limited.id, 'rate-limited: too many messages', { retry_after: 2 }])
  await tick()
  assert.equal(rejection.retryAfterMs, 2000)
  assert.equal(rejection.retryAt, 12000)
  assert.equal(rejection.category, 'relay')
  const queued = relay.subscribe([{ kinds: [3] }])
  const cancelled = relay.subscribe([{ kinds: [4] }])
  cancelled.close()
  active.close()
  assert.deepEqual(socket.sent.at(-1), ['CLOSE', active.id])
  const before = socket.sent.length
  t.mock.timers.tick(1999)
  assert.equal(socket.sent.length, before)
  t.mock.timers.tick(1)
  assert.deepEqual(socket.sent.at(-1), ['REQ', queued.id, { kinds: [3] }])
  assert.ok(!socket.sent.some(frame => frame[1] === cancelled.id))
})

test('publication retry_after is preserved and delaying work is bounded by its deadline', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const event = signedEvent()
  const first = relay.publish(event)
  const rejected = assert.rejects(first, error => error.retryAfterMs === 300000 && error.message === 'rate-limited: busy')
  socket.receive(['OK', event.id, false, 'rate-limited: busy', { retry_after: 999999 }])
  await tick()
  await rejected
  relay.publishTimeout = 10
  const next = assert.rejects(relay.publish(signedEvent()), /PUBLISH_TIMEOUT/)
  t.mock.timers.tick(10)
  await next
  t.mock.timers.tick(300000)
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('malformed or unrelated retry metadata cannot stall a connection', async t => {
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  for (const [reason, retry] of [['blocked: no', 60], ['rate-limited: busy', '60'], ['rate-limited: busy', -1]]) {
    let rejection
    const sub = relay.subscribe([{}], { onclose: error => { rejection = error } })
    socket.receive(['CLOSED', sub.id, reason, { retry_after: retry }])
    await tick()
    assert.equal(rejection.retryAfterMs, undefined)
    const next = relay.subscribe([{}])
    assert.equal(socket.sent.at(-1)[1], next.id)
    next.close()
  }
})

test('pool operation timeout cancels a publication waiting for relay cooldown', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const event = signedEvent()
  const first = pool.sendEvent(event, ['wss://relay.example'])
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', event.id, false, 'rate-limited: busy', { retry_after: 2 }])
  await first
  const second = pool.sendEvent(signedEvent(), ['wss://relay.example'], { timeout: 50 })
  await tick()
  t.mock.timers.tick(50)
  const report = await (await second).promise
  assert.equal(report.success, false)
  assert.equal(report.errors[0].reason.category, 'timeout')
  t.mock.timers.tick(2000)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('delayed CLOSED uses retry_at and expired OK advice never restarts its relative wait', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 14000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  let rejection
  const first = relay.subscribe([{}], { onclose: error => { rejection = error } })
  socket.receive(['CLOSED', first.id, 'rate-limited: busy', { retry_after: 10, retry_at: 20 }])
  await tick()
  assert.equal(rejection.retryAt, 20000)
  const next = relay.subscribe([{}])
  t.mock.timers.tick(5999)
  assert.ok(!socket.sent.some(frame => frame[1] === next.id))
  t.mock.timers.tick(1)
  assert.equal(socket.sent.at(-1)[1], next.id)
  t.mock.timers.tick(5000)
  const published = signedEvent()
  const result = assert.rejects(relay.publish(published), error => error.retryAt === 20000 && error.retryAfterMs === 10000)
  socket.receive(['OK', published.id, false, 'rate-limited: busy', { retry_after: 10, retry_at: 20 }])
  await result
  const immediate = relay.subscribe([{}])
  assert.equal(socket.sent.at(-1)[1], immediate.id)
})
