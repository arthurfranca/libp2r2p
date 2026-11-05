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
  socket.receive(['CLOSED', limited.id, 'rate-limited: too many messages', '2000'])
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

test('publication relative params is preserved and delaying work is bounded by its deadline', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const event = signedEvent()
  const first = relay.publish(event)
  const rejected = assert.rejects(first, error => error.retryAfterMs === 300000 && error.message === 'rate-limited: busy')
  socket.receive(['OK', event.id, false, 'rate-limited: busy', '999999000'])
  await tick()
  await rejected
  relay.publishTimeout = 10
  const next = assert.rejects(relay.publish(signedEvent()), /PUBLISH_TIMEOUT/)
  t.mock.timers.tick(10)
  await next
  t.mock.timers.tick(300000)
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
})

test('bulk publications are serial and paced per relay', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const firstEvent = signedEvent()
  const secondEvent = signedEvent()
  await assert.rejects(relay.publish(signedEvent(), { useBulkPacing: 'yes' }), { code: 'INVALID_USE_BULK_PACING' })

  const first = relay.publish(firstEvent, { useBulkPacing: true })
  const second = relay.publish(secondEvent, { useBulkPacing: true })
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)

  socket.receive(['OK', firstEvent.id, true, 'saved'])
  assert.equal(await first, 'saved')
  t.mock.timers.tick(999)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  socket.receive(['OK', secondEvent.id, true, 'saved'])
  assert.equal(await second, 'saved')
})

test('cancelling a queued bulk publication releases its queue position', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const firstEvent = signedEvent()
  const cancelledEvent = signedEvent()
  const lastEvent = signedEvent()
  const cancelled = new AbortController()
  const reason = new DOMException('cancelled', 'AbortError')

  const first = relay.publish(firstEvent, { useBulkPacing: true })
  const rejected = assert.rejects(relay.publish(cancelledEvent, { useBulkPacing: true, signal: cancelled.signal }), error => error === reason)
  const last = relay.publish(lastEvent, { useBulkPacing: true })
  cancelled.abort(reason)
  await rejected
  socket.receive(['OK', firstEvent.id, true, 'saved'])
  await first
  t.mock.timers.tick(1000)
  await tick()
  const publishedIds = socket.sent.filter(frame => frame[0] === 'EVENT').map(frame => frame[1].id)
  assert.deepEqual(publishedIds, [firstEvent.id, lastEvent.id])
  socket.receive(['OK', lastEvent.id, true, 'saved'])
  assert.equal(await last, 'saved')
})

test('bulk backoff grows, yields to earlier relay advice and resets after success', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const events = Array.from({ length: 6 }, () => signedEvent())

  const first = assert.rejects(relay.publish(events[0], { useBulkPacing: true }), /rate-limited/)
  const second = assert.rejects(relay.publish(events[1], { useBulkPacing: true }), /rate-limited/)
  const third = assert.rejects(relay.publish(events[2], { useBulkPacing: true }), /rate-limited/)
  const fourth = relay.publish(events[3], { useBulkPacing: true })
  const fifth = assert.rejects(relay.publish(events[4], { useBulkPacing: true }), /rate-limited/)
  const sixth = relay.publish(events[5], { useBulkPacing: true })
  await tick()
  socket.receive(['OK', events[0].id, false, 'rate-limited: busy'])
  await first

  t.mock.timers.tick(1000)
  await tick()
  socket.receive(['OK', events[1].id, false, 'rate-limited: busy'])
  await second

  t.mock.timers.tick(2000)
  await tick()
  socket.receive(['OK', events[2].id, false, 'rate-limited: busy', '100'])
  await third

  const sentBefore = socket.sent.filter(frame => frame[0] === 'EVENT').length
  t.mock.timers.tick(999)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, sentBefore)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, sentBefore + 1)
  socket.receive(['OK', events[3].id, true, 'saved'])
  assert.equal(await fourth, 'saved')
  t.mock.timers.tick(1000)
  await tick()
  socket.receive(['OK', events[4].id, false, 'rate-limited: busy'])
  await fifth
  t.mock.timers.tick(999)
  await tick()
  const afterFailure = socket.sent.filter(frame => frame[0] === 'EVENT').length
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, afterFailure + 1)
  socket.receive(['OK', events[5].id, true, 'saved'])
  assert.equal(await sixth, 'saved')
})

test('runtime rate-limit cap updates future errors and clamps existing cooldowns', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  let capMs = 5000
  const relay = new RelayConnection('wss://relay.example/', {
    WebSocket: FakeWebSocket,
    rateLimitCapMs: () => capMs
  })
  const connection = relay.connect()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await connection
  t.after(() => relay.close())

  let rejection
  const first = relay.subscribe([{}], { onclose: error => { rejection = error } })
  socket.receive(['CLOSED', first.id, 'rate-limited: busy', '10000'])
  await tick()
  assert.equal(rejection.retryAfterMs, 5000)

  const queued = relay.subscribe([{}])
  relay.setRateLimitCapMs(500)
  t.mock.timers.tick(499)
  assert.ok(!socket.sent.some(frame => frame[1] === queued.id))
  t.mock.timers.tick(1)
  assert.ok(socket.sent.some(frame => frame[1] === queued.id))

  capMs = 2000
  let nextRejection
  const second = relay.subscribe([{}], { onclose: error => { nextRejection = error } })
  socket.receive(['CLOSED', second.id, 'rate-limited: busy', '10000'])
  await tick()
  assert.equal(nextRejection.retryAfterMs, 2000)
})

test('the runtime cap is applied to publication, AUTH and COUNT rejections', async () => {
  const relay = new RelayConnection('wss://relay.example/', {
    WebSocket: FakeWebSocket,
    rateLimitCapMs: () => 1500
  })
  const connection = relay.connect()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await connection

  const event = signedEvent()
  const published = assert.rejects(relay.publish(event), error => error.retryAfterMs === 1500)
  socket.receive(['OK', event.id, false, 'rate-limited: busy', '5000'])
  await published

  socket.receive(['AUTH', 'challenge'])
  await tick()
  const authEvent = signedEvent({ kind: 22242 })
  const authenticated = assert.rejects(relay.authenticate(async () => authEvent), error => error.retryAfterMs === 1500)
  socket.receive(['OK', authEvent.id, false, 'rate-limited: busy', '5000'])
  await authenticated

  const counted = assert.rejects(relay.countWithHll([{ kinds: [1] }]), error => error.retryAfterMs === 1500)
  const id = socket.sent.find(message => message[0] === 'COUNT')[1]
  socket.receive(['CLOSED', id, 'rate-limited: busy', '5000'])
  await counted
  await relay.close()
})

test('a concurrent success resets local backoff without clearing active relay advice', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const limitedEvent = signedEvent()
  const concurrentEvent = signedEvent()
  const futureEvent = signedEvent()
  const limited = assert.rejects(relay.publish(limitedEvent), /rate-limited/)
  const concurrent = relay.publish(concurrentEvent)
  socket.receive(['OK', limitedEvent.id, false, 'rate-limited: wait', '5000'])
  await limited
  socket.receive(['OK', concurrentEvent.id, true, 'saved'])
  await concurrent

  const future = relay.publish(futureEvent)
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  t.mock.timers.tick(4999)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 3)
  socket.receive(['OK', futureEvent.id, true, 'saved'])
  await future
})

test('a shorter concurrent hint does not shorten an existing relay deadline', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const firstEvent = signedEvent()
  const secondEvent = signedEvent()
  const futureEvent = signedEvent()
  const first = assert.rejects(relay.publish(firstEvent), /rate-limited/)
  const second = assert.rejects(relay.publish(secondEvent), /rate-limited/)
  socket.receive(['OK', firstEvent.id, false, 'rate-limited: first', '5000'])
  await first
  socket.receive(['OK', secondEvent.id, false, 'rate-limited: second', '1000'])
  await second

  const future = relay.publish(futureEvent)
  t.mock.timers.tick(1000)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 2)
  t.mock.timers.tick(4000)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 3)
  socket.receive(['OK', futureEvent.id, true, 'saved'])
  await future
})

test('RelayPool options affect existing connections before consumers observe rate limits', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const pool = new RelayPool({ WebSocket: FakeWebSocket, rateLimitCapMs: 5000, rateLimitRetries: 0 })
  t.after(() => pool.disconnectAll())
  const limited = signedEvent()
  const first = pool.sendEvent(limited, ['wss://relay.example'])
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', limited.id, false, 'rate-limited: busy', '10000'])
  const rejected = await first
  const report = await rejected.promise
  assert.equal(report.errors[0].reason.retryAfterMs, 5000)

  pool.setOptions({ rateLimitCapMs: 500 })
  const queued = signedEvent()
  const next = pool.sendEvent(queued, ['wss://relay.example'], { timeout: 2000 })
  const before = socket.sent.filter(frame => frame[0] === 'EVENT').length
  t.mock.timers.tick(499)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, before)
  t.mock.timers.tick(1)
  await tick()
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, before + 1)
  socket.receive(['OK', queued.id, true, 'saved'])
  assert.equal((await next).success, true)
})

test('malformed retry metadata falls back to local backoff without using its value', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  for (const [index, [reason, retry]] of [['blocked: no', '60000'], ['rate-limited: busy', '60.5'], ['rate-limited: busy', '-1']].entries()) {
    let rejection
    const sub = relay.subscribe([{}], { onclose: error => { rejection = error } })
    socket.receive(['CLOSED', sub.id, reason, retry])
    await tick()
    if (reason.startsWith('rate-limited:')) assert.equal(rejection.retryAfterMs, index === 1 ? 1000 : 1500)
    else assert.equal(rejection.retryAfterMs, undefined)
    const next = relay.subscribe([{}])
    if (reason.startsWith('rate-limited:')) {
      const delay = index === 1 ? 1000 : 1500
      t.mock.timers.tick(delay - 1)
      assert.notEqual(socket.sent.at(-1)?.[1], next.id)
      t.mock.timers.tick(1)
    }
    assert.equal(socket.sent.at(-1)[1], next.id)
    next.close()
  }
})

test('pool operation timeout cancels a publication waiting for relay cooldown', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const pool = new RelayPool({ WebSocket: FakeWebSocket, rateLimitRetries: 0 })
  t.after(() => pool.disconnectAll())
  const event = signedEvent()
  const first = pool.sendEvent(event, ['wss://relay.example'])
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  socket.receive(['OK', event.id, false, 'rate-limited: busy', '2000'])
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

test('relative CLOSED waits from receipt and legacy absolute params fall back to local backoff', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 14000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  let rejection
  const first = relay.subscribe([{}], { onclose: error => { rejection = error } })
  socket.receive(['CLOSED', first.id, 'rate-limited: busy', '6000'])
  await tick()
  assert.equal(rejection.retryAfterMs, 6000)
  assert.equal(rejection.retryAt, 20000)
  const next = relay.subscribe([{}])
  t.mock.timers.tick(5999)
  assert.ok(!socket.sent.some(frame => frame[1] === next.id))
  t.mock.timers.tick(1)
  assert.equal(socket.sent.at(-1)[1], next.id)
  t.mock.timers.tick(5000)
  const published = signedEvent()
  const result = assert.rejects(relay.publish(published), error => error.retryAfterMs === 1000)
  socket.receive(['OK', published.id, false, 'rate-limited: busy', '@20000'])
  await result
  const immediate = relay.subscribe([{}])
  assert.equal(socket.sent.at(-1)[1], immediate.id)
})

test('cancelling one publication subscriber preserves another subscriber for the same event', async () => {
  const { relay, socket } = await connected()
  const event = signedEvent()
  const cancelled = new AbortController(); const other = new AbortController()
  const reason = new DOMException('cancelled', 'AbortError')
  const first = assert.rejects(relay.publish(event, { signal: cancelled.signal }), error => error === reason)
  const second = relay.publish(event, { signal: other.signal })
  assert.equal(socket.sent.filter(frame => frame[0] === 'EVENT').length, 1)
  cancelled.abort(reason)
  await first
  assert.equal(socket.readyState, 1)
  socket.receive(['OK', event.id, true, 'saved'])
  assert.equal(await second, 'saved')
  await relay.close()
})

test('a cooled-down relay does not prevent another relay from completing a bulk publication', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const pool = new RelayPool({ WebSocket: FakeWebSocket, rateLimitRetries: 0 })
  t.after(() => pool.disconnectAll())
  const limited = signedEvent()
  const first = pool.sendEvent(limited, ['wss://limited.example'])
  const limitedSocket = FakeWebSocket.instances.at(-1)
  limitedSocket.open()
  await tick()
  limitedSocket.receive(['OK', limited.id, false, 'rate-limited: busy', '2000'])
  assert.equal((await first).success, false)

  const accepted = signedEvent()
  const second = pool.sendEvent(accepted, ['wss://limited.example', 'wss://healthy.example'], {
    useBulkPacing: true,
    timeout: 100
  })
  const healthySocket = FakeWebSocket.instances.at(-1)
  healthySocket.open()
  await tick()
  healthySocket.receive(['OK', accepted.id, true, 'saved'])
  assert.equal((await second).success, true)
})

test('pool abort settles reports with the original cancellation without closing a shared socket', async t => {
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const cancelled = new AbortController()
  const event = signedEvent()
  const a = pool.sendEvent(event, ['wss://cancel.example'], { signal: cancelled.signal, timeout: null })
  const socket = FakeWebSocket.instances.at(-1)
  socket.open(); await tick()
  const b = pool.sendEvent(event, ['wss://cancel.example'], { timeout: null })
  await tick()
  const reason = new DOMException('cancelled', 'AbortError')
  cancelled.abort(reason)
  const result = await a
  assert.equal(result.success, false)
  assert.equal((await result.promise).errors[0].reason, reason)
  assert.equal(socket.readyState, 1)
  socket.receive(['OK', event.id, true, 'saved'])
  assert.equal((await b).success, true)
})

test('a pre-aborted publication never creates a socket or reports a timeout', async () => {
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  const before = FakeWebSocket.instances.length
  const controller = new AbortController(); const reason = new Error('cancelled')
  controller.abort(reason)
  const result = await pool.sendEvent(signedEvent(), ['wss://preabort.example'], { signal: controller.signal })
  assert.equal(FakeWebSocket.instances.length, before)
  assert.equal(result.success, false)
  assert.equal((await result.promise).errors[0].reason, reason)
  await pool.disconnectAll()
})

test('publication rejects malformed cancellation signals before opening connections', async () => {
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  const before = FakeWebSocket.instances.length
  await assert.rejects(pool.sendEvent(signedEvent(), ['wss://invalid-signal.example'], { signal: {} }), { code: 'INVALID_RELAY_SIGNAL' })
  assert.equal(FakeWebSocket.instances.length, before)
})

test('pool cancellation preserves partial ACKs and original abort reasons', async t => {
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  t.after(() => pool.disconnectAll())
  const controller = new AbortController()
  const event = signedEvent()
  const work = pool.sendEvent(event, ['wss://partial-a.example', 'wss://partial-b.example'], { signal: controller.signal })
  const sockets = FakeWebSocket.instances.slice(-2)
  for (const socket of sockets) socket.open()
  await tick()
  sockets[0].receive(['OK', event.id, true, 'saved'])
  const accepted = await work
  assert.equal(accepted.success, true)
  const reason = new Error('cancel remaining publication')
  controller.abort(reason)
  const report = await accepted.promise
  assert.equal(report.success, true)
  assert.equal(report.fulfilled, 1)
  assert.deepEqual(report.succeededRelays, ['wss://partial-a.example'])
  assert.equal(report.errors[0].reason, reason)
  assert.ok(sockets.every(socket => socket.readyState === 1))
})

test('last publication cancellation removes an EVENT deferred by relative cooldown', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const subscription = relay.subscribe([{}])
  const limited = signedEvent()
  const refused = assert.rejects(relay.publish(limited), error => error.retryAt === 20000)
  socket.receive(['OK', limited.id, false, 'rate-limited: busy', '10000'])
  await refused
  const controller = new AbortController()
  const deferred = signedEvent()
  const reason = new Error('cancel deferred publication')
  const cancelled = assert.rejects(relay.publish(deferred, { signal: controller.signal }), error => error === reason)
  subscription.close()
  assert.equal(socket.sent.at(-1)[0], 'CLOSE')
  controller.abort(reason)
  await cancelled
  t.mock.timers.tick(10000)
  assert.ok(!socket.sent.some(frame => frame[0] === 'EVENT' && frame[1].id === deferred.id))
  assert.equal(socket.readyState, 1)
})

// Exercise every operation against every other type on the same real connection.
for (const limitedType of ['EVENT', 'AUTH', 'REQ', 'COUNT']) {
  test(`${limitedType} cooldown delays only the same message type`, async t => {
    t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
    const { relay, socket } = await connected()
    t.after(() => relay.close())
    socket.receive(['AUTH', 'challenge'])
    await tick()
    const errors = []
    const start = async type => {
      if (type === 'REQ') return relay.subscribe([{}], { onclose: error => { if (error) errors.push(error) } }).id
      if (type === 'COUNT') {
        relay.countWithHll([{}]).catch(error => errors.push(error))
        return socket.sent.at(-1)[1]
      }
      const event = signedEvent({ kind: type === 'AUTH' ? 22242 : 1 })
      const pending = type === 'EVENT' ? relay.publish(event) : relay.authenticate(async () => event)
      pending.catch(error => errors.push(error))
      await tick()
      return event.id
    }
    const id = await start(limitedType)
    socket.receive(['EVENT', 'AUTH'].includes(limitedType)
      ? ['OK', id, false, 'rate-limited: busy', '2500']
      : ['CLOSED', id, 'rate-limited: busy', '2500'])
    await tick()
    assert.equal(errors[0].retryAt, 12500)
    const before = socket.sent.length
    for (const type of ['EVENT', 'AUTH', 'REQ', 'COUNT']) await start(type)
    assert.deepEqual(socket.sent.slice(before).map(frame => frame[0]), ['EVENT', 'AUTH', 'REQ', 'COUNT'].filter(type => type !== limitedType))
    t.mock.timers.tick(2499)
    assert.equal(socket.sent.length, before + 3)
    t.mock.timers.tick(1)
    assert.equal(socket.sent.at(-1)[0], limitedType)
  })
}

test('unmatched OK and CLOSED and late CLOSE confirmations cannot create cooldowns', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  const { relay, socket } = await connected()
  t.after(() => relay.close())
  const subscription = relay.subscribe([{}])
  subscription.close()
  socket.receive(['CLOSED', subscription.id, 'rate-limited: busy', '300000'])
  socket.receive(['CLOSED', 'unknown', 'rate-limited: busy', '300000'])
  socket.receive(['OK', 'unknown', false, 'rate-limited: busy', '300000'])
  await tick()
  const next = relay.subscribe([{}])
  assert.equal(socket.sent.at(-1)[1], next.id)
  const event = signedEvent()
  const pending = relay.publish(event)
  assert.equal(socket.sent.at(-1)[1].id, event.id)
  socket.receive(['OK', event.id, true, 'saved'])
  await pending
})
