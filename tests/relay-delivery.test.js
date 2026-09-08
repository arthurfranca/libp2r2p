import assert from 'node:assert/strict'
import { test } from 'node:test'
import { RelayPool } from '../relay/index.js'
import { RelayConnection } from '../relay/services/relay-connection.js'
import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'

const A = 'wss://a.example'
const B = 'wss://b.example'
const tick = () => new Promise(resolve => setImmediate(resolve))

function signedEvent () {
  return finalizeEvent({ kind: 1, created_at: 1, tags: [], content: 'diagnostic' }, generateSecretKey())
}

// Keeps a bounded ref'ed timer while the production timers remain unref'ed.
async function bounded (promise) {
  let timer
  try {
    return await Promise.race([promise, new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Test operation timed out')), 2000)
    })])
  } finally { clearTimeout(timer) }
}

// Only the socket is controlled; validation, subscriptions and publication are real.
function fixture (t, { open = true, connectError, sendError, onSend } = {}) {
  const sockets = new Map()
  class Socket {
    constructor (url) {
      this.url = url
      this.readyState = 0
      this.frames = []
      sockets.set(url.replace(/\/$/, ''), this)
      queueMicrotask(() => {
        if (connectError) this.onerror?.({ error: connectError })
        else if (open) { this.readyState = 1; this.onopen?.() }
      })
    }
    send (text) {
      const frame = JSON.parse(text)
      this.frames.push(frame)
      if (sendError && frame[0] === 'EVENT') throw sendError
      onSend?.(frame, this)
    }
    receive (frame) { this.onmessage?.({ data: JSON.stringify(frame) }) }
    event (event) { this.receive(['EVENT', this.frames.find(frame => frame[0] === 'REQ')[1], event]) }
    eose () { this.receive(['EOSE', this.frames.find(frame => frame[0] === 'REQ')[1]]) }
    fail (error) { this.onerror?.({ error }) }
    close (details = {}) {
      this.readyState = 3
      queueMicrotask(() => this.onclose?.({ code: 1000, reason: '', wasClean: true, ...details }))
    }
  }
  const connections = []
  const pool = new RelayPool({
    _createRelay: url => {
      const relay = new RelayConnection(url, { WebSocket: Socket })
      connections.push(relay)
      return relay
    }
  })
  t.after(() => pool.disconnectAll())
  return { pool, sockets, connections, Socket }
}

for (const deduplicateAcrossRelays of [undefined, true, false]) {
  test(`event origins and immediate callbacks with deduplicateAcrossRelays=${deduplicateAcrossRelays}`, async t => {
    const { pool, sockets } = fixture(t)
    const callbacks = []
    let completed = false
    const pending = pool.getEvents({ kinds: [1] }, [A, `${A}/`, B], {
      deduplicateAcrossRelays, timeoutAfterFirstEose: null, callback: item => callbacks.push(item)
    })
    pending.then(() => { completed = true })
    await tick()
    assert.equal(sockets.size, 2)
    const event = signedEvent()
    sockets.get(A).event(event)
    sockets.get(A).event(event)
    await tick()
    assert.equal(callbacks.length, 1)
    assert.equal(completed, false)
    sockets.get(B).event(event)
    sockets.get(B).event(event)
    await tick()
    const origins = deduplicateAcrossRelays === false ? [A, B] : [A]
    assert.deepEqual(callbacks.map(item => item.relay), origins)
    assert.deepEqual(callbacks.map(item => item.event.meta.relay), origins)
    assert.ok(callbacks.every(item => item.type === 'event'))
    assert.equal(completed, false)
    sockets.get(A).eose()
    sockets.get(B).eose()
    const report = await pending
    assert.deepEqual(report.result.map(event => event.meta.relay), origins)
    assert.equal(report.result.length, origins.length)
    assert.equal(report.success, true)
    assert.deepEqual(report.errors, [])
  })
}

test('per-relay deduplication preserves limit and id-based early close', async t => {
  for (const mode of ['limit', 'ids']) {
    const { pool, sockets } = fixture(t)
    const event = signedEvent()
    const filter = mode === 'limit' ? { kinds: [1], limit: 2 } : { ids: [event.id] }
    const pending = pool.getEvents(filter, [A, B], { deduplicateAcrossRelays: false, timeoutAfterFirstEose: null })
    await tick()
    for (const relay of [A, B]) {
      sockets.get(relay).event(event)
      if (mode === 'limit') sockets.get(relay).event(event)
    }
    const report = await bounded(pending)
    assert.deepEqual(report.result.map(event => event.meta.relay), [A, B])
    assert.ok([...sockets.values()].every(socket => socket.frames.some(frame => frame[0] === 'CLOSE')))
  }
})

test('per-relay mode keeps EOSE grace, deadlines, late-event suppression and cancellation', async t => {
  const { pool, sockets } = fixture(t)
  const pending = pool.getEvents({}, [A, B], {
    deduplicateAcrossRelays: false, timeoutAfterFirstEose: null, timeout: 30
  })
  await tick()
  sockets.get(A).event(signedEvent())
  sockets.get(A).eose()
  const report = await bounded(pending)
  assert.equal(report.result.length, 1)
  assert.equal(report.errors[0].relay, B)
  assert.equal(report.errors[0].reason.message, 'GET_EVENTS_TIMEOUT')
  sockets.get(B).event(signedEvent())
  await tick()
  assert.equal(report.result.length, 1)

  const grace = pool.getEvents({}, [A, B], { deduplicateAcrossRelays: false, timeoutAfterFirstEose: 5 })
  await tick()
  const request = sockets.get(A).frames.filter(frame => frame[0] === 'REQ').at(-1)[1]
  sockets.get(A).receive(['EVENT', request, signedEvent()])
  sockets.get(A).receive(['EOSE', request])
  assert.equal((await bounded(grace)).errors.length, 0)

  const ac = new AbortController()
  const cancelled = pool.getEvents({}, [A], { deduplicateAcrossRelays: false, signal: ac.signal })
  ac.abort()
  await assert.rejects(cancelled, /Aborted/)
  await assert.rejects(pool.getEvents({}, [A], { deduplicateAcrossRelays: 'false' }), { code: 'INVALID_DEDUPLICATE_ACROSS_RELAYS' })
})

test('generator forwards the option and retains callback, yield and terminal report', async t => {
  const { pool, sockets } = fixture(t)
  const callbacks = []
  const generator = pool.getEventsGenerator({}, [A, B], {
    deduplicateAcrossRelays: false, timeoutAfterFirstEose: null, callback: item => callbacks.push(item)
  })
  const first = generator.next()
  await tick()
  const event = signedEvent()
  sockets.get(A).event(event)
  sockets.get(A).event(event)
  assert.equal((await first).value.relay, A)
  const second = generator.next()
  sockets.get(B).event(event)
  assert.equal((await second).value.relay, B)
  sockets.get(A).eose()
  sockets.get(B).eose()
  const end = await generator.next()
  assert.equal(end.done, true)
  assert.equal(end.value.result.length, 2)
  assert.equal(end.value.success, true)
  assert.deepEqual(callbacks.map(item => item.event.meta.relay), [A, B])
})

test('connection failures retain empty AggregateError messages, codes and children', async t => {
  const child = Object.assign(new Error('refused'), { code: 'ECONNREFUSED' })
  const original = Object.assign(new AggregateError([child], '', { cause: child }), { code: 'ECONNREFUSED' })
  const { pool } = fixture(t, { connectError: original })
  const { promise } = await pool.sendEvent(signedEvent(), [A])
  const { errors } = await promise
  assert.equal(errors[0].reason.category, 'connection')
  assert.equal(errors[0].reason, original)
  assert.equal(errors[0].reason.message, '')
  assert.equal(errors[0].reason.errors[0], child)
  assert.equal(errors[0].reason.cause, child)
  assert.equal(errors[0].reason.code, 'ECONNREFUSED')
})

test('publish rejection is distinguished from transport and strips internal metadata on the wire', async t => {
  const { pool, sockets } = fixture(t, {
    onSend: (frame, socket) => {
      if (frame[0] === 'EVENT') socket.receive(['OK', frame[1].id, false, 'blocked: denied'])
    }
  })
  const event = { ...signedEvent(), meta: { relay: B, relays: [B] } }
  const { promise } = await pool.sendEvent(event, [A])
  const report = await promise
  assert.equal(report.errors[0].reason.category, 'relay')
  assert.equal(report.errors[0].reason.message, 'blocked: denied')
  assert.equal(Object.hasOwn(sockets.get(A).frames[0][1], 'meta'), false)
  assert.deepEqual(event.meta, { relay: B, relays: [B] })
})

test('socket send failures retain their native code', async t => {
  const original = Object.assign(new Error('broken pipe'), { code: 'EPIPE' })
  const { pool } = fixture(t, { sendError: original })
  const { promise } = await pool.sendEvent(signedEvent(), [A])
  const report = await promise
  assert.equal(report.errors[0].reason.category, 'transport')
  assert.equal(report.errors[0].reason.code, 'EPIPE')
})

test('socket errors remain pending until close and retain close details separately', async t => {
  const { pool, sockets } = fixture(t)
  let completed = false
  const pending = pool.sendEvent(signedEvent(), [A], { timeoutUntilFirstFulfillment: null })
  pending.then(() => { completed = true })
  await tick()
  const original = Object.assign(new Error('reset'), { code: 'ECONNRESET' })
  sockets.get(A).fail(original)
  await tick()
  assert.equal(completed, false)
  sockets.get(A).close({ code: 1006, reason: 'abnormal close', wasClean: false })
  const report = await (await bounded(pending)).promise
  const error = report.errors[0].reason
  assert.equal(error.category, 'transport')
  assert.equal(error.cause, original)
  assert.equal(error.cause.code, 'ECONNRESET')
  assert.equal(error.closeCode, 1006)
  assert.equal(error.closeReason, 'abnormal close')
  assert.equal(error.wasClean, false)
  assert.equal(error.code, undefined)
})

test('operation and connection timeouts are classified without claiming rejection', async t => {
  const { pool, sockets } = fixture(t)
  const pending = pool.sendEvent(signedEvent(), [A], { timeout: 20, timeoutUntilFirstFulfillment: null })
  await tick()
  const cause = new Error('socket failure before deadline')
  sockets.get(A).fail(cause)
  const report = await (await bounded(pending)).promise
  assert.equal(report.errors[0].reason.category, 'timeout')
  assert.equal(report.errors[0].reason.message, 'PUBLISH_TIMEOUT')
  assert.equal(report.errors[0].reason.cause, cause)

  const { Socket } = fixture(t, { open: false })
  const connection = new RelayConnection(A, { WebSocket: Socket })
  await assert.rejects(bounded(connection.connect({ timeout: 5 })), { category: 'timeout', message: 'CONNECT_TIMEOUT' })
})

test('NIP-42 error wrapping preserves relay rejection context', async t => {
  const { pool } = fixture(t, {
    onSend: (frame, socket) => {
      if (frame[0] === 'EVENT') {
        socket.receive(['AUTH', 'challenge'])
        socket.receive(['OK', frame[1].id, false, 'auth-required: authenticate'])
      } else if (frame[0] === 'AUTH') socket.receive(['OK', frame[1].id, false, 'restricted: forbidden'])
    }
  })
  const { promise } = await pool.sendEvent(signedEvent(), [A], { getAuthEvent: async () => signedEvent() })
  const report = await promise
  assert.equal(report.errors[0].reason.name, 'Nip42AuthenticationError')
  assert.equal(report.errors[0].reason.category, 'relay')
  assert.equal(report.errors[0].reason.message, 'restricted: forbidden')
  assert.equal(report.errors[0].reason.cause.category, 'relay')
})

test('frozen native connection errors preserve their type and cause', async t => {
  const child = Object.assign(new Error('refused'), { code: 'ECONNREFUSED' })
  const original = Object.freeze(Object.assign(new AggregateError([child], ''), { code: 'ECONNREFUSED' }))
  const { pool } = fixture(t, { connectError: original })
  const { promise } = await pool.sendEvent(signedEvent(), [A])
  const report = await promise
  const error = report.errors[0].reason
  assert.ok(error instanceof AggregateError)
  assert.equal(error.category, 'connection')
  assert.equal(error.cause, original)
  assert.equal(error.errors[0], child)
  assert.equal(error.code, 'ECONNREFUSED')
})

test('empty negative OK remains an explicit relay rejection', async t => {
  const { pool } = fixture(t, {
    onSend: (frame, socket) => {
      if (frame[0] === 'EVENT') socket.receive(['OK', frame[1].id, false, ''])
    }
  })
  const { promise } = await pool.sendEvent(signedEvent(), [A])
  const report = await promise
  assert.equal(report.errors[0].reason.category, 'relay')
  assert.equal(report.errors[0].reason.message, 'EVENT_REJECTED')
})

test('the connection operation deadline retains a preceding transport error', async t => {
  const { Socket, sockets } = fixture(t)
  const relay = new RelayConnection(A, { WebSocket: Socket })
  t.after(() => relay.close())
  await relay.connect()
  relay.publishTimeout = 5
  const pending = relay.publish(signedEvent())
  const cause = new Error('socket error')
  sockets.get(A).fail(cause)
  await assert.rejects(bounded(pending), error => {
    assert.equal(error.category, 'timeout')
    assert.equal(error.message, 'PUBLISH_TIMEOUT')
    assert.equal(error.cause, cause)
    return true
  })
})
