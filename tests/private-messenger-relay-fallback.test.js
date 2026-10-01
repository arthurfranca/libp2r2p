import { test } from 'node:test'
import assert from 'node:assert/strict'
import { RelayPool, pickRelaysForPubkeys } from '../relay/index.js'
import { createSendRelayRouting, isReplaceableRelayFailure } from '../private-messenger/helpers/send-routing.js'
import { finalizeEvent } from '../event/index.js'

const urls = ['a', 'b', 'c', 'd', 'e'].map(name => `wss://${name}.example`)
const event = finalizeEvent({ kind: 26300, created_at: 1, tags: [['p', 'b'.repeat(64)]], content: 'same encrypted outer payload' }, new Uint8Array(32).fill(7))
const tick = () => new Promise(resolve => setImmediate(resolve))
function fixture (t, { reasons = {}, online = true } = {}) {
  const state = { reasons, online, probes: 0, current: true, held: new Set(), pending: [], now: 1000 }
  const sends = []; const batches = []; const exclusions = new Map()
  const pool = new RelayPool({
    WebSocket: class {
      constructor (url) {
        this.url = url; this.readyState = 0
        queueMicrotask(() => { this.readyState = 1; this.onopen?.() })
      }

      send (raw) {
        const [type, value] = JSON.parse(raw)
        if (type !== 'EVENT') return
        sends.push({ relay: this.url, event: value })
        const reason = state.reasons[this.url] || ''
        const settle = () => this.onmessage?.({ data: JSON.stringify(['OK', value.id, !reason, reason]) })
        if (state.held.has(this.url)) state.pending.push(settle)
        else queueMicrotask(settle)
      }

      close () { this.readyState = 3; queueMicrotask(() => this.onclose?.({ code: 1000, reason: '', wasClean: true })) }
    }
  })
  t.after(() => pool.disconnectAll())
  const publisher = options => options._publish(options.event, [...options.relayToReceivers.keys(), ...options.recoveryRelays].filter((url, i, list) => list.indexOf(url) === i))
  const controller = new AbortController()
  const routing = () => createSendRelayRouting({
    peer: 'peer', relaysByPubkey: { peer: { read: urls, write: ['wss://write-only.example'] } },
    exclusions, pickRelays: pickRelaysForPubkeys, recoveryRelays: urls.slice(0, 2),
    publish: publisher, publishNymEvent: publisher,
    sendEvent: (event, relays) => { batches.push([...relays]); return pool.sendEvent(event, relays, { timeout: 2000 }) },
    isOnline: async () => { state.probes++; return state.online },
    isCurrent: () => state.current, signal: controller.signal, now: () => state.now
  })
  return { state, sends, batches, exclusions, controller, routing, publish: route => route._publish({ event, ...route }) }
}

test('policy rejection rotates two read relays at a time and preserves the exact signed event', async t => {
  const f = fixture(t, { reasons: { [urls[0]]: 'blocked: only some kinds', [urls[1]]: 'restricted: members only' } })
  const result = await f.publish(f.routing())
  assert.equal(result.success, true)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4)])
  assert.ok(f.sends.every(send => JSON.stringify(send.event) === JSON.stringify(event)))
  assert.deepEqual((await result.promise).succeededRelays, urls.slice(2, 4))
  await tick()
  const next = f.routing()
  assert.deepEqual([...next.relayToReceivers.keys()], urls.slice(2, 4))
  assert.deepEqual(next.recoveryRelays, [], 'recovery mirrors cannot restore rejected relays')
  await f.publish(next)
  assert.deepEqual(f.batches.at(-1), urls.slice(2, 4))
})

test('exhaustion includes every read relay, emits one final report and permits a fresh retry', async t => {
  const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])) })
  const result = await f.publish(f.routing())
  assert.equal(result.success, false)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), [urls[4]]])
  const report = await result.promise
  assert.equal(report.total, 5)
  assert.equal(report.fulfilled, 0)
  assert.deepEqual(report.errors.map(error => error.relay), urls)
  assert.ok(report.errors.every(error => error.reason.category === 'relay'))
  f.state.reasons = {}
  assert.equal((await f.publish(f.routing())).success, true)
  assert.deepEqual(f.batches.at(-1), urls.slice(0, 2))
})

test('a slow redundant rejection does not delay an accepted send and preserves its healthy relay', async t => {
  const f = fixture(t, { reasons: { [urls[1]]: 'blocked: policy' } })
  f.state.held.add(urls[1])
  const result = await f.publish(f.routing())
  assert.equal(result.success, true)
  assert.equal(f.state.pending.length, 1)
  f.state.pending.pop()()
  await result.promise; await tick()
  assert.deepEqual([...f.routing().relayToReceivers.keys()], [urls[0], urls[2]])
  assert.equal(f.batches.length, 1, 'no redundant resend after acceptance')
})

test('offline, invalid messages, local authorization and unknown errors never rotate relays', async t => {
  for (const reason of ['invalid: bad signature', 'unknown: failure']) {
    const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, reason])) })
    assert.equal((await f.publish(f.routing())).success, false)
    assert.equal(f.batches.length, 1)
    assert.equal(f.state.probes, 0)
    assert.equal(f.exclusions.size, 0)
  }
  const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])), online: false })
  const deferred = await f.publish(f.routing())
  assert.equal(deferred.success, false)
  assert.equal(deferred.retryWhenAvailable, true)
  assert.equal(deferred.retryWhenOnline, true)
  assert.equal(f.batches.length, 1)
  assert.equal(f.exclusions.size, 0)
  for (const reason of [new Error('PERMISSION_DENIED'), Object.assign(new Error('blocked: local permission'), { name: 'Nip42AuthenticationError' }), Object.assign(new Error('blocked: unrelated'), { category: 'validation' })]) assert.equal(isReplaceableRelayFailure(reason), false)
  assert.equal(isReplaceableRelayFailure(Object.assign(new Error('PUBLISH_TIMEOUT'), { category: 'timeout' })), true)
})

test('cancellation and session pause prevent a replacement publication', async t => {
  for (const abort of [true, false]) {
    const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])) })
    f.state.held = new Set(urls.slice(0, 2))
    const work = f.publish(f.routing())
    while (f.state.pending.length < 2) await tick()
    if (abort) f.controller.abort()
    else f.state.current = false
    f.state.pending.splice(0).forEach(settle => settle())
    const result = await work
    assert.equal(result.success, false)
    assert.equal(result.retryWhenOnline, false)
    assert.equal(f.batches.length, 1)
    assert.equal(f.exclusions.size, 0)
  }
})

test('failed relay preferences expire and other fragments reuse the replacement pair', async t => {
  const f = fixture(t, { reasons: { [urls[0]]: 'rate-limited: slow down', [urls[1]]: 'pow: more work' } })
  const routing = f.routing()
  await f.publish(routing)
  await f.publish(routing)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), urls.slice(2, 4)])
  f.state.now += 5 * 60 * 1000
  assert.deepEqual([...f.routing().relayToReceivers.keys()], urls.slice(0, 2))
})
