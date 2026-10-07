import { test } from 'node:test'
import assert from 'node:assert/strict'
import { pickRelaysForPubkeys } from '../relay/index.js'
import { finalizeEvent } from '../event/index.js'
import { createSendRelayRouting } from '../private-messenger/helpers/send-routing.js'
import { createPrivateMessageSession } from '../private-messenger/session/index.js'
import { PrivateMessenger } from '../private-messenger/index.js'

const peer = 'b'.repeat(64)
const primaries = Array.from({ length: 10 }, (_, i) => `wss://primary-${i}.example`)
const fallback = 'wss://fallback.example'
const event = finalizeEvent({ kind: 26300, created_at: 1, tags: [], content: 'unchanged ciphertext' }, new Uint8Array(32).fill(7))
const flush = async () => { for (let n = 0; n < 40; n++) await Promise.resolve() }
function clock () {
  let now = 0; let serial = 0
  const timers = new Map()
  return {
    now: () => now, timers,
    setTimer: (fn, delay) => { const id = ++serial; timers.set(id, { fn, at: now + delay }); return id },
    clearTimer: id => timers.delete(id),
    async advance (ms) {
      const until = now + ms
      await flush()
      while (true) {
        const due = [...timers].filter(([, task]) => task.at <= until).sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0]
        if (!due) break
        timers.delete(due[0]); now = due[1].at; due[1].fn(); await flush()
      }
      now = until; await flush()
    }
  }
}
function fixture ({ delay = 3000, read = primaries, fallbacks = [fallback], outcomes = {}, online = true, primaryRelays, primaryRelayToReceivers, peers = [peer], relaysByPubkey } = {}) {
  const time = clock(); const calls = []; const exclusions = new Map(); const controller = new AbortController(); const pause = new AbortController()
  const state = { online, current: true, probes: 0 }
  const sendEvent = async (value, relays, { signal } = {}) => {
    assert.equal(value, event)
    calls.push({ at: time.now(), relays: [...relays], event: value })
    const early = Promise.withResolvers(); const full = Promise.withResolvers(); const results = new Map(); const tasks = []
    const finish = () => {
      if (results.size !== relays.length) return
      signal?.removeEventListener('abort', abort)
      const succeededRelays = relays.filter(relay => results.get(relay).success)
      full.resolve({ total: relays.length, fulfilled: succeededRelays.length, succeededRelays, errors: relays.filter(relay => !results.get(relay).success).map(relay => ({ relay, reason: results.get(relay).reason })) })
      early.resolve({ success: succeededRelays.length > 0, promise: full.promise })
    }
    const settle = (relay, result) => {
      if (results.has(relay)) return
      results.set(relay, result)
      if (result.success) early.resolve({ success: true, promise: full.promise })
      finish()
    }
    const abort = () => {
      for (const id of tasks) time.clearTimer(id)
      for (const relay of relays) settle(relay, { reason: signal.reason })
    }
    signal?.addEventListener('abort', abort, { once: true })
    for (const relay of relays) {
      const result = outcomes[relay] || { after: 30000, reason: Object.assign(new Error('PUBLISH_TIMEOUT'), { category: 'timeout' }) }
      tasks.push(time.setTimer(() => settle(relay, result), result.after))
    }
    if (signal?.aborted) abort()
    return early.promise
  }
  const publish = options => options._publish(options.event, [...options.relayToReceivers.keys(), ...options.recoveryRelays])
  const routing = createSendRelayRouting({
    peers, relaysByPubkey: relaysByPubkey || Object.fromEntries(peers.map(pubkey => [pubkey, { read }])), primaryRelays, primaryRelayToReceivers,
    fallbackRelays: fallbacks, fallbackDelayMs: delay, exclusions, pickRelays: pickRelaysForPubkeys, recoveryRelays: [], publish, publishNymEvent: publish,
    sendEvent, isOnline: async () => { state.probes++; return state.online }, isCurrent: () => state.current,
    signal: controller.signal, pauseSignal: pause.signal, now: time.now, setTimer: time.setTimer, clearTimer: time.clearTimer
  })
  return { time, calls, state, exclusions, controller, pause, publish: () => routing._publish({ event, ...routing }) }
}

test('ten silent primaries yield to fallback at three seconds, keeping deadlines and exact bytes', async () => {
  const f = fixture({ outcomes: { [fallback]: { after: 1, success: true } } })
  let result
  f.publish().then(value => { result = value })
  await f.time.advance(2999)
  assert.equal(f.calls.length, 1); assert.equal(result, undefined)
  await f.time.advance(2)
  assert.equal(result.success, true)
  assert.deepEqual(f.calls.map(call => call.at), [0, 3000])
  assert.deepEqual(f.calls[1].relays, [fallback])
  assert.ok(f.calls.every(call => JSON.stringify(call.event) === JSON.stringify(event)))
  assert.equal(f.exclusions.size, 0, 'waiting three seconds is not a failure')
  await f.time.advance(26999)
  assert.equal((await result.promise).errors.length, 2)
  await flush()
  assert.equal(f.exclusions.size, 2, 'actual late failures inform the next send')
  assert.equal(f.calls.length, 2, 'late failures cannot resume the winning race')
  assert.equal(f.time.timers.size, 0)
})

test('early primary acceptance cancels the fallback and returns before redundant outcomes', async () => {
  const f = fixture({ outcomes: { [primaries[0]]: { after: 10, success: true } } })
  const work = f.publish()
  await f.time.advance(10)
  assert.equal((await work).success, true)
  await f.time.advance(30000)
  assert.equal(f.calls.length, 1)
})

test('primary exhaustion starts fallback immediately instead of waiting the remaining delay', async () => {
  const reason = Object.assign(new Error('blocked: policy'), { category: 'relay' })
  const f = fixture({ read: primaries.slice(0, 2), outcomes: Object.fromEntries([...primaries.slice(0, 2).map(relay => [relay, { after: 10, reason }]), [fallback, { after: 1, success: true }]]) })
  const work = f.publish()
  await f.time.advance(11)
  assert.equal((await work).success, true)
  assert.equal(f.calls[1].at, 10)
})

test('failed fallback leaves healthy late primaries eligible and never starts one relay twice', async () => {
  const f = fixture({ outcomes: { [primaries[0]]: { after: 5000, success: true }, [fallback]: { after: 1, reason: Object.assign(new Error('error: unavailable'), { category: 'relay' }) } } })
  const work = f.publish()
  await f.time.advance(5000)
  assert.equal((await work).success, true)
  assert.equal(f.calls.length, 2)
  assert.equal(new Set(f.calls.flatMap(call => call.relays)).size, f.calls.flatMap(call => call.relays).length)
  await f.time.advance(25000)
})

test('later primary failures neither renew the fallback deadline nor exhaust it prematurely', async () => {
  const reason = Object.assign(new Error('error: unavailable'), { category: 'relay' })
  const f = fixture({ outcomes: Object.fromEntries([...primaries.slice(0, 2).map(relay => [relay, { after: 1000, reason }]), [fallback, { after: 1, success: true }]]) })
  const work = f.publish()
  await f.time.advance(3001)
  assert.equal((await work).success, true)
  assert.deepEqual(f.calls.map(call => call.at), [0, 1000, 3000])
  await f.time.advance(30000)
})

test('offline confirmation prevents fallback while an already started primary may still accept', async () => {
  const f = fixture({ online: false, outcomes: { [primaries[0]]: { after: 5000, success: true } } })
  const work = f.publish()
  await f.time.advance(5000)
  assert.equal((await work).success, true)
  assert.equal(f.calls.length, 1)
  await f.time.advance(25000)
})

test('offline without acceptance stays retryable and does not open alternative routes', async () => {
  const f = fixture({ online: false })
  const work = f.publish()
  await f.time.advance(30000)
  const result = await work
  assert.equal(result.success, false); assert.equal(result.retryWhenAvailable, true); assert.equal(result.retryWhenOnline, true)
  assert.equal(f.calls.length, 1); assert.equal(f.time.timers.size, 0)
})

test('consumer cancellation and pause stop both lanes but preserve different retry semantics', async () => {
  for (const type of ['consumer', 'pause']) {
    const f = fixture()
    const work = f.publish()
    await f.time.advance(3000)
    assert.equal(f.calls.length, 2)
    const reason = new Error(type === 'pause' ? 'PRIVATE_MESSENGER_PAUSED' : 'cancelled')
    ;(type === 'pause' ? f.pause : f.controller).abort(reason)
    await flush()
    const result = await work
    assert.equal(result.success, false)
    assert.equal(Boolean(result.retryWhenAvailable), type === 'pause')
    assert.ok((await result.promise).errors.every(error => error.reason === reason))
    assert.equal(f.time.timers.size, 0)
  }
})

test('explicit routes and default null retain sequential primary exhaustion', async () => {
  for (const explicit of [false, true]) {
    const f = fixture({ delay: explicit ? 3000 : null, read: primaries.slice(0, 2), ...(explicit ? { primaryRelays: primaries.slice(0, 2) } : {}), outcomes: { [fallback]: { after: 1, success: true } } })
    const work = f.publish()
    await f.time.advance(30000)
    assert.equal(f.calls[1].at, 30000)
    await f.time.advance(1)
    assert.equal((await work).success, true)
  }
})

test('terminal remote errors and local authentication failures do not start speculative fallback', async () => {
  for (const reason of [Object.assign(new Error('invalid: signature'), { category: 'relay' }), new Error('unknown'), Object.assign(new Error('auth failed'), { name: 'Nip42AuthenticationError' })]) {
    const f = fixture({ outcomes: Object.fromEntries(primaries.slice(0, 2).map(relay => [relay, { after: 10, reason }])) })
    const work = f.publish()
    await f.time.advance(3000)
    assert.equal((await work).success, false)
    assert.equal(f.calls.length, 1)
  }
})

test('absolute cooldown and exclusions are not reset by the speculative deadline', async () => {
  const reason = Object.assign(new Error('rate-limited: cooldown'), { category: 'relay', retryAt: 300000, retryAfterMs: 300000 })
  const f = fixture({ outcomes: Object.fromEntries(primaries.slice(0, 2).map(relay => [relay, { after: 1, reason }])) })
  const work = f.publish()
  await f.time.advance(3000)
  assert.deepEqual(f.calls.slice(0, 2).map(call => call.relays), [primaries.slice(0, 2), primaries.slice(2, 4)])
  assert.equal(f.calls[2].at, 3000)
  assert.equal(f.exclusions.size, 2)
  f.controller.abort(); await work; await flush()
})

test('early deadline cannot bypass a fallback excluded by a previous relay refusal', async () => {
  const f = fixture({ outcomes: { [primaries[0]]: { after: 5000, success: true } } })
  f.exclusions.set(fallback, 300000)
  const work = f.publish()
  await f.time.advance(3000)
  assert.equal(f.calls.length, 1)
  assert.equal(f.exclusions.get(fallback), 300000)
  await f.time.advance(2000)
  assert.equal((await work).success, true)
  await f.time.advance(25000)
})

test('multi-recipient partial coverage cannot complete before the remaining member is accepted', async () => {
  const bob = 'b'.repeat(64); const carol = 'c'.repeat(64)
  const f = fixture({ peers: [bob, carol], relaysByPubkey: { [bob]: { read: [primaries[0]] }, [carol]: { read: [primaries[1]] } }, outcomes: { [primaries[0]]: { after: 1, success: true }, [fallback]: { after: 1, success: true } } })
  let result
  f.publish().then(value => { result = value })
  await f.time.advance(1)
  assert.equal(result, undefined)
  await f.time.advance(3000)
  assert.equal(result.success, true)
  assert.equal(f.calls.at(-1).at, 3000)
  await f.time.advance(30000)
})

test('fallback delay validates public input consistently on messenger and session', () => {
  for (const invalid of [-1, NaN, Infinity, 0.5, 2147483648, '3000']) {
    assert.throws(() => new PrivateMessenger({ fallbackDelayMs: invalid }), { code: 'INVALID_FALLBACK_DELAY' })
    assert.throws(() => createPrivateMessageSession({ fallbackDelayMs: invalid }), { code: 'INVALID_FALLBACK_DELAY' })
  }
  for (const value of [null, 0, 3000, 2147483647]) assert.equal(new PrivateMessenger({ fallbackDelayMs: value }).fallbackDelayMs, value)
})

test('messenger pauses retire the publication signal and resume creates a fresh lifetime', async () => {
  const messenger = new PrivateMessenger()
  const original = messenger.sendRoutingPause.signal
  await messenger.pause('vault')
  assert.equal(original.aborted, true)
  assert.equal(original.reason.message, 'PRIVATE_MESSENGER_PAUSED')
  assert.deepEqual(original.reason.pauseReasons, ['vault'])
  await messenger.resume('vault')
  assert.notEqual(messenger.sendRoutingPause.signal, original)
  assert.equal(messenger.sendRoutingPause.signal.aborted, false)
  await messenger.close()
})
