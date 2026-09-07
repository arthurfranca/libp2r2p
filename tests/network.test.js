import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createConnectivityMonitor, isOnline } from '../network/index.js'

const flush = () => new Promise(resolve => setImmediate(resolve))

function harness (check) {
  const timers = new Set()
  const target = new EventTarget()
  const monitor = createConnectivityMonitor({
    check,
    eventTarget: target,
    document: target,
    _random: () => 0.5,
    _setTimeout: function (handler, delay) {
      assert.equal(this, globalThis)
      const timer = { handler, delay }
      timers.add(timer)
      return timer
    },
    _clearTimeout: function (timer) {
      assert.equal(this, globalThis)
      timers.delete(timer)
    }
  })
  return {
    ...monitor,
    target,
    timers,
    async tick (delay) {
      assert.equal(timers.size, 1)
      const timer = [...timers][0]
      assert.equal(timer.delay, delay)
      timers.delete(timer)
      timer.handler()
      await flush()
    }
  }
}

test('one probe loop recovers without a browser online event, with capped backoff', async () => {
  let online = false
  let probes = 0
  let notifications = 0
  const h = harness(async () => { probes++; return online })
  const stopA = h.onOnline(() => { notifications++ })
  const stopB = h.onOnline(() => { notifications++ })
  await flush()
  assert.equal(probes, 1)
  for (const delay of [5000, 15000, 30000, 60000]) await h.tick(delay)
  assert.equal(notifications, 0)
  online = true
  await h.tick(60000)
  assert.equal(notifications, 2)
  await h.tick(60000)
  assert.equal(notifications, 2)
  stopA()
  assert.equal(h.timers.size, 1)
  stopB()
  assert.equal(h.timers.size, 0)
})

test('native online is a probe trigger, not proof of connectivity', async () => {
  let online = false
  let notifications = 0
  const h = harness(async () => online)
  const stop = h.onOnline(() => { notifications++ })
  await flush()
  h.target.dispatchEvent(new Event('online'))
  await flush()
  assert.equal(notifications, 0)
  online = true
  h.target.dispatchEvent(new Event('focus'))
  await flush()
  assert.equal(notifications, 1)
  online = false
  await h.tick(60000)
  online = true
  h.target.dispatchEvent(new Event('visibilitychange'))
  await flush()
  assert.equal(notifications, 2)
  stop()
})

test('last unsubscribe aborts probes and prevents stale callbacks or timers', async () => {
  let finish
  let signal
  let called = false
  let probes = 0
  const h = harness(options => {
    probes++
    signal = options.signal
    return new Promise(resolve => { finish = resolve })
  })
  const stop = h.onOnline(() => { called = true })
  await flush()
  stop()
  stop()
  assert.equal(signal.aborted, true)
  finish(true)
  await flush()
  h.target.dispatchEvent(new Event('online'))
  await flush()
  assert.equal(probes, 1)
  assert.equal(called, false)
  assert.equal(h.timers.size, 0)
})

test('late subscribers receive confirmed connectivity without repeating existing callbacks', async () => {
  const h = harness(async () => true)
  let first = 0
  let second = 0
  const stopA = h.onOnline(() => { first++ })
  await flush()
  const stopB = h.onOnline(() => { second++ })
  await flush()
  assert.equal(first, 1)
  assert.equal(second, 1)
  stopA()
  stopB()
})

test('isOnline shares concurrent fetches and respects abort and the offline flag', async t => {
  let finish
  let calls = 0
  t.mock.method(globalThis, 'fetch', () => {
    calls++
    return new Promise(resolve => { finish = resolve })
  })
  const first = isOnline()
  const second = isOnline()
  assert.equal(calls, 1)
  finish(new Response())
  assert.deepEqual(await Promise.all([first, second]), [true, true])
  const controller = new AbortController()
  const aborted = isOnline({ signal: controller.signal })
  controller.abort()
  await assert.rejects(aborted, { name: 'AbortError' })
  const navigatorDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'navigator')
  t.after(() => Object.defineProperty(globalThis, 'navigator', navigatorDescriptor))
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { onLine: false } })
  assert.equal(await isOnline(), false)
  assert.equal(calls, 2)
})
