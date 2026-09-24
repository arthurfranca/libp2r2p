import { test } from 'node:test'
import assert from 'node:assert/strict'
import { ReadAdmission } from '../relay/helpers/read-admission.js'

const relay = 'wss://example.com/'
const tick = () => new Promise(resolve => setImmediate(resolve))

test('admission atomically reserves feed slots, releases independently and respects FIFO', async () => {
  const queue = new ReadAdmission({ maxSubscriptionsPerRelay: 3, maxConcurrentHistoryPerRelay: 1 })
  const first = await queue.acquire(relay, { live: true, history: true })
  let second
  let third
  const next = queue.acquire(relay, { live: true, history: true }).then(slots => { second = slots })
  const last = queue.acquire(relay, { history: true }).then(slots => { third = slots })
  await tick()
  assert.equal(second, undefined)
  assert.equal(third, undefined)
  first.history.release()
  await next
  assert.equal(queue.states.get(relay).active, 3)
  assert.equal(third, undefined)
  first.history.release() // idempotent
  second.history.release()
  await last
  assert.equal(queue.states.get(relay).active, 3)
  first.live.release()
  second.live.release()
  third.history.release()
  assert.equal(queue.states.size, 0)
})

test('queue capacity, expiry and abort reject without consuming slots or retaining work', async () => {
  const queue = new ReadAdmission({ maxConcurrentHistoryPerRelay: 1, maxQueuedReadsPerRelay: 1 })
  const first = await queue.acquire(relay, { history: true })
  const controller = new AbortController()
  const waiting = queue.acquire(relay, { history: true, signal: controller.signal })
  const cancelled = assert.rejects(waiting, { code: 'RELAY_READ_CANCELLED', phase: 'admission', relay })
  await assert.rejects(queue.acquire(relay, { history: true }), { code: 'RELAY_READ_QUEUE_FULL' })
  controller.abort()
  await cancelled
  // Keep Node alive while the unref'ed queue timer fires.
  const keepAlive = setTimeout(() => {}, 100)
  try {
    await assert.rejects(queue.acquire(relay, { history: true, queueTimeout: 5 }), { code: 'RELAY_READ_QUEUE_TIMEOUT' })
  } finally { clearTimeout(keepAlive) }
  first.history.release()
  assert.equal(queue.states.size, 0)
})

test('disconnect cancels every queued job without admitting a cancelled sibling', async () => {
  const queue = new ReadAdmission({ maxConcurrentHistoryPerRelay: 1 })
  const first = await queue.acquire(relay, { history: true })
  const pending = Array.from({ length: 6 }, () => assert.rejects(queue.acquire(relay, { history: true }), { code: 'RELAY_DISCONNECTED' }))
  queue.cancelQueued(relay)
  await Promise.all(pending)
  assert.equal(queue.states.get(relay).active, 1)
  first.history.release()
  assert.equal(queue.states.size, 0)
})

test('separate connections have independent budgets and invalid capacities reject', async () => {
  const queue = new ReadAdmission({ maxSubscriptionsPerRelay: 1 })
  const first = await queue.acquire(relay, { live: true })
  const second = await queue.acquire('wss://other.example/', { live: true })
  await assert.rejects(queue.acquire(relay, { live: true, history: true }), { code: 'RELAY_READ_CAPACITY' })
  first.live.release()
  second.live.release()
  assert.throws(() => new ReadAdmission({ maxSubscriptionsPerRelay: 0 }), { code: 'INVALID_RELAY_READ_CAPACITY' })
})
