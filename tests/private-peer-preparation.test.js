import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createPeerPreparation } from '../private-messenger/session/helpers/peer-preparation.js'

const flush = async () => { for (let i = 0; i < 30; i++) await Promise.resolve() }
function fixture (t, prepare) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 10000 })
  let connected = true
  const listeners = new Set(); const errors = []; const ready = []
  const queue = createPeerPreparation({
    prepare, onReady: peer => ready.push(peer), onError: error => errors.push(error),
    isRetryable: error => error.category === 'timeout', random: () => 0.5,
    online: async () => connected, onOnline: fn => { listeners.add(fn); return () => listeners.delete(fn) }
  })
  t.after(() => queue.close())
  return { queue, errors, ready, listeners, offline: () => { connected = false }, online: () => { connected = true; for (const fn of [...listeners]) fn() }, tick: async ms => { t.mock.timers.tick(ms); await flush() } }
}

test('peer retry respects absolute advice, parks offline and resumes without occupying slots', async t => {
  let attempts = 0
  const f = fixture(t, async peer => {
    if (peer === 'slow' && ++attempts < 3) throw Object.assign(new Error('timeout'), { category: 'timeout', retryAt: 15000 })
    return peer
  })
  f.queue.reconcile(['slow']); f.queue.setAvailable(true)
  const result = f.queue.request('slow')
  await flush()
  await f.tick(4999); assert.equal(attempts, 1)
  f.offline(); await f.tick(1)
  assert.equal(attempts, 1); assert.equal(f.listeners.size, 1)
  f.queue.reconcile(['slow', 'other'])
  assert.equal(await f.queue.request('other'), 'other')
  f.online(); await f.tick(0)
  assert.equal(attempts, 2)
  await f.tick(1999); assert.equal(attempts, 2)
  await f.tick(1)
  assert.equal(await result, 'slow'); assert.equal(attempts, 3)
  assert.equal(f.listeners.size, 0)
})

test('unknown preparation failures stay isolated until explicitly retried', async t => {
  let attempts = 0; const failure = new Error('permission denied')
  const f = fixture(t, async peer => { if (peer === 'a' && ++attempts === 1) throw failure; return peer })
  f.queue.reconcile(['a', 'b']); f.queue.setAvailable(true)
  await assert.rejects(f.queue.request('a'), error => error === failure)
  assert.equal(await f.queue.request('b'), 'b')
  await f.tick(300000); assert.equal(attempts, 1)
  f.queue.reconcile(['a', 'b']); await flush(); assert.equal(attempts, 1)
  assert.equal(await f.queue.request('a', { retry: true }), 'a')
  assert.equal(attempts, 2)
})

test('closing preparation removes its offline observer and never resurrects a removed peer', async t => {
  let calls = 0
  const f = fixture(t, async () => { calls++; throw Object.assign(new Error('timeout'), { category: 'timeout' }) })
  f.queue.reconcile(['a']); f.queue.setAvailable(true)
  const pending = f.queue.request('a'); pending.catch(() => {})
  await flush(); f.offline(); await f.tick(1000)
  assert.equal(f.listeners.size, 1)
  f.queue.close()
  await assert.rejects(pending, /CHAT_UNAVAILABLE/)
  assert.equal(f.listeners.size, 0)
  f.online(); await f.tick(300000); assert.equal(calls, 1)
})
