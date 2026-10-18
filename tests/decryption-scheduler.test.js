import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createDecryptionScheduler } from '../private-messenger/services/decryption-scheduler.js'

function fixture (ready = true) {
  let time = 0; let sequence = 0
  const timers = new Map()
  const scheduler = createDecryptionScheduler({
    ready, now: () => time,
    setTimer: (fn, delay) => { const id = ++sequence; timers.set(id, { fn, at: time + delay }); return id },
    clearTimer: id => timers.delete(id)
  })
  const flush = async () => { for (let i = 0; i < 20; i++) await Promise.resolve() }
  const tick = async milliseconds => {
    const end = time + milliseconds
    for (let count = 0; count < 1000; count++) {
      const next = [...timers].sort(([, a], [, b]) => a.at - b.at)[0]
      if (!next || next[1].at > end) { time = end; await flush(); return }
      time = Math.max(time, next[1].at); timers.delete(next[0]); next[1].fn(); await flush()
    }
    assert.fail('scheduler did not settle')
  }
  return { scheduler, tick, flush, timers, jump: milliseconds => { time += milliseconds } }
}

test('closed startup gate delays reads and starts the idle window only on release', async () => {
  const f = fixture(false); const calls = []
  const controller = new AbortController()
  const read = f.scheduler.waitReady(controller.signal).then(() => calls.push('read'))
  const job = f.scheduler.run('a', () => calls.push('retry'))
  await f.tick(10000)
  assert.deepEqual(calls, [])
  f.scheduler.setReady(true); await read
  await f.tick(249); assert.deepEqual(calls, ['read'])
  await f.tick(1); await job
  assert.deepEqual(calls, ['read', 'retry'])
  assert.equal(f.timers.size, 0)
  f.scheduler.close()
})

test('busy channels do not block idle ones and new requests do not renew the five-second deadline', async () => {
  const f = fixture(); const calls = []
  const release = f.scheduler.activity('busy')
  const first = f.scheduler.run('busy', () => calls.push('busy'))
  const idle = f.scheduler.run('idle', () => calls.push('idle'))
  await f.tick(250); await idle
  assert.deepEqual(calls, ['idle'])
  await f.tick(4500)
  const second = f.scheduler.run('busy', () => calls.push('busy again'))
  await f.tick(249); assert.deepEqual(calls, ['idle'])
  await f.tick(1); await first
  assert.deepEqual(calls, ['idle', 'busy'])
  await f.tick(4999); assert.equal(calls.length, 2)
  await f.tick(1); await second
  assert.deepEqual(calls, ['idle', 'busy', 'busy again'])
  release(); f.scheduler.close()
})

test('priority yields after four records and overdue channels override foreground preference', async () => {
  const f = fixture(); const calls = []
  f.scheduler.priority('a')
  const process = async (peer, count) => {
    for (let i = 0; i < count; i++) await f.scheduler.run(peer, () => calls.push(peer))
  }
  const work = Promise.all([process('a', 6), process('b', 2)])
  await f.tick(250); await work
  assert.deepEqual(calls.slice(0, 5), ['a', 'a', 'a', 'a', 'b'])
  f.scheduler.finish('a'); f.scheduler.finish('b')
  const release = f.scheduler.activity('b')
  const old = f.scheduler.run('b', () => calls.push('overdue'))
  await f.tick(4999)
  const priority = f.scheduler.run('a', () => calls.push('priority'))
  f.jump(1); await f.tick(0); await Promise.all([old, priority])
  assert.deepEqual(calls.slice(-2), ['overdue', 'priority'])
  release(); f.scheduler.close()
})

test('foreground persistence delays retries without turning the deadline into an unbounded wait', async () => {
  const f = fixture(); let called = false
  f.scheduler.foreground(true)
  const work = f.scheduler.run('a', () => { called = true })
  await f.tick(4999); assert.equal(called, false)
  await f.tick(1); await work; assert.equal(called, true)
  f.scheduler.foreground(false); f.scheduler.close()
})

test('a later request gets a fresh deadline and a stale startup release cannot open the gate', async () => {
  const f = fixture(false); let reads = 0; let retries = 0
  const release = f.scheduler.activity('a')
  const waiting = f.scheduler.waitReady().then(() => reads++)
  f.scheduler.setReady(true); f.scheduler.setReady(false)
  await f.flush(); assert.equal(reads, 0)
  f.scheduler.setReady(true); await waiting
  await f.tick(20000)
  const retry = f.scheduler.run('a', () => retries++)
  await f.tick(4999); assert.equal(retries, 0)
  await f.tick(1); await retry; assert.equal(retries, 1)
  release(); f.scheduler.close()
})

test('pause signals, channel removal and close release waiters without late work', async () => {
  const f = fixture(false); let calls = 0
  const controller = new AbortController()
  const waiting = assert.rejects(f.scheduler.waitReady(controller.signal), { name: 'AbortError' })
  const queued = assert.rejects(f.scheduler.run('a', () => calls++, controller.signal), { name: 'AbortError' })
  controller.abort(); await Promise.all([waiting, queued])
  const removed = assert.rejects(f.scheduler.run('b', () => calls++), /PRIVATE_CHANNEL_CHANGED/)
  f.scheduler.remove('b'); await removed
  const closing = assert.rejects(f.scheduler.waitReady(), /PRIVATE_MESSENGER_CLOSED/)
  const pending = assert.rejects(f.scheduler.run('c', () => calls++), /PRIVATE_MESSENGER_CLOSED/)
  f.scheduler.close(); await Promise.all([closing, pending])
  await f.tick(10000); assert.equal(calls, 0); assert.equal(f.timers.size, 0)
})
