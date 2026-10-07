import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { PrivateMessenger } from '../private-messenger/index.js'
import { createPrivateMessageSession } from '../private-messenger/session/index.js'
import { createPauseRecovery } from '../private-messenger/helpers/pause-recovery.js'
import { getEventHash } from '../event/index.js'

const owner = 'a'.repeat(64); const peer = 'b'.repeat(64); const channel = 'c'.repeat(64)
const until = async predicate => { for (let n = 0; n < 200; n++) { if (predicate()) return; await new Promise(resolve => setImmediate(resolve)) }; assert.fail('condition not reached') }
function clock () {
  let now = 0
  const timers = new Set()
  return {
    now: () => now, timers,
    setTimer: (fn, delay) => { const timer = { fn, at: now + delay, delay }; timers.add(timer); return timer },
    clearTimer: timer => timers.delete(timer),
    async tick (ms) { now += ms; for (const timer of [...timers]) if (timer.at <= now) { timers.delete(timer); await timer.fn() } }
  }
}
async function fixture (t, { save = async () => ({ result: { ok: true } }) } = {}) {
  globalThis.IDBKeyRange = IDBKeyRange
  const time = clock(); const errors = []; const records = new Map(); const sent = []; const status = []
  const network = { online: true, listeners: new Set(), watches: 0, failWatch: false }
  const store = async () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  let messenger; let hooks
  const session = createPrivateMessageSession({
    owner, mode: 'leecher', useContentKeys: false,
    signer: { getPublicKey: async () => owner, withSharedKey: () => ({ getPublicKey: async () => channel }) },
    messageStorage: { save }, openOutbox: store, openDownloads: store, FileTransfer: () => ({ observe () {} }),
    onError: error => errors.push(error), _setTimeout: time.setTimer, _clearTimeout: time.clearTimer, _random: () => 0.5,
    Messenger: async options => {
      messenger = new PrivateMessenger({
        ...options, onStateChanged: value => { status.push(value); options.onStateChanged(value) },
        offlineRecoverySeconds: 0, _indexedDB: new IDBFactory(), _BroadcastChannel: null,
        _setTimeout: time.setTimer, _clearTimeout: time.clearTimer, _random: () => 0.5,
        _isOnline: async () => network.online,
        _onOnline: fn => { network.listeners.add(fn); return () => network.listeners.delete(fn) },
        _getRelaysByPubkey: async () => ({ [owner]: { read: ['wss://test.invalid'] }, [peer]: { read: ['wss://test.invalid'] } }),
        _privateMessage: {
          watch: async value => { network.watches++; if (network.failWatch) throw Object.assign(new Error('CONTROLLED_TIMEOUT'), { category: 'timeout' }); hooks = value; return async () => {} },
          broadcastRumor: async value => { sent.push(value.rumor); return { delivery: { reports: [{ success: true }] } } }
        }
      })
      return messenger.init(options)
    }
  })
  t.after(() => session.close())
  await session.setPeers([peer]); await session.setAvailable(true)
  await session.preparePeer(peer)
  await messenger.update({}, { waitForBackground: true })
  let createdAt = 2
  const send = (to = peer) => session.enqueue({ peer: to, event: { pubkey: owner, kind: 9, tags: [], content: 'outgoing', created_at: createdAt++ } })
  return {
    session, messenger, time, network, errors, records, sent, status, send, receive: async () => {
      const event = { pubkey: peer, kind: 9, tags: [], content: 'incoming', created_at: 1 }; event.id = getEventHash(event)
      return hooks.onMessage({ event, senderPubkey: peer, outer: { created_at: 1 } })
    }
  }
}

test('status snapshots are isolated and paused sends wake without a backoff or historical completion', async t => {
  const f = await fixture(t)
  await f.messenger.pause('vault')
  const snapshot = f.messenger.readStatus(); snapshot.pauseReasons.length = 0
  assert.deepEqual(f.messenger.readStatus().pauseReasons, ['vault'])
  assert.throws(() => f.messenger.requireWritableChannel(channel), error => error.message === 'PRIVATE_MESSENGER_PAUSED' && error.pauseReasons[0] === 'vault')
  const id = await f.send()
  await until(() => f.records.get(id)?.failed)
  assert.equal(f.sent.length, 0); assert.equal(f.time.timers.size, 0)
  assert.deepEqual(f.errors.at(-1).pauseReasons, ['vault'])
  await f.send(owner)
  await until(() => f.records.size === 1)
  let release
  const held = new Promise(resolve => { release = resolve })
  f.messenger.recoverOfflineRanges = () => held
  const resume = f.messenger.resume('vault')
  await until(() => f.sent.length === 1 && !f.records.has(id))
  release([]); await resume
  assert.equal(f.status[0].paused, false)
  await f.session.close()
  assert.equal(f.status.at(-1).closed, true)
})

test('session-storage retries the actual reservation and keeps independent pauses', async t => {
  let failing = true; let saves = 0
  const f = await fixture(t, { save: async event => { if (event.pubkey === peer) { saves++; if (failing) throw new Error('TEMPORARY_STORAGE_FAILURE') }; return { result: { ok: true } } } })
  await f.receive()
  await until(() => f.messenger.pauseReasons.has('session-storage'))
  const id = await f.send()
  await until(() => f.records.get(id)?.failed)
  await f.messenger.pause('vault')
  failing = false
  await until(() => f.time.timers.size > 0)
  await f.time.tick(1000)
  assert.equal(saves, 2)
  assert.deepEqual(f.messenger.readStatus().pauseReasons, ['vault'])
  assert.equal(f.sent.length, 0)
  await f.messenger.resume('vault')
  await until(() => !f.records.has(id))
})

test('network recovery waits offline without spending backoff and route failure does not re-pause publication', async t => {
  const f = await fixture(t)
  f.network.online = false
  await f.messenger.pauseInternally('network')
  const id = await f.send()
  await until(() => f.records.get(id)?.failed)
  await f.time.tick(1000)
  assert.equal(f.network.listeners.size, 1)
  assert.equal(f.network.watches, 1)
  await f.time.tick(60000)
  assert.equal(f.network.watches, 1)
  f.network.online = true; f.network.failWatch = true
  for (const wake of [...f.network.listeners]) wake()
  await f.time.tick(0)
  await until(() => f.sent.length === 1)
  assert.equal(f.messenger.readStatus().paused, false)
  assert.equal(f.time.timers.size, 1, 'only the failed read owns recovery')
  f.network.failWatch = false
  await f.time.tick(1000)
  assert.equal(f.messenger.stopByChannel.size, 1)
  assert.equal(f.time.timers.size, 0)
})

test('internal storage repair retries without allowing explicit storage pauses to resume themselves', async t => {
  const f = await fixture(t)
  await f.messenger.pause('storage')
  assert.equal(f.time.timers.size, 0)
  await f.messenger.resume('storage')
  let failures = 2
  const flush = f.messenger.flushStateWrites.bind(f.messenger)
  f.messenger.flushStateWrites = async () => { if (failures-- > 0) throw new Error('DISK_TEMPORARILY_UNAVAILABLE'); await flush() }
  await assert.rejects(f.messenger.pauseInternally('storage'), /DISK_TEMPORARILY_UNAVAILABLE/)
  await f.time.tick(1000)
  assert.equal(f.messenger.readStatus().paused, true)
  assert.equal([...f.time.timers][0].delay, 2000)
  await f.time.tick(2000)
  assert.equal(f.messenger.readStatus().paused, false)
})

test('unchanged peers and availability do not reopen live subscriptions', async t => {
  const f = await fixture(t)
  await f.session.setPeers([peer]); await f.session.setAvailable(true)
  assert.equal(f.network.watches, 1)
  await f.messenger.update({ channels: [...f.messenger.channels.values()] })
  assert.equal(f.network.watches, 1)
})

test('cancelled paused work cannot be resurrected and close removes offline monitoring', async t => {
  const f = await fixture(t)
  f.network.online = false; await f.messenger.pauseInternally('network')
  const id = await f.send(); await until(() => f.records.get(id)?.failed)
  await f.session.cancel(id)
  await f.time.tick(1000)
  await f.session.close()
  assert.equal(f.network.listeners.size, 0); assert.equal(f.time.timers.size, 0)
  f.network.online = true
  assert.equal(f.sent.length, 0); assert.equal(f.records.size, 0)
})

test('local recovery backoff caps at thirty seconds and ignores stale work after stop', async () => {
  const time = clock(); const delays = []; const failure = new Error('temporary')
  const job = createPauseRecovery({ attempt: async () => { throw failure }, retryable: () => true, onError: () => {}, now: time.now, setTimer: time.setTimer, clearTimer: time.clearTimer, random: () => 0.5 })
  job.start()
  for (let n = 0; n < 8; n++) { const timer = [...time.timers][0]; delays.push(timer.delay); await time.tick(timer.delay) }
  assert.deepEqual(delays, [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000])
  job.stop(); assert.equal(time.timers.size, 0)
})

test('internal queue recovery retries the refused record before releasing storage and marking it seen', async t => {
  const f = await fixture(t)
  const enqueue = f.messenger.queue.enqueue.bind(f.messenger.queue)
  let failing = true
  f.messenger.queue.enqueue = async record => { if (failing) throw new Error('IDB_TEMPORARY_WRITE_FAILURE'); return enqueue(record) }
  await assert.rejects(f.receive(), /IDB_TEMPORARY_WRITE_FAILURE/)
  assert.equal(f.messenger.readStatus().pauseReasons[0], 'storage')
  assert.equal(f.messenger.readState().channels[channel].lastSeenAt, undefined)
  assert.equal(f.messenger.pendingStorageWrites.size, 1)
  await f.time.tick(1000)
  assert.equal(f.messenger.readStatus().paused, true)
  assert.equal([...f.time.timers][0].delay, 2000)
  failing = false
  await f.time.tick(2000)
  assert.equal(f.messenger.pendingStorageWrites.size, 0)
  assert.equal(f.messenger.readStatus().paused, false)
  assert.equal(f.messenger.readState().channels[channel].lastSeenAt, 1)
})

test('absolute retry advice survives connectivity wakeups and is not added to the backoff', async () => {
  const time = clock(); const listeners = new Set()
  let attempts = 0; let online = true
  const job = createPauseRecovery({
    attempt: async () => { if (++attempts === 1) throw Object.assign(new Error('rate-limited: slow down'), { retryAt: 10000 }) },
    online: async () => online, onOnline: fn => { listeners.add(fn); return () => listeners.delete(fn) },
    retryable: () => true, onError: () => {}, now: time.now, setTimer: time.setTimer, clearTimer: time.clearTimer, random: () => 0.5
  })
  await job.start({ immediate: true })
  assert.equal([...time.timers][0].at, 10000)
  await time.tick(5000); job.wake()
  assert.equal([...time.timers][0].at, 10000)
  online = false; await time.tick(5000)
  assert.equal(attempts, 1); assert.equal(listeners.size, 1)
  online = true; for (const wake of [...listeners]) wake()
  await time.tick(0)
  assert.equal(attempts, 2); assert.equal(time.timers.size, 0)
})

test('a local permission failure does not acquire automatic storage recovery', async t => {
  const f = await fixture(t)
  f.messenger.queue.enqueue = async () => { throw new DOMException('Account permission denied', 'NotAllowedError') }
  await assert.rejects(f.receive(), { name: 'NotAllowedError' })
  assert.deepEqual(f.messenger.readStatus().pauseReasons, ['storage'])
  assert.equal(f.time.timers.size, 0)
  assert.equal(f.messenger.pendingStorageWrites.size, 0)
  await f.session.setAvailable(true)
  assert.equal(f.messenger.readStatus().paused, true)
})

test('concurrent recovery connectivity checks share one cancellable probe per messenger', async t => {
  const f = await fixture(t)
  let calls = 0; let signal
  const held = Promise.withResolvers()
  f.messenger._isOnline = async options => { calls++; signal = options.signal; return held.promise }
  const a = f.messenger.checkPauseOnline(); const b = f.messenger.checkPauseOnline()
  assert.equal(a, b)
  await Promise.resolve()
  assert.equal(calls, 1)
  await f.session.close()
  assert.equal(signal.aborted, true)
  held.resolve(true)
  await a
})

test('first recovery respects existing absolute advice and repeated scheduling does not renew it', async () => {
  const time = clock(); let attempts = 0
  const job = createPauseRecovery({ attempt: async () => { attempts++ }, retryable: () => true, onError: () => {}, now: time.now, setTimer: time.setTimer, clearTimer: time.clearTimer, random: () => 0.5 })
  job.start({ retryAt: 5000 })
  await time.tick(1000)
  job.start({ retryAt: 5000 })
  assert.equal(attempts, 0)
  assert.equal([...time.timers][0].at, 5000)
  await time.tick(4000)
  assert.equal(attempts, 1)
})

test('explicit pause ownership is not overwritten by an internal interruption of the same name', async t => {
  const f = await fixture(t)
  await f.messenger.pause('network')
  await f.messenger.pauseInternally('network')
  assert.equal(f.time.timers.size, 0)
  assert.equal(f.messenger.automaticPauses.has('network'), false)
  await f.messenger.resume('network')
  await f.messenger.pauseInternally('network')
  assert.equal(f.time.timers.size, 1)
  await f.messenger.pause('network')
  assert.equal(f.time.timers.size, 0)
})

test('definitive and unknown relay watch refusals do not restore a global network pause', async t => {
  const f = await fixture(t)
  for (const prefix of ['blocked', 'restricted', 'auth-required', 'pow', 'unknown']) {
    await f.messenger.pauseInternally('network')
    const error = Object.assign(new Error(`${prefix}: controlled refusal`), { category: 'relay' })
    f.messenger._privateMessage.watch = async () => { throw error }
    await f.time.tick(1000)
    assert.equal(f.messenger.readStatus().paused, false)
    assert.equal(f.time.timers.size, 0)
    assert.equal(f.errors.at(-1), error)
  }
})

test('state observation rejects malformed callback arguments with a validation code', () => {
  assert.throws(() => new PrivateMessenger({ onStateChanged: true }), error => error.name === 'ValidationError' && error.code === 'INVALID_ON_STATE_CHANGED')
})
