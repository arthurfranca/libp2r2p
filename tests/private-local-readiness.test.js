import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { PrivateMessenger } from '../private-messenger/index.js'
import { createPrivateMessageSession } from '../private-messenger/session/index.js'

globalThis.IDBKeyRange = IDBKeyRange
const owner = 'a'.repeat(64)
const peer = index => index.toString(16).padStart(64, '0')
const gate = () => Promise.withResolvers()
const until = async predicate => {
  for (let n = 0; n < 2000; n++) { if (predicate()) return; await new Promise(resolve => setImmediate(resolve)) }
  assert.fail('condition not reached')
}
function fixture (t, { creation, keys = new Map(), watch = async () => async () => {} } = {}) {
  const records = new Map(); const sent = []; const errors = []; const states = []; const keyCalls = []; const instances = []
  let factories = 0
  const store = async () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  const session = createPrivateMessageSession({
    owner, mode: 'leecher', useContentKeys: false,
    signer: { getPublicKey: async () => owner, withSharedKey: value => ({ getPublicKey: async () => { keyCalls.push(value); await keys.get(value)?.promise; return value } }) },
    messageStorage: { save: async () => ({ result: { ok: true } }) }, openOutbox: store,
    openDownloads: async () => ({ list: async () => [], close () {} }), FileTransfer: () => ({ observe () {} }),
    onError: error => errors.push(error),
    Messenger: async options => {
      factories++
      const messenger = new PrivateMessenger({
        ...options, onStateChanged: state => { states.push(state); options.onStateChanged(state) },
        _indexedDB: new IDBFactory(), _BroadcastChannel: null, offlineRecoverySeconds: 0,
        _onOnline: () => () => {}, _isOnline: async () => true,
        _getRelaysByPubkey: async values => Object.fromEntries(values.map(value => [value, { read: ['wss://test.invalid'] }])),
        _privateMessage: { watch, broadcastRumor: async ({ rumor }) => { sent.push(rumor); return { delivery: { reports: [{ success: true }] } } } }
      })
      instances.push(messenger)
      await messenger.init(options)
      await creation?.promise
      return messenger
    }
  })
  t.after(async () => { for (const key of keys.values()) key.resolve(); creation?.resolve(); await session.close() })
  return {
    session, records, sent, errors, states, keyCalls, instances, factories: () => factories,
    send: (recipient, content = 'test') => session.enqueue({ peer: recipient, event: { pubkey: owner, kind: 9, tags: [], content, created_at: 1 } })
  }
}

test('unlock and concurrent contacts create one messenger without a signer pause', async t => {
  const creation = gate()
  const f = fixture(t, { creation })
  await f.session.setAvailable(false)
  const unlocking = f.session.setAvailable(true)
  await until(() => f.instances.length)
  const contacts = f.session.setPeers([peer(1), peer(2)])
  creation.resolve()
  await Promise.all([unlocking, contacts])
  await f.session.preparePeer(peer(1))
  const id = await f.send(peer(1))
  await until(() => !f.records.has(id))
  assert.equal(f.factories(), 1)
  assert.equal(f.states.some(state => state.pauseReasons.includes('signer')), false)
  assert.equal(f.errors.length, 0)
  assert.equal(f.sent.length, 1)
})

test('foreground preparation passes three blocked background contacts and sends in conversation order', async t => {
  const keys = new Map([1, 2, 3].map(index => [peer(index), gate()]))
  const f = fixture(t, { keys })
  await f.session.setPeers([1, 2, 3, 4, 5].map(peer)); await f.session.setAvailable(true)
  await until(() => f.keyCalls.length === 3)
  const waiting = await f.send(peer(1), 'slow contact')
  const first = await f.send(peer(5), 'first')
  const second = await f.send(peer(5), 'second')
  await until(() => !f.records.has(first) && !f.records.has(second))
  assert.deepEqual(f.sent.map(event => event.content), ['first', 'second'])
  assert.equal(f.records.get(waiting).localSaved[0], true)
  assert.equal(f.keyCalls.includes(peer(4)), false)
  assert.equal(f.keyCalls.filter(value => value === peer(5)).length, 1)
  assert.equal(f.factories(), 1)
})

test('removal and a newer account lock discard delayed preparation', async t => {
  const delayed = gate()
  const f = fixture(t, { keys: new Map([[peer(1), delayed]]) })
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true)
  const preparing = f.session.preparePeer(peer(1)); preparing.catch(() => {})
  await until(() => f.keyCalls.length)
  await f.session.setAvailable(false)
  await assert.rejects(preparing, /CHAT_UNAVAILABLE/)
  await f.session.setPeers([])
  delayed.resolve()
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(f.instances[0].readStatus().pauseReasons, ['signer'])
  await f.session.setAvailable(true)
  assert.equal(f.instances[0].channels.size, 0)
  assert.equal(f.factories(), 1)
})

test('unknown subscription failure on resume does not restore signer or block a writer', async t => {
  const failure = new Error('controlled subscription failure')
  let failing = false
  const f = fixture(t, { watch: async () => { if (failing) throw failure; return async () => {} } })
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true); await f.session.preparePeer(peer(1))
  const messenger = f.instances[0]
  await messenger.update({}, { waitForBackground: true })
  await f.session.setAvailable(false)
  failing = true
  await f.session.setAvailable(true)
  await until(() => f.errors.some(error => error.cause === failure))
  assert.equal(messenger.readStatus().paused, false)
  const id = await f.send(peer(1))
  await until(() => !f.records.has(id))
  assert.equal(f.sent.length, 1)
})

test('local readiness does not wait for remote work; explicit waiting shares the work', async t => {
  const held = gate(); let watches = 0
  const f = fixture(t, { watch: async () => { watches++; await held.promise; return async () => {} } })
  t.after(() => held.resolve())
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true); await f.session.preparePeer(peer(1))
  const messenger = f.instances[0]
  await until(() => watches === 1)
  let settled = false
  const waiting = messenger.update({}, { waitForBackground: true }).then(() => { settled = true })
  await messenger.update({})
  assert.equal(settled, false)
  const id = await f.send(peer(1))
  await until(() => !f.records.has(id))
  assert.equal(settled, false)
  held.resolve(); await waiting
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true)
  await messenger.update({}, { waitForBackground: true })
  assert.equal(watches, 1)
  assert.equal(f.keyCalls.length, 1)
})

test('resume cannot remove a newer pause while mandatory storage is pending', async t => {
  const f = fixture(t)
  await f.session.setAvailable(true)
  const messenger = f.instances[0]
  await messenger.pause('signer')
  const held = gate(); const original = messenger.flushStateWrites.bind(messenger)
  let calls = 0
  messenger.flushStateWrites = async () => { if (++calls === 1) await held.promise; return original() }
  const resuming = messenger.resume('signer')
  await messenger.pause('signer')
  held.resolve(); await resuming
  assert.deepEqual(messenger.readStatus().pauseReasons, ['signer'])
})

test('remote setup is bounded without consuming local preparation or send readiness', async t => {
  const held = gate(); let watches = 0
  const f = fixture(t, { watch: async () => { watches++; await held.promise; return async () => {} } })
  t.after(() => held.resolve())
  await f.session.setPeers(Array.from({ length: 8 }, (_, index) => peer(index + 1)))
  await f.session.setAvailable(true); await f.session.preparePeer(peer(8))
  await until(() => watches === 3)
  const id = await f.send(peer(8)); await until(() => !f.records.has(id))
  assert.equal(watches, 3)
  assert.equal(f.sent.length, 1)
  held.resolve()
})

test('failed background history preserves its error without restoring signer', async t => {
  const f = fixture(t)
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true); await f.session.preparePeer(peer(1))
  const messenger = f.instances[0]
  await messenger.update({}, { waitForBackground: true })
  const failure = new Error('controlled history failure')
  messenger.recoverOfflineRanges = async () => { throw failure }
  await messenger.pause('signer')
  await assert.rejects(messenger.resume('signer', { waitForBackground: true }), error => error === failure)
  assert.equal(messenger.readStatus().paused, false)
  assert.equal(f.errors.at(-1).cause, failure)
  assert.equal(f.errors.at(-1).phase, 'history')
  const id = await f.send(peer(1)); await until(() => !f.records.has(id))
})

test('cancelled presence cannot install a timer for a later channel generation', async t => {
  const f = fixture(t)
  await f.session.setPeers([peer(1)]); await f.session.setAvailable(true); await f.session.preparePeer(peer(1))
  const messenger = f.instances[0]
  await messenger.update({}, { waitForBackground: true })
  messenger.offlineRecoverySeconds = 60
  messenger.channels.get(peer(1)).offlineRecoverySeconds = 60
  const held = gate(); let calls = 0
  messenger.publishSeederPresence = async () => { if (++calls === 1) await held.promise }
  const old = messenger.startPresencePublisher(peer(1))
  await until(() => calls === 1)
  await messenger.pause('signer'); await messenger.resume('signer')
  await messenger.startPresencePublisher(peer(1))
  const timer = messenger.presenceTimers.get(peer(1))
  held.resolve(); await old
  assert.equal(messenger.presenceTimers.get(peer(1)), timer)
  assert.equal(calls, 2)
})
