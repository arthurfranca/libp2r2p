import { test } from 'node:test'
import { readFile } from 'node:fs/promises'
import assert from 'node:assert/strict'
import { createSessionMessenger } from './helpers/session-messenger.js'
import { getEventHash } from '../event/index.js'
import { createPrivateMessageSession } from '../private-messenger/session/index.js'

const owner = 'a'.repeat(64)
const peer = 'b'.repeat(64)
const event = { pubkey: owner, kind: 9, content: 'test', tags: [], created_at: 1 }
const until = async predicate => {
  for (let i = 0; i < 100; i++) { if (predicate()) return; await new Promise(resolve => setTimeout(resolve, 2)) }
  assert.fail('condition not reached')
}
async function fixture (t, { publish, fallbackRelays, save = async () => ({ result: { ok: true } }), ...sessionOptions } = {}) {
  const errors = []; const sendErrors = []; const records = new Map(); const onlineListeners = new Set(); const priorities = []
  const decryptionReady = []; const decryptionPriorities = []
  const store = () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  let messengerOptions; let messenger
  const session = createPrivateMessageSession({
    owner, fallbackRelays, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save }, openOutbox: store, openDownloads: store,
    Messenger: async options => {
      messengerOptions = options
      return (messenger = createSessionMessenger(options, {
        broadcastRumor: publish,
        setPendingDecryptionReady: value => decryptionReady.push(value),
        setPendingDecryptionPriority: value => decryptionPriorities.push(value),
        prioritizeRange: async (channelPubkey, request) => { priorities.push({ channelPubkey, request }); return true }
      }))
    },
    FileTransfer: () => ({ observe () {} }),
    _onOnline: listener => { onlineListeners.add(listener); return () => onlineListeners.delete(listener) },
    onError: error => errors.push(error), onSendError: (error, context) => sendErrors.push({ error, context }),
    ...sessionOptions
  })
  t.after(() => session.close())
  await session.setPeers([peer]); await session.setAvailable(true)
  return { session, messenger: () => messenger, messengerOptions: () => messengerOptions, errors, sendErrors, records, onlineListeners, priorities, decryptionReady, decryptionPriorities, reconnect: () => Promise.all([...onlineListeners].map(listener => listener())) }
}

test('session enables content-key lookup by default and forwards it to the messenger', async t => {
  const f = await fixture(t)
  assert.equal(f.messengerOptions().useContentKeys, true)
})

test('session forwards pending-decryption admission and maps peer priority to prepared channels', async t => {
  const f = await fixture(t, { pendingDecryptionReady: false })
  assert.equal(f.messengerOptions().pendingDecryptionReady, false)
  assert.equal(f.decryptionReady.at(-1), false)
  f.session.setPendingDecryptionPriority(peer)
  await f.session.preparePeer(peer)
  assert.equal(f.decryptionPriorities.at(-1), peer)
  f.session.setPendingDecryptionReady(true)
  assert.equal(f.decryptionReady.at(-1), true)
  f.session.setPendingDecryptionPriority(owner)
  assert.equal(f.decryptionPriorities.at(-1), null)
  f.session.setPendingDecryptionPriority(peer)
  await f.session.setPeers([])
  assert.equal(f.decryptionPriorities.at(-1), null)
  assert.throws(() => f.session.setPendingDecryptionReady(null), { code: 'INVALID_PENDING_DECRYPTION_READY' })
  assert.throws(() => f.session.setPendingDecryptionPriority('invalid'), { code: 'INVALID_PENDING_DECRYPTION_PRIORITY' })
})

test('session can disable content-key lookup', async t => {
  const f = await fixture(t, { useContentKeys: false })
  assert.equal(f.messengerOptions().useContentKeys, false)
})

test('recovery archive waits for active foreground persistence', async t => {
  const localStarted = Promise.withResolvers()
  const releaseLocal = Promise.withResolvers()
  const archived = Promise.withResolvers()
  let archiveCalled = false
  const content = Buffer.from(`${JSON.stringify(['payload'])}\n${JSON.stringify([peer, 'ciphertext'])}\n`).toString('base64')
  const seed = {
    channelPubkey: peer,
    publication: {
      channelPubkey: peer,
      events: [{ id: 'publication-id' }],
      seeds: [{
        channelPubkey: peer,
        router: {
          kind: 26300,
          pubkey: 'c'.repeat(64),
          created_at: 10,
          tags: [['f', owner], ['c', '0', '1']],
          content
        },
        innerEventIdsByRowIndex: { 1: getEventHash(event) }
      }]
    },
    target: {},
    retentionSeconds: 60
  }
  const recoveryStorage = {
    seeds: {
      async putMany (rows) { archiveCalled = true; archived.resolve(rows) },
      async hasMany (rows) { return rows.map(() => false) },
      async has () { return false },
      async * iterate () {}
    },
    authorizations: { async * iterate () {} }
  }
  const f = await fixture(t, {
    save: async () => { localStarted.resolve(); await releaseLocal.promise; return { result: { ok: true } } },
    publish: async options => {
      await options.onPreparedSeed(seed)
      return { delivery: { reports: [{ success: true }] } }
    },
    recoveryStorage
  })
  f.messenger().offlineRecoverySecondsFor = () => 60

  await f.session.enqueue({ peer, event })
  await localStarted.promise
  await new Promise(resolve => setTimeout(resolve, 30))
  assert.equal(archiveCalled, false)
  releaseLocal.resolve()
  const rows = await archived.promise
  assert.equal(rows.length, 1)
})

test('session prefetches content keys for the owner and current peers', async t => {
  const calls = []
  const proof = { iykcPubkey: 'c'.repeat(64), iykcProof: `${1}:${'a'.repeat(128)}` }
  const f = await fixture(t, {
    _getIykcProofs: async pubkeys => {
      calls.push(pubkeys)
      return { [peer]: proof }
    }
  })

  assert.deepEqual(await f.session.prefetchContentKeys(), { [peer]: proof })
  assert.deepEqual(calls, [[owner, peer]])
})

test('session prefetch is best-effort when the lookup fails or is disabled', async t => {
  const failing = await fixture(t, { _getIykcProofs: async () => { throw new Error('offline') } })
  assert.deepEqual(await failing.session.prefetchContentKeys([peer]), {})

  const disabled = await fixture(t, {
    useContentKeys: false,
    _getIykcProofs: async () => { throw new Error('must not run') }
  })
  assert.deepEqual(await disabled.session.prefetchContentKeys(), {})
})

test('session forwards prioritized ranges to the messenger channel', async t => {
  const f = await fixture(t)

  assert.equal(await f.session.prioritizeRange(peer, { since: 1000 }), true)
  assert.equal(await f.session.prioritizeRange(peer, { type: 'tail' }), true)
  await until(() => f.priorities.length === 2)

  assert.ok(f.priorities.every(entry => entry.channelPubkey === peer))
  assert.deepEqual(f.priorities.map(entry => entry.request.type), ['unread-page', 'tail'])
  assert.equal(f.priorities[0].request.since, 1000)
})

test('session validates prioritized range input', async t => {
  const f = await fixture(t)
  await assert.rejects(() => f.session.prioritizeRange('nope', { since: 1 }), /INVALID_PRIORITY_PEER/)
  await assert.rejects(() => f.session.prioritizeRange(peer, { since: 1, type: 'kind' }), /INVALID_PRIORITY_TYPE/)
  await assert.rejects(() => f.session.prioritizeRange(peer, { type: 'unread-page' }), /PRIORITY_SINCE_REQUIRED/)
  await assert.rejects(() => f.session.prioritizeRange(peer, { since: 1.5 }), /INVALID_PRIORITY_RANGE/)
})

test('session queues prioritized ranges until the channel exists', async t => {
  const priorities = []
  const session = createPrivateMessageSession({
    owner,
    recoveryStorage: null,
    signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save: async () => ({ result: { ok: true } }) },
    Messenger: async options => createSessionMessenger(options, {
      prioritizeRange: async (channelPubkey, request) => { priorities.push({ channelPubkey, request }); return true }
    }),
    FileTransfer: () => ({ observe () {} }),
    openOutbox: async () => ({ list: async () => [], put: async () => {}, remove: async () => {}, close: async () => {} }),
    openDownloads: async () => ({ list: async () => [], put: async () => {}, remove: async () => {}, close: async () => {} }),
    onError: () => {}
  })
  t.after(() => session.close())

  assert.equal(await session.prioritizeRange(peer, { since: 1000 }), true)
  assert.equal(priorities.length, 0)
  await session.setPeers([peer])
  await session.setAvailable(true)
  await until(() => priorities.length === 1)
  assert.deepEqual(priorities[0], {
    channelPubkey: peer,
    request: { since: 1000, until: undefined, type: 'unread-page' }
  })
})

test('a failing download intent is reported once across reconfigures', async t => {
  const gate = Promise.withResolvers()
  const root = 'a'.repeat(64)
  const id = getEventHash({ kind: 0, pubkey: owner, created_at: 0, tags: [], content: `${peer}:${root}` })
  const entry = { id, file: { peer, root, size: 1 } }
  let downloads = 0
  const f = await fixture(t, {
    FileTransfer: () => ({
      observe () {},
      download: async () => { downloads++; return gate.promise },
      cancel () {}
    }),
    openDownloads: async () => ({ list: async () => [entry], put: async () => {}, remove: async () => {}, close: async () => {} })
  })

  await f.session.setPeers([peer])
  gate.reject(Object.assign(new Error('FILE_DOWNLOAD_STALLED'), { code: 'FILE_DOWNLOAD_STALLED' }))
  await until(() => f.errors.length >= 1)
  await new Promise(resolve => setTimeout(resolve, 20))

  assert.equal(downloads, 1)
  assert.equal(f.errors.filter(error => error.code === 'FILE_DOWNLOAD_STALLED').length, 1)
})

test('send errors identify the owning message when a quoted event is rejected', async t => {
  const f = await fixture(t, { publish: async () => ({ delivery: { reports: [{ success: false, total: 1, errors: [{ relay: 'wss://test.invalid', reason: new Error('blocked: policy') }] }] } }) })
  const quote = { ...event, content: 'quote' }
  const id = await f.session.enqueue({ peer, event, context: [quote] })
  await until(() => f.sendErrors.length === 1)
  assert.deepEqual(f.sendErrors[0].context, { id, peer })
  assert.equal(f.sendErrors[0].error, f.errors[0])
  assert.equal(f.sendErrors[0].error.eventId, getEventHash(quote))
  assert.equal(f.sendErrors[0].error.code, 'MESSAGE_NOT_PUBLISHED')
  await f.session.retry(id)
  assert.equal(f.sendErrors.length, 2)
  assert.equal('error' in f.records.get(id), false, 'native diagnostics are never persisted')
})

test('self-chat persistence failures retain their custom code and main ID', async t => {
  const f = await fixture(t, { save: async () => ({ result: { ok: false, code: 'permission_denied' } }) })
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => f.sendErrors.length === 1)
  assert.equal(f.sendErrors[0].error.code, 'PERMISSION_DENIED')
  assert.deepEqual(f.sendErrors[0].context, { id, peer: owner })
})

test('cancelled sends and deletions do not report send failures', async t => {
  const gate = Promise.withResolvers()
  let started = false
  const f = await fixture(t, { publish: async () => { started = true; await gate.promise; throw new Error('offline') } })
  const id = await f.session.enqueue({ peer, event })
  await until(() => started)
  await f.session.cancel(id)
  gate.resolve()
  await f.session.retry(id)
  await f.session.close()
  assert.equal(f.sendErrors.length, 0)
  const g = await fixture(t, { publish: async () => { throw new Error('offline') } })
  await g.session.enqueue({ peer, event: { ...event, kind: 5 }, deletion: true })
  await until(() => g.errors.length === 1)
  assert.equal(g.sendErrors.length, 0)
})

test('offline fallback stays pending without send-error feedback until availability returns', async t => {
  let online = false
  const f = await fixture(t, {
    publish: async () => ({
      delivery: {
        reports: online
          ? [{ success: true }]
          : [{ success: false, retryWhenAvailable: true, total: 2, promise: Promise.resolve({ total: 2, fulfilled: 0, errors: [{ relay: 'wss://test.invalid', reason: Object.assign(new Error('PUBLISH_TIMEOUT'), { category: 'timeout' }) }] }) }]
      }
    })
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.errors.length === 1)
  assert.equal(f.records.get(id).status, 'pending')
  assert.equal(f.records.get(id).failed, true)
  assert.equal(f.sendErrors.length, 0)
  online = true
  await f.session.setAvailable(true)
  await until(() => !f.records.has(id))
  assert.equal(f.sendErrors.length, 0)
})

test('a publication circuit keeps the send pending until its absolute retry deadline', async t => {
  t.mock.method(Date, 'now', () => 1000)
  const timers = []
  const f = await fixture(t, {
    _setTimeout: (fn, delay) => { const timer = { fn, delay, cancelled: false }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    publish: async () => {
      throw Object.assign(new Error('RELAY_CIRCUIT_OPEN'), {
        code: 'RELAY_CIRCUIT_OPEN',
        category: 'timeout',
        retryAt: 6000,
        retryWhenAvailable: true
      })
    }
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.errors.some(error => error.code === 'RELAY_CIRCUIT_OPEN'))
  assert.equal(f.records.get(id).status, 'pending')
  assert.equal(f.sendErrors.length, 0)
  assert.ok(timers.some(timer => !timer.cancelled && timer.delay >= 5000))
})

test('a paused messenger parks the send and wakes on state release without a retry timer', async t => {
  const timers = []
  let publications = 0
  const f = await fixture(t, {
    _setTimeout: (fn, delay) => { const timer = { fn, delay, cancelled: false }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    publish: async () => { publications++; return { delivery: { reports: [{ success: true }] } } }
  })
  await f.messenger().pause('network')
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(id)?.status === 'pending' && f.records.get(id)?.failed === true)
  assert.equal(f.sendErrors.length, 0)
  assert.equal(publications, 0)
  assert.equal(timers.length, 0)
  await f.messenger().resume('network')
  await until(() => !f.records.has(id))
  assert.equal(publications, 1)
  assert.equal(f.sendErrors.length, 0)
  assert.equal(timers.length, 0)
})

test('a transient outage owns a fresh online listener and resumes without an account-state change', async t => {
  let online = false
  const f = await fixture(t, {
    publish: async () => ({ delivery: { reports: online ? [{ success: true }] : [{ success: false, retryWhenAvailable: true, retryWhenOnline: true, promise: Promise.resolve({ errors: [] }) }] } })
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.onlineListeners.size === 1)
  assert.equal(f.records.get(id).status, 'pending')
  assert.equal(f.sendErrors.length, 0)
  online = true
  await f.reconnect()
  await until(() => !f.records.has(id))
  assert.equal(f.onlineListeners.size, 0)
  assert.equal(f.sendErrors.length, 0)
})

test('cancel, signer unavailability and close release pending online listeners', async t => {
  for (const operation of ['cancel', 'unavailable', 'close']) {
    let publications = 0
    const f = await fixture(t, {
      publish: async () => { publications++; return { delivery: { reports: [{ success: false, retryWhenAvailable: true, retryWhenOnline: true, promise: Promise.resolve({ errors: [] }) }] } } }
    })
    const id = await f.session.enqueue({ peer, event })
    await until(() => f.onlineListeners.size === 1)
    const staleCallback = [...f.onlineListeners][0]
    if (operation === 'cancel') await f.session.cancel(id)
    else if (operation === 'unavailable') await f.session.setAvailable(false)
    else await f.session.close()
    assert.equal(f.onlineListeners.size, 0)
    await staleCallback()
    assert.equal(publications, 1)
    assert.equal(f.sendErrors.length, 0)
  }
})

test('session forwards normalized fallback relays and rejects invalid public configuration', async t => {
  const f = await fixture(t, { fallbackRelays: ['wss://FALLBACK.example/', 'wss://fallback.example'] })
  assert.deepEqual(f.messengerOptions().fallbackRelays, ['wss://fallback.example'])
  assert.throws(() => createPrivateMessageSession({ fallbackRelays: 'wss://fallback.example' }), { code: 'INVALID_FALLBACK_RELAYS' })
})

test('transient availability failures without a pause still use bounded send retries', async t => {
  const timers = []
  let available = false
  const f = await fixture(t, {
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    _random: () => 0.5,
    publish: async () => {
      if (!available) throw new Error('CHAT_UNAVAILABLE')
      return { delivery: { reports: [{ success: true }] } }
    }
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(id)?.failed)
  assert.equal(f.messenger().readStatus().paused, false)
  assert.equal(timers[0].delay, 1000)
  available = true
  await timers[0].fn()
  await until(() => !f.records.has(id))
  assert.equal(f.sendErrors.length, 0)
})

test('a pause overtaking a pending availability retry parks it until state release', async t => {
  const timers = []
  let publications = 0; let available = false
  const f = await fixture(t, {
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    publish: async () => {
      publications++
      if (!available) throw new Error('CHAT_UNAVAILABLE')
      return { delivery: { reports: [{ success: true }] } }
    }
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(id)?.failed)
  await f.messenger().pause('network')
  assert.equal(timers[0].cancelled, true)
  await timers[0].fn()
  assert.equal(publications, 1, 'even a stale timer cannot repeat a known pause')
  available = true
  await f.messenger().resume('network')
  await until(() => !f.records.has(id))
  assert.equal(publications, 2)
})

test('a pause raised during publication uses current state even without error reason metadata', async t => {
  let publications = 0
  const timers = []
  const f = await fixture(t, {
    _setTimeout: (fn, delay) => { timers.push({ fn, delay }); return timers.at(-1) },
    publish: async () => {
      if (++publications === 1) {
        await f.messenger().pause('storage')
        throw new Error('PRIVATE_MESSENGER_PAUSED')
      }
      return { delivery: { reports: [{ success: true }] } }
    }
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(id)?.failed)
  assert.equal(timers.length, 0)
  await f.messenger().resume('storage')
  await until(() => !f.records.has(id))
  assert.equal(publications, 2)
})

test('session rejects factories missing state observation and closes rejected candidates', async t => {
  for (const missing of ['reader', 'notification', 'snapshot']) {
    let closes = 0
    const session = createPrivateMessageSession({
      owner, signer: { getPublicKey: async () => owner }, messageStorage: {},
      openOutbox: async () => ({ list: async () => [], close () {} }),
      Messenger: async options => {
        const status = { closed: false, paused: false, pauseReasons: [] }
        if (missing !== 'notification') options.onStateChanged(status)
        return {
          ...(missing !== 'reader' ? { readStatus: () => missing === 'snapshot' ? { paused: false } : status } : {}),
          close: async () => { closes++; options.onStateChanged({ closed: true, paused: false, pauseReasons: [] }) }
        }
      }, onError: () => {}
    })
    t.after(() => session.close())
    await assert.rejects(session.setAvailable(true), { name: 'ValidationError', code: missing === 'snapshot' ? 'INVALID_MESSENGER_STATUS' : 'MESSENGER_STATE_CONTRACT_REQUIRED' })
    assert.equal(closes, 1)
  }
})

test('callbacks from a rejected factory cannot unpause a replacement messenger', async t => {
  let staleCallback; let current; let attempts = 0; let publications = 0
  const records = new Map()
  const session = createPrivateMessageSession({
    owner, useContentKeys: false, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save: async () => ({ result: { ok: true } }) },
    openOutbox: async () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} }),
    openDownloads: async () => ({ list: async () => [], close () {} }), FileTransfer: () => ({ observe () {} }),
    Messenger: async options => {
      if (++attempts === 1) {
        staleCallback = options.onStateChanged
        staleCallback({ closed: false, paused: false, pauseReasons: [] })
        return { close () {} }
      }
      current = createSessionMessenger(options, { broadcastRumor: async () => { publications++; return { delivery: { reports: [{ success: true }] } } } })
      await current.pause('vault')
      return current
    }, onError: () => {}
  })
  t.after(() => session.close())
  await session.setPeers([peer])
  await assert.rejects(session.setAvailable(true), { code: 'MESSENGER_STATE_CONTRACT_REQUIRED' })
  await session.setAvailable(true)
  const id = await session.enqueue({ peer, event })
  await until(() => records.get(id)?.failed)
  staleCallback({ closed: false, paused: false, pauseReasons: [] })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(publications, 0)
  assert.equal(records.get(id).status, 'pending')
  await current.resume('vault')
  await until(() => !records.has(id))
  assert.equal(publications, 1)
})

test('session forwards fallback delay without changing the default policy', async t => {
  const unchanged = await fixture(t)
  assert.equal(unchanged.messengerOptions().fallbackDelayMs, null)
  const early = await fixture(t, { fallbackDelayMs: 3000 })
  assert.equal(early.messengerOptions().fallbackDelayMs, 3000)
})

test('remote publication cannot block local self or another peer copy', async t => {
  const gate = Promise.withResolvers(); let publishing = false
  const saved = []; const states = []
  const f = await fixture(t, {
    publish: async () => { publishing = true; await gate.promise; return { delivery: { reports: [{ success: true }] } } },
    save: async value => { saved.push(value.content); return { result: { ok: true } } },
    onOutbox: entries => states.push(structuredClone(entries))
  })
  t.after(() => gate.resolve())
  await f.session.enqueue({ peer, event })
  await until(() => publishing)
  const self = await f.session.enqueue({ peer: owner, event: { ...event, content: 'self' } })
  await f.session.enqueue({ peer, event: { ...event, content: 'next remote' } })
  await until(() => saved.includes('self') && saved.includes('next remote'))
  assert.ok(states.some(entries => entries.some(entry => entry.id === self && entry.status === 'saved')))
  gate.resolve()
})

test('self succeeds with network paused and does not checkpoint before cleanup', async t => {
  const writes = []; const rows = new Map(); const states = []
  const cleanup = Promise.withResolvers(); let removing = false
  const f = await fixture(t, {
    publish: () => assert.fail('self must not publish'),
    openOutbox: async () => ({
      list: async () => [], put: async entry => { writes.push(entry.id); rows.set(entry.id, structuredClone(entry)) },
      has: async id => rows.has(id),
      remove: async id => { removing = true; await cleanup.promise; rows.delete(id) }, close () {}
    }),
    onOutbox: entries => states.push(structuredClone(entries))
  })
  t.after(() => cleanup.resolve())
  await f.messenger().pause('network')
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => removing)
  assert.equal(writes.length, 1)
  assert.ok(states.some(entries => entries.some(entry => entry.id === id && entry.status === 'saved')))
  const next = await f.session.enqueue({ peer: owner, event: { ...event, content: 'second' } })
  await until(() => states.some(entries => entries.some(entry => entry.id === next && entry.status === 'saved')))
  cleanup.resolve()
})

test('a blocked local store cannot hold remote ACK, and local failure retries without republishing', async t => {
  const gate = Promise.withResolvers(); const states = []
  let publications = 0; let saves = 0
  const f = await fixture(t, {
    save: async () => { saves++; if (saves === 1) { await gate.promise; throw new Error('local storage temporarily unavailable') }; return { result: { ok: true } } },
    publish: async () => { publications++; return { delivery: { reports: [{ success: true }] } } },
    onOutbox: entries => states.push(structuredClone(entries))
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => states.at(-1)?.some(entry => entry.id === id && entry.status === 'saved'))
  assert.equal(publications, 1)
  assert.equal(f.records.has(id), true)
  assert.equal(states.at(-1)[0].localPersistence.state, 'pending')
  gate.resolve()
  await until(() => states.at(-1)?.some(entry => entry.localPersistence.state === 'failed'))
  assert.equal(states.at(-1)[0].status, 'saved')
  assert.equal(f.sendErrors.length, 0)
  await f.session.retry(id)
  await until(() => !f.records.has(id))
  assert.equal(publications, 1)
  assert.equal(saves, 2)
})

test('reload resumes pending local persistence of a confirmed remote entry without retransmission', async t => {
  let published = 0; let saved = 0
  const id = getEventHash(event)
  const rows = new Map([[id, { id, peer, event: { ...event, id }, events: [event], files: [], index: 1, fileIndex: 0, chunkIndex: 0, localSaved: [false], deliveryConfirmed: true, status: 'saved' }]])
  const f = await fixture(t, {
    publish: async () => { published++; throw new Error('must not republish') },
    save: async value => { saved++; assert.equal(getEventHash(value), id); return { result: { ok: true } } },
    openOutbox: async () => ({ list: async () => [...rows.values()], put: async entry => rows.set(id, structuredClone(entry)), remove: async id => rows.delete(id), close () {} })
  })
  await until(() => !rows.has(id))
  assert.equal(saved, 1); assert.equal(published, 0)
  assert.equal(f.errors.length, 0)
})

test('cleanup failure retries only removal, preserving saved status and send callbacks', async t => {
  const timers = []; const rows = new Map(); const states = []
  let saves = 0; let removes = 0
  const f = await fixture(t, {
    save: async () => { saves++; return { result: { ok: true } } },
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer }, _clearTimeout: () => {},
    openOutbox: async () => ({ list: async () => [], put: async entry => rows.set(entry.id, structuredClone(entry)), remove: async id => { if (++removes === 1) throw new Error('cleanup unavailable'); rows.delete(id) }, close () {} }),
    onOutbox: entries => states.push(structuredClone(entries))
  })
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => timers.length > 0)
  assert.equal(states.at(-1).find(entry => entry.id === id).status, 'saved')
  assert.equal(f.sendErrors.length, 0)
  await timers.at(-1).fn()
  await until(() => !rows.has(id))
  assert.equal(saves, 1)
  assert.equal(removes, 2)
})

test('local self commit does not await messenger construction', async t => {
  const gate = Promise.withResolvers(); const saved = []
  const f = createPrivateMessageSession({
    owner, signer: {}, recoveryStorage: null, useContentKeys: false,
    messageStorage: { save: async value => { saved.push(value); return { result: { ok: true } } } },
    openOutbox: async () => ({ list: async () => [], put: async () => {}, remove: async () => {}, close () {} }),
    openDownloads: async () => ({ list: async () => [], close () {} }),
    Messenger: async options => { await gate.promise; return createSessionMessenger(options) },
    FileTransfer: () => ({ observe () {} }), _onOnline: () => () => {}, onError: () => {}
  })
  const configuring = f.setAvailable(true)
  t.after(async () => { gate.resolve(); await configuring; await f.close() })
  await f.enqueue({ peer: owner, event })
  await until(() => saved.length === 1)
  gate.resolve()
})

test('self local persistence overlaps the encrypted checkpoint without exposing an unadmitted entry', async t => {
  const checkpointStarted = Promise.withResolvers()
  const releaseCheckpoint = Promise.withResolvers()
  const localStarted = Promise.withResolvers()
  const states = []
  const rows = new Map()
  t.after(() => releaseCheckpoint.resolve())
  const f = await fixture(t, {
    save: async () => { localStarted.resolve(); return { result: { ok: true } } },
    onOutbox: entries => states.push(structuredClone(entries)),
    openOutbox: async () => ({
      list: async () => [],
      put: async entry => {
        checkpointStarted.resolve()
        await releaseCheckpoint.promise
        rows.set(entry.id, structuredClone(entry))
      },
      remove: async id => rows.delete(id),
      close () {}
    })
  })
  const id = getEventHash({ ...event, pubkey: owner })
  const enqueued = f.session.enqueue({ peer: owner, event })
  await Promise.all([checkpointStarted.promise, localStarted.promise])
  assert.equal(states.some(entries => entries.some(entry => entry.id === id)), false)
  assert.equal(rows.has(id), false)
  releaseCheckpoint.resolve()
  assert.equal(await enqueued, id)
  await until(() => states.some(entries => entries.some(entry => entry.id === id && entry.status === 'saved')))
  await until(() => !rows.has(id))
  assert.equal(f.errors.length, 0)
  assert.equal(f.sendErrors.length, 0)
})

test('concurrent self admissions use one serial local processor', async t => {
  let active = 0
  let maximum = 0
  const committed = []
  const f = await fixture(t, {
    save: async value => {
      active++
      maximum = Math.max(maximum, active)
      await new Promise(resolve => setTimeout(resolve, 2))
      committed.push(getEventHash(value))
      active--
      return { result: { ok: true } }
    }
  })
  const first = f.session.enqueue({ peer: owner, event })
  const second = f.session.enqueue({ peer: owner, event: { ...event, created_at: 2, content: 'second' } })
  const ids = await Promise.all([first, second])
  await until(() => committed.length === 2 && [...f.records.values()].every(entry => !ids.includes(entry.id)))
  assert.equal(maximum, 1)
  assert.equal(new Set(committed).size, 2)
})

test('remote local persistence still waits for the encrypted checkpoint', async t => {
  const checkpointStarted = Promise.withResolvers()
  const releaseCheckpoint = Promise.withResolvers()
  let saves = 0
  t.after(() => releaseCheckpoint.resolve())
  const f = await fixture(t, {
    save: async () => { saves++; return { result: { ok: true } } },
    openOutbox: async () => ({
      list: async () => [],
      put: async () => {
        checkpointStarted.resolve()
        await releaseCheckpoint.promise
      },
      remove: async () => {},
      close () {}
    })
  })
  const enqueued = f.session.enqueue({ peer, event })
  await checkpointStarted.promise
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(saves, 0)
  releaseCheckpoint.resolve()
  await enqueued
  await until(() => saves === 1)
})

test('a failed self checkpoint drops the unadmitted entry and retry reuses its identity', async t => {
  const localStarted = Promise.withResolvers()
  const states = []
  const committed = new Set()
  const rows = new Map()
  let puts = 0
  const f = await fixture(t, {
    save: async value => {
      localStarted.resolve()
      committed.add(getEventHash(value))
      return { result: { ok: true } }
    },
    onOutbox: entries => states.push(structuredClone(entries)),
    openOutbox: async () => ({
      list: async () => [],
      put: async entry => {
        if (++puts === 1) {
          await localStarted.promise
          throw new Error('checkpoint unavailable')
        }
        rows.set(entry.id, structuredClone(entry))
      },
      remove: async id => rows.delete(id),
      close () {}
    })
  })
  const id = getEventHash({ ...event, pubkey: owner })
  await assert.rejects(f.session.enqueue({ peer: owner, event }), /checkpoint unavailable/)
  assert.equal(committed.has(id), true)
  assert.equal(states.some(entries => entries.some(entry => entry.id === id)), false)
  assert.equal(await f.session.enqueue({ peer: owner, event }), id)
  await until(() => states.some(entries => entries.some(entry => entry.id === id && entry.status === 'saved')))
  assert.equal(committed.size, 1)
  assert.equal(f.sendErrors.length, 0)
})

test('cancel fences a self local save racing an unadmitted checkpoint', async t => {
  const checkpointStarted = Promise.withResolvers()
  const releaseCheckpoint = Promise.withResolvers()
  const localStarted = Promise.withResolvers()
  const states = []
  let saves = 0
  t.after(() => releaseCheckpoint.resolve())
  const f = await fixture(t, {
    save: async () => {
      saves++
      localStarted.resolve()
      await releaseCheckpoint.promise
      return { result: { ok: true } }
    },
    onOutbox: entries => states.push(structuredClone(entries)),
    openOutbox: async () => ({
      list: async () => [],
      put: async () => {
        checkpointStarted.resolve()
        await releaseCheckpoint.promise
      },
      remove: async () => {},
      close () {}
    })
  })
  const id = getEventHash({ ...event, pubkey: owner })
  const enqueued = f.session.enqueue({ peer: owner, event })
  await Promise.all([checkpointStarted.promise, localStarted.promise])
  const cancelled = f.session.cancel(id)
  releaseCheckpoint.resolve()
  await cancelled
  await assert.rejects(enqueued, /CHAT_UNAVAILABLE/)
  assert.equal(saves, 1)
  assert.equal(states.some(entries => entries.some(entry => entry.id === id)), false)
})

test('self local failure discovered during admission remains retryable', async t => {
  const states = []
  const localFailed = Promise.withResolvers()
  let attempts = 0
  const f = await fixture(t, {
    save: async () => {
      attempts++
      if (attempts === 1) {
        localFailed.resolve()
        throw Object.assign(new Error('storage unavailable'), { code: 'STORAGE_UNAVAILABLE' })
      }
      return { result: { ok: true } }
    },
    onOutbox: entries => states.push(structuredClone(entries)),
    openOutbox: async () => {
      const rows = new Map()
      return {
        list: async () => [],
        put: async entry => {
          await localFailed.promise
          rows.set(entry.id, structuredClone(entry))
        },
        remove: async id => rows.delete(id),
        close () {}
      }
    }
  })
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => states.some(entries => entries.some(entry => entry.id === id && entry.status === 'error')))
  await f.session.retry(id)
  await until(() => states.some(entries => entries.some(entry => entry.id === id && entry.status === 'saved')))
  assert.equal(attempts, 2)
  assert.equal(f.errors.length, 1)
  assert.equal(f.sendErrors.length, 1)
})

test('close waits for an initial outbox write and preserves the intent without late admission', async t => {
  const write = Promise.withResolvers(); let writing = false; let closed = false; let saves = 0; let retained = false
  const f = await fixture(t, {
    save: async () => { saves++; return { result: { ok: true } } },
    openOutbox: async () => ({ list: async () => [], put: async () => { writing = true; await write.promise; retained = true }, remove: async () => { assert.equal(closed, false); retained = false }, close: () => { closed = true } })
  })
  const enqueue = f.session.enqueue({ peer: owner, event })
  await until(() => writing)
  const closing = f.session.close()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(closed, false)
  write.resolve()
  assert.equal(await enqueue, getEventHash(event))
  await closing
  assert.equal(saves, 1)
  assert.equal(retained, true)
  assert.equal(closed, true)
})

test('self replay after a crash preserves identity and does not duplicate persisted inner events', async t => {
  const rows = new Map(); const committed = new Set(); let writes = 0
  const cleanup = Promise.withResolvers(); let cleaning = false
  const store = { list: async () => [...rows.values()].map(value => structuredClone(value)), put: async entry => rows.set(entry.id, structuredClone(entry)), remove: async id => { cleaning = true; await cleanup.promise; rows.delete(id) }, close () {} }
  const save = async value => { writes++; committed.add(getEventHash(value)); return { result: { ok: true } } }
  const f = await fixture(t, { save, openOutbox: async () => store })
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => cleaning)
  const crashSnapshot = structuredClone(rows.get(id))
  assert.deepEqual(crashSnapshot.localSaved, [false], 'self does not write the redundant checkpoint')
  cleanup.resolve(); await f.session.close()
  rows.set(id, crashSnapshot)
  const replay = await fixture(t, { save, openOutbox: async () => store })
  await until(() => !rows.has(id))
  assert.equal(committed.size, 1)
  assert.equal(writes, 2)
  await replay.session.close()
})

test('live inbox persistence and ACK remain independent of an outgoing publication', async t => {
  const gate = Promise.withResolvers(); let publishing = false; let acknowledged = false
  const saved = []
  const f = await fixture(t, {
    publish: async () => { publishing = true; await gate.promise; return { delivery: { reports: [{ success: true }] } } },
    save: async value => { saved.push(value.content); return { result: { ok: true } } }
  })
  t.after(() => gate.resolve())
  await f.session.enqueue({ peer, event })
  await until(() => publishing)
  const incoming = { ...event, pubkey: peer, content: 'incoming' }
  let delivery = { message: { channelPubkey: peer, senderPubkey: peer, event: { ...incoming, id: getEventHash(incoming) } }, ack: async () => { acknowledged = true }, nack: () => assert.fail('valid incoming must not nack') }
  f.messenger().nextMessage = async () => { const value = delivery; delivery = null; return value }
  await f.messengerOptions().onMessageQueued()
  assert.equal(acknowledged, true)
  assert.ok(saved.includes('incoming'))
  gate.resolve()
})

test('network urgency is shared, renewed only by user intent and removed on cancel/lock/close', async t => {
  let now = 0
  const listeners = new Map(); const windows = []
  const f = await fixture(t, {
    _now: () => now,
    _onOnline: (listener, options) => { windows.push(options.expediteForMs); listeners.set(listener, options); return () => listeners.delete(listener) }
  })
  await f.messenger().pause('network')
  const a = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(a)?.failed)
  assert.equal(listeners.size, 1); assert.equal(windows.at(-1), 30000)
  now = 10000
  const b = await f.session.enqueue({ peer, event: { ...event, created_at: 2 } })
  await until(() => f.records.get(b)?.failed)
  assert.equal(listeners.size, 1); assert.equal(windows.at(-1), 30000)
  await f.session.cancel(b)
  assert.equal(listeners.size, 1); assert.equal(windows.at(-1), 20000)
  now = 20000
  await f.session.retry(a)
  assert.equal(listeners.size, 1); assert.equal(windows.at(-1), 30000)
  await until(() => f.records.get(a)?.failed)
  const calls = windows.length
  // An observed connectivity confirmation never releases an explicit pause.
  await [...listeners.keys()][0]()
  assert.deepEqual(f.messenger().readStatus().pauseReasons, ['network'])
  assert.equal(listeners.size, 0)
  assert.equal(windows.length, calls)
  await f.session.setAvailable(false)
  await f.session.setAvailable(true)
  await until(() => listeners.size === 1)
  assert.equal(windows.at(-1), 0) // unlock/restored work cannot renew urgency
  await f.session.close()
  assert.equal(listeners.size, 0)
})

test('self-chat never subscribes to urgent network checks', async t => {
  let checks = 0
  const f = await fixture(t, { _onOnline: () => { checks++; return () => {} } })
  await f.messenger().pause('network')
  await f.session.enqueue({ peer: owner, event })
  await until(() => f.records.size === 0)
  assert.equal(checks, 0)
})

test('restored outbox work waits online normally without opening an urgent window', async t => {
  const id = getEventHash(event); const windows = []
  const records = new Map([[id, { id, peer, event: { ...event, id }, events: [event], files: [], index: 0, fileIndex: 0, chunkIndex: 0, localSaved: [true], status: 'pending' }]])
  let snapshot = []
  const f = await fixture(t, {
    openOutbox: async () => ({ list: async () => [...records.values()], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async key => records.delete(key), close () {} }),
    Messenger: async options => { const messenger = createSessionMessenger(options); await messenger.pause('network'); return messenger },
    onOutbox: entries => { snapshot = entries },
    _onOnline: (_listener, options) => { windows.push(options.expediteForMs); return () => {} }
  })
  await until(() => snapshot[0]?.failed)
  assert.deepEqual(windows, [0])
  await f.session.close()
})

test('manual offline retry replaces its wait without stopping the shared monitor', async t => {
  const live = new Set(); let stoppedAll = false; let established = false
  const f = await fixture(t, {
    publish: async () => { throw Object.assign(new Error('OFFLINE'), { retryWhenAvailable: true, retryWhenOnline: true }) },
    _onOnline: listener => {
      live.add(listener)
      return () => { live.delete(listener); if (established && !live.size) stoppedAll = true }
    }
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => live.size === 1)
  established = true
  await f.session.retry(id)
  await until(() => f.records.get(id)?.failed)
  assert.equal(stoppedAll, false)
  assert.equal(live.size, 1)
  established = false
  await f.session.close()
})

test('unchanged account availability retains the network wait; removing a peer ends its demand', async t => {
  let subscriptions = 0; let live = 0
  const f = await fixture(t, {
    _onOnline: () => { subscriptions++; live++; return () => { live-- } }
  })
  await f.messenger().pause('network')
  await f.session.enqueue({ peer, event })
  await until(() => live === 1)
  await f.session.setAvailable(true)
  assert.equal(subscriptions, 1); assert.equal(live, 1)
  await f.session.setPeers([])
  assert.equal(live, 0)
})

test('a terminal send failure does not keep network urgency alive', async t => {
  let checks = 0
  const f = await fixture(t, { publish: async () => { throw new Error('PERMISSION_DENIED') }, _onOnline: () => { checks++; return () => {} } })
  await f.session.enqueue({ peer, event })
  await until(() => f.sendErrors.length === 1)
  await f.messenger().pause('network')
  assert.equal(checks, 0)
})

test('automatic local retry cannot clear a terminal remote failure', async t => {
  const gate = Promise.withResolvers(); const timers = []
  let publications = 0; let saves = 0
  const f = await fixture(t, {
    save: async () => { if (++saves === 1) { await gate.promise; throw new Error('storage unavailable') }; return { result: { ok: true } } },
    publish: async () => { publications++; throw Object.assign(new Error('PERMISSION_DENIED'), { code: 'PERMISSION_DENIED' }) },
    _setTimeout: (fn, delay) => { const value = { fn, delay }; timers.push(value); return value }, _clearTimeout () {}
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => f.records.get(id)?.failed)
  gate.resolve()
  await until(() => timers.length > 0)
  await timers[0].fn()
  await until(() => saves === 2)
  assert.equal(publications, 1)
  assert.equal(f.records.get(id).failed, true)
  assert.equal(f.records.get(id).retryable, false)
})

test('close drains a held local save after remote confirmation before closing storage', async () => {
  const gate = Promise.withResolvers(); const states = []; const closed = []
  const f = createPrivateMessageSession({
    owner, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) }, recoveryStorage: null,
    messageStorage: { save: async () => { await gate.promise; closed.push('local-commit'); return { result: { ok: true } } } },
    openOutbox: async () => ({ list: async () => [], put: async () => {}, remove: async () => { closed.push('cleanup') }, close: async () => { closed.push('storage-close') } }),
    openDownloads: async () => ({ list: async () => [], close () {} }),
    Messenger: async options => createSessionMessenger(options, { broadcastRumor: async () => ({ delivery: { reports: [{ success: true }] } }) }), FileTransfer: () => ({ observe () {} }),
    onOutbox: entries => states.push(structuredClone(entries)), onError: assert.fail, _onOnline: () => () => {}
  })
  await f.setPeers([peer]); await f.setAvailable(true)
  const id = await f.enqueue({ peer, event })
  await until(() => states.at(-1)?.some(entry => entry.id === id && entry.status === 'saved'))
  let finished = false
  const closing = f.close().then(() => { finished = true })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(finished, false)
  assert.equal(closed.includes('storage-close'), false)
  gate.resolve(); await closing
  assert.ok(closed.indexOf('local-commit') < closed.indexOf('storage-close'))
})

test('self-chat retry is pending while its next local attempt runs', async t => {
  const held = Promise.withResolvers(); const states = []
  let attempts = 0
  t.after(() => held.resolve())
  const f = await fixture(t, { save: async () => { if (++attempts === 1) throw new Error('storage unavailable'); await held.promise; return { result: { ok: true } } }, onOutbox: value => states.push(structuredClone(value)) })
  const id = await f.session.enqueue({ peer: owner, event })
  await until(() => states.at(-1)?.some(entry => entry.status === 'error'))
  const retry = f.session.retry(id)
  await until(() => attempts === 2)
  assert.equal(states.at(-1).find(entry => entry.id === id).status, 'pending')
  held.resolve(); await retry
  await until(() => !f.records.has(id))
})

test('a local tombstone aborts an envelope still held at its durable checkpoint', async t => {
  const local = Promise.withResolvers(); const checkpoint = Promise.withResolvers()
  const checkpointStarted = Promise.withResolvers(); const rows = new Map()
  let signal; let transmissions = 0
  t.after(() => { local.resolve(); checkpoint.resolve() })
  const f = await fixture(t, {
    save: async () => { await local.promise; return { result: { ok: false, code: 'blocked' } } },
    publish: async options => {
      signal = options.signal
      await options.onPreparedSeed({ publication: { events: [{ id: 'prepared-envelope' }], seeds: [] } })
      signal.throwIfAborted()
      transmissions++
      return { delivery: { reports: [{ success: true }] } }
    },
    openOutbox: async () => ({
      list: async () => [], put: async (entry, options) => {
        if (options?.existing && entry.prepared) { checkpointStarted.resolve(); await checkpoint.promise }
        rows.set(entry.id, structuredClone(entry))
      }, remove: async id => rows.delete(id), close () {}
    })
  })
  await f.session.enqueue({ peer, event })
  await checkpointStarted.promise
  local.resolve()
  await until(() => signal.aborted)
  checkpoint.resolve()
  await until(() => !rows.size)
  assert.equal(transmissions, 0)
})

test('confirmed local retry retains its failure summary until the durable local commit', async t => {
  const gate = Promise.withResolvers(); const states = []
  let saves = 0
  t.after(() => gate.resolve())
  const f = await fixture(t, {
    save: async () => { if (++saves === 1) throw new Error('local storage failed'); if (saves === 2) await gate.promise; return { result: { ok: true } } },
    publish: async () => ({ delivery: { reports: [{ success: true }] } }),
    onOutbox: value => states.push(structuredClone(value))
  })
  const id = await f.session.enqueue({ peer, event })
  await until(() => states.at(-1)?.some(entry => entry.id === id && entry.status === 'saved' && entry.localPersistence.state === 'failed'))
  const retried = f.session.retry(id)
  await until(() => saves === 2)
  await f.session.enqueue({ peer: owner, event: { ...event, content: 'unrelated next message', created_at: 2 } })
  const pending = states.at(-1).find(entry => entry.id === id)
  assert.equal(pending.status, 'saved')
  assert.equal(pending.localPersistence.state, 'failed', 'unrelated snapshots cannot hide the warning before the retry commits')
  assert.equal(pending.localPersistence.retryable, false, 'the same local attempt is already admitted')
  gate.resolve(); await retried
  await until(() => !f.records.has(id))
})

test('legacy failed local intents retain manual retry policy when normalized into the new flow', async t => {
  const legacy = JSON.parse(await readFile(new URL('./fixtures/private-outbox/0.11.24-initial.json', import.meta.url)))
  Object.assign(legacy, { peer: owner, failed: true, retryable: false, status: 'error' })
  const rows = new Map([[legacy.id, legacy]]); const states = []
  let saves = 0
  const f = await fixture(t, {
    save: async () => { saves++; return { result: { ok: true } } },
    openOutbox: async () => ({ list: async () => [...rows.values()], put: async entry => rows.set(entry.id, structuredClone(entry)), remove: async id => rows.delete(id), close () {} }),
    onOutbox: value => states.push(structuredClone(value))
  })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(saves, 0, 'upgrading must not retry a permanent old local failure automatically')
  assert.equal(states.at(-1)[0].localPersistence.state, 'failed')
  assert.equal(states.at(-1)[0].localPersistence.retryable, false)
  await f.session.retry(legacy.id)
  await until(() => !rows.has(legacy.id))
  assert.equal(saves, 1)
})
