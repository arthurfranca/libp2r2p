import { test } from 'node:test'
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
  const store = () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  let messengerOptions; let messenger
  const session = createPrivateMessageSession({
    owner, fallbackRelays, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save }, openOutbox: store, openDownloads: store,
    Messenger: async options => {
      messengerOptions = options
      return (messenger = createSessionMessenger(options, {
        broadcastRumor: publish,
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
  return { session, messenger: () => messenger, messengerOptions: () => messengerOptions, errors, sendErrors, records, onlineListeners, priorities, reconnect: () => Promise.all([...onlineListeners].map(listener => listener())) }
}

test('session enables content-key lookup by default and forwards it to the messenger', async t => {
  const f = await fixture(t)
  assert.equal(f.messengerOptions().useContentKeys, true)
})

test('session can disable content-key lookup', async t => {
  const f = await fixture(t, { useContentKeys: false })
  assert.equal(f.messengerOptions().useContentKeys, false)
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
