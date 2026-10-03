import { test } from 'node:test'
import assert from 'node:assert/strict'
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
  const errors = []; const sendErrors = []; const records = new Map(); const onlineListeners = new Set()
  const store = () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  let messengerOptions
  const session = createPrivateMessageSession({
    owner, fallbackRelays, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save }, openOutbox: store, openDownloads: store,
    Messenger: async options => { messengerOptions = options; return { update () {}, resume () {}, pause () {}, close () {}, nextMessage: async () => null, broadcastRumor: publish } },
    FileTransfer: () => ({ observe () {} }),
    _onOnline: listener => { onlineListeners.add(listener); return () => onlineListeners.delete(listener) },
    onError: error => errors.push(error), onSendError: (error, context) => sendErrors.push({ error, context }),
    ...sessionOptions
  })
  t.after(() => session.close())
  await session.setPeers([peer]); await session.setAvailable(true)
  return { session, messengerOptions: () => messengerOptions, errors, sendErrors, records, onlineListeners, reconnect: () => Promise.all([...onlineListeners].map(listener => listener())) }
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
