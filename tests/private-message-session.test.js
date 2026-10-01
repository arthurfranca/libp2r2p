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
async function fixture (t, { publish, save = async () => ({ result: { ok: true } }) } = {}) {
  const errors = []; const sendErrors = []; const records = new Map()
  const store = () => ({ list: async () => [], put: async entry => records.set(entry.id, structuredClone(entry)), remove: async id => records.delete(id), close () {} })
  const session = createPrivateMessageSession({
    owner, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    messageStorage: { save }, openOutbox: store, openDownloads: store,
    Messenger: async () => ({ update () {}, resume () {}, pause () {}, close () {}, nextMessage: async () => null, broadcastRumor: publish }),
    FileTransfer: () => ({ observe () {} }),
    onError: error => errors.push(error), onSendError: (error, context) => sendErrors.push({ error, context })
  })
  t.after(() => session.close())
  await session.setPeers([peer]); await session.setAvailable(true)
  return { session, errors, sendErrors, records }
}

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
