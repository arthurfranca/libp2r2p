import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createPrivateMessageSession } from '../private-messenger/session/index.js'
import { createSessionMessenger } from './helpers/session-messenger.js'
import { getEventHash } from '../event/index.js'
import { withPendingSeeds } from '../private-messenger/session/helpers/publication-journal.js'
import { prepareIrfsFile } from '../irfs/index.js'
import { createFileMetadata } from '../nip94/index.js'
import { nfileEncode } from '../nip19/index.js'
import { readFile } from 'node:fs/promises'

const owner = 'a'.repeat(64), peer = 'b'.repeat(64)
const text = (content = 'text') => ({ pubkey: owner, kind: 9, created_at: Math.floor(Date.now() / 1000), tags: [], content })
const until = async predicate => {
  for (let n = 0; n < 300; n++) { if (await predicate()) return; await new Promise(resolve => setTimeout(resolve, 2)) }
  assert.fail('condition not reached')
}
function packet (id) {
  const seed = { channelPubkey: peer, router: { kind: 26300, pubkey: id, created_at: Math.floor(Date.now() / 1000), tags: [['f', owner]] }, jsonl: `${JSON.stringify(['payload'])}\n${JSON.stringify([peer, 'encrypted key'])}\n` }
  return { channelPubkey: peer, receiverPubkeys: [peer], events: [{ id }], seeds: [seed] }
}
async function fixture (t, { rows = new Map(), put = async () => {}, putMany, archive = async () => {}, transport = async () => {}, initial = [], ...sessionOptions } = {}) {
  const states = [], errors = [], sent = [], local = [], snapshots = initial
  for (const entry of initial) rows.set(entry.id, structuredClone(entry))
  let sequence = 1, options
  const session = createPrivateMessageSession({
    owner, signer: { withSharedKey: () => ({ getPublicKey: async () => peer }) },
    recoveryStorage: { seeds: { put: archive, ...(putMany ? { putMany } : {}), async * iterate () {}, has: async () => false } },
    messageStorage: { save: async event => { local.push(event); return { result: { ok: true } } } },
    openOutbox: async () => ({
      list: async () => snapshots.map(value => structuredClone(value)),
      put: async (entry, flags) => { await put(entry, flags); rows.set(entry.id, structuredClone(entry)) },
      remove: async id => rows.delete(id), close () {}
    }),
    openDownloads: async () => ({ list: async () => [], close () {} }),
    Messenger: async value => {
      options = value
      return createSessionMessenger(value, {
        offlineRecoverySecondsFor: () => 3600,
        broadcastRumor: async call => {
          const publication = call.preparedPublication || packet((sequence++).toString(16).padStart(64, '0'))
          await call.onPreparedSeed({ ...publication.seeds[0], publication })
          sent.push(publication.events[0].id)
          await transport(call)
          return { delivery: { reports: [{ success: true }] } }
        }
      })
    },
    FileTransfer: () => ({ observe () {} }), _onOnline: () => () => {},
    onOutbox: value => states.push(structuredClone(value)), onError: error => errors.push(error),
    ...sessionOptions
  })
  t.after(() => session.close())
  await session.setPeers([peer]); await session.setAvailable(true)
  return { session, states, errors, rows, sent, local, options: () => options }
}

test('ACK confirms two texts while archival is blocked and each ACK is durably checkpointed', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve())
  let archiving = false
  const f = await fixture(t, { archive: async () => { archiving = true; await gate.promise } })
  const first = await f.session.enqueue({ peer, event: text('first') })
  await until(() => archiving && f.rows.get(first)?.status === 'saved')
  assert.equal(f.rows.get(first).prepared.accepted, true)
  const second = await f.session.enqueue({ peer, event: text('second') })
  await until(() => f.rows.get(second)?.status === 'saved')
  assert.equal(f.rows.get(second).prepared.accepted, true)
  assert.equal(f.sent.length, 2)
  assert.ok(f.states.some(entries => entries.find(entry => entry.id === first)?.status === 'saved'))
  gate.resolve()
  await until(() => f.rows.size === 0)
})

test('a failed preparation checkpoint never transmits an envelope and retry keeps its identity', async t => {
  let fail = true
  const f = await fixture(t, { put: async entry => { if (entry.prepared && fail) throw new Error('disk unavailable') } })
  const id = await f.session.enqueue({ peer, event: text() })
  await until(() => f.errors.length)
  assert.deepEqual(f.sent, [])
  fail = false
  await f.session.retry(id)
  await until(() => f.sent.length === 1)
  assert.equal(f.sent[0], '1'.padStart(64, '0'))
})

test('reload after ACK archives only, while an unconfirmed checkpoint republishes the identical packet', async t => {
  for (const accepted of [false, true]) {
    const event = text(`reload ${accepted}`), id = getEventHash(event)
    const prepared = { publication: packet('f'.repeat(64)), recover: true, retentionSeconds: 3600, receivedAt: event.created_at, target: { index: 0 }, accepted, archived: false }
    const entry = { id, peer, event: { ...event, id }, events: [event], files: [], localSaved: [true], index: accepted ? 1 : 0, fileIndex: 0, chunkIndex: 0, status: accepted ? 'saved' : 'pending', prepared }
    const f = await fixture(t, { initial: [entry] })
    try {
      await until(() => f.states.some(values => values.some(value => value.id === id && value.status === 'saved')) && !f.rows.has(id))
    } catch {
      assert.fail(JSON.stringify({ accepted, sent: f.sent.length, errors: f.errors.map(error => error.message), rows: [...f.rows.values()].map(row => ({ index: row.index, status: row.status, accepted: row.prepared?.accepted, archived: row.prepared?.archived })) }))
    }
    assert.deepEqual(f.sent, accepted ? [] : ['f'.repeat(64)])
    await f.session.close()
  }
})

test('actual 0.11.24 snapshots retain identity and progress in the current pipeline', async t => {
  for (const stage of ['initial', 'partial']) {
    const entry = JSON.parse(await readFile(new URL(`./fixtures/private-outbox/0.11.24-${stage}.json`, import.meta.url)))
    const f = await fixture(t, { initial: [entry] })
    await until(() => !f.rows.has(entry.id))
    assert.equal(f.sent.length, stage === 'partial' ? 1 : 2)
    assert.equal(f.local.length, stage === 'partial' ? 0 : 1)
    assert.equal(f.errors.length, 0)
    assert.ok(f.states.some(entries => entries.some(value => value.id === entry.id && value.status === 'saved')))
    await f.session.close()
  }
})

test('an archive failure preserves confirmation and retry never republishes the message', async t => {
  let fail = true
  const f = await fixture(t, { archive: async () => { if (fail) throw new Error('archive unavailable') } })
  const id = await f.session.enqueue({ peer, event: text() })
  await until(() => f.errors.length && f.rows.get(id)?.status === 'saved')
  assert.equal(f.sent.length, 1)
  fail = false
  await f.session.retry(id)
  await until(() => !f.rows.has(id))
  assert.equal(f.sent.length, 1)
  assert.ok(f.states.filter(entries => entries.some(entry => entry.id === id && entry.status === 'saved')).length)
})

test('the session archives each publication through one batched seed write', async t => {
  const writes = []
  const diagnostics = []
  const f = await fixture(t, {
    putMany: async rows => {
      writes.push(rows)
      return rows
    },
    archive: async () => assert.fail('serial put should be used only as fallback'),
    onDebug: entry => diagnostics.push(entry)
  })
  const id = await f.session.enqueue({ peer, event: text('batched archive') })
  await until(() => !f.rows.has(id))
  assert.equal(writes.length, 1)
  assert.equal(writes[0].length, 1)
  assert.equal(writes[0][0].channelPubkey, peer)
  const result = diagnostics.filter(entry => entry.stage === 'recovery-archive-result')
  assert.equal(result.length, 1)
  assert.equal(result[0].publicationId, '1'.padStart(64, '0'))
  assert.equal(result[0].seedRecordId, writes[0][0].recordId)
  assert.deepEqual({
    rowCount: result[0].rowCount,
    uniqueCount: result[0].uniqueCount,
    storedCount: result[0].storedCount,
    skippedCount: result[0].skippedCount
  }, { rowCount: 1, uniqueCount: 1, storedCount: 1, skippedCount: 0 })
  assert.equal('content' in result[0], false)
  assert.equal('event' in result[0], false)
})

test('cancel after transmission retains recovery until archival finishes without publishing again', async t => {
  const ack = Promise.withResolvers(), archive = Promise.withResolvers()
  t.after(() => { ack.resolve(); archive.resolve() })
  const f = await fixture(t, { archive: async () => archive.promise, transport: async () => ack.promise })
  const id = await f.session.enqueue({ peer, event: text() })
  await until(() => f.sent.length === 1)
  await f.session.cancel(id)
  assert.equal(f.rows.get(id).cancelled, true)
  assert.equal(f.states.at(-1).some(entry => entry.id === id), false)
  ack.resolve(); archive.resolve()
  await until(() => !f.rows.has(id))
  assert.equal(f.sent.length, 1)
})

test('close drains an active archive and its cleanup before closing the outbox', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve())
  let archiving = false, closed = false
  const f = await fixture(t, { archive: async () => { archiving = true; await gate.promise } })
  const id = await f.session.enqueue({ peer, event: text() })
  await until(() => archiving && f.rows.get(id)?.status === 'saved')
  const closing = f.session.close().then(() => { closed = true })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(closed, false)
  gate.resolve(); await closing
  assert.equal(f.rows.size, 0)
})

test('a watchtower upload yields its slot while one chunk awaits archival and other text confirms', async t => {
  const file = await prepareIrfsFile(new Uint8Array(51001).fill(7))
  t.after(() => file.close())
  const chunks = await Array.fromAsync(file.chunks())
  const gate = Promise.withResolvers(); t.after(() => gate.resolve())
  const uploads = []
  const f = await fixture(t, {
    mode: 'watchtower', archive: async () => gate.promise,
    chunkStorage: { read: async (_, index) => chunks[index] },
    FileTransfer: () => ({
      observe () {}, authorizeSeeding: async () => {}, publishChunk: async (_, value, options) => {
        const index = uploads.length
        const publication = options.preparedPublication || packet((index + 100).toString(16).padStart(64, '0'))
        publication.seeds[0].router.tags.push(['i', String(index)])
        await options.onPreparedSeed({ ...publication.seeds[0], publication })
        uploads.push(index)
      }
    })
  })
  const metadata = createFileMetadata({ root: file.root, size: file.size, mime: 'application/octet-stream', filename: 'test.bin', service: 'irfs', url: `https://nostr.alt/${nfileEncode({ root: file.root })}`, created_at: Math.floor(Date.now() / 1000) })
  const upload = await f.session.enqueue({ peer, event: metadata })
  await until(() => uploads.length === 1)
  const id = await f.session.enqueue({ peer, event: text('passes the upload') })
  await until(() => f.states.some(entries => entries.some(entry => entry.id === id && entry.status === 'saved')))
  assert.equal(uploads.length, 1)
  assert.equal(f.rows.get(upload).prepared.accepted, true)
  gate.resolve()
  await until(() => uploads.length === 2 && !f.rows.has(upload))
})

test('pending durable seeds are readable during an archive write and deduplicate stored rows', async () => {
  const pending = { recordId: 'pending', channelPubkey: peer, receiverPubkey: peer }
  const stored = { recordId: 'stored', channelPubkey: peer, receiverPubkey: peer }
  const missing = { recordId: 'missing', channelPubkey: peer, receiverPubkey: peer }
  const storage = withPendingSeeds({
    async * iterate () { yield stored },
    hasMany: async rows => rows.map(row => row.recordId === 'stored')
  }, () => [pending])
  assert.equal(await storage.has(pending), true)
  assert.equal(await storage.has(stored), true)
  assert.deepEqual(await storage.hasMany([pending, stored, pending, missing]), [true, true, true, false])
  assert.deepEqual(await Array.fromAsync(storage.iterate({ channelPubkey: peer })), [pending, stored])

  const serial = withPendingSeeds({ async * iterate () {}, has: async row => row.recordId === 'stored' }, () => [pending])
  assert.deepEqual(await serial.hasMany([pending, stored, missing]), [true, true, false])
})

test('an actual 0.11.24 upload resumes at its next chunk without resaving local copies', async t => {
  const entry = JSON.parse(await readFile(new URL('./fixtures/private-outbox/0.11.24-upload.json', import.meta.url)))
  const file = await prepareIrfsFile(new Uint8Array(51001).fill(7))
  t.after(() => file.close())
  const chunks = await Array.fromAsync(file.chunks()), reads = []
  const f = await fixture(t, {
    initial: [entry], chunkStorage: { read: async (_, index) => { reads.push(index); return chunks[index] } },
    FileTransfer: () => ({
      observe () {}, authorizeSeeding: async () => {}, publishChunk: async (_, chunk, options) => {
        const publication = packet('e'.repeat(64))
        await options.onPreparedSeed({ ...publication.seeds[0], publication })
      }
    })
  })
  await until(() => !f.rows.has(entry.id))
  assert.deepEqual(reads, [1])
  assert.equal(f.sent.length, 1, 'metadata still needs its own publication')
  assert.equal(f.local.length, 0)
  assert.equal(f.errors.length, 0)
})

test('an initial call without an attempt timestamp preserves the publication-local budget', async t => {
  const starts = []
  const f = await fixture(t, { transport: async call => starts.push(call.attemptStartedAt) })
  const id = await f.session.enqueue({ peer, event: text() })
  await until(() => !f.rows.has(id))
  assert.deepEqual(starts, [undefined])
})

test('queued journal checkpoints coalesce but the exact prepared publication remains behind its durable barrier', async () => {
  const { createPublicationJournal } = await import('../private-messenger/session/helpers/publication-journal.js')
  const first = Promise.withResolvers(); const second = Promise.withResolvers(); const writes = []
  const entry = { id: 'coalesced', localSaved: [false], index: 0 }
  const journal = createPublicationJournal({ entries: new Map([[entry.id, entry]]), store: () => ({ put: async (value, options) => { writes.push(value); assert.equal(options.existing, true); await (writes.length === 1 ? first.promise : second.promise) } }), archive: async () => {}, changed () {}, onError: assert.fail, debug () {} })
  const initial = journal.write(entry)
  await new Promise(resolve => setImmediate(resolve))
  entry.localSaved[0] = true
  const progress = journal.write(entry)
  let staged = false
  const checkpoint = journal.stage(entry, { publication: { events: [{ id: 'exact-envelope' }], seeds: [] } }, { index: 0 }, 0).then(() => { staged = true })
  first.resolve(); await initial
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(writes.length, 2)
  assert.equal(staged, false)
  assert.deepEqual(writes[1].localSaved, [true])
  assert.equal(writes[1].prepared.publication.events[0].id, 'exact-envelope')
  second.resolve(); await Promise.all([progress, checkpoint])
  assert.equal(staged, true)
  await journal.close()
})
