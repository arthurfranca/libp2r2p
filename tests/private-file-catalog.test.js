import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { createQueue } from '../idb-queue/index.js'
import { createPrivateFileTransfer, FILE_CHUNKS_REQUEST_CODE, FILE_CHUNKS_REPLY_CODE, IRFS_CHUNK_RECORD_TYPE } from '../private-messenger/file/index.js'
import { prepareIrfsFile } from '../irfs/index.js'

globalThis.IDBKeyRange = IDBKeyRange
const owner = '11'.repeat(32), peer = '22'.repeat(32), control = '33'.repeat(32), channel = '44'.repeat(32), outsider = '55'.repeat(32)
const tick = () => new Promise(resolve => setTimeout(resolve, 10))
async function until (check) { for (let n = 0; n < 100; n++) { if (await check()) return; await tick() }; assert.fail('condition not reached') }
async function fixture () {
  const file = await prepareIrfsFile(new Uint8Array(51001).fill(3))
  const chunks = await Array.fromAsync(file.chunks())
  const rows = new Map(chunks.map((event, index) => [index, event]))
  const replies = [], errors = []
  let clock = 1000, retention = 100, reads = 0, feed
  const parent = { mode: 'seeder' }
  const messenger = {
    prefix: `catalog:${crypto.randomUUID()}`, _indexedDB: new IDBFactory(), extensions: new Set(), userPubkey: owner,
    channels: new Map([[control, parent]]), desiredChannels: new Set([control]),
    requireWritableChannel: () => parent, offlineRecoverySecondsFor: () => retention,
    resolveSendRouting: async () => ({}), eventExpirationSecondsFor: () => 604800, contentKeyLookup: () => undefined,
    recoverySeeders: () => [peer], resolveWatchRelays: async () => ['wss://test.invalid'], ask: async () => {}
  }
  const descriptor = { controlChannelPubkey: control, peerPubkey: peer, root: file.root, size: file.size }
  let beforeRead = async () => {}
  const options = {
    messenger, _now: () => clock, _hedgeMs: 5, _idleMs: 100,
    resolveChannel: async () => ({ getPublicKey: async () => channel }),
    storage: { read: async (_, index) => { reads++; await beforeRead(index); return rows.get(index) }, save: async event => rows.set(Number(event.tags[1][1]), event) },
    onError: error => errors.push(error),
    _messages: { reply: async reply => replies.push(reply), broadcastRumor: async options => { assert.equal(options.onPreparedSeed, undefined); return { delivery: { reports: [{ success: true }] } } } },
    _transport: { subscribe: options => { feed = options; return { ready: Promise.resolve(), close: async () => {} } }, fetch: async () => {} }
  }
  let manager = createPrivateFileTransfer(options)
  async function openQueue (suffix) {
    return createQueue({ prefix: `${messenger.prefix}:${suffix}`, indexedDB: messenger._indexedDB, indexes: { key: { keyPath: 'key', unique: true }, channel: 'fileChannelPubkey' } })
  }
  return {
    descriptor, messenger, chunks, rows, replies, errors, options, openQueue,
    get manager () { return manager }, get feed () { return feed }, get reads () { return reads },
    time: value => { clock = value }, retention: value => { retention = value }, beforeRead: value => { beforeRead = value },
    async restart () { await manager.close(); manager = createPrivateFileTransfer(options) },
    async grant (sharedAt = clock, receiverPubkeys = [peer]) { await manager.authorizeSeeding(descriptor, { receiverPubkeys, sharedAt }) },
    async ask ({ receiver = peer, controlChannel = control, dataChannel = channel, provenance = 'direct', sender = receiver, ranges = [[0, 1]] } = {}) {
      const question = { id: '66'.repeat(32), pubkey: receiver, tags: [['r', owner]] }
      const message = { question, senderPubkey: sender, provenance, payload: { code: FILE_CHUNKS_REQUEST_CODE, payload: { fileChannelPubkey: dataChannel, missingRanges: ranges } } }
      assert.equal([...messenger.extensions][0].handleAsk(controlChannel, message), true)
      await tick(); await tick()
    },
    async close () { await manager.close(); file.close() }
  }
}

test('durable grants serve local chunks after restart without ciphertext seeds', async () => {
  const f = await fixture()
  try {
    await f.manager.register(f.descriptor)
    await f.ask()
    assert.equal(f.reads, 0, 'registration and a guessed root cannot authorize storage reads')
    await f.grant()
    await f.manager.publishChunk(f.descriptor, f.chunks[0])
    await f.restart()
    await f.ask()
    await until(() => f.replies.some(reply => reply.payload.isLast))
    const records = f.replies.flatMap(reply => reply.payload.jsonl.split('\n').filter(Boolean).map(JSON.parse))
    assert.deepEqual(records.map(record => record.recordType), [IRFS_CHUNK_RECORD_TYPE, IRFS_CHUNK_RECORD_TYPE])
    assert.deepEqual(records.map(record => record.index), [0, 1])
    assert.equal(records[0].content, f.chunks[0].content)
    assert.equal(await f.replies[0].privateChannelSigner.getPublicKey(), channel)
    assert.equal(f.replies[0].receiverPubkey, peer)
    const seeds = await f.openQueue('file-seeds')
    assert.equal((await Array.fromAsync(seeds.storedItemsBy('key'))).length, 0)
    await seeds.close()
    assert.deepEqual(f.errors, [])
  } finally { await f.close() }
})

test('authorization checks conversation, data channel, sender and recipient before reading storage', async () => {
  const f = await fixture()
  try {
    await f.grant()
    for (const input of [{ receiver: outsider }, { controlChannel: outsider }, { dataChannel: outsider }, { provenance: 'hearsay' }, { sender: outsider }]) await f.ask(input)
    assert.equal(f.reads, 0)
    assert.equal(f.replies.length, 0)
    await assert.rejects(f.manager.authorizeSeeding(f.descriptor, { receiverPubkeys: [], sharedAt: 1000 }), /INVALID_FILE_AUTHORIZATION_RECEIVERS/)
  } finally { await f.close() }
})

test('absolute expiry survives replay and restart; only a newer explicit share renews it', async () => {
  const f = await fixture()
  try {
    await f.grant(1000)
    f.time(1090); await f.grant(1000)
    await f.restart()
    f.time(1100); await f.ask()
    assert.equal(f.reads, 0)
    const catalog = await f.openQueue('file-authorizations')
    assert.equal((await Array.fromAsync(catalog.storedItemsBy('key'))).length, 0)
    await catalog.close()
    await f.grant(1000); await f.ask()
    assert.equal(f.reads, 0, 'expired replay cannot recreate a usable grant')
    await f.grant(1100); await f.ask()
    await until(() => f.replies.some(reply => reply.payload.isLast))
    assert.equal(f.reads, 2)
  } finally { await f.close() }
})

test('shorter channel retention and expiry during a read prevent publication', async () => {
  const f = await fixture()
  try {
    await f.grant()
    f.time(1060); f.retention(50); await f.ask()
    assert.equal(f.reads, 0)
    f.retention(100); await f.grant(1060)
    f.beforeRead(async () => f.time(1160))
    await f.ask(); await tick()
    assert.equal(f.replies.length, 0)
    assert.equal(f.reads, 1)
  } finally { await f.close() }
})

test('missing and corrupted local positions do not prevent serving valid remaining chunks', async () => {
  const f = await fixture()
  try {
    await f.grant()
    f.rows.delete(0)
    await f.ask(); await until(() => f.replies.some(reply => reply.payload.isLast))
    assert.equal(JSON.parse(f.replies[0].payload.jsonl).index, 1)
    f.replies.length = 0
    f.rows.set(0, { ...f.chunks[0], content: 'broken' })
    await f.ask(); await until(() => f.replies.some(reply => reply.payload.isLast))
    assert.equal(JSON.parse(f.replies[0].payload.jsonl).index, 1)
    assert.ok(f.errors.length)
  } finally { await f.close() }
})

test('IRFS replies validate root and index, deduplicate, and never grant the responder access', async () => {
  const f = await fixture()
  try {
    f.rows.clear()
    const downloading = f.manager.download({ ...f.descriptor, sharedAt: 1000 })
    await until(() => f.feed)
    const records = f.chunks.map((event, index) => ({ recordType: IRFS_CHUNK_RECORD_TYPE, index, total: 2, proof: event.tags[1][3], content: event.content }))
    const deliver = async (rows, sender = peer) => f.feed.onEvent({ kind: 7330, pubkey: sender, created_at: 1000, tags: [['r', owner], ['h', FILE_CHUNKS_REPLY_CODE]], content: JSON.stringify({ jsonl: rows.map(row => JSON.stringify(row)).join('\n') + '\n' }) }, {}, { senderPubkey: sender, provenance: 'direct' })
    await deliver(records, outsider)
    assert.equal(f.rows.size, 0)
    await deliver([{ ...records[0], index: 1 }])
    assert.equal(f.rows.size, 0)
    await deliver([records[1], records[1], records[0]])
    await downloading
    assert.equal(f.rows.size, 2)
    assert.equal(f.rows.get(0).pubkey, undefined)
    await f.ask()
    assert.equal(f.replies.length, 0, 'a recovery reply must not create a grant')
  } finally { await f.close() }
})

test('direct persisted delivery authorizes only the actual receiver', async () => {
  const f = await fixture()
  try {
    f.rows.clear()
    const downloading = f.manager.download({ ...f.descriptor, sharedAt: 1000 })
    await until(() => f.feed)
    for (let index = 0; index < f.chunks.length; index++) await f.feed.onEvent(f.chunks[index], {}, { senderPubkey: peer, router: { tags: [['i', String(index)]] } })
    await downloading
    const catalog = await f.openQueue('file-authorizations')
    const grants = await Array.fromAsync(catalog.storedItemsBy('key'))
    assert.deepEqual(grants.map(grant => grant.receiverPubkey), [owner])
    await f.ask()
    assert.equal(f.replies.length, 0, 'the announcer must not gain access to local file data')
    await catalog.close()
  } finally { await f.close() }
})

test('catalog persistence failure propagates without authorizing a publication', async () => {
  const f = await fixture()
  let failed
  try {
    await f.manager.close()
    const indexedDB = f.messenger._indexedDB
    const messenger = { ...f.messenger, extensions: new Set(), _indexedDB: { open: (name, version) => { if (name.includes('file-authorizations')) throw new Error('catalog quota'); return indexedDB.open(name, version) } } }
    failed = createPrivateFileTransfer({ ...f.options, messenger })
    await assert.rejects(failed.authorizeSeeding(f.descriptor, { receiverPubkeys: [peer], sharedAt: 1000 }), /catalog quota/)
    assert.equal(f.replies.length, 0)
  } finally { await failed?.close(); await f.close() }
})
