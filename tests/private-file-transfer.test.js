import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { createPrivateFileTransfer, decodeMissingRanges, fileChannelInfo, FILE_CHUNKS_REQUEST_CODE } from '../private-messenger/file/index.js'
import { prepareIrfsFile } from '../irfs/index.js'
import { wrapEvent, unwrapRouterEvent } from '../private-channel/index.js'
import { compactSeedRouterRows, compactRecordsFromSeed } from '../private-messenger/recovery/index.js'
import NsecSigner from './helpers/test-signer.js'
import { bytesToHex } from '../base16/index.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'

globalThis.IDBKeyRange = IDBKeyRange
const owner = '11'.repeat(32), peer = '22'.repeat(32), control = '33'.repeat(32), data = '44'.repeat(32), second = '55'.repeat(32)
async function fixture ({ onError, bytes = 51001, local = [], ask = async () => {}, read, save, seeders = [peer], history = async () => { throw new Error('relay unavailable') } } = {}) {
  const prepared = await prepareIrfsFile(new Uint8Array(bytes).fill(7))
  const chunks = []
  for await (const chunk of prepared.chunks()) chunks.push(chunk)
  const stored = new Map(local.map(index => [index, chunks[index]]))
  const parent = { pubkey: control, mode: 'seeder' }
  let feed, closes = 0
  const messenger = { extensions: new Set(), prefix: `test:${crypto.randomUUID()}`, _indexedDB: new IDBFactory(), userPubkey: owner, desiredChannels: new Set([control]), channels: new Map([[control, parent]]), requireWritableChannel: () => parent, recoverySeeders: () => seeders, resolveWatchRelays: async () => ['wss://test.invalid'], offlineRecoverySecondsFor: () => 604800, ask: options => ask(options, feed), resolveSendRouting: async () => ({ relays: ['wss://test.invalid'] }), eventExpirationSecondsFor: () => 604800, contentKeyLookup: () => undefined }
  const manager = createPrivateFileTransfer({ onError, messenger, resolveChannel: async () => ({ getPublicKey: async () => data }), storage: { read: async (root, index) => { read?.(index); return stored.get(index) }, save: async event => { await save?.(event); stored.set(Number(event.tags[1][1]), event) } }, _transport: { subscribe: options => { feed = options; return { ready: Promise.resolve(), close: async () => { closes++ } } }, fetch: history }, _hedgeMs: 5, _idleMs: 40 })
  const descriptor = { controlChannelPubkey: control, peerPubkey: peer, root: prepared.root, size: prepared.size }
  const deliver = (feed, index) => feed.onEvent(chunks[index], {}, { senderPubkey: peer, router: { tags: [['i', String(index)]] } })
  return { manager, messenger, descriptor, stored, chunks, deliver, closes: () => closes, close: async () => { await manager.close(); prepared.close() } }
}

test('file convention and bounded disjoint request ranges', () => {
  assert.equal(fileChannelInfo('ab'.repeat(32)), `dm:media:${'ab'.repeat(32)}`)
  assert.deepEqual(decodeMissingRanges([[0, 2], [7, 8]]), [0, 1, 2, 7, 8])
  for (const ranges of [[], [[0, 16]], [[0, 2], [2, 3]], [[2, 1]], [[-1, 0]], [[0, 1, 2]], [[0, Infinity]]]) assert.throws(() => decodeMissingRanges(ranges))
})

test('failed relay history still requests dm seeds and completes only after persisted chunks', async () => {
  let asks = 0
  const fixtureValue = await fixture({
    local: [0], ask: async (options, feed) => {
      asks++
      assert.equal(options.channelPubkey, control)
      assert.equal(options.code, FILE_CHUNKS_REQUEST_CODE)
      assert.deepEqual(options.payload, { fileChannelPubkey: data, missingRanges: [[1, 1]] })
      await fixtureValue.deliver(feed, 1)
    }
  })
  try { await fixtureValue.manager.download(fixtureValue.descriptor); assert.equal(asks, 1); assert.equal(fixtureValue.stored.size, 2); assert.ok(fixtureValue.closes()) } finally { await fixtureValue.close() }
})

test('hedges recompute missing indices and preserve useful late responses', async () => {
  const asks = []
  const f = await fixture({
    seeders: [peer, second], bytes: 102001, ask: async (options, feed) => {
      asks.push(options)
      if (asks.length === 1) await f.deliver(feed, 0)
      else { assert.deepEqual(options.payload.missingRanges, [[1, 2]]); await f.deliver(feed, 2); await f.deliver(feed, 1) }
    }
  })
  try { await f.manager.download(f.descriptor); assert.equal(asks.length, 2); assert.equal(f.stored.size, 3) } finally { await f.close() }
})

test('storage failure cannot complete, and retry recomputes from persisted chunks', async () => {
  let failed = false
  const f = await fixture({ save: async () => { if (!failed) { failed = true; throw new Error('quota') } }, ask: async (_, feed) => { await f.deliver(feed, 0); await f.deliver(feed, 1) } })
  try { await assert.rejects(f.manager.download(f.descriptor), /quota/); await f.manager.download(f.descriptor); assert.equal(f.stored.size, 2) } finally { await f.close() }
})

test('large and unknown files require action; pause cancels active work', async () => {
  const f = await fixture({ bytes: 1048577 })
  try {
    await assert.rejects(f.manager.download(f.descriptor), /REQUIRES_ACTION/)
    const pending = f.manager.download(f.descriptor, { manual: true })
    const rejection = assert.rejects(pending, /cancelled/)
    await new Promise(resolve => setTimeout(resolve, 10))
    for (const extension of f.messenger.extensions) extension.pause()
    await rejection
  } finally { await f.close() }
})

test('local ciphertext seeds recover for a recipient with no relay echo', async () => {
  const a = generateSecretKey(), b = generateSecretKey()
  const alice = new NsecSigner(bytesToHex(a), getPublicKey(a)), bob = new NsecSigner(bytesToHex(b), getPublicKey(b))
  const bobPubkey = await bob.getPublicKey()
  const channel = alice.withSharedKey(bobPubkey, 'dm:media:test')
  const file = await prepareIrfsFile(new Uint8Array(51001).fill(4))
  const [event] = await Array.fromAsync(file.chunks())
  const seeds = []
  const memory = new Map()
  const temporaryStorageArea = { getItem: key => memory.get(key) ?? null, setItem: (key, value) => memory.set(key, value), removeItem: key => memory.delete(key) }
  const wrappers = await wrapEvent({ temporaryStorageArea, _getIykcProofs: async () => ({}), senderSigner: alice, privateChannelSigner: channel, receivers: [bobPubkey], event, fileChunkIndex: 0, onPreparedSeed: seed => { seeds.push(...compactSeedRouterRows(seed)) } })
  assert.ok(wrappers.length > 1)
  assert.equal(seeds.length, 1)
  const [record] = compactRecordsFromSeed(seeds[0], { receiverPubkey: bobPubkey })
  assert.deepEqual(record.router.tags.find(tag => tag[0] === 'i'), ['i', '0'])
  assert.equal(record.router.tags.find(tag => tag[0] === 'p')[1], bobPubkey)
  const recovered = await unwrapRouterEvent({ router: record.router, receiverSigner: bob, receiverPubkey: bobPubkey, channelPubkey: await channel.getPublicKey() })
  assert.equal(recovered.content, event.content)
  file.close()
})

test('batches stay bounded and advance only after their persisted indices arrive', async () => {
  const batches = []
  const f = await fixture({
    bytes: 51000 * 18 + 7, ask: async (options, feed) => {
      const indices = decodeMissingRanges(options.payload.missingRanges)
      batches.push(indices)
      for (const index of indices.toReversed()) await f.deliver(feed, index)
    }
  })
  try { await f.manager.download(f.descriptor); assert.deepEqual(batches.map(batch => batch.length), [16, 3]) } finally { await f.close() }
})

test('wrong router index cannot persist a valid chunk under a different position', async () => {
  const f = await fixture({
    ask: async (_, feed) => {
      await feed.onEvent(f.chunks[0], {}, { senderPubkey: peer, router: { tags: [['i', '1']] } })
      assert.equal(f.stored.size, 0)
      await f.deliver(feed, 1); await f.deliver(feed, 0)
    }
  })
  try { await f.manager.download(f.descriptor); assert.equal(f.stored.size, 2) } finally { await f.close() }
})

test('unknown-size manual download learns the total and checks the final size', async () => {
  const f = await fixture({ ask: async (options, feed) => { for (const index of decodeMissingRanges(options.payload.missingRanges)) await f.deliver(feed, index) } })
  try {
    const descriptor = { ...f.descriptor }; delete descriptor.size
    await assert.rejects(f.manager.download(descriptor), /REQUIRES_ACTION/)
    const result = await f.manager.download(descriptor, { manual: true })
    assert.equal(result.size, 51001)
  } finally { await f.close() }
})

test('watchtower keeps durable recipient seeds and replies on data, never on dm', async () => {
  const f = await fixture()
  await f.close()
  f.messenger.channels.get(control).mode = 'watchtower'
  // Reuse the same isolated messenger/database with a controlled publication adapter.
  const replies = []
  const manager = createPrivateFileTransfer({
    messenger: f.messenger, resolveChannel: async () => ({ getPublicKey: async () => data }), storage: { read: async () => null, save: async () => {} }, _messages: {
      async broadcastRumor (options) {
        await options.onPreparedSeed({ router: { kind: 26300, pubkey: '66'.repeat(32), created_at: Math.floor(Date.now() / 1000), tags: [['f', owner], ['i', String(options.fileChunkIndex)]] }, jsonl: `${JSON.stringify(['ciphertext'])}\n${JSON.stringify([peer, 'recipient-ciphertext'])}\n` })
        return { delivery: { reports: [{ success: true }] } }
      },
      async reply (options) { replies.push(options) }
    }
  })
  try {
    await manager.publishChunk(f.descriptor, f.chunks[0])
    const question = { id: '77'.repeat(32), pubkey: peer, tags: [['r', owner]] }
    const message = { question, senderPubkey: peer, provenance: 'direct', payload: { code: FILE_CHUNKS_REQUEST_CODE, payload: { fileChannelPubkey: data, missingRanges: [[0, 0]] } } }
    const handler = [...f.messenger.extensions][0]
    await handler.handleAsk(control, { ...message, provenance: 'hearsay' })
    await handler.handleAsk('99'.repeat(32), message)
    assert.equal(replies.length, 0)
    assert.equal(handler.handleAsk(control, message), true, 'DM dispatch returns before the seed batch is published')
    for (let attempt = 0; !replies.some(reply => reply.payload.isLast) && attempt < 100; attempt++) await new Promise(resolve => setTimeout(resolve, 2))
    assert.ok(replies.length)
    assert.equal(await replies[0].privateChannelSigner.getPublicKey(), data)
    assert.equal(replies[0].code, 'fileChunksReply_p5cc')
    assert.equal(replies[0].question.id, question.id)
    assert.equal(JSON.parse(replies[0].payload.jsonl.trim()).router.tags.find(tag => tag[0] === 'i')[1], '0')
    f.messenger.offlineRecoverySecondsFor = () => 0
    replies.length = 0
    await handler.handleAsk(control, message)
    await new Promise(resolve => setTimeout(resolve, 20))
    assert.equal(replies.length, 0)
  } finally { await manager.close() }
})

test('concurrent registration shares one signer and rejects conflicting sizes', async () => {
  const f = await fixture()
  try {
    const a = f.manager.register(f.descriptor)
    await assert.rejects(f.manager.register({ ...f.descriptor, size: f.descriptor.size + 1 }), /CONFLICTING/)
    assert.equal(await a, await f.manager.register(f.descriptor))
  } finally { await f.close() }
})

test('automatic consumers join an active manual transfer without rescanning stored chunks', async () => {
  let reads = 0
  const started = Promise.withResolvers()
  const f = await fixture({ bytes: 1048577, read: () => { reads++ }, ask: async () => started.resolve() })
  try {
    const first = Promise.allSettled([f.manager.download(f.descriptor, { manual: true })])
    await started.promise
    const before = reads
    const followers = Promise.allSettled(Array.from({ length: 8 }, () => f.manager.download(f.descriptor)))
    await new Promise(resolve => setTimeout(resolve, 5))
    assert.equal(reads, before)
    f.manager.cancel(control, f.descriptor.root)
    for (const result of [...await first, ...await followers]) {
      assert.equal(result.status, 'rejected')
      assert.match(result.reason.message, /cancelled/)
    }
  } finally { await f.close() }
})

test('default file storage retains published chunks across coordinator restart and serves an authorized peer', async () => {
  const f = await fixture()
  await f.manager.close()
  const replies = []
  const options = {
    messenger: f.messenger, resolveChannel: async () => ({ getPublicKey: async () => data }),
    _messages: { broadcastRumor: async () => ({ delivery: { reports: [{ success: true }] } }), reply: async value => { replies.push(value) } }
  }
  let manager = createPrivateFileTransfer(options)
  try {
    await manager.authorizeSeeding(f.descriptor, { receiverPubkeys: [peer], sharedAt: Math.floor(Date.now() / 1000) })
    for (const chunk of f.chunks) await manager.publishChunk(f.descriptor, chunk)
    await manager.close(); manager = createPrivateFileTransfer(options)
    assert.ok(await manager.readChunk(f.descriptor.root, 1))
    const question = { id: '77'.repeat(32), pubkey: peer, tags: [['r', owner]] }
    const handler = [...f.messenger.extensions][0]
    handler.handleAsk(control, { question, senderPubkey: peer, provenance: 'direct', payload: { code: FILE_CHUNKS_REQUEST_CODE, payload: { fileChannelPubkey: data, missingRanges: [[0, 1]] } } })
    for (let attempt = 0; !replies.some(reply => reply.payload.isLast) && attempt < 100; attempt++) await new Promise(resolve => setTimeout(resolve, 2))
    assert.equal(replies.flatMap(reply => reply.payload.jsonl.trim().split('\n')).filter(Boolean).length, 2)
    assert.equal(JSON.parse(replies[0].payload.jsonl.trim()).recordType, 'irfsChunk_v1')
  } finally { await manager.close(); await f.close() }
})


test('completing a file cancels unfinished history without reporting its own AbortError', async () => {
  const errors = []
  let aborted = false
  const f = await fixture({
    onError: error => errors.push(error),
    history: ({ signal }) => new Promise((resolve, reject) => {
      signal.addEventListener('abort', () => { aborted = true; reject(signal.reason) }, { once: true })
    }),
    ask: async (_, feed) => { await f.deliver(feed, 0); await f.deliver(feed, 1) }
  })
  try {
    await f.manager.download(f.descriptor)
    assert.equal(aborted, true)
    assert.deepEqual(errors, [])
  } finally { await f.close() }
})

test('history failure remains visible when peers can finish the file', async () => {
  const errors = []
  const failure = new Error('Relay disconnected')
  const f = await fixture({
    onError: error => errors.push(error),
    history: async () => { throw failure },
    ask: async (_, feed) => { await f.deliver(feed, 0); await f.deliver(feed, 1) }
  })
  try {
    await f.manager.download(f.descriptor)
    assert.deepEqual(errors, [failure])
  } finally { await f.close() }
})
