import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { createQueue } from '../idb-queue/index.js'
import { migrateMessageQueue } from '../private-messenger/services/migrate-message-queue.js'
import {
  createPendingDecryption,
  pendingContentKeyGroup,
  pendingMissingContentKeyGroup
} from '../private-messenger/services/pending-decryption.js'
import * as channel from '../private-channel/index.js'
import Signer from './helpers/test-signer.js'
import { generateSecretKey } from '../key/index.js'
import { bytesToHex } from '../base16/index.js'
import { ValidationError } from '../error/index.js'

afterEach(() => Signer.releaseAll())
const signer = () => Signer.getOrCreate(bytesToHex(generateSecretKey()))
const currentIndexes = { byChannel: 'channelPubkey', byChannelTypeEventId: { keyPath: ['channelPubkey', 'type', 'event.id', 'provenance'], unique: true } }
globalThis.IDBKeyRange = IDBKeyRange
const temporary = new Map()
globalThis.sessionStorage = {
  getItem: key => temporary.get(key) ?? null,
  setItem: (key, value) => temporary.set(key, String(value)),
  removeItem: key => temporary.delete(key)
}

test('migrates only the known three-field index and preserves order, provenance and bytes', async () => {
  const indexedDB = new IDBFactory()
  const prefix = 'migration'
  const old = await createQueue({ prefix, indexedDB, indexes: { byChannel: 'channelPubkey', byChannelTypeEventId: { keyPath: ['channelPubkey', 'type', 'event.id'], unique: true } } })
  for (const [id, senderPubkey] of [['one', 'alice'], ['two', undefined]]) await old.enqueue({ channelPubkey: 'channel', type: 'message', event: { id, pubkey: 'alice' }, ...(senderPubkey ? { senderPubkey } : {}) })
  await old.close()
  await Promise.all([migrateMessageQueue(indexedDB, prefix), migrateMessageQueue(indexedDB, prefix)])
  const queue = await createQueue({ prefix, indexedDB, indexes: currentIndexes })
  const items = []
  for await (const item of queue.storedItems()) items.push(item)
  assert.deepEqual(items.map(item => [item.event.id, item.provenance]), [['one', 'direct'], ['two', 'hearsay']])
  assert.ok(await queue.someBy('byChannelTypeEventId', ['channel', 'message', 'one', 'direct']))
  const capacity = await queue.getCapacity()
  assert.ok(capacity.usedBytes > 0)
  await queue.close()
  await migrateMessageQueue(indexedDB, prefix)
  const reopened = await createQueue({ prefix, indexedDB, indexes: currentIndexes })
  assert.deepEqual(await reopened.getCapacity(), capacity)
  await reopened.close()
  const wrong = await createQueue({ prefix: 'unknown', indexedDB, indexes: { ...currentIndexes, byChannelTypeEventId: 'wrong' } })
  await wrong.close()
  await migrateMessageQueue(indexedDB, 'unknown')
  await assert.rejects(createQueue({ prefix: 'unknown', indexedDB, indexes: currentIndexes }), /QUEUE_INDEX_SCHEMA_MISMATCH/)
})

test('ciphertext pending queue survives restart, deduplicates, caps MAC attempts and evicts FIFO', async () => {
  const indexedDB = new IDBFactory()
  const open = () => createPendingDecryption({ prefix: 'pending', indexedDB, maxBytes: 2400 })
  let pending = await open()
  const failure = id => ({ error: new ValidationError('INVALID_MAC'), record: { format: 'outer_v1', channelPubkey: 'channel', outer: { id, content: 'encrypted'.repeat(35) } } })
  await pending.save(failure('first'))
  await pending.save(failure('first'))
  let [first] = await pending.records()
  assert.equal((await pending.records()).length, 1)
  await pending.failed(first, new ValidationError('INVALID_MAC'))
  await pending.close()
  pending = await open()
  ;[first] = await pending.records()
  assert.equal(first.macAttempts, 2)
  await pending.failed(first, new ValidationError('INVALID_MAC'))
  assert.deepEqual(await pending.records(), [])
  assert.equal(await pending.current(first), null)
  assert.equal((await pending.current(first, { force: true })).macAttempts, 3)
  assert.equal((await pending.records({ force: true })).length, 1)
  await pending.save({ error: new ValidationError('INVALID_MAC'), record: { format: 'outer_v1', channelPubkey: 'channel', outer: { id: 'oversized', content: 'x'.repeat(3000) } } })
  assert.equal((await pending.records({ force: true })).length, 1)
  for (let i = 0; i < 10; i++) await pending.save(failure(`next-${i}`))
  assert.equal(await pending.queue.someBy('byId', first.id), false)
  assert.ok((await pending.queue.getCapacity()).usedBytes <= 2400)
  await pending.clearChannel('channel')
  assert.deepEqual(await pending.records({ force: true }), [])
  await pending.close()
})

test('pending records retain only validated content-key coordinates and replace them on failure', async () => {
  const indexedDB = new IDBFactory()
  const pending = await createPendingDecryption({ prefix: 'content-key-metadata', indexedDB })
  const receiver = '11'.repeat(32)
  const own = '22'.repeat(32)
  const peer = '33'.repeat(32)
  const record = { format: 'outer_v1', channelPubkey: 'channel', outer: { id: 'encrypted', content: 'ciphertext' } }
  const missing = Object.assign(new Error('CONTENT_KEY_NOT_FOUND'), {
    code: 'CONTENT_KEY_NOT_FOUND',
    receiverPubkey: receiver,
    ownContentPubkey: own,
    peerContentPubkey: peer,
    hasOwnContentKey: false
  })
  await pending.save({ error: missing, record })
  let [item] = await pending.records()
  assert.deepEqual(
    Object.fromEntries(['receiverPubkey', 'ownContentPubkey', 'peerContentPubkey', 'hasOwnContentKey'].map(key => [key, item[key]])),
    { receiverPubkey: receiver, ownContentPubkey: own, peerContentPubkey: peer, hasOwnContentKey: false }
  )
  assert.equal(pendingContentKeyGroup(item), `${receiver}:${own}`)
  assert.equal(pendingMissingContentKeyGroup(item), `${receiver}:${own}`)

  await pending.failed(item, Object.assign(new Error('invalid MAC'), {
    code: 'INVALID_MAC',
    receiverPubkey: receiver,
    ownContentPubkey: '44'.repeat(32),
    hasOwnContentKey: true
  }))
  ;[item] = await pending.records()
  assert.equal(item.code, 'INVALID_MAC')
  assert.equal(item.ownContentPubkey, '44'.repeat(32))
  assert.equal(item.peerContentPubkey, undefined)
  assert.equal(item.hasOwnContentKey, true)
  assert.equal(pendingMissingContentKeyGroup(item), '')
  await pending.close()
})

test('failed migration rolls back rows and a blocked upgrade never resets the database', async () => {
  const indexedDB = new IDBFactory()
  const indexes = { byChannel: 'channelPubkey', byChannelTypeEventId: { keyPath: ['channelPubkey', 'type', 'event.id'], unique: true } }
  let old = await createQueue({ prefix: 'rollback', indexedDB, indexes })
  await old.enqueue({ channelPubkey: 'channel', type: 'message' })
  const capacity = await old.getCapacity()
  await old.close()
  await assert.rejects(migrateMessageQueue(indexedDB, 'rollback'))
  old = await createQueue({ prefix: 'rollback', indexedDB, indexes })
  assert.deepEqual(await old.getCapacity(), capacity)
  await old.close()
  old = await createQueue({ prefix: 'blocked', indexedDB, indexes })
  await old.enqueue({ channelPubkey: 'channel', type: 'message', event: { id: 'kept', pubkey: 'alice' } })
  await old.close()
  const db = await new Promise((resolve, reject) => {
    const request = indexedDB.open('blocked:idb-queue')
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
  db.onversionchange = () => {}
  await assert.rejects(migrateMessageQueue(indexedDB, 'blocked'), /IDB_DATABASE_BLOCKED/)
  db.close()
  await new Promise(resolve => setTimeout(resolve, 5))
  await migrateMessageQueue(indexedDB, 'blocked')
  const migrated = await createQueue({ prefix: 'blocked', indexedDB, indexes: currentIndexes })
  assert.ok(await migrated.someBy('byChannelTypeEventId', ['channel', 'message', 'kept', 'hearsay']))
  await migrated.close()
})

for (const code of ['CONTENT_KEY_NOT_FOUND', 'INVALID_MAC']) {
  test(`history continues after persisted ${code} and fragmented ciphertext replays after restart`, async () => {
    const indexedDB = new IDBFactory()
    const alice = signer(); const bob = signer()
    const alicePubkey = await alice.getPublicKey(); const bobPubkey = await bob.getPublicKey()
    const original = { kind: 9, created_at: 10, tags: [], content: 'private-message-body'.repeat(4000) }
    const damaged = await channel.wrapEvent({ senderSigner: alice, receivers: [bobPubkey], event: original, _getIykcProofs: async () => ({}) })
    assert.ok(damaged.length > 1)
    const good = await channel.wrapEvent({ senderSigner: alice, receivers: [bobPubkey], event: { ...original, content: 'newer valid message', created_at: 11 }, _getIykcProofs: async () => ({}) })
    const parts = []
    for (const outer of damaged) {
      const router = JSON.parse(Buffer.from(await alice.nip44v3Decrypt(alicePubkey, channel.PRIVATE_BROADCAST_KIND, '', outer.content), 'base64').toString())
      parts.push(Buffer.from(router.content, 'base64'))
    }
    const brokenCiphertext = Buffer.concat(parts).toString().split('\n').filter(Boolean).map(line => JSON.parse(line)).find(row => row[0] === bobPubkey)[1]
    let held = true
    let attempts = 0
    const receiver = {
      getPublicKey: () => bobPubkey,
      nip44v3Decrypt: (...args) => {
        if (held && args[3] === brokenCiphertext) { attempts++; throw new ValidationError(code) }
        return bob.nip44v3Decrypt(...args)
      }
    }
    let pending = await createPendingDecryption({ prefix: code, indexedDB })
    const delivered = []
    let activeProcessing = 0
    await channel.fetchHistory({
      receiverSigner: receiver, receiverPubkey: bobPubkey, privateChannelSigner: alice, privateChannelPubkeys: [alicePubkey],
      relays: ['wss://fixture.example'], receivedChunkIndexedDB: indexedDB, partial: true, since: 0, until: 20,
      _onProcessing: pubkey => {
        assert.equal(pubkey, alicePubkey); activeProcessing++
        return () => { activeProcessing-- }
      },
      onDecryptionFailure: failure => { assert.equal(activeProcessing, 1); return pending.save(failure) },
      onEvent: event => { assert.equal(activeProcessing, 1); delivered.push(event) },
      _getEvents: async () => ({ result: [...damaged, ...good].map(event => ({ event })), relays: [{ relay: 'wss://fixture.example', status: 'eose' }] })
    })
    assert.equal(activeProcessing, 0)
    assert.equal(delivered.length, 1)
    assert.equal(delivered[0].content, 'newer valid message')
    const records = await pending.records()
    assert.equal(records.length, 1)
    assert.equal(JSON.stringify(records).includes('private-message-body'), false)
    await channel.fetchHistory({
      receiverSigner: receiver, receiverPubkey: bobPubkey, privateChannelSigner: alice, privateChannelPubkeys: [alicePubkey],
      relays: ['wss://fixture.example'], receivedChunkIndexedDB: indexedDB, receivedChunkScope: 'duplicate-history', since: 0, until: 20,
      isDecryptionPending: record => pending.has(record),
      onDecryptionFailure: () => { throw new Error('DUPLICATE_MUST_NOT_DECRYPT') },
      _getEvents: async () => ({ result: damaged.map(event => ({ event })), relays: [{ relay: 'wss://fixture.example', status: 'eose' }] })
    })
    assert.equal(attempts, 1, 'queued ciphertext does not bypass the retry budget through history')
    // A failed durable handoff must leave history incomplete instead of
    // advancing the interval as if the ciphertext had been saved.
    await assert.rejects(channel.fetchHistory({
      receiverSigner: receiver, receiverPubkey: bobPubkey, privateChannelSigner: alice, privateChannelPubkeys: [alicePubkey],
      relays: ['wss://fixture.example'], receivedChunkIndexedDB: indexedDB, receivedChunkScope: 'failed-write', since: 0, until: 20,
      onDecryptionFailure: async () => { throw new Error('PENDING_WRITE_FAILED') },
      _getEvents: async () => ({ result: damaged.map(event => ({ event })), relays: [{ relay: 'wss://fixture.example', status: 'eose' }] })
    }), /PENDING_WRITE_FAILED/)
    await pending.save({ error: new ValidationError(code), record: { ...records[0].packet, outer: { id: 'another-copy' }, router: { ...records[0].packet.router, id: 'another-wrapper', sig: 'another-signature', created_at: 99 } } })
    assert.equal((await pending.records()).length, 1)
    await pending.close()
    pending = await createPendingDecryption({ prefix: code, indexedDB })
    held = false
    await channel.replayDeferredDecryptions({
      records: await pending.records(), receiverSigner: receiver, receiverPubkey: bobPubkey,
      privateChannelSigner: alice, receivedChunkIndexedDB: indexedDB,
      onEvent: event => delivered.push(event), onRecovered: record => pending.recovered(record),
      onFailure: (record, error) => pending.failed(record, error)
    })
    assert.equal(delivered[1].content, original.content)
    assert.equal(delivered[1].created_at, 10)
    assert.deepEqual(await pending.records(), [])
    await pending.close()
  })
}

test('fragmented outer replay keeps one processor across scheduled record boundaries', async () => {
  const indexedDB = new IDBFactory()
  const alice = signer(); const bob = signer()
  const channelPubkey = await alice.getPublicKey(); const receiverPubkey = await bob.getPublicKey()
  const original = { kind: 9, created_at: 10, tags: [], content: 'fragmented-body'.repeat(24000) }
  const packets = await channel.wrapEvent({ senderSigner: alice, receivers: [receiverPubkey], event: original, _getIykcProofs: async () => ({}) })
  assert.ok(packets.length > 4)
  const records = packets.map((outer, index) => ({ id: String(index), channelPubkey, packet: { format: 'outer_v1', channelPubkey, outer } }))
  const recovered = new Set(); const delivered = []
  let admissions = 0
  await channel.replayDeferredDecryptions({
    records, receiverSigner: bob, receiverPubkey, privateChannelSigner: alice, receivedChunkIndexedDB: indexedDB,
    _runRecord: async (_record, work) => { admissions++; await new Promise(resolve => setTimeout(resolve, 0)); await work() },
    onEvent: event => delivered.push(event), onRecovered: record => recovered.add(record.id),
    onFailure: (_record, error) => { throw error }
  })
  assert.equal(admissions, packets.length)
  assert.equal(delivered.length, 1)
  assert.equal(delivered[0].content, original.content)
  assert.equal(recovered.size, records.length)
})
