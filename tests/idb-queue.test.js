import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { createQueue } from '../idb-queue/index.js'

function queueFor (factory, prefix, options = {}) {
  return createQueue({ prefix, indexedDB: factory, ...options })
}

async function values (iterator) {
  const out = []
  for await (const item of iterator) out.push(item.value)
  return out
}

test('idb queue supports ordered queue and positional operations', async () => {
  const queue = await queueFor(new IDBFactory(), 'ordered')

  await queue.push({ value: 'middle' })
  await queue.unshift({ value: 'first' })
  await queue.push({ value: 'last' })
  assert.equal(await queue.setAt(1, { value: 'MIDDLE' }), 1)
  assert.equal(await queue.insertAt(2, { value: 'between' }), 2)

  assert.equal((await queue.shift()).value, 'first')
  assert.equal((await queue.removeAt(1)).value, 'between')
  assert.equal((await queue.shift()).value, 'MIDDLE')
  assert.equal((await queue.pop()).value, 'last')
  assert.equal(await queue.shift(), null)
})

test('idb queue keeps predicate parity and skips sparse removals', async () => {
  const queue = await queueFor(new IDBFactory(), 'predicate')
  await queue.push({ value: 'a' })
  await queue.push({ value: 'c' })
  assert.equal(await queue.insertWhere(item => item.value === 'c', { value: 'b' }), 1)
  assert.equal(await queue.some(item => item.value === 'b'), true)
  await queue.removeWhere(item => item.value === 'b')

  assert.deepEqual(await values(queue.storedItems()), ['a', 'c'])
  assert.equal((await queue.shift()).value, 'a')
  assert.equal((await queue.shift()).value, 'c')
  assert.equal(await queue.shift(), null)
})

test('idb queue streams live values and supports reverse snapshots', async () => {
  const queue = await queueFor(new IDBFactory(), 'streams')
  await queue.push({ value: 'first' })
  await queue.push({ value: 'second' })

  assert.deepEqual(await values(queue.reverseStoredItems()), ['second', 'first'])
  const iterator = queue.items()
  assert.equal((await iterator.next()).value.value, 'first')
  assert.equal((await iterator.next()).value.value, 'second')
  const pending = iterator.next()
  await queue.push({ value: 'third' })
  assert.equal((await pending).value.value, 'third')
  await iterator.return()
})

test('idb queue enforces byte budgets and persists across queue instances', async () => {
  const factory = new IDBFactory()
  const queue = await queueFor(factory, 'capacity', { maxBytes: 450, evictionPolicy: 'fifo' })
  await queue.push({ value: 'a'.repeat(120) })
  await queue.push({ value: 'b'.repeat(120) })
  await queue.push({ value: 'c'.repeat(120) })

  assert.equal((await queue.shift()).value, 'b'.repeat(120))
  assert.equal((await queue.shift()).value, 'c'.repeat(120))

  const unbounded = await queueFor(factory, 'reopen')
  await unbounded.push({ value: 'a'.repeat(120) })
  await unbounded.push({ value: 'b'.repeat(120) })
  await unbounded.push({ value: 'c'.repeat(120) })
  const bounded = await queueFor(factory, 'reopen', { maxBytes: 450, evictionPolicy: 'fifo' })
  assert.equal((await bounded.shift()).value, 'b'.repeat(120))
  assert.equal((await bounded.shift()).value, 'c'.repeat(120))
})

test('idb queue exposes indexed equality, range, and deletion operations', async () => {
  const queue = await queueFor(new IDBFactory(), 'indexed', {
    indexes: {
      byKind: 'kind',
      byChannelTime: ['channel', 'time'],
      byKey: { keyPath: 'key', unique: true }
    }
  })
  await queue.push({ value: 'a', kind: 'message', channel: 'one', time: 10, key: 'a' })
  await queue.push({ value: 'b', kind: 'message', channel: 'one', time: 20, key: 'b' })
  await queue.push({ value: 'c', kind: 'seed', channel: 'two', time: 30, key: 'c' })

  assert.equal((await queue.getBy('byKey', 'b')).value, 'b')
  assert.equal(await queue.someBy('byKind', 'seed'), true)
  assert.deepEqual(
    await values(queue.storedItemsBy('byChannelTime', IDBKeyRange.bound(['one', 0], ['one', 20]))),
    ['a', 'b']
  )
  assert.deepEqual((await queue.removeBy('byKind', 'message')).map(item => item.value), ['a', 'b'])
  assert.deepEqual(await values(queue.storedItems()), ['c'])
})

test('idb queue evolves additive indexes and rejects incompatible definitions', async () => {
  const factory = new IDBFactory()
  const first = await queueFor(factory, 'schema', { indexes: { byKind: 'kind' } })
  await first.push({ value: 'a', kind: 'message', channel: 'one' })
  const second = await queueFor(factory, 'schema', {
    indexes: { byKind: 'kind', byChannel: 'channel' }
  })
  assert.equal((await second.getBy('byChannel', 'one')).value, 'a')
  await assert.rejects(
    queueFor(factory, 'schema', { indexes: { byKind: 'channel' } }),
    /QUEUE_INDEX_SCHEMA_MISMATCH/
  )
})

test('idb queue supports unique compound indexes', async () => {
  const queue = await queueFor(new IDBFactory(), 'compound', {
    indexes: {
      byChannelTypeEventId: {
        keyPath: ['channel', 'type', 'event.id'],
        unique: true
      }
    }
  })
  await queue.push({ value: 'first', channel: 'one', type: 'tell', event: { id: 'event' } })
  await queue.push({ value: 'second', channel: 'one', type: 'reply', event: { id: 'event' } })
  await queue.push({ value: 'third', channel: 'two', type: 'tell', event: { id: 'event' } })

  assert.equal(await queue.someBy('byChannelTypeEventId', ['one', 'tell', 'event']), true)
  await assert.rejects(
    queue.push({ value: 'duplicate', channel: 'one', type: 'tell', event: { id: 'event' } }),
    /ConstraintError/
  )
})

test('idb queue rolls back failed writes without an operation journal', async () => {
  const queue = await queueFor(new IDBFactory(), 'atomic')
  await queue.push({ value: 'kept' })

  await assert.rejects(queue.push({ value: 'bad', nonCloneable: () => {} }))
  assert.equal((await queue.shift()).value, 'kept')
  assert.equal(await queue.shift(), null)
})

test('idb queue rejects unavailable IndexedDB and clears atomically', async () => {
  await assert.rejects(createQueue({ prefix: 'missing', indexedDB: null }), /IDB_UNAVAILABLE/)

  const queue = await queueFor(new IDBFactory(), 'clear')
  await queue.push({ value: 'first' })
  await queue.push({ value: 'second' })
  await queue.clear()
  assert.equal(await queue.shift(), null)
})

test('idb queue close is idempotent and lets an active transaction finish', async () => {
  const queue = await queueFor(new IDBFactory(), 'close')
  const write = queue.push({ value: 'committed-before-close' })
  const firstClose = queue.close()
  const secondClose = queue.close()

  assert.equal(firstClose, secondClose)
  await write
  await firstClose
  await assert.rejects(queue.push({ value: 'too-late' }), /QUEUE_CLOSED/)
})

test('reject capacity preserves pending records across failed writes and reopen', async () => {
  const factory = new IDBFactory()
  const q = await queueFor(factory, 'reliable', { maxBytes: 220, evictionPolicy: 'reject' })
  await q.push({ value: 'a'.repeat(120) })
  await assert.rejects(q.push({ value: 'b'.repeat(120) }), /QUEUE_CAPACITY_EXCEEDED/)
  await assert.rejects(q.push({ value: 'c'.repeat(300) }), /QUEUE_ITEM_TOO_LARGE/)
  await q.close()
  const reopened = await queueFor(factory, 'reliable', { maxBytes: 80, evictionPolicy: 'reject' })
  assert.equal((await reopened.shift()).value, 'a'.repeat(120))
  await reopened.close()
})

test('reservations are exclusive, renewable, recoverable and token checked', async () => {
  const factory = new IDBFactory()
  const a = await queueFor(factory, 'leases', { evictionPolicy: 'reject' })
  const b = await queueFor(factory, 'leases', { evictionPolicy: 'reject' })
  try {
    await a.push({ id: 'one' })
    const first = await a.reserve()
    assert.equal(first.item.id, 'one')
    assert.equal(await b.reserve(), null)
    assert.equal(await first.renew(), true)
    assert.equal(await first.nack(), true)
    assert.equal(await first.nack(), true)
    assert.equal(await first.ack(), false)
    const second = await b.reserve({ now: Date.now() - 100, leaseMs: 1 })
    const third = await a.reserve()
    assert.equal(third.item.id, 'one')
    assert.equal(await second.ack(), false)
    assert.equal(await third.ack(), true)
    assert.equal(await third.ack(), true)
    await a.push({ id: 'two' }) // reused position must not accept an old token
    assert.equal(await second.nack(), false)
    assert.equal((await b.reserve()).item.id, 'two')
  } finally { await a.close(); await b.close() }
})
