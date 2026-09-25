import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBObjectStore } from 'fake-indexeddb'
import { createQueue } from '../idb-queue/index.js'
import { ValidationError } from '../error/index.js'

async function open (t, options = {}) {
  const queue = await createQueue({ prefix: 'put', indexedDB: new IDBFactory(), indexes: { byId: { keyPath: 'id', unique: true } }, evictionPolicy: 'reject', ...options })
  t.after(() => queue.close())
  return queue
}
const stored = async queue => Array.fromAsync(queue.storedItems())

test('putBy appends new keys and replaces in place with exact byte accounting', async t => {
  const queue = await open(t)
  assert.equal(await queue.putBy('byId', { id: 'b', text: 'old' }), true)
  await queue.putBy('byId', { id: 'a', text: 'other' })
  assert.equal(await queue.putBy('byId', { id: 'missing' }, { existingOnly: true }), false)
  for (const text of ['a longer checkpoint', 'x']) {
    assert.equal(await queue.putBy('byId', { id: 'b', text }, { existingOnly: true }), true)
    const expected = [{ id: 'b', text }, { id: 'a', text: 'other' }]
    assert.deepEqual(await stored(queue), expected)
    const reference = await open(t)
    for (const item of expected) await reference.push(item)
    assert.deepEqual(await queue.getCapacity(), await reference.getCapacity())
  }
})

test('putBy extracts nested and compound keys and snapshots caller values', async t => {
  const queue = await open(t, { indexes: { nested: { keyPath: 'meta.id', unique: true }, compound: { keyPath: ['meta.owner', 'meta.id'], unique: true } } })
  const item = { meta: { owner: 'alice', id: 'one' }, text: 'original' }
  const writing = queue.putBy('nested', item)
  item.meta.id = 'changed'
  item.text = 'changed'
  await writing
  assert.equal((await queue.getBy('nested', 'one')).text, 'original')
  await queue.putBy('compound', { meta: { owner: 'alice', id: 'one' }, text: 'updated' })
  assert.equal((await queue.getBy('compound', ['alice', 'one'])).text, 'updated')
  assert.equal((await stored(queue)).length, 1)
})

test('putBy reports invalid indexes and missing or invalid keys without mutation', async t => {
  const queue = await open(t, { indexes: { byId: { keyPath: 'id', unique: true }, ordinary: 'id', multi: { keyPath: 'ids', unique: true, multiEntry: true }, compound: { keyPath: ['owner', 'id'], unique: true } } })
  const invalid = code => error => error instanceof ValidationError && error.message === code
  for (const name of ['missing', 'ordinary', 'multi']) await assert.rejects(queue.putBy(name, { id: 'a' }), invalid('QUEUE_PUT_INDEX_INVALID'))
  for (const id of [undefined, null, NaN, true, {}, [undefined]]) await assert.rejects(queue.putBy('byId', { id }), invalid('QUEUE_PUT_KEY_INVALID'))
  await assert.rejects(queue.putBy('compound', { id: 'a' }), invalid('QUEUE_PUT_KEY_INVALID'))
  assert.deepEqual(await stored(queue), [])
  assert.equal((await queue.getCapacity()).usedBytes, 0)
})

test('putBy rejects capacity growth without losing the original record', async t => {
  const queue = await open(t, { maxBytes: 300 })
  await queue.putBy('byId', { id: 'a', text: 'a'.repeat(80) })
  await queue.putBy('byId', { id: 'b', text: 'b'.repeat(80) })
  const before = await stored(queue)
  const capacity = await queue.getCapacity()
  await assert.rejects(queue.putBy('byId', { id: 'a', text: 'a'.repeat(180) }), /QUEUE_CAPACITY_EXCEEDED/)
  await assert.rejects(queue.putBy('byId', { id: 'c', text: 'c'.repeat(80) }), /QUEUE_CAPACITY_EXCEEDED/)
  await assert.rejects(queue.putBy('byId', { id: 'a', text: 'a'.repeat(400) }), /QUEUE_ITEM_TOO_LARGE/)
  assert.deepEqual(await stored(queue), before)
  assert.deepEqual(await queue.getCapacity(), capacity)
})

test('putBy rolls back when a different unique index conflicts', async t => {
  const queue = await open(t, { indexes: { byId: { keyPath: 'id', unique: true }, alias: { keyPath: 'alias', unique: true } } })
  await queue.push({ id: 'a', alias: 'first', text: 'a'.repeat(50) })
  await queue.push({ id: 'b', alias: 'second', text: 'b'.repeat(50) })
  await queue.push({ id: 'c', alias: 'third', text: 'c'.repeat(50) })
  const before = await stored(queue)
  assert.equal(before.length, 3)
  const capacity = await queue.getCapacity()
  await assert.rejects(queue.putBy('byId', { id: 'b', alias: 'third', text: 'b'.repeat(180) }), { name: 'ConstraintError' })
  assert.deepEqual(await stored(queue), before)
  assert.deepEqual(await queue.getCapacity(), capacity)
})

test('putBy rolls back eviction and byte accounting on a storage write failure', async t => {
  const queue = await open(t, { maxBytes: 450, evictionPolicy: 'fifo' })
  for (const id of ['a', 'b', 'c']) await queue.push({ id, text: id.repeat(70) })
  const before = await stored(queue)
  const capacity = await queue.getCapacity()
  const put = IDBObjectStore.prototype.put
  t.mock.method(IDBObjectStore.prototype, 'put', function (record, ...args) {
    if (record.item?.text === 'fail'.repeat(50)) throw new Error('STORAGE_WRITE_FAILED')
    return put.call(this, record, ...args)
  })
  await assert.rejects(queue.putBy('byId', { id: 'b', text: 'fail'.repeat(50) }), /STORAGE_WRITE_FAILED/)
  assert.deepEqual(await stored(queue), before)
  assert.deepEqual(await queue.getCapacity(), capacity)
})

test('putBy propagates browser quota errors without retrying or losing pending data', async t => {
  const queue = await open(t)
  await queue.putBy('byId', { id: 'a', text: 'saved' })
  const capacity = await queue.getCapacity()
  const put = IDBObjectStore.prototype.put
  let attempts = 0
  t.mock.method(IDBObjectStore.prototype, 'put', function (record, ...args) {
    if (record.item) { attempts++; throw new DOMException('Browser quota', 'QuotaExceededError') }
    return put.call(this, record, ...args)
  })
  await assert.rejects(queue.putBy('byId', { id: 'a', text: 'checkpoint' }), { name: 'QuotaExceededError' })
  assert.equal(attempts, 1)
  assert.deepEqual(await stored(queue), [{ id: 'a', text: 'saved' }])
  assert.deepEqual(await queue.getCapacity(), capacity)
})

test('putBy follows push and setAt eviction policies', async t => {
  for (const evictionPolicy of ['opposite-end', 'fifo', 'lifo']) {
    for (const id of ['a', 'c', 'new']) {
      const queue = await open(t, { maxBytes: 450, evictionPolicy })
      const reference = await open(t, { maxBytes: 450, evictionPolicy })
      for (const key of ['a', 'b', 'c']) {
        const item = { id: key, text: key.repeat(60) }
        await queue.push(item)
        await reference.push(item)
      }
      const item = { id, text: 'expanded'.repeat(23) }
      await queue.putBy('byId', item)
      if (id === 'new') await reference.push(item)
      else await reference.setAt(id === 'a' ? 0 : 2, item)
      assert.deepEqual(await stored(queue), await stored(reference))
      assert.deepEqual(await queue.getCapacity(), await reference.getCapacity())
    }
  }
})

test('putBy invalidates every old reservation action after replacement', async t => {
  for (const action of ['ack', 'nack', 'renew']) {
    const queue = await open(t)
    await queue.push({ id: 'a', text: 'old' })
    const old = await queue.reserve()
    await queue.putBy('byId', { id: 'a', text: 'new' })
    const current = await queue.reserve()
    assert.equal(current.item.text, 'new')
    assert.equal(await old[action](), false)
    assert.equal(await current.ack(), true)
    assert.deepEqual(await stored(queue), [])
  }
})

test('putBy serializes competing inserts and removal/checkpoints across instances', async t => {
  const indexedDB = new IDBFactory()
  const a = await open(t, { indexedDB })
  const b = await open(t, { indexedDB })
  await Promise.all([a.putBy('byId', { id: 'a', text: 'first' }), b.putBy('byId', { id: 'a', text: 'second' })])
  assert.equal((await stored(a)).length, 1)
  assert.equal((await a.getBy('byId', 'a')).text, 'second')
  const removal = a.removeBy('byId', 'a')
  const checkpoint = b.putBy('byId', { id: 'a', text: 'late' }, { existingOnly: true })
  await removal
  assert.equal(await checkpoint, false)
  await a.putBy('byId', { id: 'a' })
  await Promise.all([b.putBy('byId', { id: 'a', text: 'before removal' }, { existingOnly: true }), a.removeBy('byId', 'a')])
  assert.deepEqual(await stored(a), [])
  assert.equal((await a.getCapacity()).usedBytes, 0)
})

test('putBy wakes a waiting consumer and close waits for its transaction', async t => {
  const queue = await open(t)
  const iterator = queue.items()
  const next = iterator.next()
  await queue.putBy('byId', { id: 'a' })
  assert.deepEqual((await next).value, { id: 'a' })
  await iterator.return()
  const writing = queue.putBy('byId', { id: 'b' })
  const closing = queue.close()
  assert.equal(await writing, true)
  await closing
  await assert.rejects(queue.putBy('byId', { id: 'c' }), /QUEUE_CLOSED/)
})
