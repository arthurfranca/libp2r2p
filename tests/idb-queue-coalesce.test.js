import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory } from 'fake-indexeddb'
import { createQueue } from '../idb-queue/index.js'
import { ValidationError } from '../error/index.js'

const indexes = {
  byCoalescingKey: 'coalescingKey'
}

async function open (t, options = {}) {
  const queue = await createQueue({
    prefix: 'coalesce',
    indexedDB: new IDBFactory(),
    indexes,
    evictionPolicy: 'reject',
    ...options
  })
  t.after(() => queue.close())
  return queue
}

async function stored (queue) {
  return Array.fromAsync(queue.storedItems())
}

function byVersion (left, right) {
  return (left.version || 0) - (right.version || 0)
}

test('coalesceBy replaces the oldest pending item in place and collapses duplicates', async t => {
  const queue = await open(t)
  await queue.push({ id: 'before', coalescingKey: 'other' })
  await queue.push({ id: 'oldest', coalescingKey: 'ask', version: 1, value: 'a'.repeat(40) })
  await queue.push({ id: 'after', coalescingKey: 'other', version: 1 })
  await queue.push({ id: 'duplicate', coalescingKey: 'ask', version: 2, value: 'b'.repeat(40) })
  const latest = { id: 'latest', coalescingKey: 'ask', version: 3, value: 'c'.repeat(10) }

  assert.deepEqual(
    await queue.coalesceBy('byCoalescingKey', latest, { compare: byVersion }),
    { status: 'replaced', removed: 1, position: 1 }
  )
  assert.deepEqual(await stored(queue), [
    { id: 'before', coalescingKey: 'other' },
    latest,
    { id: 'after', coalescingKey: 'other', version: 1 }
  ])

  const reference = await open(t, { indexedDB: new IDBFactory() })
  for (const item of await stored(queue)) await reference.push(item)
  assert.deepEqual(await queue.getCapacity(), await reference.getCapacity())
})

test('coalesceBy ignores stale items when a comparator is provided', async t => {
  const queue = await open(t)
  await queue.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 2 })
  assert.deepEqual(
    await queue.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 1 }, { compare: byVersion }),
    { status: 'ignored', removed: 0, position: null }
  )
  assert.deepEqual(await stored(queue), [{ coalescingKey: 'ask', version: 2 }])
})

test('coalesceBy keeps an active item and maintains one pending successor', async t => {
  const queue = await open(t)
  await queue.push({ coalescingKey: 'ask', version: 1 })
  const active = await queue.reserve()

  assert.equal(
    (await queue.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 2 }, { compare: byVersion })).status,
    'successor'
  )
  assert.deepEqual(
    await queue.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 3 }, { compare: byVersion }),
    { status: 'replaced', removed: 0, position: 1 }
  )
  assert.deepEqual(await stored(queue), [
    { coalescingKey: 'ask', version: 1 },
    { coalescingKey: 'ask', version: 3 }
  ])

  const successor = await queue.reserve()
  assert.equal(successor.item.version, 3)
  assert.equal(await active.ack(), true)
  assert.equal(await successor.ack(), true)
})

test('nack removes an active item superseded by a pending successor', async t => {
  const queue = await open(t)
  await queue.push({ coalescingKey: 'ask', version: 1 })
  const active = await queue.reserve({ nackCoalesceIndex: 'byCoalescingKey' })
  await queue.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 2 }, { compare: byVersion })

  assert.equal(await active.nack(), true)
  const successor = await queue.reserve()
  assert.equal(successor.item.version, 2)
  assert.equal(await successor.nack(), true)
  const restored = await queue.reserve()
  assert.equal(restored.item.version, 2)
  assert.equal(await restored.ack(), true)
})

test('coalesceBy rolls back capacity growth without losing the previous item', async t => {
  const queue = await open(t, { maxBytes: 420 })
  await queue.push({ coalescingKey: 'ask', version: 1, value: 'a'.repeat(100) })
  await queue.push({ coalescingKey: 'other', version: 1, value: 'b'.repeat(100) })
  const before = await stored(queue)
  const capacity = await queue.getCapacity()

  await assert.rejects(
    queue.coalesceBy('byCoalescingKey', {
      coalescingKey: 'ask',
      version: 2,
      value: 'c'.repeat(240)
    }, { compare: byVersion }),
    { code: 'QUEUE_CAPACITY_EXCEEDED' }
  )
  assert.deepEqual(await stored(queue), before)
  assert.deepEqual(await queue.getCapacity(), capacity)
})

test('coalesceBy serializes competing instances and validates its index', async t => {
  const indexedDB = new IDBFactory()
  const first = await open(t, { indexedDB })
  const second = await open(t, { indexedDB })
  await Promise.all([
    first.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 1 }, { compare: byVersion }),
    second.coalesceBy('byCoalescingKey', { coalescingKey: 'ask', version: 2 }, { compare: byVersion })
  ])
  assert.deepEqual(await stored(first), [{ coalescingKey: 'ask', version: 2 }])

  await assert.rejects(
    first.coalesceBy('missing', { coalescingKey: 'ask' }),
    error => error instanceof ValidationError && error.code === 'QUEUE_COALESCE_INDEX_INVALID'
  )
  await assert.rejects(
    first.coalesceBy('byCoalescingKey', {}, {}),
    error => error instanceof ValidationError && error.code === 'QUEUE_COALESCE_KEY_INVALID'
  )
  await assert.rejects(
    first.reserve({ nackCoalesceIndex: 'missing' }),
    error => error instanceof ValidationError && error.code === 'QUEUE_NACK_COALESCE_INDEX_INVALID'
  )
})
