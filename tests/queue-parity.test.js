import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory } from 'fake-indexeddb'
import { createQueue as createIdbQueue } from '../idb-queue/index.js'
import { createQueue as createWebStorageQueue } from '../web-storage-queue/index.js'

const CORE_METHODS = [
  'enqueue', 'push', 'pop', 'unshift', 'shift', 'items', 'reverseItems',
  'storedItems', 'reverseStoredItems', 'setAt', 'insertAt', 'insertWhere',
  'removeAt', 'removeWhere', 'some', 'clear'
]

function createStorageArea () {
  const values = new Map()
  return {
    getItem: key => values.get(String(key)) ?? null,
    removeItem: key => { values.delete(String(key)) },
    setItem: (key, value) => { values.set(String(key), String(value)) }
  }
}

async function call (queue, method, ...args) {
  return queue[method](...args)
}

async function storedValues (queue) {
  const values = []
  for await (const value of queue.storedItems()) values.push(value)
  return values
}

async function coreTrace (queue) {
  const returns = []
  returns.push(await call(queue, 'push', { value: 'middle' }))
  returns.push(await call(queue, 'unshift', { value: 'first' }))
  returns.push(await call(queue, 'push', { value: 'last' }))
  returns.push(await call(queue, 'setAt', 1, { value: 'MIDDLE' }))
  returns.push(await call(queue, 'insertAt', 2, { value: 'between' }))
  const beforeRemoval = await storedValues(queue)
  const hasBetween = await call(queue, 'some', item => item.value === 'between')
  await call(queue, 'removeWhere', item => item.value === 'between')

  return {
    returns,
    beforeRemoval,
    hasBetween,
    afterRemoval: await storedValues(queue),
    drained: [
      await call(queue, 'shift'),
      await call(queue, 'pop'),
      await call(queue, 'shift'),
      await call(queue, 'shift')
    ]
  }
}

test('idb queue preserves the web-storage queue core API and semantics', async () => {
  const webStorageQueue = createWebStorageQueue({
    prefix: 'web',
    storageArea: createStorageArea()
  })
  const idbQueue = await createIdbQueue({
    prefix: 'idb',
    indexedDB: new IDBFactory()
  })

  assert.deepEqual(Object.keys(webStorageQueue).sort(), [...CORE_METHODS].sort())
  assert.deepEqual(
    Object.keys(idbQueue).filter(method => CORE_METHODS.includes(method)).sort(),
    [...CORE_METHODS].sort()
  )
  assert.equal(typeof idbQueue.getBy, 'function')
  assert.equal(typeof idbQueue.someBy, 'function')
  assert.equal(typeof idbQueue.removeBy, 'function')
  assert.equal(typeof idbQueue.storedItemsBy, 'function')

  assert.deepEqual(await coreTrace(idbQueue), await coreTrace(webStorageQueue))
})

test('queues preserve caller ids without injecting queue positions', async () => {
  const queues = [
    createWebStorageQueue({ prefix: 'web', storageArea: createStorageArea() }),
    await createIdbQueue({ prefix: 'idb', indexedDB: new IDBFactory() })
  ]

  for (const queue of queues) {
    await call(queue, 'push', { value: 'generated' })
    await call(queue, 'push', { id: 'caller-id', value: 'provided' })
    assert.deepEqual(await call(queue, 'shift'), { value: 'generated' })
    assert.deepEqual(await call(queue, 'shift'), { id: 'caller-id', value: 'provided' })
  }
})

test('both queue types require a prefix and predicate callbacks', async () => {
  const storageArea = createStorageArea()
  assert.throws(() => createWebStorageQueue({ storageArea }), /QUEUE_PREFIX_REQUIRED/)
  await assert.rejects(createIdbQueue({ indexedDB: new IDBFactory() }), /QUEUE_PREFIX_REQUIRED/)

  const webStorageQueue = createWebStorageQueue({ prefix: 'web', storageArea })
  const idbQueue = await createIdbQueue({ prefix: 'idb', indexedDB: new IDBFactory() })
  for (const method of ['insertWhere', 'removeWhere', 'some']) {
    assert.throws(() => webStorageQueue[method](), /QUEUE_PREDICATE_REQUIRED/)
    await assert.rejects(idbQueue[method](), /QUEUE_PREDICATE_REQUIRED/)
  }
})
