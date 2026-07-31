import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory } from 'fake-indexeddb'
import { run } from '../idb/index.js'

function deferred () {
  let resolve
  let reject
  // eslint-disable-next-line promise/param-names
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

function transactionDone (tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error || new Error('IDB_TRANSACTION_ABORTED'))
    tx.onerror = () => reject(tx.error || new Error('IDB_TRANSACTION_FAILED'))
  })
}

function openRowsDb () {
  const indexedDB = new IDBFactory()
  return new Promise((resolve, reject) => {
    const request = indexedDB.open('idb-run-test', 1)
    request.onerror = () => reject(request.error)
    request.onupgradeneeded = () => {
      const store = request.result.createObjectStore('rows', { keyPath: 'id' })
      store.createIndex('byGroup', 'group')
    }
    request.onsuccess = () => resolve(request.result)
  })
}

test('run performs store and index requests inside a shared transaction', async () => {
  const db = await openRowsDb()
  const tx = db.transaction(['rows'], 'readwrite')
  const done = transactionDone(tx)

  await run('put', [{ id: 'one', group: 'alpha', value: 1 }], 'rows', null, { db, tx })
  await run('put', [{ id: 'two', group: 'beta', value: 2 }], 'rows', null, { db, tx })
  const found = await run('get', ['alpha'], 'rows', 'byGroup', { db, tx })

  assert.equal(found.result.id, 'one')
  await done
  db.close()
})

test('run supports the reusable deferred cursor pattern', async () => {
  const db = await openRowsDb()
  const setup = db.transaction(['rows'], 'readwrite')
  const setupDone = transactionDone(setup)
  await run('put', [{ id: 'one', group: 'alpha' }], 'rows', null, { db, tx: setup })
  await run('put', [{ id: 'two', group: 'beta' }], 'rows', null, { db, tx: setup })
  await setupDone

  const tx = db.transaction(['rows'], 'readonly')
  const done = transactionDone(tx)
  const p = deferred()
  let cursor = (await run('openCursor', [], 'rows', null, { db, tx, p })).result
  const ids = []
  while (cursor) {
    ids.push(cursor.value.id)
    Object.assign(p, deferred())
    cursor.continue()
    cursor = (await p.promise).result
  }

  assert.deepEqual(ids, ['one', 'two'])
  await done
  db.close()
})

test('run aborts a transaction when a request fails', async () => {
  const db = await openRowsDb()
  const tx = db.transaction(['rows'], 'readwrite')
  const done = transactionDone(tx)

  await run('add', [{ id: 'one', group: 'alpha' }], 'rows', null, { db, tx })
  await assert.rejects(
    run('add', [{ id: 'one', group: 'beta' }], 'rows', null, { db, tx })
  )
  await assert.rejects(done)

  const read = db.transaction(['rows'], 'readonly')
  const row = await run('get', ['one'], 'rows', null, { db, tx: read })
  assert.equal(row.result, undefined)
  db.close()
})
