import test from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { PrivateMessenger } from '../private-messenger/index.js'
import {
  PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS,
  PRIVATE_MESSENGER_STORAGE_RETENTION_MS
} from '../private-messenger/services/storage-maintenance.js'

const REGISTRY_DATABASE = 'libp2r2p:private-messenger:registry:idb'

function signer (pubkey) {
  return { getPublicKey: () => pubkey, withSharedKey: () => ({}) }
}

function fakePrivateMessage () {
  return {
    watch: async () => () => {},
    unwatch: () => {}
  }
}

async function createMessenger (indexedDB, userPubkey, options = {}) {
  return new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB,
    ...options
  }).init({
    userSigner: signer(userPubkey),
    channels: [{ signer: signer(`${userPubkey}-channel`), relays: ['wss://relay.example'] }]
  })
}

function readRegistryRecord (indexedDB, userPubkey) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(REGISTRY_DATABASE)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const db = request.result
      const tx = db.transaction(['bundles'], 'readonly')
      const get = tx.objectStore('bundles').get(userPubkey)
      get.onerror = () => reject(get.error)
      get.onsuccess = () => resolve(get.result || null)
      tx.oncomplete = () => db.close()
    }
  })
}

async function databaseNames (indexedDB) {
  return new Set((await indexedDB.databases()).map(database => database.name))
}

function openDatabase (indexedDB, name) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
  })
}

test('private messenger registers storage before opening its identity databases', async () => {
  const inner = new IDBFactory()
  const userPubkey = 'registration-first'
  const failing = {
    open (name, version) {
      if (name === `libp2r2p:private-messenger:${userPubkey}:idb-queue`) {
        throw new Error('IDENTITY_DATABASE_FAILURE')
      }
      return version === undefined ? inner.open(name) : inner.open(name, version)
    },
    deleteDatabase: name => inner.deleteDatabase(name)
  }

  await assert.rejects(createMessenger(failing, userPubkey), /IDENTITY_DATABASE_FAILURE/)
  const record = await readRegistryRecord(inner, userPubkey)
  assert.equal(record.userPubkey, userPubkey)
  assert.equal(record.status, 'ready')
  assert.equal(record.leaseUntil, 0)
})

test('private messenger keeps an inactive storage set for 59 days and removes it at 60 days', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  const userPubkey = 'retained-user'
  let now = 10_000
  Date.now = () => now
  globalThis.IDBKeyRange = IDBKeyRange

  try {
    const messenger = await createMessenger(indexedDB, userPubkey)
    await messenger.queue.enqueue({ id: 'unconsumed' })
    await messenger.close()

    now += PRIVATE_MESSENGER_STORAGE_RETENTION_MS - 24 * 60 * 60 * 1000
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 0)
    let names = await databaseNames(indexedDB)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:idb-queue`), true)

    now += 24 * 60 * 60 * 1000
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 1)
    names = await databaseNames(indexedDB)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:idb-queue`), false)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:seeds:idb-queue`), false)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:state:idb`), false)
    assert.equal(await readRegistryRecord(indexedDB, userPubkey), null)
  } finally {
    Date.now = originalNow
  }
})

test('storage heartbeat refreshes the identity lease at most on its hourly timer', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  const timers = []
  let now = 20_000
  Date.now = () => now

  try {
    const messenger = await createMessenger(indexedDB, 'heartbeat-user', {
      _storageSetInterval: (fn, ms) => {
        const timer = { fn, ms }
        timers.push(timer)
        return timer
      },
      _storageClearInterval: () => {}
    })
    const before = await readRegistryRecord(indexedDB, 'heartbeat-user')
    const heartbeat = timers.find(timer => timer.ms === PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS)
    assert.ok(heartbeat)

    now += PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS
    await heartbeat.fn()
    const after = await readRegistryRecord(indexedDB, 'heartbeat-user')
    assert.equal(after.lastUsedAt, now)
    assert.ok(after.leaseUntil > before.leaseUntil)
    await messenger.close()
  } finally {
    Date.now = originalNow
  }
})

test('closing one of two active instances preserves the other instance lease', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  const userPubkey = 'multi-tab-user'
  let now = 25_000
  Date.now = () => now

  try {
    const first = await createMessenger(indexedDB, userPubkey)
    const second = await createMessenger(indexedDB, userPubkey)
    await first.close()

    const afterFirstClose = await readRegistryRecord(indexedDB, userPubkey)
    assert.ok(afterFirstClose.leaseUntil > now)

    now += PRIVATE_MESSENGER_STORAGE_RETENTION_MS
    await second.touchStorageActivity({ force: true })
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 0)
    assert.equal((await databaseNames(indexedDB)).has(
      `libp2r2p:private-messenger:${userPubkey}:idb-queue`
    ), true)

    await second.close()
    assert.equal((await readRegistryRecord(indexedDB, userPubkey)).leaseUntil, 0)
  } finally {
    Date.now = originalNow
  }
})

test('a partial storage-set deletion remains pending and resumes idempotently', async () => {
  const originalNow = Date.now
  const inner = new IDBFactory()
  const userPubkey = 'retry-user'
  const failedDatabase = `libp2r2p:private-messenger:${userPubkey}:idb-queue`
  let now = 30_000
  let failDelete = false
  Date.now = () => now
  const indexedDB = {
    open: (name, version) => version === undefined ? inner.open(name) : inner.open(name, version),
    deleteDatabase (name) {
      if (failDelete && name === failedDatabase) {
        const request = { error: new Error('INJECTED_DELETE_FAILURE') }
        queueMicrotask(() => request.onerror?.())
        return request
      }
      return inner.deleteDatabase(name)
    }
  }

  try {
    const messenger = await createMessenger(indexedDB, userPubkey)
    await messenger.close()
    now += PRIVATE_MESSENGER_STORAGE_RETENTION_MS
    failDelete = true

    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 1)
    const pending = await readRegistryRecord(inner, userPubkey)
    assert.equal(pending.status, 'delete_pending')
    assert.equal(pending.attempts, 1)

    failDelete = false
    now = pending.nextAttemptAt
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 1)
    assert.equal(await readRegistryRecord(inner, userPubkey), null)
    const names = await databaseNames(inner)
    assert.equal(names.has(failedDatabase), false)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:seeds:idb-queue`), false)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:state:idb`), false)
  } finally {
    Date.now = originalNow
  }
})

test('a blocked deletion keeps its durable operation live until the handle closes', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  const userPubkey = 'blocked-user'
  let now = 35_000
  Date.now = () => now

  try {
    const messenger = await createMessenger(indexedDB, userPubkey)
    await messenger.close()
    const blocker = await openDatabase(
      indexedDB,
      `libp2r2p:private-messenger:${userPubkey}:state:idb`
    )
    now += PRIVATE_MESSENGER_STORAGE_RETENTION_MS
    let settled = false
    const maintenance = PrivateMessenger.maintainStorage({ indexedDB })
      .then(value => { settled = true; return value })
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(settled, false)

    blocker.close()
    assert.equal(await maintenance, 1)
    assert.equal(await readRegistryRecord(indexedDB, userPubkey), null)
  } finally {
    Date.now = originalNow
  }
})

test('reactivating an identity cancels a pending deletion before reopening its databases', async () => {
  const originalNow = Date.now
  const inner = new IDBFactory()
  const userPubkey = 'reactivated-user'
  const failedDatabase = `libp2r2p:private-messenger:${userPubkey}:idb-queue`
  let now = 40_000
  let failDelete = false
  Date.now = () => now
  const indexedDB = {
    open: (name, version) => version === undefined ? inner.open(name) : inner.open(name, version),
    deleteDatabase (name) {
      if (failDelete && name === failedDatabase) {
        const request = { error: new Error('INJECTED_DELETE_FAILURE') }
        queueMicrotask(() => request.onerror?.())
        return request
      }
      return inner.deleteDatabase(name)
    }
  }

  try {
    const first = await createMessenger(indexedDB, userPubkey)
    await first.close()
    now += PRIVATE_MESSENGER_STORAGE_RETENTION_MS
    failDelete = true
    await PrivateMessenger.maintainStorage({ indexedDB })
    assert.equal((await readRegistryRecord(inner, userPubkey)).status, 'delete_pending')

    failDelete = false
    const second = await createMessenger(indexedDB, userPubkey)
    const active = await readRegistryRecord(inner, userPubkey)
    assert.equal(active.status, 'ready')
    assert.equal(active.attempts, 0)
    assert.equal(active.lastUsedAt, now)
    const names = await databaseNames(inner)
    assert.equal(names.has(failedDatabase), true)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:seeds:idb-queue`), true)
    assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:state:idb`), true)
    await second.close()
  } finally {
    Date.now = originalNow
  }
})

test('private messenger close is idempotent and waits for queued storage work', async () => {
  const indexedDB = new IDBFactory()
  const messenger = await createMessenger(indexedDB, 'close-user')
  let release
  const gate = new Promise(resolve => { release = resolve })
  const operation = messenger.runQueueOperation(() => gate)
  let closed = false
  const firstClose = messenger.close().then(() => { closed = true })
  const secondClose = messenger.close()

  await Promise.resolve()
  assert.equal(closed, false)
  release()
  await operation
  await firstClose
  await secondClose
  assert.equal(closed, true)
})

test('private messenger close stops network immediately and awaits watcher teardown', async () => {
  const indexedDB = new IDBFactory()
  let release
  let stopped = false
  const gate = new Promise(resolve => { release = resolve })
  const messenger = await createMessenger(indexedDB, 'watcher-close-user', {
    _privateMessage: {
      watch: async () => () => {
        stopped = true
        return gate
      },
      unwatch: () => {}
    }
  })
  let closed = false
  const closing = messenger.close().then(() => { closed = true })

  assert.equal(stopped, true)
  await Promise.resolve()
  assert.equal(closed, false)
  release()
  await closing
  assert.equal(closed, true)
})

test('private messenger close waits for an initialization already in flight', async () => {
  const indexedDB = new IDBFactory()
  let releasePubkey
  const pubkey = new Promise(resolve => { releasePubkey = resolve })
  const messenger = new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  })
  const initializing = messenger.init({
    userSigner: { getPublicKey: () => pubkey, withSharedKey: () => ({}) },
    channels: []
  })
  let closed = false
  const closing = messenger.close().then(() => { closed = true })

  await Promise.resolve()
  assert.equal(closed, false)
  releasePubkey('closing-init-user')
  await assert.rejects(initializing, /PRIVATE_MESSENGER_CLOSED/)
  await closing
  assert.equal(closed, true)
  assert.equal((await readRegistryRecord(indexedDB, 'closing-init-user')).leaseUntil, 0)
})
