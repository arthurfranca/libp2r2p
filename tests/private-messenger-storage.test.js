import test from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { PrivateMessenger } from '../private-messenger/index.js'
import { createChannelStateStore } from '../private-messenger/services/channel-state.js'
import {
  PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS,
  PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS
} from '../private-messenger/services/storage-maintenance.js'

const REGISTRY_DATABASE = 'libp2r2p:private-messenger:registry:idb'

class TestBroadcastChannel {
  static channels = new Map()

  constructor (name) {
    this.name = name
    const channels = TestBroadcastChannel.channels.get(name) || new Set()
    channels.add(this)
    TestBroadcastChannel.channels.set(name, channels)
  }

  postMessage (data) {
    for (const channel of TestBroadcastChannel.channels.get(this.name) || []) {
      if (channel === this) continue
      queueMicrotask(() => channel.onmessage?.({ data }))
    }
  }

  close () {
    const channels = TestBroadcastChannel.channels.get(this.name)
    channels?.delete(this)
    if (!channels?.size) TestBroadcastChannel.channels.delete(this.name)
  }

  unref () {}
}

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
      const tx = db.transaction(['storageSets'], 'readonly')
      const get = tx.objectStore('storageSets').get(userPubkey)
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

    now += PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS - 24 * 60 * 60 * 1000
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

    now += PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS
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
    now += PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS
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
    now += PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS
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
    now += PRIVATE_MESSENGER_IDENTITY_STORAGE_RETENTION_MS
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

test('storage retention is persisted per identity and the last configuring instance wins', async () => {
  const indexedDB = new IDBFactory()
  const userPubkey = 'policy-user'
  const first = await createMessenger(indexedDB, userPubkey, {
    staleChannelSeconds: 10,
    identityStorageRetentionSeconds: 20
  })
  let record = await readRegistryRecord(indexedDB, userPubkey)
  assert.equal(record.staleChannelSeconds, 10)
  assert.equal(record.identityStorageRetentionSeconds, 20)
  assert.equal(record.policyRevision, 1)

  const second = await createMessenger(indexedDB, userPubkey, {
    staleChannelSeconds: 30,
    identityStorageRetentionSeconds: 40
  })
  record = await readRegistryRecord(indexedDB, userPubkey)
  assert.equal(record.staleChannelSeconds, 30)
  assert.equal(record.identityStorageRetentionSeconds, 40)
  assert.equal(record.policyRevision, 2)

  await first.touchStorageActivity({ force: true })
  record = await readRegistryRecord(indexedDB, userPubkey)
  assert.equal(record.staleChannelSeconds, 30)
  assert.equal(record.identityStorageRetentionSeconds, 40)
  assert.equal(record.policyRevision, 2)

  await first.update({ staleChannelSeconds: 50 })
  record = await readRegistryRecord(indexedDB, userPubkey)
  assert.equal(record.staleChannelSeconds, 50)
  assert.equal(record.identityStorageRetentionSeconds, 40)
  assert.equal(record.policyRevision, 3)

  await first.close()
  await second.close()
})

test('storage policy changes propagate to another active instance', async () => {
  const indexedDB = new IDBFactory()
  const userPubkey = 'broadcast-policy-user'
  const first = await createMessenger(indexedDB, userPubkey, {
    _BroadcastChannel: TestBroadcastChannel,
    staleChannelSeconds: 10,
    identityStorageRetentionSeconds: 20
  })
  const second = await createMessenger(indexedDB, userPubkey, {
    _BroadcastChannel: TestBroadcastChannel,
    staleChannelSeconds: 30,
    identityStorageRetentionSeconds: 40
  })
  await new Promise(resolve => setImmediate(resolve))
  await first.storagePolicyRefreshTail
  assert.equal(first.staleChannelSeconds, 30)
  assert.equal(first.identityStorageRetentionSeconds, 40)

  await first.update({ staleChannelSeconds: 50 })
  await new Promise(resolve => setImmediate(resolve))
  await second.storagePolicyRefreshTail
  assert.equal(second.staleChannelSeconds, 50)
  assert.equal(second.identityStorageRetentionSeconds, 40)

  await first.close()
  await second.close()
})

test('identity storage retention is independent per identity', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1_000_000
  Date.now = () => now
  try {
    const short = await createMessenger(indexedDB, 'short-retention', {
      identityStorageRetentionSeconds: 10
    })
    const long = await createMessenger(indexedDB, 'long-retention', {
      identityStorageRetentionSeconds: 20
    })
    await short.close()
    await long.close()

    now += 10_000
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 1)
    assert.equal(await readRegistryRecord(indexedDB, 'short-retention'), null)
    assert.ok(await readRegistryRecord(indexedDB, 'long-retention'))

    now += 10_000
    assert.equal(await PrivateMessenger.maintainStorage({ indexedDB }), 1)
    assert.equal(await readRegistryRecord(indexedDB, 'long-retention'), null)
  } finally {
    Date.now = originalNow
  }
})

test('zero identity storage retention removes the storage set after its final lease closes', async () => {
  const indexedDB = new IDBFactory()
  const userPubkey = 'zero-retention'
  const messenger = await createMessenger(indexedDB, userPubkey, {
    identityStorageRetentionSeconds: 0
  })
  assert.ok(await readRegistryRecord(indexedDB, userPubkey))
  await messenger.close()
  assert.equal(await readRegistryRecord(indexedDB, userPubkey), null)
  const names = await databaseNames(indexedDB)
  assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:idb-queue`), false)
  assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:seeds:idb-queue`), false)
  assert.equal(names.has(`libp2r2p:private-messenger:${userPubkey}:state:idb`), false)
})

test('an actively leased channel is not pruned after its stale-channel deadline', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 2_000_000_000_000
  Date.now = () => now
  try {
    const messenger = await createMessenger(indexedDB, 'active-channel-user', {
      staleChannelSeconds: 24 * 60 * 60
    })
    messenger.updateChannelState('active-channel-user-channel', {
      lastWatchedAt: Math.floor(now / 1000) - 2 * 24 * 60 * 60
    })
    await messenger.flushStateWrites()

    await messenger.cleanupStaleChannels()
    assert.ok(messenger.readState().channels['active-channel-user-channel'])

    now += PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS
    await messenger.runStorageHeartbeat()
    assert.equal(
      messenger.readState().channels['active-channel-user-channel'].lastWatchedAt,
      Math.floor(now / 1000)
    )
    await messenger.close()
  } finally {
    Date.now = originalNow
  }
})

test('removing a configured channel starts its stale-channel retention window', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 2_050_000_000_000
  Date.now = () => now
  try {
    const messenger = await createMessenger(indexedDB, 'removed-channel-user', {
      staleChannelSeconds: 10
    })
    const pubkey = 'removed-channel-user-channel'
    await messenger.update({ channels: [] })
    assert.equal(messenger.readState().channels[pubkey].lastWatchedAt, Math.floor(now / 1000))

    now += 10_000
    await messenger.cleanupStaleChannels()
    assert.ok(messenger.readState().channels[pubkey])

    now += 1_000
    await messenger.cleanupStaleChannels()
    assert.equal(messenger.readState().channels[pubkey], undefined)
    await messenger.close()
  } finally {
    Date.now = originalNow
  }
})

test('another active lease protects its channel from stale cleanup', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 2_100_000_000_000
  Date.now = () => now
  const userPubkey = 'shared-channel-user'
  try {
    const first = await new PrivateMessenger({
      _privateMessage: fakePrivateMessage(),
      _indexedDB: indexedDB,
      staleChannelSeconds: 1
    }).init({
      userSigner: signer(userPubkey),
      channels: [{ signer: signer('first-channel'), relays: ['wss://relay.example'] }]
    })
    first.updateChannelState('first-channel', { lastWatchedAt: Math.floor(now / 1000) - 10 })
    await first.flushStateWrites()
    const second = await new PrivateMessenger({
      _privateMessage: fakePrivateMessage(),
      _indexedDB: indexedDB,
      staleChannelSeconds: 1
    }).init({
      userSigner: signer(userPubkey),
      channels: [{ signer: signer('second-channel'), relays: ['wss://relay.example'] }]
    })

    await second.cleanupStaleChannels()
    assert.ok(second.readState().channels['first-channel'])

    await first.close()
    now += 2_000
    await second.cleanupStaleChannels()
    assert.equal(second.readState().channels['first-channel'], undefined)
    await second.close()
  } finally {
    Date.now = originalNow
  }
})

test('one instance heartbeat does not overwrite another instance channel state', async () => {
  const indexedDB = new IDBFactory()
  const userPubkey = 'incremental-channel-state-user'
  const first = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  }).init({
    userSigner: signer(userPubkey),
    channels: [{ signer: signer('first-channel'), relays: ['wss://relay.example'] }]
  })
  const second = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  }).init({
    userSigner: signer(userPubkey),
    channels: [{ signer: signer('second-channel'), relays: ['wss://relay.example'] }]
  })

  await first.runStorageHeartbeat()
  const stateStore = await createChannelStateStore({
    prefix: `libp2r2p:private-messenger:${userPubkey}`,
    indexedDB
  })
  const channels = await stateStore.load()
  assert.ok(channels['first-channel'])
  assert.ok(channels['second-channel'])
  await stateStore.close()
  await first.close()
  await second.close()
})
