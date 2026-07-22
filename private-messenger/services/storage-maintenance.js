import { run } from '../../idb/index.js'
import { cleanupReceivedChunkStorage } from '../../private-channel/services/received-chunks.js'
import { cleanupTemporaryStorage } from '../../temporary-storage/index.js'

const REGISTRY_DATABASE = 'libp2r2p:private-messenger:registry:idb'
const REGISTRY_VERSION = 1
const BUNDLES_STORE = 'bundles'
const LEASES_STORE = 'leases'
const LOCK_NAME = 'libp2r2p:private-messenger:storage-maintenance'
const LEASE_MS = 2 * 60 * 60 * 1000 // 2 hours
const RETENTION_MS = 60 * 24 * 60 * 60 * 1000 // 60 days
const MAX_RETRY_MS = 5 * 60 * 1000

const localLocks = new WeakMap()
const retryTimers = new WeakMap()

/*
IndexedDB schema, shared by every PrivateMessenger instance on this origin:

database "libp2r2p:private-messenger:registry:idb", version 1

bundles, keyPath "userPubkey"
  userPubkey    primary signer public key and storage-set coordinate
  lastUsedAt    most recent durable activity in milliseconds
  leaseUntil    active-instance lease expiry in milliseconds
  status        "ready" or "delete_pending"
  attempts      consecutive failed storage-set deletions
  nextAttemptAt earliest retry time in milliseconds

leases, keyPath "key"
  key         `${userPubkey}:${leaseId}`
  userPubkey  principal signer public key
  leaseId     per-PrivateMessenger instance identifier
  leaseUntil  instance lease expiry in milliseconds

leases indexes
  byUserPubkey  userPubkey
*/

function transactionDone (tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error || new Error('IDB_TRANSACTION_ABORTED'))
    tx.onerror = () => reject(tx.error || new Error('IDB_TRANSACTION_FAILED'))
  })
}

function openRegistry (indexedDB) {
  if (!indexedDB?.open) return Promise.reject(new Error('IDB_UNAVAILABLE'))
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(REGISTRY_DATABASE, REGISTRY_VERSION)
    request.onerror = () => reject(request.error || new Error('IDB_OPEN_FAILED'))
    request.onblocked = () => reject(new Error('IDB_DATABASE_BLOCKED'))
    request.onupgradeneeded = () => {
      const db = request.result
      if (!db.objectStoreNames.contains(BUNDLES_STORE)) {
        db.createObjectStore(BUNDLES_STORE, { keyPath: 'userPubkey' })
      }
      if (!db.objectStoreNames.contains(LEASES_STORE)) {
        const leases = db.createObjectStore(LEASES_STORE, { keyPath: 'key' })
        leases.createIndex('byUserPubkey', 'userPubkey')
      }
    }
    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      resolve(db)
    }
  })
}

async function withRegistryTransaction (indexedDB, mode, work) {
  const db = await openRegistry(indexedDB)
  try {
    const tx = db.transaction([BUNDLES_STORE, LEASES_STORE], mode)
    const done = transactionDone(tx)
    try {
      const result = await work(tx)
      await done
      return result
    } catch (err) {
      try { tx.abort() } catch {}
      try { await done } catch {}
      throw err
    }
  } finally {
    db.close()
  }
}

function leaseKey (userPubkey, leaseId) {
  return `${userPubkey}:${leaseId}`
}

async function readLeases (tx, userPubkey) {
  const keyRange = globalThis.IDBKeyRange
  const records = keyRange?.only
    ? (await run('getAll', [keyRange.only(userPubkey)], LEASES_STORE, 'byUserPubkey', { tx })).result
    : (await run('getAll', [], LEASES_STORE, null, { tx })).result
  return records.filter(record => record?.userPubkey === userPubkey)
}

async function refreshLeaseUntil (tx, bundle, now, { removeExpired = false } = {}) {
  const leases = await readLeases(tx, bundle.userPubkey)
  let leaseUntil = 0
  for (const lease of leases) {
    const expiresAt = Math.max(0, Number(lease.leaseUntil) || 0)
    if (expiresAt <= now) {
      if (removeExpired) await run('delete', [lease.key], LEASES_STORE, null, { tx })
      continue
    }
    leaseUntil = Math.max(leaseUntil, expiresAt)
  }
  bundle.leaseUntil = leaseUntil
  return leaseUntil
}

function normalizeBundle (record) {
  if (!record?.userPubkey) return null
  return {
    userPubkey: String(record.userPubkey),
    lastUsedAt: Math.max(0, Number(record.lastUsedAt) || 0),
    leaseUntil: Math.max(0, Number(record.leaseUntil) || 0),
    status: record.status === 'delete_pending' ? 'delete_pending' : 'ready',
    attempts: Math.max(0, Math.floor(Number(record.attempts) || 0)),
    nextAttemptAt: Math.max(0, Number(record.nextAttemptAt) || 0)
  }
}

function withLocalLock (indexedDB, work) {
  const prior = localLocks.get(indexedDB) || Promise.resolve()
  const next = prior.catch(() => {}).then(work)
  localLocks.set(indexedDB, next.catch(() => {}))
  return next
}

function withMaintenanceLock (indexedDB, work) {
  if (!indexedDB?.open) return Promise.reject(new Error('IDB_UNAVAILABLE'))
  const locks = globalThis.navigator?.locks
  if (typeof locks?.request === 'function') return locks.request(LOCK_NAME, work)
  return withLocalLock(indexedDB, work)
}

function storageDatabaseNames (userPubkey) {
  const prefix = `libp2r2p:private-messenger:${userPubkey}`
  return [
    `${prefix}:idb-queue`,
    `${prefix}:seeds:idb-queue`,
    `${prefix}:state:idb`
  ]
}

function deleteDatabase (indexedDB, name) {
  return new Promise(resolve => {
    let request
    try {
      request = indexedDB.deleteDatabase(name)
    } catch {
      resolve(false)
      return
    }
    request.onsuccess = () => resolve(true)
    request.onerror = () => resolve(false)
    // A blocked delete request remains live and will continue automatically
    // after every other connection closes. Keep the maintenance lock until
    // that definitive success/error instead of scheduling a second request.
    request.onblocked = () => {}
  })
}

async function deleteStorageSet (indexedDB, userPubkey) {
  const results = await Promise.all(storageDatabaseNames(userPubkey)
    .map(name => deleteDatabase(indexedDB, name)))
  return results.every(Boolean)
}

function retryDelay (attempts) {
  return Math.min(MAX_RETRY_MS, 1000 * (2 ** Math.min(9, Math.max(0, attempts - 1))))
}

async function markAndListDueBundles (indexedDB, now) {
  return withRegistryTransaction(indexedDB, 'readwrite', async tx => {
    const records = (await run('getAll', [], BUNDLES_STORE, null, { tx })).result
    const due = []
    for (const raw of records) {
      const bundle = normalizeBundle(raw)
      if (!bundle) continue
      await refreshLeaseUntil(tx, bundle, now, { removeExpired: true })
      const leaseActive = bundle.leaseUntil > now
      if (bundle.status === 'ready' && !leaseActive && bundle.lastUsedAt + RETENTION_MS <= now) {
        bundle.status = 'delete_pending'
        bundle.attempts = 0
        bundle.nextAttemptAt = now
      }
      await run('put', [bundle], BUNDLES_STORE, null, { tx })
      if (bundle.status === 'delete_pending' && !leaseActive && bundle.nextAttemptAt <= now) {
        due.push(bundle)
      }
    }
    return due
  })
}

async function finishDeleteAttempt (indexedDB, attempted, deleted, now) {
  return withRegistryTransaction(indexedDB, 'readwrite', async tx => {
    const current = normalizeBundle((await run('get', [attempted.userPubkey], BUNDLES_STORE, null, { tx })).result)
    if (!current || current.status !== 'delete_pending' || current.leaseUntil > now) return false
    if (deleted) {
      for (const lease of await readLeases(tx, attempted.userPubkey)) {
        await run('delete', [lease.key], LEASES_STORE, null, { tx })
      }
      await run('delete', [attempted.userPubkey], BUNDLES_STORE, null, { tx })
      return true
    }
    current.attempts++
    current.nextAttemptAt = now + retryDelay(current.attempts)
    await run('put', [current], BUNDLES_STORE, null, { tx })
    return false
  })
}

async function maintainRegistry (indexedDB, now) {
  const due = await markAndListDueBundles(indexedDB, now)
  for (const bundle of due) {
    const deleted = await deleteStorageSet(indexedDB, bundle.userPubkey)
    await finishDeleteAttempt(indexedDB, bundle, deleted, now)
  }
  const nextAttemptAt = await withRegistryTransaction(indexedDB, 'readonly', async tx => {
    const records = (await run('getAll', [], BUNDLES_STORE, null, { tx })).result
    let earliest = Infinity
    for (const raw of records) {
      const bundle = normalizeBundle(raw)
      if (bundle?.status !== 'delete_pending' || bundle.leaseUntil > now) continue
      earliest = Math.min(earliest, bundle.nextAttemptAt)
    }
    return Number.isFinite(earliest) ? earliest : null
  })
  return { processed: due.length, nextAttemptAt }
}

function schedulePendingRetry (indexedDB, temporaryStorageArea, nextAttemptAt, now) {
  const previous = retryTimers.get(indexedDB)
  if (previous) clearTimeout(previous)
  if (nextAttemptAt === null) {
    retryTimers.delete(indexedDB)
    return
  }
  const timer = setTimeout(() => {
    retryTimers.delete(indexedDB)
    maintainPrivateMessengerStorage({ indexedDB, temporaryStorageArea }).catch(() => {})
  }, Math.min(MAX_RETRY_MS, Math.max(0, nextAttemptAt - now)))
  timer?.unref?.()
  retryTimers.set(indexedDB, timer)
}

export async function maintainPrivateMessengerStorage ({
  indexedDB = globalThis.indexedDB,
  temporaryStorageArea = globalThis.sessionStorage,
  now = Date.now()
} = {}) {
  if (temporaryStorageArea) cleanupTemporaryStorage({ storageArea: temporaryStorageArea })
  await cleanupReceivedChunkStorage({ indexedDB, now })
  const result = await withMaintenanceLock(indexedDB, () => maintainRegistry(indexedDB, now))
  schedulePendingRetry(indexedDB, temporaryStorageArea, result.nextAttemptAt, now)
  return result.processed
}

export function activatePrivateMessengerStorage ({
  userPubkey,
  leaseId,
  indexedDB = globalThis.indexedDB,
  now = Date.now()
} = {}) {
  if (!userPubkey) return Promise.reject(new Error('USER_PUBKEY_REQUIRED'))
  if (!leaseId) return Promise.reject(new Error('PRIVATE_MESSENGER_LEASE_ID_REQUIRED'))
  return withMaintenanceLock(indexedDB, () => withRegistryTransaction(indexedDB, 'readwrite', async tx => {
    userPubkey = String(userPubkey)
    leaseId = String(leaseId)
    const current = normalizeBundle((await run('get', [userPubkey], BUNDLES_STORE, null, { tx })).result)
    await run('put', [{
      key: leaseKey(userPubkey, leaseId),
      userPubkey,
      leaseId,
      leaseUntil: now + LEASE_MS
    }], LEASES_STORE, null, { tx })
    const bundle = {
      userPubkey,
      lastUsedAt: now,
      leaseUntil: Math.max(current?.leaseUntil || 0, now + LEASE_MS),
      status: 'ready',
      attempts: 0,
      nextAttemptAt: 0
    }
    await run('put', [bundle], BUNDLES_STORE, null, { tx })
  }))
}

export function releasePrivateMessengerStorage ({
  userPubkey,
  leaseId,
  indexedDB = globalThis.indexedDB,
  now = Date.now()
} = {}) {
  if (!userPubkey) return Promise.resolve(false)
  if (!leaseId) return Promise.reject(new Error('PRIVATE_MESSENGER_LEASE_ID_REQUIRED'))
  return withMaintenanceLock(indexedDB, () => withRegistryTransaction(indexedDB, 'readwrite', async tx => {
    userPubkey = String(userPubkey)
    leaseId = String(leaseId)
    const current = normalizeBundle((await run('get', [userPubkey], BUNDLES_STORE, null, { tx })).result)
    if (!current) return false
    await run('delete', [leaseKey(userPubkey, leaseId)], LEASES_STORE, null, { tx })
    await refreshLeaseUntil(tx, current, now, { removeExpired: true })
    current.lastUsedAt = now
    current.status = 'ready'
    current.attempts = 0
    current.nextAttemptAt = 0
    await run('put', [current], BUNDLES_STORE, null, { tx })
    return true
  }))
}

export const PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS = 60 * 60 * 1000
export const PRIVATE_MESSENGER_STORAGE_MAINTENANCE_MS = 6 * 60 * 60 * 1000
export const PRIVATE_MESSENGER_STORAGE_RETENTION_MS = RETENTION_MS
