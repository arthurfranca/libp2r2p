import { run } from '../../idb/index.js'

const DATABASE_VERSION = 1
const CHANNELS_STORE = 'channels'

/*
IndexedDB schema, scoped per private-messenger prefix:

database `${prefix}:state:idb`, version 1

channels, keyPath "pubkey"
  pubkey  watched channel public key
  value   evolving channel-recovery state, including last-seen/watched times,
          mode, relays, seeders, offline ranges, active offline-range start,
          per-seeder activity, and sent/received content-key usage

The complete channels snapshot is replaced atomically on each persisted state
change; value remains an extensible internal object rather than a public schema.
*/

function deferred () {
  let resolve
  let reject
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve
    reject = nextReject
  })
  return { promise, resolve, reject }
}

function transactionDone (tx) {
  const pending = deferred()
  tx.oncomplete = () => pending.resolve()
  tx.onabort = () => pending.reject(tx.error || new Error('IDB_TRANSACTION_ABORTED'))
  tx.onerror = () => pending.reject(tx.error || new Error('IDB_TRANSACTION_FAILED'))
  return pending.promise
}

function openDatabase (indexedDB, name) {
  if (!indexedDB?.open) return Promise.reject(new Error('IDB_UNAVAILABLE'))
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, DATABASE_VERSION)
    request.onerror = () => reject(request.error || new Error('IDB_OPEN_FAILED'))
    request.onblocked = () => reject(new Error('IDB_DATABASE_BLOCKED'))
    request.onupgradeneeded = () => {
      const db = request.result
      if (!db.objectStoreNames.contains(CHANNELS_STORE)) {
        db.createObjectStore(CHANNELS_STORE, { keyPath: 'pubkey' })
      }
    }
    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      resolve(db)
    }
  })
}

async function transaction (db, mode, work) {
  const tx = db.transaction([CHANNELS_STORE], mode)
  // Attach completion handlers before issuing the first request. The callback
  // may await only requests belonging to this transaction.
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
}

function cloneChannels (channels) {
  return structuredClone(channels || {})
}

export async function createChannelStateStore ({ prefix, indexedDB = globalThis.indexedDB } = {}) {
  if (!prefix) throw new Error('PRIVATE_MESSENGER_STATE_PREFIX_REQUIRED')
  const db = await openDatabase(indexedDB, `${prefix}:state:idb`)
  let closed = false
  let activeTransactions = 0
  let closePromise = null
  const closeWaiters = new Set()

  async function runTransaction (mode, work) {
    if (closed) throw new Error('PRIVATE_MESSENGER_STATE_CLOSED')
    activeTransactions++
    try {
      return await transaction(db, mode, work)
    } finally {
      activeTransactions--
      if (!activeTransactions) {
        for (const resolve of closeWaiters) resolve()
        closeWaiters.clear()
      }
    }
  }

  async function load () {
    return runTransaction('readonly', async tx => {
      const records = (await run('getAll', [], CHANNELS_STORE, null, { tx })).result
      return Object.fromEntries(records
        .filter(record => typeof record?.pubkey === 'string' && record.pubkey)
        .map(({ pubkey, value }) => [pubkey, value && typeof value === 'object' ? value : {}]))
    })
  }

  async function replace (channels) {
    const snapshot = cloneChannels(channels)
    await runTransaction('readwrite', async tx => {
      await run('clear', [], CHANNELS_STORE, null, { tx })
      for (const [pubkey, value] of Object.entries(snapshot)) {
        await run('put', [{ pubkey, value }], CHANNELS_STORE, null, { tx })
      }
    })
  }

  function close () {
    if (closePromise) return closePromise
    closed = true
    closePromise = (activeTransactions
      ? new Promise(resolve => closeWaiters.add(resolve))
      : Promise.resolve())
      .then(() => { db.close() })
    return closePromise
  }

  return { load, replace, close }
}
