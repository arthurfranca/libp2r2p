import { ValidationError } from '../error/index.js'

const READ_METHODS = new Set([
  'count',
  'get',
  'getAll',
  'getAllKeys',
  'getKey',
  'openCursor',
  'openKeyCursor'
])

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

// A small wrapper around one IndexedDB request.
// It leaves database creation and schema ownership to the caller.
// Reuse `p` when advancing a cursor with `cursor.continue()`.
export async function run (method, args = [], storeName, indexName, {
  db,
  p = deferred(),
  tx,
  txMode = tx?.mode,
  storeOrIndex
} = {}) {
  if (!tx) {
    if (!db) throw new ValidationError('IDB_DATABASE_REQUIRED')
    if (!storeName) throw new ValidationError('IDB_STORE_REQUIRED')
    // Caller may pre-select it if it wants to use many different methods in a row
    txMode ??= READ_METHODS.has(method) ? 'readonly' : 'readwrite'
    tx = db.transaction([storeName], txMode)
  }

  if (!storeOrIndex) {
    if (!storeName) throw new ValidationError('IDB_STORE_REQUIRED')
    const store = tx.objectStore(storeName)
    storeOrIndex = indexName ? store.index(indexName) : store
  }

  let request
  try {
    request = storeOrIndex[method](...args)
  } catch (err) {
    p.reject(err)
    try { tx.abort() } catch {}
    return p.promise
  }

  request.onsuccess = () => {
    // don't add p to this object
    p.resolve({ result: request.result, tx, storeOrIndex })
  }
  request.onerror = () => {
    p.reject(request.error || new Error('IDB_REQUEST_FAILED'))
    try { tx.abort() } catch {}
  }

  return p.promise
}
