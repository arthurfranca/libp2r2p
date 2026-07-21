import { run } from '../idb/index.js'

const encoder = new TextEncoder()
const ITEMS_STORE = 'items'
const STATE_STORE = 'state'
const STATE_KEY = 'queue'
const DEFAULT_EVICTION_HEADROOM_RATIO = 0.1
const MAX_EVICTION_HEADROOM_BYTES = 64 * 1024 // 64 KiB

/*
IndexedDB schema, scoped per queue prefix:

database `${prefix}:idb-queue`, version managed additively

items, keyPath "position"
  position  integer ordering key between the queue head and tail
  byteSize  logical serialized size used by the queue capacity limit
  item      caller-provided queued value

items indexes
  Declared by each createQueue() caller. A requested keyPath such as "status"
  is stored as "item.status"; compound paths receive the same "item." prefix.
  The database version is incremented when declared indexes need to be added.

state, keyPath "key"
  key        state record name, currently "queue"
  head/tail  half-open range containing allocated item positions
  usedBytes  sum of the queued items' logical byte sizes

The queue state record is absent while the queue is empty.
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
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error || new Error('IDB_TRANSACTION_ABORTED'))
    tx.onerror = () => reject(tx.error || new Error('IDB_TRANSACTION_FAILED'))
  })
}

function byteLength (value) {
  return encoder.encode(String(value)).length
}

function normalizeEvictionPolicy (policy) {
  if (policy === 'opposite-end' || policy === undefined || policy === null) return 'opposite-end'
  if (policy === 'fifo' || policy === 'head') return 'head'
  if (policy === 'lifo' || policy === 'tail') return 'tail'
  throw new Error('QUEUE_INVALID_EVICTION_POLICY')
}

function normalizeState (value) {
  const head = Number.isSafeInteger(value?.head) ? value.head : 0
  const tail = Number.isSafeInteger(value?.tail) && value.tail >= head ? value.tail : head
  const usedBytes = Number.isSafeInteger(value?.usedBytes) && value.usedBytes >= 0 ? value.usedBytes : 0
  return { head, tail, usedBytes }
}

function normalizeIndexes (indexes = {}) {
  if (!indexes || typeof indexes !== 'object' || Array.isArray(indexes)) throw new Error('QUEUE_INDEXES_INVALID')

  return Object.entries(indexes).map(([name, definition]) => {
    const options = typeof definition === 'string' || Array.isArray(definition)
      ? { keyPath: definition }
      : definition
    const keyPath = options?.keyPath
    if (!name || (!Array.isArray(keyPath) && typeof keyPath !== 'string')) throw new Error('QUEUE_INDEX_INVALID')
    if (Array.isArray(keyPath) && keyPath.some(path => typeof path !== 'string' || !path)) throw new Error('QUEUE_INDEX_INVALID')
    if (typeof keyPath === 'string' && !keyPath) throw new Error('QUEUE_INDEX_INVALID')
    if (options.multiEntry && Array.isArray(keyPath)) throw new Error('QUEUE_INDEX_MULTI_ENTRY_COMPOUND')
    return {
      name,
      keyPath,
      storedKeyPath: Array.isArray(keyPath)
        ? keyPath.map(path => `item.${path}`)
        : `item.${keyPath}`,
      unique: Boolean(options.unique),
      multiEntry: Boolean(options.multiEntry)
    }
  })
}

function keyPathEqual (a, b) {
  return JSON.stringify(a) === JSON.stringify(b)
}

function openDatabase (indexedDB, name, version, onUpgrade) {
  return new Promise((resolve, reject) => {
    let request
    try {
      request = version === undefined ? indexedDB.open(name) : indexedDB.open(name, version)
    } catch (err) {
      reject(err)
      return
    }
    request.onerror = () => reject(request.error || new Error('IDB_OPEN_FAILED'))
    request.onblocked = () => reject(new Error('IDB_DATABASE_BLOCKED'))
    request.onupgradeneeded = event => {
      try {
        onUpgrade(request.result, event.target.transaction)
      } catch (err) {
        try { event.target.transaction.abort() } catch {}
        reject(err)
      }
    }
    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      resolve(db)
    }
  })
}

function ensureSchema (db, tx, indexDefinitions) {
  // Upgrades are additive: create missing stores/indexes, but reject a changed
  // definition instead of silently reinterpreting existing queued values.
  let items
  if (!db.objectStoreNames.contains(ITEMS_STORE)) {
    items = db.createObjectStore(ITEMS_STORE, { keyPath: 'position' })
  } else {
    items = tx.objectStore(ITEMS_STORE)
    if (!keyPathEqual(items.keyPath, 'position')) throw new Error('QUEUE_SCHEMA_MISMATCH')
  }

  if (!db.objectStoreNames.contains(STATE_STORE)) {
    db.createObjectStore(STATE_STORE, { keyPath: 'key' })
  } else if (!keyPathEqual(tx.objectStore(STATE_STORE).keyPath, 'key')) {
    throw new Error('QUEUE_SCHEMA_MISMATCH')
  }

  for (const definition of indexDefinitions) {
    if (!items.indexNames.contains(definition.name)) {
      items.createIndex(definition.name, definition.storedKeyPath, {
        unique: definition.unique,
        multiEntry: definition.multiEntry
      })
      continue
    }
    const existing = items.index(definition.name)
    if (
      !keyPathEqual(existing.keyPath, definition.storedKeyPath) ||
      existing.unique !== definition.unique ||
      existing.multiEntry !== definition.multiEntry
    ) {
      throw new Error('QUEUE_INDEX_SCHEMA_MISMATCH')
    }
  }
}

async function inspectSchema (db, indexDefinitions) {
  if (!db.objectStoreNames.contains(ITEMS_STORE) || !db.objectStoreNames.contains(STATE_STORE)) {
    return { missing: true, incompatible: false }
  }
  const tx = db.transaction([ITEMS_STORE, STATE_STORE], 'readonly')
  const done = transactionDone(tx)
  const items = tx.objectStore(ITEMS_STORE)
  const state = tx.objectStore(STATE_STORE)
  if (!keyPathEqual(items.keyPath, 'position') || !keyPathEqual(state.keyPath, 'key')) {
    await done
    return { missing: false, incompatible: true }
  }

  let missing = false
  for (const definition of indexDefinitions) {
    if (!items.indexNames.contains(definition.name)) {
      missing = true
      continue
    }
    const existing = items.index(definition.name)
    if (
      !keyPathEqual(existing.keyPath, definition.storedKeyPath) ||
      existing.unique !== definition.unique ||
      existing.multiEntry !== definition.multiEntry
    ) {
      await done
      return { missing: false, incompatible: true }
    }
  }
  await done
  return { missing, incompatible: false }
}

async function openQueueDatabase (indexedDB, prefix, indexDefinitions) {
  if (!indexedDB?.open) throw new Error('IDB_UNAVAILABLE')
  // A queue owns its database so independently declared indexes do not need a
  // shared application-wide schema or migration coordinator.
  const name = `${prefix}:idb-queue`

  for (let attempt = 0; attempt < 3; attempt++) {
    const db = await openDatabase(indexedDB, name, undefined, (nextDb, tx) => ensureSchema(nextDb, tx, indexDefinitions))
    const schema = await inspectSchema(db, indexDefinitions)
    if (schema.incompatible) {
      db.close()
      throw new Error('QUEUE_INDEX_SCHEMA_MISMATCH')
    }
    if (!schema.missing) return db

    const version = db.version + 1
    db.close()
    try {
      const upgraded = await openDatabase(indexedDB, name, version, (nextDb, tx) => ensureSchema(nextDb, tx, indexDefinitions))
      const upgradedSchema = await inspectSchema(upgraded, indexDefinitions)
      if (upgradedSchema.incompatible || upgradedSchema.missing) {
        upgraded.close()
        throw new Error(upgradedSchema.incompatible ? 'QUEUE_INDEX_SCHEMA_MISMATCH' : 'QUEUE_SCHEMA_UPGRADE_FAILED')
      }
      return upgraded
    } catch (err) {
      if (err?.name !== 'VersionError' || attempt === 2) throw err
    }
  }

  throw new Error('QUEUE_SCHEMA_UPGRADE_FAILED')
}

function itemForStorage (position, item) {
  // IndexedDB does not expose portable per-record byte usage, so keep a stable
  // JSON byte estimate for the queue's logical capacity limit.
  // `position` is the ordering key; queued values remain caller payloads.
  const storedItem = { ...item }
  let byteSize = 0
  let serialized = ''
  while (true) {
    serialized = JSON.stringify({ byteSize, item: storedItem })
    const nextByteSize = byteLength(serialized)
    if (nextByteSize === byteSize) break
    byteSize = nextByteSize
  }
  return { position, byteSize, item: storedItem }
}

function assertIndex (index, length, { allowEnd = false } = {}) {
  const max = allowEnd ? length : length - 1
  if (!Number.isSafeInteger(index) || index < 0 || index > max) throw new Error('QUEUE_INDEX_OUT_OF_RANGE')
}

function validDirection (direction) {
  if (direction === undefined || direction === 'next') return 'next'
  if (direction === 'prev') return 'prev'
  throw new Error('QUEUE_INVALID_DIRECTION')
}

function isQuotaExceeded (err) {
  return err?.name === 'QuotaExceededError' ||
    err?.name === 'NS_ERROR_DOM_QUOTA_REACHED' ||
    err?.code === 22 ||
    err?.code === 1014 ||
    /quota/i.test(err?.message || '')
}

export async function createQueue ({
  prefix,
  indexes = {},
  maxBytes,
  evictionPolicy = 'opposite-end',
  indexedDB = globalThis.indexedDB
} = {}) {
  if (!prefix) throw new Error('QUEUE_PREFIX_REQUIRED')

  const indexDefinitions = normalizeIndexes(indexes)
  const db = await openQueueDatabase(indexedDB, prefix, indexDefinitions)
  const configuredMaxBytes = Number.isSafeInteger(maxBytes) && maxBytes > 0 ? maxBytes : Infinity
  const configuredEvictionPolicy = normalizeEvictionPolicy(evictionPolicy)
  const waiters = new Set()
  let sessionMaxBytes = configuredMaxBytes
  let revision = 0

  function hasByteLimit () {
    return Number.isFinite(sessionMaxBytes)
  }

  function evictionDirectionFor (operation, { index = 0, length = 0 } = {}) {
    if (configuredEvictionPolicy === 'head') return 'head'
    if (configuredEvictionPolicy === 'tail') return 'tail'
    if (operation === 'unshift') return 'tail'
    if (operation === 'setAt' || operation === 'insertAt') return index <= length / 2 ? 'tail' : 'head'
    return 'head'
  }

  function evictionHeadroomBytes () {
    if (!hasByteLimit()) return 0
    // Evict a little below the limit so the next write has free capacity even
    // when its storage overhead is slightly larger than the estimate.
    return Math.min(Math.max(1, Math.floor(sessionMaxBytes * DEFAULT_EVICTION_HEADROOM_RATIO)), MAX_EVICTION_HEADROOM_BYTES)
  }

  function targetBytesAfterWrite (requiredBytes) {
    if (!hasByteLimit()) return Infinity
    return Math.max(requiredBytes, sessionMaxBytes - evictionHeadroomBytes())
  }

  function lowerSessionMaxBytes (requiredBytes) {
    if (!hasByteLimit()) return
    const next = Math.max(requiredBytes, Math.floor(sessionMaxBytes * 0.8))
    if (next < sessionMaxBytes) sessionMaxBytes = next
  }

  function wake () {
    revision++
    for (const resolve of waiters) resolve()
    waiters.clear()
  }

  async function waitForChange (knownRevision) {
    if (revision !== knownRevision) return
    await new Promise(resolve => waiters.add(resolve))
  }

  async function transaction (mode, work) {
    const tx = db.transaction([ITEMS_STORE, STATE_STORE], mode)
    const done = transactionDone(tx)
    try {
      // Keep work limited to IndexedDB requests so the transaction stays active.
      const value = await work(tx)
      await done
      return value
    } catch (err) {
      try { tx.abort() } catch {}
      try { await done } catch {}
      throw err
    }
  }

  async function readState (tx) {
    const { result } = await run('get', [STATE_KEY], STATE_STORE, null, { tx })
    return normalizeState(result)
  }

  async function writeState (tx, state) {
    if (state.head >= state.tail) {
      await run('delete', [STATE_KEY], STATE_STORE, null, { tx })
      return
    }
    await run('put', [{ key: STATE_KEY, ...state }], STATE_STORE, null, { tx })
  }

  async function allRecords (tx) {
    const { result } = await run('getAll', [], ITEMS_STORE, null, { tx })
    return result.sort((a, b) => a.position - b.position)
  }

  async function recordsForState (tx, state) {
    // `removeWhere` may leave holes, so queue order is the persisted position
    // range rather than the number of stored records.
    const records = await allRecords(tx)
    return records.filter(record => record.position >= state.head && record.position < state.tail)
  }

  function applyBounds (state, records) {
    if (!records.length) {
      state.head = 0
      state.tail = 0
      state.usedBytes = 0
      return
    }
    state.head = records[0].position
    state.tail = records[records.length - 1].position + 1
    state.usedBytes = records.reduce((total, record) => total + (record.byteSize || 0), 0)
  }

  function extendBounds (state, position) {
    if (state.head >= state.tail) {
      state.head = position
      state.tail = position + 1
      return
    }
    state.head = Math.min(state.head, position)
    state.tail = Math.max(state.tail, position + 1)
  }

  async function getRecord (tx, position) {
    return run('get', [position], ITEMS_STORE, null, { tx }).then(value => value.result || null)
  }

  async function putRecord (tx, record) {
    await run('put', [record], ITEMS_STORE, null, { tx })
  }

  async function deleteRecord (tx, position) {
    await run('delete', [position], ITEMS_STORE, null, { tx })
  }

  async function nextCursor (cursor, p) {
    Object.assign(p, deferred())
    cursor.continue()
    return (await p.promise).result
  }

  async function storedRecordAtEnd (tx, state, direction, excludedPositions = new Set()) {
    // Queue positions are the object-store primary key, so an end cursor finds
    // an eviction candidate without reading every queued record into memory.
    const p = deferred()
    const args = direction === 'tail' ? [undefined, 'prev'] : []
    let cursor = (await run('openCursor', args, ITEMS_STORE, null, { tx, p })).result
    while (cursor) {
      const record = cursor.value
      if (record.position < state.head) {
        if (direction === 'tail') return null
      } else if (record.position >= state.tail) {
        if (direction === 'head') return null
      } else if (!excludedPositions.has(record.position)) {
        return record
      }
      cursor = await nextCursor(cursor, p)
    }
    return null
  }

  async function trimBounds (tx, state, protectedPositions = new Set()) {
    // Holes are valid after predicate removal. Find only the stored endpoints,
    // while preserving a protected slot that a pending positional write needs.
    const headRecord = await storedRecordAtEnd(tx, state, 'head')
    const tailRecord = await storedRecordAtEnd(tx, state, 'tail')
    let head = headRecord?.position ?? Infinity
    let tail = tailRecord?.position ?? -Infinity
    for (const position of protectedPositions) {
      if (!Number.isSafeInteger(position) || position < state.head || position >= state.tail) continue
      head = Math.min(head, position)
      tail = Math.max(tail, position)
    }
    if (head === Infinity) {
      state.head = 0
      state.tail = 0
      state.usedBytes = 0
      return
    }
    state.head = head
    state.tail = tail + 1
  }

  async function evictOne (tx, state, { direction, protectedPositions = new Set() } = {}) {
    // Remove one actual record from the selected end, skipping holes and the
    // slot currently being replaced or inserted.
    const record = await storedRecordAtEnd(tx, state, direction, protectedPositions)
    if (!record) return false
    await deleteRecord(tx, record.position)
    state.usedBytes = Math.max(0, state.usedBytes - (record.byteSize || 0))
    await trimBounds(tx, state, protectedPositions)
    return true
  }

  async function evictToFit (tx, state, requiredBytes, options = {}) {
    if (!hasByteLimit()) return
    if (requiredBytes > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    // Make room for the write and retain eviction headroom for the next one.
    const targetBytes = targetBytesAfterWrite(requiredBytes)
    while (state.usedBytes + requiredBytes > targetBytes) {
      if (!await evictOne(tx, state, options)) break
    }
    if (state.usedBytes + requiredBytes > sessionMaxBytes) throw new Error('QUEUE_CAPACITY_EXCEEDED')
  }

  async function evictToBytes (tx, state, targetBytes, options = {}) {
    if (!hasByteLimit()) return
    while (state.usedBytes > targetBytes) {
      if (!await evictOne(tx, state, options)) break
    }
  }

  async function putItem (tx, state, position, item, options = {}) {
    const previous = await getRecord(tx, position)
    const stored = itemForStorage(position, item)
    const previousByteSize = previous?.byteSize || 0
    const delta = stored.byteSize - previousByteSize
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    if (delta > 0) await evictToFit(tx, state, delta, options)
    await putRecord(tx, stored)
    state.usedBytes = Math.max(0, state.usedBytes - previousByteSize + stored.byteSize)
    extendBounds(state, position)
    return stored
  }

  async function pushInTransaction (tx, state, item) {
    const direction = evictionDirectionFor('push')
    let position = state.tail
    let stored = itemForStorage(position, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    await evictToFit(tx, state, stored.byteSize, { direction })
    position = state.tail
    stored = itemForStorage(position, item)
    await putRecord(tx, stored)
    if (state.head >= state.tail) state.head = position
    state.tail = position + 1
    state.usedBytes += stored.byteSize
    return state.tail - state.head
  }

  async function unshiftInTransaction (tx, state, item) {
    const direction = evictionDirectionFor('unshift')
    let position = state.head - 1
    let stored = itemForStorage(position, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    await evictToFit(tx, state, stored.byteSize, { direction })
    position = state.head - 1
    stored = itemForStorage(position, item)
    await putRecord(tx, stored)
    if (state.head >= state.tail) state.tail = position + 1
    state.head = position
    state.usedBytes += stored.byteSize
    return state.tail - state.head
  }

  async function insertAtInTransaction (tx, state, index, item) {
    const length = state.tail - state.head
    assertIndex(index, length, { allowEnd: true })
    let slot = state.head + index
    let stored = itemForStorage(slot, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    await evictToFit(tx, state, stored.byteSize, { direction: evictionDirectionFor('insertAt', { index, length }) })

    const nextLength = state.tail - state.head
    const nextIndex = Math.min(index, nextLength)
    slot = state.head + nextIndex
    stored = itemForStorage(slot, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    await evictToFit(tx, state, stored.byteSize, { direction: evictionDirectionFor('insertAt', { index: nextIndex, length: nextLength }) })

    // Shift from the tail so each source record is copied before its slot is
    // reused by the preceding record.
    const records = await recordsForState(tx, state)
    for (const record of [...records].reverse()) {
      if (record.position < slot) continue
      await putRecord(tx, { ...record, position: record.position + 1 })
      await deleteRecord(tx, record.position)
    }
    if (state.head >= state.tail) state.head = slot
    state.tail++
    await putRecord(tx, stored)
    state.usedBytes += stored.byteSize
    return nextIndex
  }

  async function removeAtInTransaction (tx, state, index) {
    const length = state.tail - state.head
    assertIndex(index, length)
    const slot = state.head + index
    const removed = await getRecord(tx, slot)
    const records = await recordsForState(tx, state)
    // Delete first, then shift toward the head to close the logical gap.
    await deleteRecord(tx, slot)
    for (const record of records) {
      if (record.position <= slot) continue
      await putRecord(tx, { ...record, position: record.position - 1 })
      await deleteRecord(tx, record.position)
    }
    const oldTail = state.tail
    state.tail--
    await deleteRecord(tx, oldTail - 1)
    state.usedBytes = Math.max(0, state.usedBytes - (removed?.byteSize || 0))
    applyBounds(state, (await recordsForState(tx, { head: state.head, tail: state.tail, usedBytes: state.usedBytes })))
    return removed?.item || null
  }

  async function mutate (operation, { requiredBytes = 0, wakeWaiters = false } = {}) {
    let retried = false
    while (true) {
      try {
        const value = await transaction('readwrite', async tx => {
          const state = await readState(tx)
          const result = await operation(tx, state)
          await writeState(tx, state)
          return result
        })
        if (wakeWaiters) wake()
        return value
      } catch (err) {
        if (retried || !hasByteLimit() || !isQuotaExceeded(err)) throw err
        // Browser quota can be lower than the configured logical budget.
        // Retry once with a smaller in-memory budget after atomic rollback.
        lowerSessionMaxBytes(requiredBytes)
        retried = true
      }
    }
  }

  async function snapshot (select) {
    return transaction('readonly', async tx => select(tx))
  }

  async function push (item) {
    const requiredBytes = itemForStorage(0, item).byteSize
    return mutate((tx, state) => pushInTransaction(tx, state, item), { requiredBytes, wakeWaiters: true })
  }

  async function unshift (item) {
    const requiredBytes = itemForStorage(0, item).byteSize
    return mutate((tx, state) => unshiftInTransaction(tx, state, item), { requiredBytes, wakeWaiters: true })
  }

  async function shift () {
    return mutate(async (tx, state) => {
      while (state.head < state.tail) {
        const position = state.head
        state.head++
        const stored = await getRecord(tx, position)
        await deleteRecord(tx, position)
        if (!stored?.item) continue
        state.usedBytes = Math.max(0, state.usedBytes - (stored.byteSize || 0))
        return stored.item
      }
      state.usedBytes = 0
      return null
    })
  }

  async function pop () {
    return mutate(async (tx, state) => {
      while (state.tail > state.head) {
        const position = state.tail - 1
        state.tail--
        const stored = await getRecord(tx, position)
        await deleteRecord(tx, position)
        if (!stored?.item) continue
        state.usedBytes = Math.max(0, state.usedBytes - (stored.byteSize || 0))
        return stored.item
      }
      state.usedBytes = 0
      return null
    })
  }

  async function setAt (index, item) {
    const requiredBytes = itemForStorage(0, item).byteSize
    return mutate(async (tx, state) => {
      const length = state.tail - state.head
      assertIndex(index, length)
      const position = state.head + index
      await putItem(tx, state, position, item, {
        direction: evictionDirectionFor('setAt', { index, length }),
        protectedPositions: new Set([position])
      })
      return index
    }, { requiredBytes, wakeWaiters: true })
  }

  async function insertAt (index, item) {
    const requiredBytes = itemForStorage(0, item).byteSize
    return mutate((tx, state) => insertAtInTransaction(tx, state, index, item), { requiredBytes, wakeWaiters: true })
  }

  async function insertWhere (predicate, item, { appendIfMissing = false } = {}) {
    if (typeof predicate !== 'function') throw new Error('QUEUE_PREDICATE_REQUIRED')
    const requiredBytes = itemForStorage(0, item).byteSize
    return mutate(async (tx, state) => {
      const records = await recordsForState(tx, state)
      const byPosition = new Map(records.map(record => [record.position, record]))
      const length = state.tail - state.head
      for (let index = 0; index < length; index++) {
        const record = byPosition.get(state.head + index)
        if (record?.item && predicate(record.item, index)) return insertAtInTransaction(tx, state, index, item)
      }
      if (appendIfMissing) return insertAtInTransaction(tx, state, length, item)
      return null
    }, { requiredBytes, wakeWaiters: true })
  }

  async function removeAt (index) {
    return mutate((tx, state) => removeAtInTransaction(tx, state, index), { wakeWaiters: true })
  }

  async function removeWhere (predicate) {
    if (typeof predicate !== 'function') throw new Error('QUEUE_PREDICATE_REQUIRED')
    return mutate(async (tx, state) => {
      const records = await recordsForState(tx, state)
      const removed = new Set()
      for (const record of records) {
        let matches = false
        try { matches = Boolean(predicate(record.item)) } catch { matches = true }
        if (!matches) continue
        removed.add(record.position)
        await deleteRecord(tx, record.position)
      }
      if (removed.size) applyBounds(state, records.filter(record => !removed.has(record.position)))
    })
  }

  async function some (predicate) {
    if (typeof predicate !== 'function') throw new Error('QUEUE_PREDICATE_REQUIRED')
    return snapshot(async tx => {
      const state = await readState(tx)
      const records = await recordsForState(tx, state)
      return records.some(record => predicate(record.item))
    })
  }

  async function clear () {
    return mutate(async (tx, state) => {
      await run('clear', [], ITEMS_STORE, null, { tx })
      state.head = 0
      state.tail = 0
      state.usedBytes = 0
    })
  }

  async function getBy (indexName, query) {
    // Explicit index operations avoid scanning queue values in JavaScript.
    return snapshot(async tx => {
      const { result } = await run('get', [query], ITEMS_STORE, indexName, { tx })
      return result?.item || null
    })
  }

  async function someBy (indexName, query) {
    return snapshot(async tx => {
      const { result } = await run('getKey', [query], ITEMS_STORE, indexName, { tx })
      return result !== undefined
    })
  }

  async function removeBy (indexName, query) {
    // IndexedDB selects only matching records; deletion and state repair share
    // one transaction so an interruption cannot leave partial queue state.
    return mutate(async (tx, state) => {
      const { result } = await run('getAll', [query], ITEMS_STORE, indexName, { tx })
      for (const record of result) await deleteRecord(tx, record.position)
      if (result.length) {
        const removedBytes = result.reduce((total, record) => total + (record.byteSize || 0), 0)
        state.usedBytes = Math.max(0, state.usedBytes - removedBytes)
        await trimBounds(tx, state)
      }
      return result.map(record => record.item)
    })
  }

  async function snapshotStoredItems () {
    return snapshot(async tx => {
      const state = await readState(tx)
      return recordsForState(tx, state)
    })
  }

  async function * items () {
    while (true) {
      const knownRevision = revision
      const item = await shift()
      if (item) {
        yield item
      } else {
        await waitForChange(knownRevision)
      }
    }
  }

  async function * reverseItems () {
    while (true) {
      const knownRevision = revision
      const item = await pop()
      if (item) {
        yield item
      } else {
        await waitForChange(knownRevision)
      }
    }
  }

  async function * storedItems () {
    for (const record of await snapshotStoredItems()) yield record.item
  }

  async function * reverseStoredItems () {
    for (const record of (await snapshotStoredItems()).reverse()) yield record.item
  }

  async function * storedItemsBy (indexName, query, { direction = 'next' } = {}) {
    direction = validDirection(direction)
    const records = await snapshot(async tx => {
      const { result } = await run('getAll', [query], ITEMS_STORE, indexName, { tx })
      return result
    })
    if (direction === 'prev') records.reverse()
    for (const record of records) yield record.item
  }

  await mutate(async (tx, state) => {
    const records = await allRecords(tx)
    applyBounds(state, records)
    if (hasByteLimit()) {
      await evictToBytes(tx, state, Math.min(sessionMaxBytes, targetBytesAfterWrite(0)), {
        direction: evictionDirectionFor('recover')
      })
    }
  })

  return {
    enqueue: push,
    push,
    pop,
    unshift,
    shift,
    items,
    reverseItems,
    storedItems,
    reverseStoredItems,
    setAt,
    insertAt,
    insertWhere,
    removeAt,
    removeWhere,
    some,
    clear,
    getBy,
    someBy,
    removeBy,
    storedItemsBy
  }
}
