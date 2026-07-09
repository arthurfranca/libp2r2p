const encoder = new TextEncoder()
const ITEM_WRAPPER = 'web-storage-queue:item:v1'
const DEFAULT_EVICTION_SLACK_RATIO = 0.1
const MAX_EVICTION_SLACK_BYTES = 64 * 1024 // 64 KiB

export function createQueue ({
  prefix,
  storageArea = globalThis.localStorage,
  maxBytes,
  evictionPolicy = 'opposite-end' // 'opposite-end' = push evicts from head, unshift evicts from tail
} = {}) {
  const stateKey = `${prefix}:queue`
  const operationKey = `${prefix}:queue:operation`
  const itemPrefix = `${prefix}:queue:item:`
  const waiters = new Set()
  const configuredMaxBytes = Number.isSafeInteger(maxBytes) && maxBytes > 0 ? maxBytes : Infinity
  const configuredEvictionPolicy = normalizeEvictionPolicy(evictionPolicy)
  let sessionMaxBytes = configuredMaxBytes

  function storage () {
    return storageArea
  }

  function itemKey (id) {
    return `${itemPrefix}${id}`
  }

  function normalizeState (state) {
    const head = Number.isSafeInteger(state.head) ? state.head : 0
    const tail = Number.isSafeInteger(state.tail) && state.tail >= head ? state.tail : head
    const usedBytes = Number.isSafeInteger(state.usedBytes) && state.usedBytes >= 0 ? state.usedBytes : 0
    return { head, tail, usedBytes }
  }

  function byteLength (value) {
    return encoder.encode(String(value)).length
  }

  function hasByteLimit () {
    return Number.isFinite(sessionMaxBytes)
  }

  function normalizeEvictionPolicy (policy) {
    if (policy === 'opposite-end' || policy === undefined || policy === null) return 'opposite-end'
    if (policy === 'fifo' || policy === 'head') return 'head'
    if (policy === 'lifo' || policy === 'tail') return 'tail'
    throw new Error('QUEUE_INVALID_EVICTION_POLICY')
  }

  function evictionDirectionFor (operation, { index = 0, length = 0 } = {}) {
    if (configuredEvictionPolicy === 'head') return 'head'
    if (configuredEvictionPolicy === 'tail') return 'tail'
    if (operation === 'unshift') return 'tail'
    if (operation === 'setAt' || operation === 'insertAt') {
      return index <= length / 2 ? 'tail' : 'head'
    }
    return 'head'
  }

  function evictionSlackBytes () {
    if (!hasByteLimit()) return 0
    return Math.min(Math.max(1, Math.floor(sessionMaxBytes * DEFAULT_EVICTION_SLACK_RATIO)), MAX_EVICTION_SLACK_BYTES)
  }

  function targetBytesAfterWrite (requiredBytes) {
    if (!hasByteLimit()) return Infinity
    return Math.max(requiredBytes, sessionMaxBytes - evictionSlackBytes())
  }

  function isQuotaExceeded (err) {
    return err?.name === 'QuotaExceededError' ||
      err?.name === 'NS_ERROR_DOM_QUOTA_REACHED' ||
      err?.code === 22 ||
      err?.code === 1014 ||
      /quota/i.test(err?.message || '')
  }

  function lowerSessionMaxBytes (requiredBytes) {
    if (!hasByteLimit()) return
    const next = Math.max(requiredBytes, Math.floor(sessionMaxBytes * 0.8))
    if (next < sessionMaxBytes) sessionMaxBytes = next
  }

  function recoverHead (state) {
    let recovered = state
    while (storage().getItem(itemKey(recovered.head - 1))) {
      recovered = { ...recovered, head: recovered.head - 1 }
    }
    while (recovered.head < recovered.tail && !storage().getItem(itemKey(recovered.head))) {
      recovered = { ...recovered, head: recovered.head + 1 }
    }
    if (recovered.head !== state.head || recovered.tail !== state.tail) writeState(recovered)
    return recovered
  }

  function recoverTail (state) {
    let recovered = state
    while (storage().getItem(itemKey(recovered.tail))) {
      recovered = { ...recovered, tail: recovered.tail + 1 }
    }
    while (recovered.tail > recovered.head && !storage().getItem(itemKey(recovered.tail - 1))) {
      recovered = { ...recovered, tail: recovered.tail - 1 }
    }
    if (recovered.head !== state.head || recovered.tail !== state.tail) writeState(recovered)
    return recovered
  }

  function readStoredItemFromRaw (raw) {
    if (!raw) return null
    try {
      const parsed = JSON.parse(raw)
      if (parsed?.__type === ITEM_WRAPPER && parsed.item && Number.isSafeInteger(parsed.byteSize)) {
        return { item: parsed.item, byteSize: parsed.byteSize }
      }
      return { item: parsed, byteSize: byteLength(raw) }
    } catch {
      return { item: null, byteSize: byteLength(raw) }
    }
  }

  function readStoredItem (id) {
    return readStoredItemFromRaw(storage().getItem(itemKey(id)))
  }

  function recoverUsage (state) {
    let usedBytes = 0
    for (let id = state.head; id < state.tail; id++) {
      const stored = readStoredItem(id)
      if (stored) usedBytes += stored.byteSize
    }
    const recovered = { ...state, usedBytes }
    if (usedBytes !== state.usedBytes) writeState(recovered)
    return recovered
  }

  function protectedIdsFrom (options) {
    if (!options?.protectedIds) return new Set()
    if (options.protectedIds instanceof Set) return options.protectedIds
    return new Set(options.protectedIds)
  }

  function trimHead (state, protectedIds = new Set()) {
    while (
      state.head < state.tail &&
      !protectedIds.has(state.head) &&
      !storage().getItem(itemKey(state.head))
    ) {
      state.head++
    }
  }

  function trimTail (state, protectedIds = new Set()) {
    while (
      state.tail > state.head &&
      !protectedIds.has(state.tail - 1) &&
      !storage().getItem(itemKey(state.tail - 1))
    ) {
      state.tail--
    }
  }

  function evictOneFromHead (state, options = {}) {
    const protectedIds = protectedIdsFrom(options)
    trimHead(state, protectedIds)
    for (let id = state.head; id < state.tail; id++) {
      if (protectedIds.has(id)) continue
      const key = itemKey(id)
      const stored = readStoredItem(id)
      if (!stored) continue
      storage().removeItem(key)
      state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
      if (id === state.head) trimHead(state, protectedIds)
      writeState(state)
      return true
    }
    return false
  }

  function evictOneFromTail (state, options = {}) {
    const protectedIds = protectedIdsFrom(options)
    trimTail(state, protectedIds)
    for (let id = state.tail - 1; id >= state.head; id--) {
      if (protectedIds.has(id)) continue
      const key = itemKey(id)
      const stored = readStoredItem(id)
      if (!stored) continue
      storage().removeItem(key)
      state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
      if (id === state.tail - 1) trimTail(state, protectedIds)
      writeState(state)
      return true
    }
    return false
  }

  function evictToBytes (state, targetBytes, options = {}) {
    if (!hasByteLimit()) return state
    const { direction = 'head' } = options
    const evictOne = direction === 'tail' ? evictOneFromTail : evictOneFromHead
    while (state.usedBytes > targetBytes) {
      if (!evictOne(state, options)) break
    }
    return state
  }

  function evictToFit (state, requiredBytes, options = {}) {
    if (!hasByteLimit()) return state
    const { direction = 'head' } = options
    if (requiredBytes > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    const targetBytes = targetBytesAfterWrite(requiredBytes)
    while (state.usedBytes + requiredBytes > targetBytes) {
      const evicted = direction === 'tail'
        ? evictOneFromTail(state, options)
        : evictOneFromHead(state, options)
      if (!evicted) break
    }
    if (state.usedBytes + requiredBytes > sessionMaxBytes) throw new Error('QUEUE_CAPACITY_EXCEEDED')
    return state
  }

  function recoverByteLimit (state) {
    if (!hasByteLimit()) return state
    return evictToBytes(state, Math.min(sessionMaxBytes, targetBytesAfterWrite(0)), { direction: evictionDirectionFor('recover') })
  }

  function readState () {
    let parsed = {}
    try {
      parsed = JSON.parse(storage().getItem(stateKey) || '{}')
    } catch {
      parsed = {}
    }
    return recoverByteLimit(recoverUsage(recoverTail(recoverHead(recoverOperation(normalizeState(parsed))))))
  }

  function writeState (state, { keepEmpty = false } = {}) {
    if (!keepEmpty && state.head >= state.tail) storage().removeItem(stateKey)
    else storage().setItem(stateKey, JSON.stringify(state))
  }

  function lengthFromState (state) {
    return Math.max(0, state.tail - state.head)
  }

  function assertIndex (index, length, { allowEnd = false } = {}) {
    const max = allowEnd ? length : length - 1
    if (!Number.isSafeInteger(index) || index < 0 || index > max) throw new Error('QUEUE_INDEX_OUT_OF_RANGE')
  }

  function itemForStorage (id, item) {
    const storedItem = { id, ...item }
    let byteSize = 0
    let raw = ''
    while (true) {
      raw = JSON.stringify({ __type: ITEM_WRAPPER, byteSize, item: storedItem })
      const nextByteSize = byteLength(raw)
      if (nextByteSize === byteSize) break
      byteSize = nextByteSize
    }
    return { raw, byteSize, item: storedItem }
  }

  function setItemRaw (key, raw, state, requiredBytes, options) {
    try {
      storage().setItem(key, raw)
    } catch (err) {
      if (!isQuotaExceeded(err) || !hasByteLimit()) throw err
      lowerSessionMaxBytes(requiredBytes)
      if (options.evict === false) throw err
      evictToFit(state, requiredBytes, options)
      storage().setItem(key, raw)
    }
  }

  function writeItem (id, item, state, options = {}) {
    const previous = readStoredItem(id)
    const stored = itemForStorage(id, item)
    const previousByteSize = previous?.byteSize || 0
    const delta = stored.byteSize - previousByteSize
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    if (delta > 0) {
      if (options.evict === false) {
        if (hasByteLimit() && state.usedBytes + delta > sessionMaxBytes) throw new Error('QUEUE_CAPACITY_EXCEEDED')
      } else {
        evictToFit(state, delta, options)
      }
    }
    setItemRaw(itemKey(id), stored.raw, state, Math.max(delta, stored.byteSize), options)
    state.usedBytes = Math.max(0, state.usedBytes - previousByteSize + stored.byteSize)
    writeState(state, { keepEmpty: true })
  }

  function moveItem (from, to) {
    const raw = storage().getItem(itemKey(from))
    if (!raw) return
    storage().setItem(itemKey(to), raw)
    storage().removeItem(itemKey(from))
  }

  function readItem (id) {
    return readStoredItem(id)?.item || null
  }

  function readOperation () {
    try {
      return JSON.parse(storage().getItem(operationKey) || 'null')
    } catch {
      storage().removeItem(operationKey)
      return null
    }
  }

  function writeOperation (operation) {
    storage().setItem(operationKey, JSON.stringify(operation))
  }

  function clearOperation () {
    storage().removeItem(operationKey)
  }

  function normalizedOperation (operation) {
    if (!operation || (operation.type !== 'insert' && operation.type !== 'remove')) return null
    if (!Number.isSafeInteger(operation.head) || !Number.isSafeInteger(operation.tail) || operation.tail < operation.head) return null
    if (!Number.isSafeInteger(operation.slot) || !Number.isSafeInteger(operation.cursor)) return null
    return operation
  }

  function recoverOperation (state) {
    const operation = normalizedOperation(readOperation())
    if (!operation) {
      clearOperation()
      return state
    }

    if (operation.type === 'insert') return finishInsert(operation)
    return finishRemove(operation)
  }

  function finishInsert (operation) {
    let current = operation
    const state = { head: current.head, tail: current.tail, usedBytes: current.usedBytes || 0 }
    writeState(state)

    while (current.cursor > current.slot) {
      moveItem(current.cursor - 1, current.cursor)
      current = { ...current, cursor: current.cursor - 1 }
      writeOperation(current)
    }

    writeItem(current.slot, current.item, state, { evict: false })
    clearOperation()
    writeState(state)
    return state
  }

  function finishRemove (operation) {
    let current = operation
    const state = { head: current.head, tail: current.tail, usedBytes: current.usedBytes || 0 }
    writeState(state, { keepEmpty: true })

    while (current.cursor < current.tail) {
      moveItem(current.cursor + 1, current.cursor)
      current = { ...current, cursor: current.cursor + 1 }
      writeOperation(current)
    }

    storage().removeItem(itemKey(current.tail))
    clearOperation()
    writeState(state)
    return state
  }

  function wake () {
    for (const resolve of waiters) resolve()
    waiters.clear()
  }

  function push (item) {
    const state = readState()
    const direction = evictionDirectionFor('push')
    let id = state.tail
    let stored = itemForStorage(id, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    if (direction === 'tail') {
      evictToFit(state, stored.byteSize, { direction })
      id = state.tail
      stored = itemForStorage(id, item)
      if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    }
    state.tail = id + 1
    writeState(state)
    writeItem(id, item, state, {
      direction,
      protectedIds: new Set([id])
    })
    wake()
    return lengthFromState(state)
  }

  function unshift (item) {
    const state = readState()
    const direction = evictionDirectionFor('unshift')
    let id = state.head - 1
    let stored = itemForStorage(id, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    if (direction === 'head') {
      evictToFit(state, stored.byteSize, { direction })
      id = state.head - 1
      stored = itemForStorage(id, item)
      if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    }
    state.head = id
    writeState(state)
    writeItem(id, item, state, {
      direction,
      protectedIds: new Set([id])
    })
    wake()
    return lengthFromState(state)
  }

  function shift () {
    const state = readState()
    while (state.head < state.tail) {
      const key = itemKey(state.head)
      const stored = readStoredItem(state.head)
      state.head++
      writeState(state, { keepEmpty: true })
      storage().removeItem(key)
      if (stored) state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
      writeState(state)
      if (!stored?.item) continue
      return stored.item
    }
    return null
  }

  function pop () {
    const state = readState()
    while (state.tail > state.head) {
      const id = state.tail - 1
      const key = itemKey(id)
      const stored = readStoredItem(id)
      state.tail--
      writeState(state, { keepEmpty: true })
      storage().removeItem(key)
      if (stored) state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
      writeState(state)
      if (!stored?.item) continue
      return stored.item
    }
    return null
  }

  function setAt (index, item) {
    const state = readState()
    const length = lengthFromState(state)
    assertIndex(index, length)
    const id = state.head + index
    writeItem(id, item, state, {
      direction: evictionDirectionFor('setAt', { index, length }),
      protectedIds: new Set([id])
    })
    wake()
    return index
  }

  function insertAt (index, item) {
    const state = readState()
    const length = lengthFromState(state)
    assertIndex(index, length, { allowEnd: true })

    let slot = state.head + index
    let stored = itemForStorage(slot, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    evictToFit(state, stored.byteSize, { direction: evictionDirectionFor('insertAt', { index, length }) })
    const nextLength = lengthFromState(state)
    const nextIndex = Math.min(index, nextLength)
    slot = state.head + nextIndex
    stored = itemForStorage(slot, item)
    if (hasByteLimit() && stored.byteSize > sessionMaxBytes) throw new Error('QUEUE_ITEM_TOO_LARGE')
    evictToFit(state, stored.byteSize, { direction: evictionDirectionFor('insertAt', { index: nextIndex, length: nextLength }) })
    state.tail++
    writeState(state)
    writeOperation({
      type: 'insert',
      head: state.head,
      tail: state.tail,
      usedBytes: state.usedBytes,
      slot,
      cursor: state.tail - 1,
      item
    })
    finishInsert(readOperation())
    wake()
    return nextIndex
  }

  function insertWhere (predicate, item, { appendIfMissing = false } = {}) {
    if (typeof predicate !== 'function') throw new Error('QUEUE_PREDICATE_REQUIRED')
    const state = readState()
    const length = lengthFromState(state)

    for (let index = 0; index < length; index++) {
      const value = readItem(state.head + index)
      if (value && predicate(value, index)) return insertAt(index, item)
    }

    if (appendIfMissing) return insertAt(length, item)
    return null
  }

  function removeAt (index) {
    const state = readState()
    const length = lengthFromState(state)
    assertIndex(index, length)

    const slot = state.head + index
    const stored = readStoredItem(slot)
    state.tail--
    if (stored) state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
    writeState(state, { keepEmpty: true })
    writeOperation({
      type: 'remove',
      head: state.head,
      tail: state.tail,
      usedBytes: state.usedBytes,
      slot,
      cursor: slot
    })
    finishRemove(readOperation())
    wake()

    return stored?.item || null
  }

  async function * items () {
    while (true) {
      const item = shift()
      if (item) {
        yield item
      } else {
        await new Promise(resolve => waiters.add(resolve))
      }
    }
  }

  async function * reverseItems () {
    while (true) {
      const item = pop()
      if (item) {
        yield item
      } else {
        await new Promise(resolve => waiters.add(resolve))
      }
    }
  }

  async function * storedItems () {
    const state = readState()
    for (let id = state.head; id < state.tail; id++) {
      const item = readItem(id)
      if (item) yield item
    }
  }

  async function * reverseStoredItems () {
    const state = readState()
    for (let id = state.tail - 1; id >= state.head; id--) {
      const item = readItem(id)
      if (item) yield item
    }
  }

  function removeWhere (predicate) {
    const state = readState()
    // Bulk predicate removal leaves holes on purpose; shift/pop skip them, and
    // callers that need contiguous positions can use removeAt for compaction.
    for (let id = state.head; id < state.tail; id++) {
      const key = itemKey(id)
      const stored = readStoredItem(id)
      if (!stored) continue
      try {
        if (predicate(stored.item)) {
          storage().removeItem(key)
          state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
        }
      } catch {
        storage().removeItem(key)
        state.usedBytes = Math.max(0, state.usedBytes - stored.byteSize)
      }
    }
    writeState(state)
  }

  function some (predicate) {
    if (typeof predicate !== 'function') throw new Error('QUEUE_PREDICATE_REQUIRED')
    const state = readState()
    for (let id = state.head; id < state.tail; id++) {
      const item = readItem(id)
      if (item && predicate(item)) return true
    }
    return false
  }

  function clear () {
    const state = readState()
    for (let id = state.head; id < state.tail; id++) storage().removeItem(itemKey(id))
    storage().removeItem(stateKey)
    storage().removeItem(operationKey)
  }

  readState()

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
    clear
  }
}
