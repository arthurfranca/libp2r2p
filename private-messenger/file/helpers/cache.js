import { chunkStream } from './stream.js'
import { ValidationError } from '../../../error/index.js'
import { decodeIrfsChunk, IRFS_CHUNK_BYTES } from '../../../irfs/index.js'

export const DEFAULT_FILE_CACHE_BYTES = 256 * 1024 * 1024
const request = value => new Promise((resolve, reject) => { value.onsuccess = () => resolve(value.result); value.onerror = () => reject(value.error) })
const done = tx => new Promise((resolve, reject) => { tx.oncomplete = resolve; tx.onabort = () => reject(tx.error || new Error('FILE_CACHE_ABORTED')); tx.onerror = () => {} })

// Payload budget is useful decoded bytes, not the browser's physical disk usage.
// Files and chunk mutations commit together; durable leases fence other tabs.
export function createFileCache ({ prefix, indexedDB = globalThis.indexedDB, maxBytes = DEFAULT_FILE_CACHE_BYTES, now = Date.now }) {
  if (!indexedDB?.open || !prefix || !Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new ValidationError('INVALID_FILE_CACHE_OPTIONS')
  let closed = false
  const tokens = new Map()
  const waiters = new Set()
  const dbPromise = new Promise((resolve, reject) => {
    const opening = indexedDB.open(`${prefix}:file-chunks:idb`, 1)
    opening.onerror = () => reject(opening.error)
    opening.onupgradeneeded = () => {
      opening.result.createObjectStore('files', { keyPath: 'root' })
      opening.result.createObjectStore('chunks', { keyPath: ['root', 'index'] }).createIndex('root', 'root')
    }
    opening.onsuccess = () => { opening.result.onversionchange = () => opening.result.close(); resolve(opening.result) }
  })
  const notify = () => { for (const wake of waiters) wake() }
  const assertOpen = () => { if (closed) throw new Error('FILE_CACHE_CLOSED') }
  async function transaction (work) {
    assertOpen()
    const db = await dbPromise
    assertOpen()
    const tx = db.transaction(['files', 'chunks'], 'readwrite')
    const completion = done(tx)
    try { const result = await work(tx.objectStore('files'), tx.objectStore('chunks')); await completion; return result } catch (error) { try { tx.abort() } catch {} await completion.catch(() => {}); throw error }
  }
  async function removeRoot (files, chunks, root) {
    await new Promise((resolve, reject) => {
      const cursor = chunks.index('root').openCursor(root)
      cursor.onerror = () => reject(cursor.error)
      cursor.onsuccess = () => { if (!cursor.result) return resolve(); cursor.result.delete(); cursor.result.continue() }
    })
    await request(files.delete(root))
  }
  const live = file => Object.fromEntries(Object.entries(file.leases || {}).filter(([, lease]) => lease.until > now()))
  const cost = file => Math.max(file.used, ...Object.values(file.leases || {}).map(v => v.size))
  async function attempt (root, size, token) {
    return transaction(async (files, chunks) => {
      const all = await request(files.getAll())
      for (const file of all) file.leases = live(file)
      let file = all.find(f => f.root === root)
      if (!file) { file = { root, used: 0, createdAt: now(), leases: {} }; all.push(file) }
      file.leases[token] = { size, until: now() + 120000 }
      let total = all.reduce((sum, f) => sum + cost(f), 0)
      const victims = all.filter(f => f.root !== root && !Object.keys(f.leases).length).sort((a, b) => a.createdAt - b.createdAt || a.root.localeCompare(b.root))
      if (total - victims.reduce((sum, f) => sum + cost(f), 0) > maxBytes) return false
      for (const victim of victims) {
        if (total <= maxBytes) break
        await removeRoot(files, chunks, victim.root); total -= cost(victim)
      }
      await request(files.put(file)); return true
    })
  }
  async function wait (signal) {
    signal?.throwIfAborted(); assertOpen()
    await new Promise((resolve, reject) => {
      const finish = error => { clearTimeout(timer); waiters.delete(wake); signal?.removeEventListener('abort', abort); error ? reject(error) : resolve() }
      const wake = () => finish(), abort = () => finish(signal.reason)
      const timer = setTimeout(wake, 250)
      waiters.add(wake); signal?.addEventListener('abort', abort, { once: true })
    })
  }
  async function reserve (root, size, { signal } = {}) {
    if (!/^[0-9a-f]{64}$/.test(root)) throw new ValidationError('INVALID_FILE_ROOT')
    size ??= maxBytes
    if (!Number.isSafeInteger(size) || size < 1) throw new ValidationError('INVALID_FILE_SIZE')
    if (size > maxBytes) throw Object.assign(new Error('FILE_EXCEEDS_CACHE_CAPACITY'), { code: 'FILE_EXCEEDS_CACHE_CAPACITY', maxBytes })
    const token = crypto.randomUUID()
    while (true) {
      signal?.throwIfAborted(); assertOpen()
      if (await attempt(root, size, token)) break
      await wait(signal)
    }
    tokens.set(token, root)
    let released = false
    return async () => {
      if (released) return
      released = true; tokens.delete(token)
      if (!closed) {
        await transaction(async files => {
          const file = await request(files.get(root)); if (!file) return
          delete file.leases[token]; await request(files.put(file))
        })
      }
      notify()
    }
  }
  const timer = setInterval(() => {
    if (!tokens.size || closed) return
    transaction(async files => {
      for (const [token, root] of tokens) {
        const file = await request(files.get(root))
        if (file?.leases[token]) { file.leases[token].until = now() + 120000; await request(files.put(file)) }
      }
    }).catch(() => {})
  }, 30000)
  timer.unref?.()
  const api = {
    maxBytes, reserve,
    async read (root, index) {
      assertOpen(); const db = await dbPromise
      const tx = db.transaction('chunks', 'readonly')
      const row = await request(tx.objectStore('chunks').get([root, index]))
      return row?.event
    },
    async save (event, descriptor) {
      const decoded = decodeIrfsChunk(event)
      if (decoded.root !== descriptor.root) throw new ValidationError('FILE_CHUNK_DESCRIPTOR_MISMATCH')
      if (descriptor.size !== undefined && (decoded.total !== Math.ceil(descriptor.size / IRFS_CHUNK_BYTES) || (decoded.index === decoded.total - 1 && decoded.contentBytes.length !== descriptor.size - decoded.index * IRFS_CHUNK_BYTES))) throw new ValidationError('FILE_CHUNK_DESCRIPTOR_MISMATCH')
      const actualSize = decoded.index === decoded.total - 1 ? decoded.index * IRFS_CHUNK_BYTES + decoded.contentBytes.length : undefined
      if (actualSize > maxBytes || decoded.index * IRFS_CHUNK_BYTES + decoded.contentBytes.length > maxBytes) throw Object.assign(new Error('FILE_EXCEEDS_CACHE_CAPACITY'), { code: 'FILE_EXCEEDS_CACHE_CAPACITY' })
      const release = await reserve(decoded.root, descriptor.size ?? actualSize)
      try {
        while (true) {
          try {
            await transaction(async (files, chunks) => {
              const file = await request(files.get(decoded.root))
              const prior = await request(chunks.get([decoded.root, decoded.index]))
              file.used += decoded.contentBytes.length - (prior?.bytes || 0)
              await request(chunks.put({ root: decoded.root, index: decoded.index, bytes: decoded.contentBytes.length, event: structuredClone(event) }))
              await request(files.put(file))
            })
            break
          } catch (error) {
            if (error?.name !== 'QuotaExceededError') throw error
            const removed = await transaction(async (files, chunks) => {
              const candidates = (await request(files.getAll())).filter(f => f.root !== decoded.root && !Object.keys(live(f)).length).sort((a, b) => a.createdAt - b.createdAt)
              if (!candidates.length) return false
              await removeRoot(files, chunks, candidates[0].root)
              return true
            })
            if (!removed) throw error
          }
        }
      } finally { await release() }
    },
    async stream (descriptor, { signal } = {}) {
      const release = await reserve(descriptor.root, descriptor.size, { signal })
      return chunkStream({ read: api.read, root: descriptor.root, size: descriptor.size, signal, release })
    },
    async close () {
      if (closed) return
      clearInterval(timer)
      await transaction(async files => {
        for (const [token, root] of tokens) { const file = await request(files.get(root)); if (file) { delete file.leases[token]; await request(files.put(file)) } }
      })
      closed = true; tokens.clear(); notify(); (await dbPromise).close()
    }
  }
  return api
}
