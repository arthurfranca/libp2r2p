import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory } from 'fake-indexeddb'
import { createFileCache } from 'libp2r2p/private-messenger/file'
import { prepareIrfsFile } from 'libp2r2p/irfs'
async function file (n, value) {
  const prepared = await prepareIrfsFile(new Uint8Array(n).fill(value))
  const chunks = []; for await (const chunk of prepared.chunks()) chunks.push(chunk)
  return { root: prepared.root, size: n, chunks, close: () => prepared.close() }
}
test('cache evicts inactive roots, deduplicates and preserves chunks across restart', async () => {
  const indexedDB = new IDBFactory(), options = { prefix: 'cache-test', indexedDB, maxBytes: 100 }
  let cache = createFileCache(options)
  const a = await file(60, 1), b = await file(60, 2)
  await cache.save(a.chunks[0], a); await cache.save(a.chunks[0], a)
  await cache.save(b.chunks[0], b)
  assert.equal(await cache.read(a.root, 0), undefined)
  assert.ok(await cache.read(b.root, 0))
  await cache.close(); cache = createFileCache(options)
  assert.ok(await cache.read(b.root, 0))
  await assert.rejects(cache.reserve(a.root, 101), { code: 'FILE_EXCEEDS_CACHE_CAPACITY' })
  await cache.close(); a.close(); b.close()
})
test('concurrent caches wait for active reservations and cancellation releases waiters', async () => {
  const indexedDB = new IDBFactory(), options = { prefix: 'lease-test', indexedDB, maxBytes: 100 }
  const a = createFileCache(options), b = createFileCache(options)
  const f = await file(60, 3), g = await file(60, 4)
  const release = await a.reserve(f.root, 60)
  await a.save(f.chunks[0], f)
  const abort = new AbortController()
  const waiting = b.reserve(g.root, 60, { signal: abort.signal })
  abort.abort(); await assert.rejects(waiting, { name: 'AbortError' })
  let completed = false
  const pending = b.save(g.chunks[0], g).then(() => { completed = true })
  await new Promise(resolve => setTimeout(resolve, 10)); assert.equal(completed, false)
  await release(); await pending
  assert.equal(await a.read(f.root, 0), undefined)
  const stream = await b.stream(g), reader = stream.getReader()
  assert.deepEqual((await reader.read()).value, new Uint8Array(60).fill(4))
  assert.equal((await reader.read()).done, true)
  await a.close(); await b.close(); f.close(); g.close()
})

test('aborting an idle reader releases its durable file reservation', async () => {
  const cache = createFileCache({ prefix: 'stream-abort', indexedDB: new IDBFactory(), maxBytes: 102001 })
  const a = await file(102001, 5), b = await file(102001, 6)
  for (const chunk of a.chunks) await cache.save(chunk, a)
  const signal = new AbortController()
  const stream = await cache.stream(a, { signal: signal.signal })
  const reader = stream.getReader()
  await reader.read()
  signal.abort()
  await assert.rejects(reader.read(), { name: 'AbortError' })
  const release = await cache.reserve(b.root, b.size)
  await release(); await cache.close(); a.close(); b.close()
})
