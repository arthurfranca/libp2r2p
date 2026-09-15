import { test } from 'node:test'
import assert from 'node:assert/strict'
import { prepareIrfsFile, decodeIrfsChunk } from '../irfs/index.js'

test('IRFS reconstructs boundary sizes and repeats proofs without retaining byte copies', async () => {
  for (const size of [1, 51000, 51001, 102000, 51000 * 7 + 23]) {
    const input = Uint8Array.from({ length: size }, (_, i) => i % 251)
    const file = await prepareIrfsFile(new Blob([input]))
    const chunks = await Array.fromAsync(file.chunks({ created_at: 1 }))
    const decoded = chunks.map(decodeIrfsChunk)
    assert.ok(decoded.every(chunk => chunk.root === file.root && chunk.total === chunks.length))
    assert.deepEqual(new Uint8Array(await new Blob(decoded.map(c => c.contentBytes)).arrayBuffer()), input)
    assert.deepEqual(await Array.fromAsync(file.chunks({ created_at: 1 })), chunks)
    file.close()
    await assert.rejects(Array.fromAsync(file.chunks()), /closed/)
  }
})

test('IRFS validates input, aborts preparation/iteration and releases resources', async () => {
  await assert.rejects(prepareIrfsFile(new Blob()), { code: 'EMPTY_IRFS_FILE' })
  await assert.rejects(prepareIrfsFile('bytes'), { code: 'INVALID_IRFS_FILE' })
  const controller = new AbortController()
  await assert.rejects(prepareIrfsFile(new Blob([new Uint8Array(102000)]), {
    signal: controller.signal, onProgress: () => controller.abort()
  }), { name: 'AbortError' })
  const file = await prepareIrfsFile(new Uint8Array(51001))
  const read = new AbortController()
  const iterator = file.chunks({ signal: read.signal })
  const { value } = await iterator.next()
  read.abort()
  await assert.rejects(iterator.next(), { name: 'AbortError' })
  assert.throws(() => decodeIrfsChunk({ ...value, content: '' }), { code: 'INVALID_IRFS_CHUNK' })
  file.close()
})
