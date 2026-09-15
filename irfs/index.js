import NMMR, { InMemoryMMR } from 'nmmr'
import { encode, decode } from '../base93/index.js'
import { bytesToBase16 } from '../base16/index.js'
import { ValidationError } from '../error/index.js'

export const IRFS_CHUNK_BYTES = 51000
export const IRFS_CHUNK_KIND = 34601

// Blob/File input is immutable and seekable: retain it instead of copying its
// bytes to temporary IndexedDB. Only tree hashes and leaf positions stay in RAM.
export async function prepareIrfsFile (input, { signal, onProgress } = {}) {
  let blob = input instanceof Uint8Array ? new Blob([input]) : input
  if (!(blob instanceof Blob)) throw new ValidationError('INVALID_IRFS_FILE')
  if (!blob.size) throw new ValidationError('EMPTY_IRFS_FILE')
  let tree = new InMemoryMMR()
  let positions = []
  const size = blob.size
  const total = Math.ceil(size / IRFS_CHUNK_BYTES)
  let closed = false
  const check = currentSignal => {
    signal?.throwIfAborted()
    currentSignal?.throwIfAborted()
    if (closed) throw new Error('IRFS preparation closed')
  }
  const close = () => {
    closed = true
    blob = tree = positions = null
    signal?.removeEventListener('abort', close)
  }
  signal?.addEventListener('abort', close, { once: true })
  try {
    for (let index = 0; index < total; index++) {
      check()
      const bytes = new Uint8Array(await blob.slice(index * IRFS_CHUNK_BYTES, (index + 1) * IRFS_CHUNK_BYTES).arrayBuffer())
      check()
      positions.push(Number(tree.append(bytes).leafIdx))
      onProgress?.({ completed: Math.min(size, (index + 1) * IRFS_CHUNK_BYTES), total: size })
      // Yield to input and cancellation even when a Blob read resolves immediately.
      if (index % 32 === 31) await new Promise(resolve => setTimeout(resolve, 0))
    }
    const root = bytesToBase16(tree.bagThePeaks())
    return {
      root, size, total, close,
      async * chunks ({ created_at: createdAt = Math.floor(Date.now() / 1000), signal: readSignal } = {}) {
        if (!Number.isSafeInteger(createdAt) || createdAt < 0) throw new ValidationError('INVALID_IRFS_TIMESTAMP')
        for (let index = 0; index < total; index++) {
          check(readSignal)
          const bytes = new Uint8Array(await blob.slice(index * IRFS_CHUNK_BYTES, (index + 1) * IRFS_CHUNK_BYTES).arrayBuffer())
          check(readSignal)
          const hashes = tree.getProofArray(positions[index])
          const proof = new Uint8Array(hashes.length * 32)
          hashes.forEach((hash, offset) => proof.set(hash, offset * 32))
          yield { kind: IRFS_CHUNK_KIND, created_at: createdAt, tags: [['d', NMMR.deriveChunkId(root, index)], ['mmr', String(index), String(total), encode(proof)]], content: encode(bytes) }
        }
      }
    }
  } catch (error) { close(); throw error }
}

export function decodeIrfsChunk (event) {
  try {
    if (event?.kind !== IRFS_CHUNK_KIND || !Array.isArray(event.tags)) throw new Error('Invalid kind or tags')
    const d = event.tags.filter(tag => tag[0] === 'd')
    const mmr = event.tags.filter(tag => tag[0] === 'mmr')
    if (d.length !== 1 || d[0].length !== 2 || mmr.length !== 1 || mmr[0].length !== 4) throw new Error('Invalid chunk tags')
    const [, index, total, encodedProof] = mmr[0]
    const contentBytes = decode(event.content)
    const proof = decode(encodedProof)
    const root = NMMR.calculateRoot({ contentBytes, index, total, proof })
    if (!contentBytes.length || contentBytes.length > IRFS_CHUNK_BYTES || (Number(index) < Number(total) - 1 && contentBytes.length !== IRFS_CHUNK_BYTES)) throw new Error('Invalid chunk length')
    if (NMMR.deriveChunkId(root, index) !== d[0][1]) throw new Error('Invalid chunk ID')
    return { root, index: Number(index), total: Number(total), contentBytes, proof }
  } catch (cause) { throw new ValidationError('INVALID_IRFS_CHUNK', { cause }) }
}
