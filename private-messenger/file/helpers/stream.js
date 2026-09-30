import { decodeIrfsChunk } from '../../../irfs/index.js'
import { ValidationError } from '../../../error/index.js'

export function chunkStream ({ read, root, size, signal, release = async () => {} }) {
  let index = 0, total, bytes = 0, settled = false, controller
  const finish = async () => {
    if (settled) return
    settled = true; signal?.removeEventListener('abort', abort)
    await release()
  }
  const abort = () => { if (settled) return; controller.error(signal.reason); finish().catch(() => {}) }
  return new ReadableStream({
    start (value) { controller = value; signal?.addEventListener('abort', abort, { once: true }); if (signal?.aborted) abort() },
    async pull () {
      try {
        signal?.throwIfAborted()
        const event = await read(root, index)
        signal?.throwIfAborted()
        if (settled) return
        if (!event) throw new Error('FILE_UNAVAILABLE')
        const chunk = decodeIrfsChunk(event)
        if (chunk.root !== root || chunk.index !== index || (total !== undefined && total !== chunk.total)) throw new ValidationError('FILE_CHUNK_DESCRIPTOR_MISMATCH')
        total = chunk.total; index++; bytes += chunk.contentBytes.length
        if (index === total && size !== undefined && bytes !== size) throw new ValidationError('FILE_SIZE_MISMATCH')
        controller.enqueue(chunk.contentBytes)
        if (index === total) { await finish(); controller.close() }
      } catch (error) { if (!settled) { await finish(); controller.error(error) } }
    },
    cancel: finish
  }, { highWaterMark: 1 })
}
