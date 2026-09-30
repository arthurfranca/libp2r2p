import { createQueue } from '../../../idb-queue/index.js'

// Only ciphertext and opaque event IDs are persisted here. Chunks are committed
// to the configured file store before an entry is accepted.
export async function createChatOutbox ({ owner, signer, indexedDB = globalThis.indexedDB, namespace = 'outbox', prefix = 'libp2r2p:private-messenger:session' }) {
  const queue = await createQueue({
    prefix: `${prefix}:${namespace}:${owner}`,
    indexes: { byId: { keyPath: 'id', unique: true } },
    evictionPolicy: 'reject',
    indexedDB
  })
  return {
    async put (entry, { existing = false } = {}) {
      const bytes = new TextEncoder().encode(JSON.stringify(entry)).buffer
      const ciphertext = await signer.nip44v3.encrypt(owner, 9, `${prefix}:${namespace}`, bytes)
      return queue.putBy('byId', { id: entry.id, ciphertext }, { existingOnly: existing })
    },
    async list () {
      const entries = []
      for await (const row of queue.storedItemsBy('byId')) {
        const bytes = await signer.nip44v3.decrypt(owner, 9, `${prefix}:${namespace}`, row.ciphertext)
        const entry = JSON.parse(new TextDecoder().decode(bytes))
        if (entry.id !== row.id) throw new Error('INVALID_OUTBOX_ENTRY')
        entries.push(entry)
      }
      return entries
    },
    has: id => queue.someBy('byId', id),
    remove: id => queue.removeBy('byId', id),
    close: () => queue.close()
  }
}
