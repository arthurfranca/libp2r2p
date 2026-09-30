import { ValidationError } from '../../../error/index.js'
import { createQueue } from '../../../idb-queue/index.js'

const localLocks = new WeakMap()
const keyFor = row => `${row.controlChannelPubkey}:${row.fileChannelPubkey}:${row.receiverPubkey}`

// A grant contains no payload, key material or retention reference in the file
// store. Serialize read/merge/write with other coordinators for this identity.
export function createFileCatalog ({ messenger, now, storage }) {
  if (storage) {
    const valid = row => {
      const parent = messenger.channels.get(row.controlChannelPubkey)
      return parent?.mode === 'seeder' && row.expiresAt > now() && row.sharedAt + messenger.offlineRecoverySecondsFor(parent) > now()
    }
    return {
      ready: Promise.resolve(),
      async authorize (file, receivers, sharedAt) {
        const { controlChannelPubkey, fileChannelPubkey, peerPubkey, root, size } = file
        for (const receiverPubkey of receivers) {
          const row = { controlChannelPubkey, fileChannelPubkey, peerPubkey, root, size, receiverPubkey, sharedAt, expiresAt: sharedAt + messenger.offlineRecoverySecondsFor(controlChannelPubkey) }
          if (valid(row)) await storage.put(row)
        }
      },
      async find (controlChannelPubkey, fileChannelPubkey, receiverPubkey) {
        const row = await storage.find({ controlChannelPubkey, fileChannelPubkey, receiverPubkey })
        return row && valid(row) ? row : null
      },
      prune: () => storage.prune({ now: now() }),
      async close () {}
    }
  }
  const prefix = `${messenger.prefix}:file-authorizations`
  let database
  const db = () => (database ??= createQueue({ prefix, indexedDB: messenger._indexedDB, evictionPolicy: 'reject', indexes: { key: { keyPath: 'key', unique: true }, channel: 'fileChannelPubkey' } }))
  function lock (work) {
    if (globalThis.navigator?.locks?.request) return navigator.locks.request(prefix, work)
    let locks = localLocks.get(messenger._indexedDB)
    if (!locks) localLocks.set(messenger._indexedDB, (locks = new Map()))
    const task = (locks.get(prefix) || Promise.resolve()).catch(() => {}).then(work)
    locks.set(prefix, task.catch(() => {}))
    return task
  }
  function isValid (row) {
    const parent = messenger.channels.get(row.controlChannelPubkey)
    return parent?.mode === 'seeder' && row.expiresAt > now() && row.sharedAt + messenger.offlineRecoverySecondsFor(parent) > now()
  }
  async function put (file, receiverPubkey, sharedAt, expiresAt) {
    const { controlChannelPubkey, fileChannelPubkey, peerPubkey, root, size } = file
    const row = { controlChannelPubkey, fileChannelPubkey, peerPubkey, root, size, receiverPubkey, sharedAt, expiresAt }
    row.key = keyFor(row)
    if (!isValid(row)) return
    const queue = await db()
    const prior = await queue.getBy('key', row.key)
    if (prior && (prior.root !== root || prior.peerPubkey !== peerPubkey)) throw new ValidationError('FILE_AUTHORIZATION_CONFLICT')
    // Replaying an older share cannot shorten or renew a newer grant.
    if (prior && prior.sharedAt >= sharedAt && isValid(prior)) return
    await queue.putBy('key', row)
  }
  async function pruneUnlocked () {
    await (await db()).removeWhere(row => !isValid(row))
  }
  const ready = lock(pruneUnlocked)

  return {
    ready,
    async authorize (file, receivers, sharedAt) {
      await ready
      return lock(async () => {
        await pruneUnlocked()
        const expiresAt = sharedAt + messenger.offlineRecoverySecondsFor(file.controlChannelPubkey)
        for (const receiver of receivers) await put(file, receiver, sharedAt, expiresAt)
      })
    },
    async find (controlChannelPubkey, fileChannelPubkey, receiverPubkey) {
      await ready
      return lock(async () => {
        const queue = await db()
        const key = keyFor({ controlChannelPubkey, fileChannelPubkey, receiverPubkey })
        const row = await queue.getBy('key', key)
        if (row && !isValid(row)) { await queue.removeBy('key', key); return null }
        return row
      })
    },
    async prune () { await ready; return lock(pruneUnlocked) },
    async close () { await ready.catch(() => {}); await lock(async () => { if (database) await (await database.catch(() => null))?.close() }) }
  }
}
