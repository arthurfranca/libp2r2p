import { deliveryInfo } from '../../private-channel/helpers/rumor.js'

const encoder = new TextEncoder()
const indexName = 'byChannelTypeEventId'
const oldPath = ['item.channelPubkey', 'item.type', 'item.event.id']
const newPath = [...oldPath, 'item.provenance']
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b)

function open (indexedDB, name, version, upgrade) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, version)
    let rejected = false
    request.onblocked = () => { rejected = true; reject(new Error('IDB_DATABASE_BLOCKED')) }
    request.onerror = () => reject(request.error)
    request.onupgradeneeded = () => {
      if (rejected) { request.transaction.abort(); return }
      try { upgrade?.(request.result, request.transaction) } catch (error) {
        request.transaction.abort(); reject(error)
      }
    }
    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      if (rejected) db.close()
      else resolve(db)
    }
  })
}

function matchesLegacy (items) {
  if (!items.indexNames.contains(indexName)) return false
  const index = items.index(indexName)
  return equal(items.keyPath, 'position') && equal(index.keyPath, oldPath) && index.unique && !index.multiEntry &&
    items.indexNames.contains('byChannel') && items.index('byChannel').keyPath === 'item.channelPubkey' &&
    !items.index('byChannel').unique && !items.index('byChannel').multiEntry
}

// The generic queue remains strict. Only this historical messenger schema is
// migrated, atomically with its records and capacity accounting.
export async function migrateMessageQueue (indexedDB, prefix) {
  if (!indexedDB?.open) throw new Error('IDB_UNAVAILABLE')
  const name = `${prefix}:idb-queue`
  for (let attempt = 0; attempt < 3; attempt++) {
    const db = await open(indexedDB, name)
    if (!db.objectStoreNames.contains('items') || !db.objectStoreNames.contains('state')) { db.close(); return }
    const tx = db.transaction(['items', 'state'], 'readonly')
    const legacy = matchesLegacy(tx.objectStore('items')) && tx.objectStore('state').keyPath === 'key'
    const version = db.version + 1
    await new Promise((resolve, reject) => { tx.oncomplete = resolve; tx.onabort = () => reject(tx.error) })
    db.close()
    if (!legacy) return
    try {
      const upgraded = await open(indexedDB, name, version, (_db, tx) => {
        const items = tx.objectStore('items')
        if (!matchesLegacy(items)) return
        items.deleteIndex(indexName)
        items.createIndex(indexName, newPath, { unique: true })
        let delta = 0
        const cursor = items.openCursor()
        cursor.onsuccess = () => {
          try {
            const row = cursor.result
            if (!row) {
              const state = tx.objectStore('state')
              const request = state.get('queue')
              request.onsuccess = () => {
                if (request.result) state.put({ ...request.result, usedBytes: request.result.usedBytes + delta })
              }
              return
            }
            const record = row.value
            const item = { ...record.item }
            const sender = item.senderPubkey || item.meta?.senderPubkey || item.meta?.router?.tags?.find(tag => tag[0] === 'f')?.[1] || ''
            const info = deliveryInfo(item.event, sender)
            item.senderPubkey = sender
            item.provenance = item.provenance === 'hearsay' ? 'hearsay' : info.provenance
            let byteSize = 0
            while (true) {
              const size = encoder.encode(JSON.stringify({ byteSize, item })).length
              if (size === byteSize) break
              byteSize = size
            }
            delta += byteSize - record.byteSize
            row.update({ ...record, item, byteSize })
            row.continue()
          } catch { tx.abort() }
        }
      })
      upgraded.close()
      return
    } catch (error) {
      if (error?.name !== 'VersionError' || attempt === 2) throw error
    }
  }
}
