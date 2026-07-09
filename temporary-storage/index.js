export const TEMPORARY_STORAGE_KEYS_KEY = 'libp2r2p:temporary-storage:keys'

function storage () {
  return globalThis.localStorage
}

function normalizeKeys (keys) {
  if (!Array.isArray(keys)) return []

  const out = []
  const seen = new Set()
  for (const key of keys) {
    if (typeof key !== 'string' || !key || key === TEMPORARY_STORAGE_KEYS_KEY || seen.has(key)) continue
    seen.add(key)
    out.push(key)
  }
  return out
}

function readTrackedKeys () {
  try {
    return normalizeKeys(JSON.parse(storage().getItem(TEMPORARY_STORAGE_KEYS_KEY) || '[]'))
  } catch {
    return []
  }
}

function writeTrackedKeys (keys) {
  const normalized = normalizeKeys(keys)
  if (normalized.length) storage().setItem(TEMPORARY_STORAGE_KEYS_KEY, JSON.stringify(normalized))
  else storage().removeItem(TEMPORARY_STORAGE_KEYS_KEY)
}

function trackTemporaryKey (key) {
  const tracked = readTrackedKeys()
  if (tracked.includes(key)) return
  writeTrackedKeys(tracked.concat(key))
}

function untrackTemporaryKeys (keys) {
  const remove = new Set(normalizeKeys(keys))
  if (!remove.size) return
  writeTrackedKeys(readTrackedKeys().filter(key => !remove.has(key)))
}

export function cleanupTemporaryStorage () {
  for (const key of readTrackedKeys()) storage().removeItem(key)
  storage().removeItem(TEMPORARY_STORAGE_KEYS_KEY)
}

export function getTemporaryItem (key) {
  return storage().getItem(key)
}

export function setTemporaryItem (key, value) {
  if (typeof key !== 'string' || !key || key === TEMPORARY_STORAGE_KEYS_KEY) throw new Error('INVALID_TEMPORARY_STORAGE_KEY')
  trackTemporaryKey(key)
  storage().setItem(key, value)
}

export function removeTemporaryItems (keys) {
  const normalized = normalizeKeys(keys)
  for (const key of normalized) storage().removeItem(key)
  untrackTemporaryKeys(normalized)
}
