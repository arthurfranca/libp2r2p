import { ValidationError } from '../error/index.js'

export const TEMPORARY_STORAGE_KEYS_KEY = 'libp2r2p:temporary-storage:keys'

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

export function createTemporaryStorage ({ storageArea = globalThis.sessionStorage } = {}) {
  function storage () {
    return storageArea
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

  function cleanup () {
    for (const key of readTrackedKeys()) storage().removeItem(key)
    storage().removeItem(TEMPORARY_STORAGE_KEYS_KEY)
  }

  function getItem (key) {
    return storage().getItem(key)
  }

  function setItem (key, value) {
    if (typeof key !== 'string' || !key || key === TEMPORARY_STORAGE_KEYS_KEY) throw new ValidationError('INVALID_TEMPORARY_STORAGE_KEY')
    trackTemporaryKey(key)
    storage().setItem(key, value)
  }

  function removeItems (keys) {
    const normalized = normalizeKeys(keys)
    for (const key of normalized) storage().removeItem(key)
    untrackTemporaryKeys(normalized)
  }

  return { cleanup, getItem, setItem, removeItems }
}

export function cleanupTemporaryStorage ({ storageArea = globalThis.sessionStorage } = {}) {
  createTemporaryStorage({ storageArea }).cleanup()
}

export function getTemporaryItem (key, { storageArea = globalThis.sessionStorage } = {}) {
  return createTemporaryStorage({ storageArea }).getItem(key)
}

export function setTemporaryItem (key, value, { storageArea = globalThis.sessionStorage } = {}) {
  createTemporaryStorage({ storageArea }).setItem(key, value)
}

export function removeTemporaryItems (keys, { storageArea = globalThis.sessionStorage } = {}) {
  createTemporaryStorage({ storageArea }).removeItems(keys)
}
