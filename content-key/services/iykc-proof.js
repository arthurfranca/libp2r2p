import { CONTENT_KEY_KIND, parseContentKeyEvent } from '../event/index.js'
import { pickRelaysForPubkeys, getRelaysByPubkey, relayPool } from '../../relay/index.js'

const QUERY_CACHE_MS = 40 * 60 * 1000
const NEGATIVE_CACHE_MS = 5 * 60 * 1000
const IYKC_CACHE_MAX_ITEMS = 10000
const HEX_PUBKEY = /^[0-9a-f]{64}$/i
const contentKeysByPubkey = Object.create(null)
const iykcCacheTimersByPubkey = Object.create(null)
const iykcCacheAddedAtByPubkey = Object.create(null)
const inFlightByPubkey = new Map()

const getEvents = (...args) => relayPool.getEvents(...args)

function hasCachedKey (cache, key) {
  return Object.prototype.hasOwnProperty.call(cache, key)
}

function maybeUnref (timer) {
  timer?.unref?.()
  return timer
}

function uniquePubkeys (pubkeys, { requireHex = false } = {}) {
  const values = [...new Set(pubkeys || [])].filter(Boolean)
  return requireHex ? values.filter(pubkey => HEX_PUBKEY.test(pubkey)) : values
}

function cloneContentKey (contentKey) {
  return contentKey
    ? {
        iykcPubkey: contentKey.iykcPubkey,
        iykcProof: contentKey.iykcProof
      }
    : null
}

function deleteCachedValue (cache, timers, addedAt, key) {
  clearTimeout(timers[key])
  delete cache[key]
  delete timers[key]
  delete addedAt[key]
}

function pruneCache (cache, timers, addedAt, maxItems) {
  const keys = Object.keys(cache)
  if (keys.length <= maxItems) return

  keys
    .sort((a, b) => (addedAt[a] || 0) - (addedAt[b] || 0))
    .slice(0, keys.length - maxItems)
    .forEach(key => deleteCachedValue(cache, timers, addedAt, key))
}

function setCachedValue (cache, timers, addedAt, key, value, cacheMs) {
  cache[key] = value
  addedAt[key] = Date.now()
  clearTimeout(timers[key])
  if (cacheMs > 0) {
    timers[key] = maybeUnref(setTimeout(() => {
      deleteCachedValue(cache, timers, addedAt, key)
    }, cacheMs))
  } else {
    delete timers[key]
  }
}

export function clearContentKeyCache () {
  for (const timer of Object.values(iykcCacheTimersByPubkey)) clearTimeout(timer)
  for (const key of Object.keys(contentKeysByPubkey)) delete contentKeysByPubkey[key]
  for (const key of Object.keys(iykcCacheTimersByPubkey)) delete iykcCacheTimersByPubkey[key]
  for (const key of Object.keys(iykcCacheAddedAtByPubkey)) delete iykcCacheAddedAtByPubkey[key]
  inFlightByPubkey.clear()
}

async function queryContentKeyProofs (pubkeyList, { _getEvents, _getRelaysByPubkey, cacheMs, negativeCacheMs }) {
  const relaysByPubkey = await _getRelaysByPubkey(pubkeyList, { _getEvents, cacheMs })
  const relayToAuthors = pickRelaysForPubkeys(pubkeyList, relaysByPubkey)
  const eventGroups = await Promise.all(
    [...relayToAuthors.entries()]
      .map(async ([relay, authors]) => {
        try {
          const { result } = await _getEvents({
            kinds: [CONTENT_KEY_KIND],
            authors,
            limit: authors.length
          }, [relay], {
            timeout: 5000,
            timeoutAfterFirstEose: null
          })
          return result.map(({ event }) => event)
        } catch {
          // Discovery is best-effort: a failing relay group must not fail an
          // otherwise publishable message.
          return []
        }
      })
  )

  const latestByPubkey = {}
  for (const event of eventGroups.flat()) {
    const parsed = parseContentKeyEvent(event)
    if (!parsed) continue
    if (!latestByPubkey[event.pubkey] || event.created_at > latestByPubkey[event.pubkey].created_at) {
      latestByPubkey[event.pubkey] = { created_at: event.created_at, ...parsed }
    }
  }

  const out = {}
  for (const pubkey of pubkeyList) {
    const entry = latestByPubkey[pubkey]
    const proof = entry
      ? { iykcPubkey: entry.iykcPubkey, iykcProof: entry.iykcProof }
      : null
    if (proof) {
      setCachedValue(contentKeysByPubkey, iykcCacheTimersByPubkey, iykcCacheAddedAtByPubkey, pubkey, cloneContentKey(proof), cacheMs)
      out[pubkey] = proof
    } else if (negativeCacheMs > 0) {
      // Misses expire quickly so a peer that publishes a key right after a
      // prefetch is picked up without waiting for the positive cache window.
      setCachedValue(contentKeysByPubkey, iykcCacheTimersByPubkey, iykcCacheAddedAtByPubkey, pubkey, null, negativeCacheMs)
    }
  }
  pruneCache(contentKeysByPubkey, iykcCacheTimersByPubkey, iykcCacheAddedAtByPubkey, IYKC_CACHE_MAX_ITEMS)
  return out
}

export async function getIykcProofs (pubkeys, {
  _getEvents = getEvents,
  _getRelaysByPubkey = getRelaysByPubkey,
  cacheMs = QUERY_CACHE_MS,
  negativeCacheMs = NEGATIVE_CACHE_MS
} = {}) {
  const pubkeyList = uniquePubkeys(pubkeys, { requireHex: _getEvents === getEvents })
  if (!pubkeyList.length) return {}

  const out = {}
  const waiting = []
  const toQuery = []
  for (const pubkey of pubkeyList) {
    if (hasCachedKey(contentKeysByPubkey, pubkey)) {
      const cached = cloneContentKey(contentKeysByPubkey[pubkey])
      if (cached) out[pubkey] = cached
      continue
    }
    const pending = inFlightByPubkey.get(pubkey)
    if (pending) waiting.push([pubkey, pending])
    else toQuery.push(pubkey)
  }

  if (toQuery.length) {
    const query = queryContentKeyProofs(toQuery, { _getEvents, _getRelaysByPubkey, cacheMs, negativeCacheMs })
    for (const pubkey of toQuery) inFlightByPubkey.set(pubkey, query)
    try {
      Object.assign(out, await query)
    } finally {
      for (const pubkey of toQuery) {
        if (inFlightByPubkey.get(pubkey) === query) inFlightByPubkey.delete(pubkey)
      }
    }
  }

  for (const [pubkey, pending] of waiting) {
    const found = await pending.catch(() => null)
    if (found?.[pubkey]) out[pubkey] = found[pubkey]
    else {
      const cached = cloneContentKey(contentKeysByPubkey[pubkey])
      if (cached) out[pubkey] = cached
    }
  }
  return out
}
