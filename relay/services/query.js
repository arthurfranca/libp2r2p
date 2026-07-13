import { freeRelays, seedRelays } from '../constants/index.js'
import { relayPool } from './relay-pool.js'

const QUERY_CACHE_MS = 40 * 60 * 1000
const RELAY_CACHE_MAX_ITEMS = 500
const HEX_PUBKEY = /^[0-9a-f]{64}$/i
const relaysByPubkey = Object.create(null)
const relayCacheTimersByPubkey = Object.create(null)
const relayCacheAddedAtByPubkey = Object.create(null)
const relayCacheEventCreatedAtByPubkey = Object.create(null)

const getEvents = (...args) => relayPool.getEvents(...args)
const getEventsFeedGenerator = (...args) => relayPool.getEventsFeedGenerator(...args)

function hasCachedKey (cache, key) {
  return Object.prototype.hasOwnProperty.call(cache, key)
}

function maybeUnref (timer) {
  timer?.unref?.()
  return timer
}

function cloneRelays (relays) {
  return {
    read: [...(relays?.read || [])],
    write: [...(relays?.write || [])]
  }
}

// NIP-65 relay-list tags without a marker apply to both read and write use.
function parseRelayListEvent (event) {
  const out = { read: [], write: [] }
  if (!event || event.kind !== 10002) return out
  for (const tag of event.tags) {
    if (tag[0] !== 'r' || typeof tag[1] !== 'string') continue
    if (tag[2] === 'read') out.read.push(tag[1])
    else if (tag[2] === 'write') out.write.push(tag[1])
    else { out.read.push(tag[1]); out.write.push(tag[1]) }
  }
  out.read = [...new Set(out.read)]
  out.write = [...new Set(out.write)]
  return out
}

function uniquePubkeys (pubkeys, { requireHex = false } = {}) {
  const values = [...new Set(pubkeys || [])].filter(Boolean)
  return requireHex ? values.filter(pubkey => HEX_PUBKEY.test(pubkey)) : values
}

function relayListCreatedAt (event) {
  return Number.isFinite(event?.created_at) ? event.created_at : 0
}

function relaySetsEqual (a, b) {
  const left = new Set(a || [])
  const right = new Set(b || [])
  if (left.size !== right.size) return false
  for (const value of left) if (!right.has(value)) return false
  return true
}

function relaySetChanges (previous, next) {
  const read = !relaySetsEqual(previous?.read, next?.read)
  const write = !relaySetsEqual(previous?.write, next?.write)
  return {
    read,
    write,
    both: read || write
  }
}

function relayTypeChanged (changes, relayType) {
  if (relayType === 'read') return changes.read
  if (relayType === 'write') return changes.write
  return changes.both
}

function deleteCachedRelay (pubkey) {
  clearTimeout(relayCacheTimersByPubkey[pubkey])
  delete relaysByPubkey[pubkey]
  delete relayCacheTimersByPubkey[pubkey]
  delete relayCacheAddedAtByPubkey[pubkey]
  delete relayCacheEventCreatedAtByPubkey[pubkey]
}

function setCachedRelays (pubkey, relays, createdAt, cacheMs) {
  relaysByPubkey[pubkey] = cloneRelays(relays)
  relayCacheAddedAtByPubkey[pubkey] = Date.now()
  relayCacheEventCreatedAtByPubkey[pubkey] = createdAt
  clearTimeout(relayCacheTimersByPubkey[pubkey])
  if (cacheMs > 0) {
    relayCacheTimersByPubkey[pubkey] = maybeUnref(setTimeout(() => {
      deleteCachedRelay(pubkey)
    }, cacheMs))
  } else {
    delete relayCacheTimersByPubkey[pubkey]
  }
}

function pruneRelayCache () {
  const keys = Object.keys(relaysByPubkey)
  if (keys.length <= RELAY_CACHE_MAX_ITEMS) return

  keys
    .sort((a, b) => (relayCacheAddedAtByPubkey[a] || 0) - (relayCacheAddedAtByPubkey[b] || 0))
    .slice(0, keys.length - RELAY_CACHE_MAX_ITEMS)
    .forEach(deleteCachedRelay)
}

export function clearRelayQueryCache () {
  for (const timer of Object.values(relayCacheTimersByPubkey)) clearTimeout(timer)
  for (const key of Object.keys(relaysByPubkey)) delete relaysByPubkey[key]
  for (const key of Object.keys(relayCacheTimersByPubkey)) delete relayCacheTimersByPubkey[key]
  for (const key of Object.keys(relayCacheAddedAtByPubkey)) delete relayCacheAddedAtByPubkey[key]
  for (const key of Object.keys(relayCacheEventCreatedAtByPubkey)) delete relayCacheEventCreatedAtByPubkey[key]
}

export function cacheRelayListEvent (event, { cacheMs = QUERY_CACHE_MS } = {}) {
  if (!event || event.kind !== 10002 || !event.pubkey) return null
  const createdAt = relayListCreatedAt(event)
  const previousCreatedAt = relayCacheEventCreatedAtByPubkey[event.pubkey]
  if (previousCreatedAt != null && createdAt <= previousCreatedAt) return null

  const previousRelays = hasCachedKey(relaysByPubkey, event.pubkey)
    ? cloneRelays(relaysByPubkey[event.pubkey])
    : null
  const relays = parseRelayListEvent(event)
  const changes = relaySetChanges(previousRelays, relays)
  setCachedRelays(event.pubkey, relays, createdAt, cacheMs)
  pruneRelayCache()

  return {
    pubkey: event.pubkey,
    event,
    relays: cloneRelays(relays),
    previousRelays,
    changes
  }
}

export function subscribeRelayListUpdates (pubkeys, {
  relayType = 'both',
  onChange,
  relays = seedRelays,
  cacheMs = QUERY_CACHE_MS,
  _eventsFeedGenerator = getEventsFeedGenerator
} = {}) {
  const authors = uniquePubkeys(pubkeys, { requireHex: _eventsFeedGenerator === getEventsFeedGenerator })
  if (!authors.length) return () => {}

  let closed = false
  const controller = new AbortController()

  async function consumeRelayListUpdates () {
    try {
      for await (const event of _eventsFeedGenerator({
        kinds: [10002],
        authors
      }, relays, {
        signal: controller.signal,
        timeout: 5000,
        timeoutAfterFirstEose: null
      })) {
        if (closed || !authors.includes(event.pubkey)) continue
        const update = cacheRelayListEvent(event, { cacheMs })
        if (!update || !relayTypeChanged(update.changes, relayType)) continue
        onChange?.({
          ...update,
          relayType
        })
      }
    } catch (error) {
      if (!closed && error?.message !== 'Aborted') console.error('relay-list watch failed:', error)
    }
  }

  consumeRelayListUpdates()

  return () => {
    closed = true
    controller.abort()
  }
}

export async function getRelaysByPubkey (pubkeys, { _getEvents = getEvents, cacheMs = QUERY_CACHE_MS } = {}) {
  const pubkeyList = uniquePubkeys(pubkeys, { requireHex: _getEvents === getEvents })
  if (!pubkeyList.length) return {}

  const out = {}
  const missingPubkeys = []
  for (const pubkey of pubkeyList) {
    if (hasCachedKey(relaysByPubkey, pubkey)) out[pubkey] = cloneRelays(relaysByPubkey[pubkey])
    else missingPubkeys.push(pubkey)
  }
  if (!missingPubkeys.length) return out

  const { result: events } = await _getEvents({
    kinds: [10002],
    authors: missingPubkeys,
    limit: missingPubkeys.length
  }, seedRelays, {
    timeout: 5000,
    timeoutAfterFirstEose: null
  })

  const latestByPubkey = {}
  for (const event of events) {
    if (!missingPubkeys.includes(event.pubkey)) continue
    if (!latestByPubkey[event.pubkey] || event.created_at > latestByPubkey[event.pubkey].created_at) {
      latestByPubkey[event.pubkey] = event
    }
  }

  for (const pubkey of missingPubkeys) {
    const relays = latestByPubkey[pubkey]
      ? parseRelayListEvent(latestByPubkey[pubkey])
      : { read: freeRelays.slice(0, 2), write: freeRelays.slice(0, 2) }
    setCachedRelays(pubkey, relays, relayListCreatedAt(latestByPubkey[pubkey]), cacheMs)
    out[pubkey] = relays
  }
  pruneRelayCache()
  return out
}
