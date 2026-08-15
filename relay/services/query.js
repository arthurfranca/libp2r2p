import { freeRelays, seedRelays } from '../constants/index.js'
import { relayPool } from './relay-pool.js'
import { isValidPublicRelayUrl, normalizeRelayUrl } from '../../url/index.js'

const QUERY_CACHE_MS = 40 * 60 * 1000
const RELAY_CACHE_MAX_ITEMS = 500
const HEX_PUBKEY = /^[0-9a-f]{64}$/i
const relaysByPubkey = Object.create(null)
const relayCacheTimersByPubkey = Object.create(null)
const relayCacheAddedAtByPubkey = Object.create(null)
const relayCacheEventCreatedAtByPubkey = Object.create(null)
const relayCacheEventIdByPubkey = Object.create(null)
const relayCacheEventByPubkey = Object.create(null)
const relayRequestsByPubkey = new Map()

const getEvents = (...args) => relayPool.getEvents(...args)
const getEventsFeedGenerator = (...args) => relayPool.getEventsFeedGenerator(...args)
const RELAY_LIST_QUERY_TIMEOUT_MS = 5000
const RELAY_LIST_QUERY_TIMEOUT_AFTER_FIRST_EOSE_MS = 500

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

function cloneRelayListEvent (event) {
  if (!event) return null
  return { ...event, tags: [...(event.tags || [])] }
}

// NIP-65 relay-list tags without a marker apply to both read and write use.
export function parseRelayListEvent (event, relayUrlPolicy) {
  const out = { read: [], write: [] }
  if (!event || event.kind !== 10002) return out
  for (const tag of event.tags || []) {
    if (tag[0] !== 'r' || typeof tag[1] !== 'string') continue
    let relay
    try {
      relay = normalizeRelayUrl(tag[1])
    } catch {
      continue
    }
    if (!isValidPublicRelayUrl(relay, relayUrlPolicy)) continue
    if (tag[2] === 'read') out.read.push(relay)
    else if (tag[2] === 'write') out.write.push(relay)
    else { out.read.push(relay); out.write.push(relay) }
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

function isNewerRelayListEvent (candidate, current) {
  if (!candidate) return false
  if (!current) return true
  const candidateCreatedAt = relayListCreatedAt(candidate)
  const currentCreatedAt = relayListCreatedAt(current)
  if (candidateCreatedAt !== currentCreatedAt) return candidateCreatedAt > currentCreatedAt
  if (typeof candidate.id !== 'string') return false
  return typeof current.id !== 'string' || candidate.id < current.id
}

function areRelaySetsEqual (a, b) {
  const left = new Set(a || [])
  const right = new Set(b || [])
  if (left.size !== right.size) return false
  for (const value of left) if (!right.has(value)) return false
  return true
}

function relaySetChanges (previous, next) {
  const read = !areRelaySetsEqual(previous?.read, next?.read)
  const write = !areRelaySetsEqual(previous?.write, next?.write)
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
  delete relayCacheEventIdByPubkey[pubkey]
  delete relayCacheEventByPubkey[pubkey]
}

function setCachedRelays (pubkey, relays, event, cacheMs) {
  relaysByPubkey[pubkey] = cloneRelays(relays)
  relayCacheAddedAtByPubkey[pubkey] = Date.now()
  relayCacheEventCreatedAtByPubkey[pubkey] = relayListCreatedAt(event)
  relayCacheEventIdByPubkey[pubkey] = typeof event?.id === 'string' ? event.id : null
  relayCacheEventByPubkey[pubkey] = event || null
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
  for (const key of Object.keys(relayCacheEventIdByPubkey)) delete relayCacheEventIdByPubkey[key]
  for (const key of Object.keys(relayCacheEventByPubkey)) delete relayCacheEventByPubkey[key]
}

export function cacheRelayListEvent (event, { cacheMs = QUERY_CACHE_MS, relayUrlPolicy } = {}) {
  if (!event || event.kind !== 10002 || !event.pubkey) return null
  const previousCreatedAt = relayCacheEventCreatedAtByPubkey[event.pubkey]
  const previousEvent = previousCreatedAt == null
    ? null
    : { created_at: previousCreatedAt, id: relayCacheEventIdByPubkey[event.pubkey] }
  if (!isNewerRelayListEvent(event, previousEvent)) return null

  const previousRelays = hasCachedKey(relaysByPubkey, event.pubkey)
    ? cloneRelays(relaysByPubkey[event.pubkey])
    : null
  const relays = parseRelayListEvent(event, relayUrlPolicy)
  const changes = relaySetChanges(previousRelays, relays)
  setCachedRelays(event.pubkey, relays, event, cacheMs)
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
  relayUrlPolicy,
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
        const update = cacheRelayListEvent(event, { cacheMs, relayUrlPolicy })
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

async function loadMissingRelays (missingPubkeys, {
  getEvents,
  cacheMs,
  timeout = RELAY_LIST_QUERY_TIMEOUT_MS,
  timeoutAfterFirstEose = RELAY_LIST_QUERY_TIMEOUT_AFTER_FIRST_EOSE_MS,
  relayUrlPolicy,
  emptyRelaysFallback = freeRelays.slice(0, 2)
}) {
  const { result: events } = await getEvents({
    kinds: [10002],
    authors: missingPubkeys,
    limit: missingPubkeys.length
  }, seedRelays, {
    timeout,
    timeoutAfterFirstEose
  })

  const latestByPubkey = {}
  for (const event of events || []) {
    if (!missingPubkeys.includes(event.pubkey)) continue
    if (isNewerRelayListEvent(event, latestByPubkey[event.pubkey])) latestByPubkey[event.pubkey] = event
  }

  for (const pubkey of missingPubkeys) {
    const fetchedEvent = latestByPubkey[pubkey]
    const cachedEvent = relayCacheEventByPubkey[pubkey] || null
    const event = isNewerRelayListEvent(fetchedEvent, cachedEvent) ? fetchedEvent : cachedEvent
    const relays = event
      ? parseRelayListEvent(event, relayUrlPolicy)
      : { read: [...emptyRelaysFallback], write: [...emptyRelaysFallback] }
    setCachedRelays(pubkey, relays, event, cacheMs)
  }
  pruneRelayCache()
}

export async function getRelaysByPubkey (pubkeys, {
  _getEvents = getEvents,
  cacheMs = QUERY_CACHE_MS,
  includeEvents = false,
  forceRefresh = false,
  timeout = RELAY_LIST_QUERY_TIMEOUT_MS,
  timeoutAfterFirstEose = RELAY_LIST_QUERY_TIMEOUT_AFTER_FIRST_EOSE_MS,
  relayUrlPolicy,
  emptyRelaysFallback = freeRelays.slice(0, 2)
} = {}) {
  const pubkeyList = uniquePubkeys(pubkeys, { requireHex: _getEvents === getEvents })
  if (!pubkeyList.length) return {}

  const loadPubkeys = forceRefresh
    ? pubkeyList
    : pubkeyList.filter(pubkey => !hasCachedKey(relaysByPubkey, pubkey))
  const pubkeysToLoad = loadPubkeys.filter(pubkey => !relayRequestsByPubkey.has(pubkey))
  if (pubkeysToLoad.length) {
    const request = loadMissingRelays(pubkeysToLoad, {
      getEvents: _getEvents,
      cacheMs,
      timeout,
      timeoutAfterFirstEose,
      relayUrlPolicy,
      emptyRelaysFallback
    }).finally(() => {
      for (const pubkey of pubkeysToLoad) {
        if (relayRequestsByPubkey.get(pubkey) === request) relayRequestsByPubkey.delete(pubkey)
      }
    })
    for (const pubkey of pubkeysToLoad) relayRequestsByPubkey.set(pubkey, request)
  }

  await Promise.all([...new Set(
    loadPubkeys.map(pubkey => relayRequestsByPubkey.get(pubkey)).filter(Boolean)
  )])

  return Object.fromEntries(pubkeyList
    .filter(pubkey => hasCachedKey(relaysByPubkey, pubkey))
    .map(pubkey => {
      const entry = cloneRelays(relaysByPubkey[pubkey])
      if (includeEvents) entry.event = cloneRelayListEvent(relayCacheEventByPubkey[pubkey])
      return [pubkey, entry]
    }))
}
