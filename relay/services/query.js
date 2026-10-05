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
      for await (const item of _eventsFeedGenerator({
        kinds: [10002],
        authors
      }, relays, {
        signal: controller.signal,
        timeout: 5000,
        timeoutAfterFirstEose: null
      })) {
        if (item.type === 'error') { console.error('relay-list watch failed:', item.error); continue }
        if (item.type !== 'event') continue
        const { event } = item
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
  getEvents, cacheMs, timeout, timeoutAfterFirstEose, relayUrlPolicy,
  emptyRelaysFallback, excludeRelays, signal
}) {
  let response
  try {
    response = await getEvents({ kinds: [10002], authors: missingPubkeys, limit: missingPubkeys.length },
      seedRelays.filter(relay => !excludeRelays.includes(relay)), { timeout, timeoutAfterFirstEose, signal })
    signal.throwIfAborted()
  } catch (error) {
    return { entries: {}, report: { authors: missingPubkeys, relays: [], error } }
  }
  const report = { authors: missingPubkeys, relays: response.relays || [], errors: response.errors || [] }
  const complete = response.relays
    ? response.relays.length > 0 && response.relays.every(item => item.status === 'eose')
    : !response.errors?.length
  const latestByPubkey = {}
  for (const { event } of response.result || []) {
    if (!missingPubkeys.includes(event.pubkey)) continue
    if (isNewerRelayListEvent(event, latestByPubkey[event.pubkey])) latestByPubkey[event.pubkey] = event
  }
  const entries = {}
  for (const pubkey of missingPubkeys) {
    const fetchedEvent = latestByPubkey[pubkey]
    const cachedEvent = relayCacheEventByPubkey[pubkey] || null
    const event = isNewerRelayListEvent(fetchedEvent, cachedEvent) ? fetchedEvent : cachedEvent
    const relays = event
      ? parseRelayListEvent(event, relayUrlPolicy)
      : { read: [...emptyRelaysFallback], write: [...emptyRelaysFallback] }
    entries[pubkey] = { ...relays, event }
    // A failed/incomplete lookup may use fallbacks for this call, but must not
    // turn that failure into a forty-minute negative discovery cache entry.
    if (fetchedEvent || complete) setCachedRelays(pubkey, relays, event, cacheMs)
  }
  pruneRelayCache()
  return { entries, report }
}

// Each caller owns its wait, not another caller's shared discovery request.
function waitForDiscovery (record, signal) {
  signal?.throwIfAborted()
  record.users++
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason)
    signal?.addEventListener('abort', abort, { once: true })
    record.promise.then(resolve, reject).finally(() => signal?.removeEventListener('abort', abort))
  }).finally(() => {
    if (--record.users === 0 && !record.finished) {
      record.controller.abort()
      record.remove()
    }
  })
}

export async function getRelaysByPubkey (pubkeys, {
  _getEvents = getEvents,
  cacheMs = QUERY_CACHE_MS,
  includeEvents = false,
  forceRefresh = false,
  timeout = RELAY_LIST_QUERY_TIMEOUT_MS,
  timeoutAfterFirstEose = RELAY_LIST_QUERY_TIMEOUT_AFTER_FIRST_EOSE_MS,
  relayUrlPolicy,
  emptyRelaysFallback = freeRelays.slice(0, 2),
  excludeRelays = [],
  signal,
  onQueryResult
} = {}) {
  signal?.throwIfAborted()
  const pubkeyList = uniquePubkeys(pubkeys, { requireHex: _getEvents === getEvents })
  if (!pubkeyList.length) return {}
  // Different exclusions cannot share a request that contacts a refused relay.
  const scope = JSON.stringify([...new Set(excludeRelays)].sort())
  const key = pubkey => `${pubkey}:${scope}`
  const loadPubkeys = forceRefresh ? pubkeyList : pubkeyList.filter(pubkey => !hasCachedKey(relaysByPubkey, pubkey))
  const pubkeysToLoad = loadPubkeys.filter(pubkey => !relayRequestsByPubkey.has(key(pubkey)))
  if (pubkeysToLoad.length) {
    const record = { controller: new AbortController(), users: 0, finished: false }
    record.remove = () => {
      for (const pubkey of pubkeysToLoad) {
        if (relayRequestsByPubkey.get(key(pubkey)) === record) relayRequestsByPubkey.delete(key(pubkey))
      }
    }
    record.promise = loadMissingRelays(pubkeysToLoad, {
      getEvents: _getEvents, cacheMs, timeout, timeoutAfterFirstEose, relayUrlPolicy,
      emptyRelaysFallback, excludeRelays, signal: record.controller.signal
    }).finally(() => { record.finished = true; record.remove() })
    for (const pubkey of pubkeysToLoad) relayRequestsByPubkey.set(key(pubkey), record)
  }
  const entries = {}
  const records = [...new Set(loadPubkeys.map(pubkey => relayRequestsByPubkey.get(key(pubkey))).filter(Boolean))]
  await Promise.all(records.map(async record => {
    const result = await waitForDiscovery(record, signal)
    signal?.throwIfAborted()
    // Deliver the same native diagnostics to every interested caller, including
    // callers that joined an already running query.
    onQueryResult?.(result.report)
    if (result.report.error) throw result.report.error
    Object.assign(entries, result.entries)
  }))
  signal?.throwIfAborted()
  return Object.fromEntries(pubkeyList.map(pubkey => {
    const source = hasCachedKey(relaysByPubkey, pubkey) ? relaysByPubkey[pubkey] : entries[pubkey]
    const entry = cloneRelays(source || { read: emptyRelaysFallback, write: emptyRelaysFallback })
    if (includeEvents) entry.event = cloneRelayListEvent(relayCacheEventByPubkey[pubkey] || entries[pubkey]?.event)
    return [pubkey, entry]
  }))
}
