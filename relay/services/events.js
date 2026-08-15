import { freeRelays } from '../constants/index.js'
import { pickRelaysForPubkeys } from '../helpers/routing.js'
import { relayPool } from './relay-pool.js'
import { getRelaysByPubkey } from './query.js'

const DEFAULT_MAX_PER_PUBKEY = 2
const DEFAULT_FALLBACK_RELAY_COUNT = 3

const getEvents = (...args) => relayPool.getEvents(...args)

function hasOwn (object, key) {
  return Object.prototype.hasOwnProperty.call(object, key)
}

function eventAddress (event) {
  const dTag = event.tags?.find(tag => tag[0] === 'd')
  return `${event.kind}:${event.pubkey}:${dTag?.[1] ?? ''}`
}

// NIP-01 ordering for replaceable and addressable events.
function isNewerEvent (candidate, current) {
  if (!candidate) return false
  if (!current) return true
  if (candidate.created_at !== current.created_at) return candidate.created_at > current.created_at
  if (typeof candidate.id !== 'string') return false
  return typeof current.id !== 'string' || candidate.id < current.id
}

function mergeLatestEvents (target, events) {
  for (const event of events) {
    const address = eventAddress(event)
    if (isNewerEvent(event, target[address])) target[address] = event
  }
  return target
}

// Records which relays were already queried for each pubkey.
function getSelectedRelaysByPubkey (pubkeys, relayToAuthors) {
  const selected = Object.fromEntries(pubkeys.map(pubkey => [pubkey, new Set()]))
  for (const [relay, authors] of relayToAuthors) {
    for (const pubkey of authors) selected[pubkey]?.add(relay)
  }
  return selected
}

// Fetches batched events and keeps the newest event per address.
async function fetchLatestEventsByRelay (relayToAuthors, { kinds, dTagsByPubkey, getEvents }) {
  const requests = []
  for (const [relay, authors] of relayToAuthors) {
    const authorsByD = new Map()
    for (const pubkey of authors) {
      const d = dTagsByPubkey?.[pubkey] ?? ''
      if (!authorsByD.has(d)) authorsByD.set(d, [])
      authorsByD.get(d).push(pubkey)
    }
    for (const [d, dAuthors] of authorsByD) {
      const filter = { kinds, authors: dAuthors }
      if (d) filter['#d'] = [d]
      requests.push(getEvents(filter, [relay])
        .then(response => ({ requested: new Set(dAuthors), events: response.result || [] })))
    }
  }

  const results = await Promise.allSettled(requests)
  const latestByAddress = {}
  for (const result of results) {
    if (result.status !== 'fulfilled') continue
    const { requested, events } = result.value
    mergeLatestEvents(latestByAddress, events.filter(event =>
      kinds.includes(event?.kind) && requested.has(event?.pubkey)
    ))
  }
  return latestByAddress
}

/**
 * Fetches the latest replaceable (or addressable, with `dTagsByPubkey`) events
 * for many pubkeys through their NIP-65 relays in two batched passes.
 *
 * The first pass routes through up to `maxPerPubkey` relays per author,
 * preferring relays shared by more authors. Authors still missing after that
 * pass are retried on every remaining relay plus `fallbackRelays`, excluding
 * relays already queried for them.
 *
 * When `relaysByPubkey` is provided, it is reused as-is for the pubkeys it
 * covers; only missing pubkeys are discovered, and the merged map is returned
 * so it can be passed back on a later call.
 */
export async function getLatestEventsByPubkey (pubkeys, {
  kinds,
  dTagsByPubkey,
  relayType = 'write',
  maxPerPubkey = DEFAULT_MAX_PER_PUBKEY,
  fallbackRelays = freeRelays.slice(0, DEFAULT_FALLBACK_RELAY_COUNT),
  relaysByPubkey,
  relayListOptions,
  _getRelaysByPubkey = getRelaysByPubkey,
  _getEvents = getEvents
} = {}) {
  const authors = [...new Set(pubkeys || [])].filter(Boolean)
  if (!authors.length) return { events: [], byPubkey: {}, relaysByPubkey: {} }
  if (!Array.isArray(kinds) || kinds.length === 0) throw new Error('Missing kinds')
  const type = relayType === 'read' ? 'read' : 'write'

  const relaysByAuthor = { ...(relaysByPubkey || {}) }
  const missingRelayAuthors = authors.filter(pubkey => !hasOwn(relaysByAuthor, pubkey))
  if (missingRelayAuthors.length) {
    let discovered
    try {
      discovered = await _getRelaysByPubkey(missingRelayAuthors, relayListOptions)
    } catch (error) {
      console.error('Failed to discover publisher relays:', error)
      discovered = Object.fromEntries(missingRelayAuthors.map(pubkey => [pubkey, { read: [], write: [] }]))
    }
    for (const pubkey of missingRelayAuthors) {
      relaysByAuthor[pubkey] = discovered?.[pubkey] || { read: [], write: [] }
    }
  }

  const primaryAuthors = authors.filter(pubkey => relaysByAuthor[pubkey]?.[type]?.length)
  const primaryRoutes = pickRelaysForPubkeys(primaryAuthors, relaysByAuthor, { maxPerPubkey, relayType })
  const selectedRelays = getSelectedRelaysByPubkey(authors, primaryRoutes)
  const latestByAddress = await fetchLatestEventsByRelay(primaryRoutes, {
    kinds,
    dTagsByPubkey,
    getEvents: _getEvents
  })
  const foundPubkeys = new Set(Object.values(latestByAddress).map(event => event.pubkey))
  const missingAuthors = authors.filter(pubkey => !foundPubkeys.has(pubkey))

  if (missingAuthors.length) {
    const fallbackRelaysByPubkey = Object.fromEntries(missingAuthors.map(pubkey => [
      pubkey, { [type]: [...new Set([...(relaysByAuthor[pubkey]?.[type] || []), ...fallbackRelays])] }
    ]))
    const fallbackRoutes = pickRelaysForPubkeys(missingAuthors, fallbackRelaysByPubkey, {
      maxPerPubkey: Infinity,
      relayType,
      excludeRelaysByPubkey: selectedRelays,
      emptyRelaysFallback: []
    })
    mergeLatestEvents(latestByAddress, Object.values(
      await fetchLatestEventsByRelay(fallbackRoutes, {
        kinds,
        dTagsByPubkey,
        getEvents: _getEvents
      })
    ))
  }

  const events = Object.values(latestByAddress)
  const byPubkey = {}
  for (const event of events) byPubkey[event.pubkey] = event
  return { events, byPubkey, relaysByPubkey: relaysByAuthor }
}
