import { decodeHll, encodeHll, estimateHllCount, mergeHll } from '../helpers/hll.js'
import { createPublishSettlements, firstFulfillment, publishSummary } from '../helpers/publish.js'
import { maybeUnref } from '../helpers/timer.js'
import { normalizeURL } from 'nostr-tools/utils'
import { RelayConnection } from './relay-connection.js'

const CONNECTION_TIMEOUT_MS = 3000
const COUNT_TIMEOUT_MS = 5000
const COUNT_TIMEOUT_AFTER_FIRST_COUNT_MS = 500
const SEND_TIMEOUT_UNTIL_FIRST_FULFILLMENT_MS = 3000
const SEND_TIMEOUT_MS = 30000

// Returns a function that should be called for each received event (valid or invalid).
// Calls onSatisfied() and stops counting once the filter is fully satisfied per relay:
//   - limit: close after that many events have been received (counting invalid ones too,
//     since the relay counts them toward its own limit)
//   - ids: close once all requested ids have been seen
// Both conditions are independent; whichever triggers first wins.
function makeEarlyCloseChecker (filter, onSatisfied) {
  let count = 0
  const remainingIds = (filter.ids?.length > 0) ? new Set(filter.ids) : null
  const limit = filter.limit > 0 ? filter.limit : null
  let satisfied = false

  return (event) => {
    if (satisfied) return
    count++
    if (remainingIds && event?.id) remainingIds.delete(event.id)
    if ((limit !== null && count >= limit) || (remainingIds !== null && remainingIds.size === 0)) {
      satisfied = true
      onSatisfied()
    }
  }
}

function relayResultForSettlement (relay, settlement) {
  if (settlement.status === 'fulfilled') {
    return {
      relay,
      success: true,
      outcome: settlement.value || 'published'
    }
  }

  return {
    relay,
    success: false,
    outcome: settlement.outcome || 'failed',
    reason: settlement.reason
  }
}

function notifyRelayResult (onRelayResult, result) {
  if (!onRelayResult) return
  try {
    Promise.resolve(onRelayResult(result)).catch(error => {
      console.error('RelayPool onRelayResult failed:', error)
    })
  } catch (error) {
    console.error('RelayPool onRelayResult failed:', error)
  }
}

function requiresNip42Auth (reason) {
  return reason.message.startsWith('auth-required:') || reason.message.startsWith('restricted:')
}

function countResponseError () {
  return new Error('INVALID_COUNT_RESPONSE')
}

function countTimeoutError () {
  return new Error('COUNT_TIMEOUT')
}

function getEventsTimeoutError () {
  return new Error('GET_EVENTS_TIMEOUT')
}

function normalizedRelayUrls (relays) {
  const urls = []
  const seen = new Set()

  for (const relay of relays || []) {
    const normalizedUrl = normalizeURL(relay)
    if (seen.has(normalizedUrl)) continue
    seen.add(normalizedUrl)
    // Keep the caller's first spelling in reports and metadata while using the
    // canonical spelling for pooled connection ownership.
    urls.push(relay)
  }

  return urls
}

function isCountResponse (payload) {
  return Number.isSafeInteger(payload?.count) && payload.count >= 0
}

class Nip42AuthenticationError extends Error {
  constructor (reason) {
    super(reason.message, { cause: reason })
    this.name = 'Nip42AuthenticationError'
  }
}

// Interacts with Nostr relays
export class RelayPool {
  #relays = new Map()
  #relayTimeouts = new Map()
  #liveSubCounts = new Map() // url -> number of active live subscriptions
  #timeout = 30000 // 30 seconds

  #scheduleIdleDisconnect (url) {
    clearTimeout(this.#relayTimeouts.get(url))
    this.#relayTimeouts.set(url, maybeUnref(setTimeout(() => this.disconnect(url), this.#timeout)))
  }

  // Opens a normalized pooled connection. Failed connects are evicted so a later
  // retry creates a fresh RelayConnection instead of reusing broken socket state.
  async #getRelay (url) {
    const normalizedUrl = normalizeURL(url)
    let relay = this.#relays.get(normalizedUrl)
    if (!relay) {
      relay = new RelayConnection(normalizedUrl)
      this.#relays.set(normalizedUrl, relay)
    }

    try {
      // nostr-tools cancels its socket attempt when this deadline elapses.
      await relay.connect({ timeout: CONNECTION_TIMEOUT_MS })
    } catch (error) {
      if (this.#relays.get(normalizedUrl) === relay) {
        this.#relays.delete(normalizedUrl)
        clearTimeout(this.#relayTimeouts.get(normalizedUrl))
        this.#relayTimeouts.delete(normalizedUrl)
      }
      try {
        await relay.close()
      } catch {}
      throw error
    }

    // Only reset idle timeout when no live subscriptions are holding this relay open.
    if (!this.#liveSubCounts.get(normalizedUrl)) this.#scheduleIdleDisconnect(normalizedUrl)
    return relay
  }

  #incrementLiveSub (url) {
    const normalizedUrl = normalizeURL(url)
    this.#liveSubCounts.set(normalizedUrl, (this.#liveSubCounts.get(normalizedUrl) ?? 0) + 1)
    // Cancel any pending idle timeout — this relay must stay open
    clearTimeout(this.#relayTimeouts.get(normalizedUrl))
    this.#relayTimeouts.delete(normalizedUrl)
  }

  #decrementLiveSub (url) {
    const normalizedUrl = normalizeURL(url)
    const next = (this.#liveSubCounts.get(normalizedUrl) ?? 1) - 1
    if (next <= 0) {
      this.#liveSubCounts.delete(normalizedUrl)
      // No more live subscriptions — start the idle timer if the relay is still pooled
      if (this.#relays.has(normalizedUrl)) {
        this.#scheduleIdleDisconnect(normalizedUrl)
      }
    } else {
      this.#liveSubCounts.set(normalizedUrl, next)
    }
  }

  // Disconnect from a relay
  async disconnect (url) {
    const normalizedUrl = normalizeURL(url)
    if (this.#relays.has(normalizedUrl)) {
      const relay = this.#relays.get(normalizedUrl)
      if (relay.ws?.readyState < 2) await relay.close()?.catch(console.log)
      this.#relays.delete(normalizedUrl)
      clearTimeout(this.#relayTimeouts.get(normalizedUrl))
      this.#relayTimeouts.delete(normalizedUrl)
    }
  }

  // Disconnect from all relays
  async disconnectAll () {
    for (const url of [...this.#relays.keys()]) {
      await this.disconnect(url)
    }
  }

  // NIP-42 retries happen inside one relay attempt, so sendEvent still reports
  // exactly one terminal outcome for each relay URL.
  async #publishEvent (relay, event, getAuthEvent) {
    try {
      await relay.publish(event)
      return 'published'
    } catch (error) {
      const reason = error instanceof Error ? error : new Error(String(error))
      if (!getAuthEvent || !requiresNip42Auth(reason)) throw reason

      try {
        await relay.authenticate(getAuthEvent)
      } catch (error) {
        const authReason = error instanceof Error ? error : new Error(String(error))
        throw new Nip42AuthenticationError(authReason)
      }
      await relay.publish(event)
      return 'published'
    }
  }

  // Collects COUNT replies only until they are useful: the first usable reply
  // opens a short window for a higher count or a mergeable HLL from peers.
  // null disables either timer: no grace waits for all relays or the deadline.
  async countEvents (filter, relays, {
    timeout = COUNT_TIMEOUT_MS,
    timeoutAfterFirstCount = COUNT_TIMEOUT_AFTER_FIRST_COUNT_MS,
    signal
  } = {}) {
    if (!filter || typeof filter !== 'object' || Array.isArray(filter)) {
      throw new Error('COUNT_FILTER_REQUIRED')
    }
    if (signal?.aborted) throw new Error('Aborted')

    const urls = normalizedRelayUrls(relays)
    if (!urls.length) {
      return { count: null, approximate: false, errors: [], success: false }
    }

    const countController = new AbortController()
    const pending = new Set(urls)
    const errors = []
    let count = null
    let approximate = false
    let registers = null
    let isResolved = false
    let graceTimer = null
    let timeoutTimer = null

    return await new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timeoutTimer)
        clearTimeout(graceTimer)
        signal?.removeEventListener('abort', onAbort)
        countController.abort()
      }

      const finish = ({ timedOut = false, aborted = false } = {}) => {
        if (isResolved) return
        isResolved = true

        if (timedOut) {
          for (const relay of pending) errors.push({ relay, reason: countTimeoutError() })
        }
        cleanup()
        if (aborted) {
          reject(new Error('Aborted'))
          return
        }

        const result = {
          count,
          approximate,
          errors,
          success: count !== null
        }
        if (registers) {
          result.hll = encodeHll(registers)
          result.hllCount = estimateHllCount(registers)
        }
        resolve(result)
      }

      const onAbort = () => finish({ aborted: true })
      signal?.addEventListener('abort', onAbort, { once: true })
      if (timeout !== null) {
        timeoutTimer = maybeUnref(setTimeout(() => finish({ timedOut: true }), timeout))
      }

      const settleRelay = (relay) => pending.delete(relay)
      const finishIfComplete = () => {
        if (pending.size === 0) finish()
      }

      const handleResponse = (relay, payload) => {
        if (isResolved || !settleRelay(relay)) return
        if (!isCountResponse(payload)) {
          errors.push({ relay, reason: countResponseError() })
          finishIfComplete()
          return
        }

        // Prefer an exact count when equal relay counts disagree on approximate.
        if (count === null || payload.count > count || (payload.count === count && approximate && payload.approximate !== true)) {
          count = payload.count
          approximate = payload.approximate === true
        }

        const hll = decodeHll(payload.hll)
        if (hll) {
          if (!registers) registers = new Uint8Array(hll.length)
          mergeHll(registers, hll)
        }

        if (count !== null && timeoutAfterFirstCount !== null && !graceTimer && pending.size > 0) {
          graceTimer = maybeUnref(setTimeout(finish, timeoutAfterFirstCount))
        }
        finishIfComplete()
      }

      const handleError = (relay, error) => {
        if (isResolved || !settleRelay(relay)) return
        const reason = error instanceof Error ? error : new Error(String(error))
        errors.push({ relay, reason })
        finishIfComplete()
      }

      for (const relay of urls) {
        this.#getRelay(relay)
          .then(async connection => {
            if (isResolved) return null
            return await connection.countWithHll([filter], { signal: countController.signal })
          })
          .then(
            payload => { if (payload !== null) handleResponse(relay, payload) },
            error => handleError(relay, error)
          )
      }
    })
  }

  // Collects a one-shot relay read. The first EOSE with events opens a short
  // grace window; null disables that window so callers wait for every relay or
  // the operation deadline. Event ids are deduplicated across relay responses.
  async getEvents (filter, relays, { timeout = 5000, timeoutAfterFirstEose = 500, callback, signal } = {}) {
    const urls = normalizedRelayUrls(relays)
    if (!urls.length) return { result: [], errors: [], success: false }
    if (signal?.aborted) throw new Error('Aborted')

    const subscriptions = new Map()
    const pending = new Set(urls)
    const normalCloseUrls = new Set()
    const errors = []
    const events = []
    const eventIds = new Set()
    let completed = 0
    let isResolved = false
    let eoseTimer = null
    let timeoutTimer = null

    return await new Promise((resolve, reject) => {
      const closeSubscriptions = () => {
        for (const sub of subscriptions.values()) sub.close()
        subscriptions.clear()
      }

      const cleanup = () => {
        clearTimeout(timeoutTimer)
        clearTimeout(eoseTimer)
        signal?.removeEventListener('abort', onAbort)
        closeSubscriptions()
      }

      const finish = () => {
        if (isResolved) return
        isResolved = true
        cleanup()
        resolve({
          result: events,
          errors,
          success: events.length > 0 || completed > 0
        })
      }

      const finishIfComplete = () => {
        if (pending.size === 0) finish()
      }

      const settleRelay = (url, reason) => {
        if (isResolved || !pending.delete(url)) return
        subscriptions.delete(url)
        if (reason) {
          errors.push({ reason, relay: url })
          if (callback) callback({ type: 'error', error: reason, relay: url })
        } else {
          completed++
        }
        finishIfComplete()
      }

      const onAbort = () => {
        if (isResolved) return
        isResolved = true
        cleanup()
        reject(new Error('Aborted'))
      }

      const timeoutPending = () => {
        if (isResolved) return
        for (const url of pending) {
          errors.push({ reason: getEventsTimeoutError(), relay: url })
        }
        finish()
      }

      signal?.addEventListener('abort', onAbort, { once: true })
      if (timeout !== null) timeoutTimer = maybeUnref(setTimeout(timeoutPending, timeout))

      for (const url of urls) {
        this.#getRelay(url).then(relay => {
          if (isResolved || !pending.has(url)) return
          let hasEvents = false
          let sub

          // Actual EOSE and filter satisfaction share the same graceful close path.
          const handleEose = () => {
            if (isResolved || !pending.has(url)) return
            // nostr-tools reports "closed by caller" through onclose(). This is a
            // successful local completion, not a relay failure.
            normalCloseUrls.add(url)
            sub.close()
            if (hasEvents && timeoutAfterFirstEose !== null && !eoseTimer && !isResolved) {
              eoseTimer = maybeUnref(setTimeout(finish, timeoutAfterFirstEose))
            }
          }

          const checkEarlyClose = makeEarlyCloseChecker(filter, handleEose)
          sub = relay.subscribe([filter], {
            onevent: (event) => {
              if (isResolved || !pending.has(url)) return
              hasEvents = true
              // Keep filter-limit accounting per relay, but only expose one copy of
              // a matching event when several relays return the same id.
              if (!event?.id || !eventIds.has(event.id)) {
                if (event?.id) eventIds.add(event.id)
                event.meta = { relay: url }
                events.push(event)
                if (callback) callback({ type: 'event', event, relay: url })
              }
              checkEarlyClose(event)
            },
            oninvalidevent: () => {
              if (!isResolved && pending.has(url)) checkEarlyClose()
            },
            onclose: error => {
              const reason = normalCloseUrls.delete(url) || error === undefined
                ? null
                : error instanceof Error ? error : new Error(String(error))
              settleRelay(url, reason)
            },
            oneose: handleEose
          })
          if (isResolved || !pending.has(url)) sub.close()
          else subscriptions.set(url, sub)
        }).catch(error => {
          const reason = error instanceof Error ? error : new Error(String(error))
          settleRelay(url, reason)
        })
      }
    })
  }

  async * getEventsGenerator (filter, relays, options = {}) {
    const queue = []
    let p = Promise.withResolvers()
    let isDone = false

    const userCallback = options.callback
    const callback = item => {
      queue.push(item)
      if (userCallback) userCallback(item)
      p.resolve()
      p = Promise.withResolvers()
    }

    const methodPromise = this.getEvents(filter, relays, { ...options, callback })
      .catch(err => { if (err?.message !== 'Aborted') console.error('Error in getEvents:', err) })
      .finally(() => {
        isDone = true
        p.resolve()
      })

    // eslint-disable-next-line no-unmodified-loop-condition
    while (!isDone || queue.length > 0) {
      if (queue.length > 0) yield queue.shift()
      else await p.promise
    }

    return await methodPromise
  }

  // Returns a strictly-live stream. `ready` reports the first initial EOSE window,
  // while `readyRelays` follows relays that are currently past their own EOSE.
  getLiveEventsGenerator (filter, relays, options = {}) {
    const ready = Promise.withResolvers()
    const readyRelays = new Set()
    const stream = this.#getLiveEventsGenerator(filter, relays, options, { ready, readyRelays })

    Object.defineProperties(stream, {
      ready: {
        enumerable: false,
        value: ready.promise
      },
      readyRelays: {
        enumerable: false,
        get: () => Object.freeze([...readyRelays])
      }
    })

    return stream
  }

  // Each relay becomes live after its own EOSE. This prevents a slow peer from
  // suppressing already-live events from another relay.
  async * #getLiveEventsGenerator (filter, relays, {
    signal,
    timeoutAfterFirstEose = 500,
    timeoutForReconnectGap = 5000,
    timeoutAfterFirstReconnectGapEose = 500,
    _gapEventsGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}, { ready, readyRelays }) {
    const urls = normalizedRelayUrls(relays)
    const queue = []
    let p = Promise.withResolvers()
    let isDone = false
    const liveSubs = new Map() // url → live sub
    const retryTimers = new Map()
    const initialPending = new Set(urls)
    const initialErrors = []
    let readyTimer = null
    let isReady = false

    // Internal abort controller to cancel any in-flight reconnect gap fills on teardown
    const gapAc = new AbortController()

    // Strip time-range fields — we manage them internally
    const baseFilter = { ...filter }
    delete baseFilter.since
    delete baseFilter.until

    // Preserve until for forwarding to the live sub filter and the teardown timer
    const filterUntil = filter.until > 0 ? filter.until : null

    // lastSeenAt: the highest created_at received so far; used as since on reconnect gap fill
    let lastSeenAt = (filter.since > 0) ? filter.since : null

    // Bounded dedup set to handle overlap between reconnect gap fill and live sub.
    const seenIds = new Set()

    let untilTimer = null
    const finishReady = () => {
      if (isReady) return
      isReady = true
      clearTimeout(readyTimer)
      ready.resolve(Object.freeze({
        relays: Object.freeze([...readyRelays]),
        errors: Object.freeze([...initialErrors])
      }))
    }

    const teardown = () => {
      if (isDone) return
      isDone = true
      clearTimeout(untilTimer)
      finishReady()
      gapAc.abort()
      for (const timer of retryTimers.values()) clearTimeout(timer)
      retryTimers.clear()
      liveSubs.forEach(sub => sub.close())
      liveSubs.clear()
      p.resolve()
    }

    const pushEvent = (event, url) => {
      if (isDone || (event.id && seenIds.has(event.id))) return
      if (event.id) {
        if (seenIds.size >= 500) seenIds.delete(seenIds.values().next().value) // evict oldest
        seenIds.add(event.id)
      }
      if (event.created_at > (lastSeenAt ?? 0)) lastSeenAt = event.created_at
      event.meta = { relay: url }
      queue.push(event)
      p.resolve()
      p = Promise.withResolvers()
    }

    if (signal?.aborted) {
      finishReady()
      return
    }
    signal?.addEventListener('abort', teardown, { once: true })

    const maybeFinishInitialReady = () => {
      if (initialPending.size === 0) finishReady()
    }

    const markInitialEose = (url) => {
      readyRelays.add(url)
      if (isReady) return
      initialPending.delete(url)
      if (timeoutAfterFirstEose !== null && !readyTimer) {
        readyTimer = maybeUnref(setTimeout(finishReady, timeoutAfterFirstEose))
      }
      maybeFinishInitialReady()
    }

    const markInitialError = (url, reason) => {
      if (!initialPending.delete(url)) return
      initialErrors.push({ relay: url, reason })
      maybeFinishInitialReady()
    }

    const scheduleReconnect = (url, reconnectDelay) => {
      if (isDone || retryTimers.has(url)) return
      const nextDelay = Math.min(reconnectDelay * 2, 5 * 60_000)
      const timer = maybeUnref(setTimeout(() => {
        retryTimers.delete(url)
        subscribeToRelay(url, lastSeenAt, nextDelay)
      }, reconnectDelay))
      retryTimers.set(url, timer)
    }

    // Schedule teardown when the wall clock reaches filter.until
    if (filterUntil !== null) {
      const msUntil = filterUntil * 1000 - Date.now()
      untilTimer = maybeUnref(setTimeout(teardown, Math.max(0, msUntil)))
    }

    // Runs a reconnect gap fill for a single relay and returns a promise that resolves
    // when it completes. now is shared with the live sub so both use the same boundary.
    const runReconnectGapFill = (url, gapSince, now) => {
      const gapUntil = filterUntil !== null ? Math.min(now, filterUntil) : now
      const gapFilter = { ...baseFilter, since: gapSince, until: gapUntil }
      const gapGen = _gapEventsGenerator(gapFilter, [url], {
        timeout: timeoutForReconnectGap,
        timeoutAfterFirstEose: timeoutAfterFirstReconnectGapEose,
        signal: gapAc.signal
      })
      return (async () => {
        for await (const item of gapGen) {
          if (item?.type === 'event') pushEvent(item.event, url)
        }
      })().catch(err => {
        if (!isDone) console.error(`Reconnect gap fill error for ${url}:`, err)
      })
    }

    const subscribeToRelay = (url, gapSince, reconnectDelay = 1000) => {
      const now = Math.floor(Date.now() / 1000)
      // Don't reconnect if we're past the until boundary
      if (filterUntil !== null && now >= filterUntil) return
      this.#getRelay(url).then(relay => {
        if (isDone) return

        // Buffer post-EOSE live events while a reconnect gap fill is running so
        // the historical gap is yielded before newer socket events.
        let liveBuffer = (gapSince !== null && gapSince > 0) ? [] : null
        let liveEose = false
        let liveSub

        // Open the live sub first so the relay starts buffering incoming events
        // before we scan its database for the reconnect gap fill.
        // Forward until to the relay so it can enforce the boundary server-side.
        const liveFilter = { ...baseFilter, since: now, limit: 0 }
        if (filterUntil !== null) liveFilter.until = filterUntil
        liveSub = relay.subscribe([liveFilter], {
          onevent: (event) => {
            // A limit:0 relay may still send retained events before EOSE. Do not
            // expose them from a strictly-live stream.
            if (!liveEose) return
            if (liveBuffer) liveBuffer.push(event)
            else pushEvent(event, url)
          },
          onclose: error => {
            if (liveSubs.get(url) === liveSub) liveSubs.delete(url)
            else if (liveSubs.has(url)) return
            readyRelays.delete(url)
            if (!liveEose && !isDone) {
              const reason = error instanceof Error
                ? error
                : new Error(error ? String(error) : 'LIVE_SUBSCRIPTION_CLOSED')
              markInitialError(url, reason)
            }
            if (isDone) return
            scheduleReconnect(url, reconnectDelay)
          },
          oneose: () => {
            if (isDone || (liveSubs.has(url) && liveSubs.get(url) !== liveSub)) return
            liveEose = true
            markInitialEose(url)
          }
        })
        if (isDone) { liveSub.close(); return }
        liveSubs.set(url, liveSub)

        if (gapSince !== null && gapSince > 0) {
          runReconnectGapFill(url, gapSince, now).then(() => {
            if (isDone) return
            const buf = liveBuffer
            liveBuffer = null
            for (const event of buf) pushEvent(event, url)
          })
        }
      }).catch(err => {
        readyRelays.delete(url)
        const reason = err instanceof Error ? err : new Error(String(err))
        markInitialError(url, reason)
        if (isDone) return
        console.error(`Live subscription error at ${url}:`, reason)
        scheduleReconnect(url, reconnectDelay)
      })
    }

    if (!urls.length) {
      finishReady()
      return
    }

    for (const url of urls) {
      this.#incrementLiveSub(url)
      subscribeToRelay(url, null) // no initial gap fill — that's getEventsFeedGenerator's job
    }

    try {
      // eslint-disable-next-line no-unmodified-loop-condition
      while (!isDone || queue.length > 0) {
        if (queue.length > 0) yield queue.shift()
        else await p.promise
      }
    } finally {
      signal?.removeEventListener('abort', teardown)
      for (const url of urls) this.#decrementLiveSub(url)
      teardown()
    }
  }

  // All-in-one event feed generator. For live:true, handles the full sequence:
  //
  // - live:true (default): unless filter.limit === 0, starts the live sub immediately
  //   (so no incoming events are missed), runs an initial one-shot fetch of stored events
  //   concurrently, yields stored events first, then flushes buffered live events (deduped
  //   against stored ones), then yields live events indefinitely. With limit:0 the relay
  //   sends no stored events, so the fetch is skipped and only the live sub runs.
  // - live:false: one-shot fetch via getEventsGenerator. timeoutAfterFirstEose
  //   short-circuits after the fastest relay with events EOSEs, or waits for all
  //   relays when null.
  //
  // All underlying generators are injectable for testing.
  async * getEventsFeedGenerator (filter, relays, {
    signal,
    live = true,
    timeout = 5000,
    timeoutAfterFirstEose = 500,
    _liveGenerator = (...args) => this.getLiveEventsGenerator(...args),
    _eventsGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}) {
    if (!live) {
      const gen = _eventsGenerator(filter, relays, { timeout, timeoutAfterFirstEose, signal })
      for await (const item of gen) {
        if (item?.type === 'event') yield item.event
      }
      return
    }

    // limit:0 means "no stored events, live only" — skip the initial fetch.
    if (filter.limit === 0) {
      for await (const event of _liveGenerator(filter, relays, { signal })) {
        yield event
      }
      return
    }

    // Start live generator immediately so the relay opens the live sub and starts
    // buffering incoming events before we query stored ones.
    // Relays always send stored matching events before EOSE (unless limit:0),
    // so the initial fetch + buffering is always needed.
    const liveGen = _liveGenerator(filter, relays, { signal })
    const liveBuffer = []
    let liveDone = false
    let liveWake = Promise.withResolvers()

    const bgLoop = (async () => {
      try {
        for await (const event of liveGen) {
          liveBuffer.push(event)
          liveWake.resolve()
          liveWake = Promise.withResolvers()
        }
      } finally {
        liveDone = true
        liveWake.resolve()
      }
    })()

    try {
      // Yield stored events from the initial one-shot fetch
      const fetchGen = _eventsGenerator(filter, relays, { timeout, timeoutAfterFirstEose, signal })

      const seenIds = new Set()
      for await (const item of fetchGen) {
        if (item?.type === 'event' && !seenIds.has(item.event.id)) {
          seenIds.add(item.event.id)
          yield item.event
        }
      }

      // Flush buffered live events that arrived during the initial fetch, deduping
      // against stored ones (overlap is possible around the fetch's until boundary)
      while (liveBuffer.length > 0) {
        const event = liveBuffer.shift()
        if (!seenIds.has(event.id)) {
          seenIds.add(event.id)
          yield event
        }
      }

      // Yield subsequent live events directly — no more overlap with stored events
      // eslint-disable-next-line no-unmodified-loop-condition
      while (!liveDone || liveBuffer.length > 0) {
        while (liveBuffer.length > 0) yield liveBuffer.shift()
        if (!liveDone) await liveWake.promise
      }
    } finally {
      liveGen.return()
      await bgLoop
    }
  }

  // Returns after the first acknowledgement window. timeout is one deadline for
  // the whole operation, while timeoutUntilFirstFulfillment controls only this
  // initial return and closes pending reports when it fails. null disables either
  // timer independently. onRelayResult receives one
  // { relay, success, outcome, reason? } result per relay as it settles; outcome
  // is published, duplicate, muted, failed, or timed-out. getAuthEvent is used
  // only after auth-required or restricted publish failures, then retries once.
  // Await `promise` for the complete report, including every relay outcome.
  async sendEvent (event, relays, {
    timeout = SEND_TIMEOUT_MS,
    timeoutUntilFirstFulfillment = SEND_TIMEOUT_UNTIL_FIRST_FULFILLMENT_MS,
    getAuthEvent,
    onRelayResult
  } = {}) {
    const urls = normalizedRelayUrls(relays)
    if (!urls.length) {
      const promise = Promise.resolve(publishSummary([], urls, {
        includeSucceededRelays: true
      }))
      return { total: 0, success: false, promise }
    }

    const eventToSend = event.meta ? { ...event } : event
    if (eventToSend.meta) delete eventToSend.meta

    const sendDeferreds = urls.map(() => Promise.withResolvers())
    const sendPromises = sendDeferreds.map(({ promise }) => promise)

    // Starts before connection work so every relay shares one real deadline.
    const settlement = createPublishSettlements(sendPromises, timeout, {
      onSettled: (settlement, index) => {
        notifyRelayResult(onRelayResult, relayResultForSettlement(urls[index], settlement))
      }
    })

    // Resolves after every relay settles (or reaches the operation timeout) as
    // { success, total, fulfilled, succeededRelays, errors },
    // where errors contains { relay, reason } entries for failed relays.
    const promise = settlement.promise
      .then(settlements => publishSummary(settlements, urls, {
        includeSucceededRelays: true
      }))

    urls.forEach((url, index) => {
      const deferred = sendDeferreds[index]
      ;(async () => {
        try {
          const relay = await this.#getRelay(url)
          return await this.#publishEvent(relay, eventToSend, getAuthEvent)
        } catch (err) {
          const reason = err instanceof Error ? err : new Error(String(err))
          if (reason instanceof Nip42AuthenticationError) throw reason
          if (reason.message.startsWith('duplicate:')) return 'duplicate'
          if (reason.message.startsWith('mute:')) {
            console.info([url, reason.message].filter(Boolean).join(' - '))
            return 'muted'
          }
          throw reason
        }
      })().then(deferred.resolve, deferred.reject)
    })

    const success = await firstFulfillment(sendPromises, timeoutUntilFirstFulfillment, {
      fallback: promise.then(report => report.success)
    })
    if (!success) settlement.timeout()

    return {
      total: urls.length,
      success,
      promise
    }
  }
}

// NIP-42 permits multiple pubkeys to authenticate on one connection, so callers
// can share a RelayPool without splitting connections by authenticated identity.
export const relayPool = new RelayPool()
