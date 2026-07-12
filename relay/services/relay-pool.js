import { decodeHll, encodeHll, estimateHllCount, mergeHll } from '../helpers/hll.js'
import { firstFulfillment, publishSummary, settlePublishPromise } from '../helpers/publish.js'
import { maybeUnref } from '../helpers/timer.js'
import { RelayConnection } from './relay-connection.js'

const COUNT_TIMEOUT_MS = 5000
const COUNT_TIMEOUT_AFTER_FIRST_COUNT_MS = 500
const SEND_FIRST_FULFILLMENT_TIMEOUT_MS = 3000
const SEND_SETTLEMENT_TIMEOUT_MS = 30000

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

  // Get a relay connection, creating one if it doesn't exist
  async #getRelay (url) {
    if (this.#relays.has(url)) {
      // Only reset idle timeout when no live subscriptions are holding this relay open
      if (!this.#liveSubCounts.get(url)) {
        clearTimeout(this.#relayTimeouts.get(url))
        this.#relayTimeouts.set(url, maybeUnref(setTimeout(() => this.disconnect(url), this.#timeout)))
      }
      const relay = this.#relays.get(url)
      // Reconnect if needed to avoid SendingOnClosedConnection errors
      await relay.connect()
      return relay
    }

    const relay = new RelayConnection(url)
    this.#relays.set(url, relay)

    await relay.connect()

    if (!this.#liveSubCounts.get(url)) {
      this.#relayTimeouts.set(url, maybeUnref(setTimeout(() => this.disconnect(url), this.#timeout)))
    }

    return relay
  }

  #incrementLiveSub (url) {
    this.#liveSubCounts.set(url, (this.#liveSubCounts.get(url) ?? 0) + 1)
    // Cancel any pending idle timeout — this relay must stay open
    clearTimeout(this.#relayTimeouts.get(url))
    this.#relayTimeouts.delete(url)
  }

  #decrementLiveSub (url) {
    const next = (this.#liveSubCounts.get(url) ?? 1) - 1
    if (next <= 0) {
      this.#liveSubCounts.delete(url)
      // No more live subscriptions — start the idle timer if the relay is still pooled
      if (this.#relays.has(url)) {
        this.#relayTimeouts.set(url, maybeUnref(setTimeout(() => this.disconnect(url), this.#timeout)))
      }
    } else {
      this.#liveSubCounts.set(url, next)
    }
  }

  // Disconnect from a relay
  async disconnect (url) {
    if (this.#relays.has(url)) {
      const relay = this.#relays.get(url)
      if (relay.ws.readyState < 2) await relay.close()?.catch(console.log)
      this.#relays.delete(url)
      clearTimeout(this.#relayTimeouts.get(url))
      this.#relayTimeouts.delete(url)
    }
  }

  // Disconnect from all relays
  async disconnectAll () {
    for (const url of this.#relays.keys()) {
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
  async countEvents (filter, relays, {
    timeout = COUNT_TIMEOUT_MS,
    timeoutAfterFirstCount = COUNT_TIMEOUT_AFTER_FIRST_COUNT_MS,
    signal
  } = {}) {
    if (!filter || typeof filter !== 'object' || Array.isArray(filter)) {
      throw new Error('COUNT_FILTER_REQUIRED')
    }
    if (signal?.aborted) throw new Error('Aborted')

    const urls = [...new Set(relays || [])]
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
      timeoutTimer = maybeUnref(setTimeout(() => finish({ timedOut: true }), timeout))

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

        if (count !== null && !graceTimer && pending.size > 0) {
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

  // Get events from a list of relays
  async getEvents (filter, relays, { timeout = 5000, callback, signal } = {}) {
    const events = []
    const promises = relays.map(async (url) => {
      let sub
      let isClosed = false
      const p = Promise.withResolvers()
      const t = Promise.withResolvers()

      // Handle abort signal
      if (signal?.aborted) throw new Error('Aborted')
      const onAbort = () => {
        if (isClosed) return
        isClosed = true
        sub?.close()
        t.reject(new Error('Aborted'))
      }
      signal?.addEventListener('abort', onAbort, { once: true })

      const timer = maybeUnref(setTimeout(() => {
        if (isClosed) return
        isClosed = true
        sub?.close()
        t.reject(new Error(`timeout: ${url}`))
      }, timeout))

      ;(async () => {
        try {
          const relay = await this.#getRelay(url)
          if (isClosed) return

          // Shared resolve path: EOSE, early close (limit/ids satisfied), or normal close
          const resolveRelay = () => {
            if (isClosed) return
            isClosed = true
            sub?.close()
            p.resolve()
          }

          const checkEarlyClose = makeEarlyCloseChecker(filter, resolveRelay)

          sub = relay.subscribe([filter], {
            onevent: (event) => {
              event.meta = { relay: url }
              events.push(event)
              if (callback) callback({ type: 'event', event, relay: url })
              checkEarlyClose(event)
            },
            oninvalidevent: (event) => {
              checkEarlyClose(event)
            },
            onclose: err => {
              if (isClosed) return
              isClosed = true
              let reason
              if (err !== undefined) {
                reason = err instanceof Error ? err : new Error(String(err))
                if (callback) callback({ type: 'error', error: reason, relay: url })
              }
              // May have closed normally, without error
              reason ? p.reject(reason) : p.resolve()
            },
            oneose: resolveRelay
          })
        } catch (err) {
          if (callback) callback({ type: 'error', error: err, relay: url })
          p.reject(err)
        }
      })()

      try {
        await Promise.race([p.promise, t.promise])
      } finally {
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
      }
    })

    const results = await Promise.allSettled(promises)
    const rejectedResults = results.filter(v => v.status === 'rejected')

    return {
      result: events,
      errors: rejectedResults.map(v => ({ reason: v.reason, relay: relays[results.indexOf(v)] })),
      success: events.length > 0 || results.length !== rejectedResults.length
    }
  }

  // First to reply with EOSE and events should trigger a short timeout for the rest
  async getEventsAsap (filter, relays, { timeout = 5000, timeoutAfterFirstEose = 500, callback, signal } = {}) {
    const subs = new Map()
    const errors = []
    const events = []
    let closedRelaySubs = 0
    let isResolved = false
    let eoseTimer = null
    const p = Promise.withResolvers()

    const finalize = () => {
      if (isResolved) return
      isResolved = true
      clearTimeout(timer)
      if (eoseTimer) clearTimeout(eoseTimer)
      signal?.removeEventListener('abort', onAbort)
      subs.forEach(sub => sub.close())
      p.resolve({
        result: events,
        errors,
        success: events.length > 0 || relays.length !== errors.length
      })
    }

    // Handle abort
    const onAbort = () => {
      if (isResolved) return
      isResolved = true
      clearTimeout(timer)
      if (eoseTimer) clearTimeout(eoseTimer)
      subs.forEach(sub => sub.close())
      p.reject(new Error('Aborted'))
    }

    if (signal?.aborted) return Promise.reject(new Error('Aborted'))
    signal?.addEventListener('abort', onAbort, { once: true })

    const timer = maybeUnref(setTimeout(() => {
      finalize()
    }, timeout))

    const markClosedAndMaybeFinish = () => {
      closedRelaySubs += 1
      if (!isResolved && closedRelaySubs >= relays.length) {
        finalize()
      }
    }

    for (const url of relays) {
      this.#getRelay(url).then(relay => {
        if (isResolved) return
        let hasEvents = false

        // Shared EOSE path: actual EOSE or early close (limit/ids satisfied)
        const handleEose = () => {
          if (!subs.has(url)) return // already closed
          sub.close()
          if (hasEvents && !eoseTimer && !isResolved) {
            eoseTimer = maybeUnref(setTimeout(finalize, timeoutAfterFirstEose))
          }
        }

        const checkEarlyClose = makeEarlyCloseChecker(filter, handleEose)

        const sub = relay.subscribe([filter], {
          onevent: (event) => {
            if (isResolved) return
            hasEvents = true
            event.meta = { relay: url }
            events.push(event)
            if (callback) callback({ type: 'event', event, relay: url })
            checkEarlyClose(event)
          },
          oninvalidevent: (event) => {
            if (isResolved) return
            checkEarlyClose(event)
          },
          onclose: (err) => {
            subs.delete(url)
            if (err !== undefined) {
              const reason = err instanceof Error ? err : new Error(String(err))
              errors.push({ reason, relay: url })
              if (callback) callback({ type: 'error', error: reason, relay: url })
            }
            markClosedAndMaybeFinish()
          },
          oneose: handleEose
        })
        subs.set(url, sub)
      }).catch(error => {
        errors.push({ reason: error, relay: url })
        if (callback) callback({ type: 'error', error, relay: url })
        console.error(`Nostr relay error at ${url}: ${error}`)
        markClosedAndMaybeFinish()
      })
    }

    return p.promise
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

  async * getEventsAsapGenerator (filter, relays, options = {}) {
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

    const methodPromise = this.getEventsAsap(filter, relays, { ...options, callback })
      .catch(err => { if (err?.message !== 'Aborted') console.error('Error in getEventsAsap:', err) })
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

  // Yields live nostr events from the given relays. Stops naturally when filter.until
  // is set and the wall clock reaches that timestamp. Also stops on signal abort or
  // for-await loop exit (break/return/throw — all trigger the finally block).
  //
  // Handles two concerns:
  // 1. Live stream: a limit:0 sub keeps the relay connection open so future events are
  //    delivered in real time. filter.until is forwarded to the relay so it can enforce
  //    the boundary server-side too.
  // 2. Reconnect gap fill: on each disconnect, fetches events missed since the last event
  //    seen, with exponential backoff (1s → 5 min cap). filter.since is used as the
  //    initial gap boundary if no events have been seen yet.
  //
  // Initial fetching of stored events is the responsibility of getEventsFeedGenerator.
  // Reconnect gap fills delegate to getEventsAsapGenerator (when gapTimeoutAfterFirstEose
  // is set, default 500ms) or getEventsGenerator (when null). Both are injectable.
  // Reconnect gap events are deduplicated against live events.
  async * getLiveEventsGenerator (filter, relays, {
    signal,
    gapTimeout = 5000,
    gapTimeoutAfterFirstEose = 500,
    _gapAsapGenerator = (...args) => this.getEventsAsapGenerator(...args),
    _gapFetchGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}) {
    const queue = []
    let p = Promise.withResolvers()
    let isDone = false
    const liveSubs = new Map() // url → live sub

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

    // Bounded dedup set to handle overlap between reconnect gap fill and live sub
    const seenIds = new Set()

    let untilTimer = null
    const teardown = () => {
      if (isDone) return
      isDone = true
      clearTimeout(untilTimer)
      gapAc.abort()
      liveSubs.forEach(sub => sub.close())
      liveSubs.clear()
      p.resolve()
    }

    const pushEvent = (event, url) => {
      if (isDone || seenIds.has(event.id)) return
      if (seenIds.size >= 500) seenIds.delete(seenIds.values().next().value) // evict oldest
      seenIds.add(event.id)
      if (event.created_at > (lastSeenAt ?? 0)) lastSeenAt = event.created_at
      event.meta = { relay: url }
      queue.push(event)
      p.resolve()
      p = Promise.withResolvers()
    }

    if (signal?.aborted) return
    signal?.addEventListener('abort', teardown, { once: true })

    // Schedule teardown when the wall clock reaches filter.until
    if (filterUntil !== null) {
      const msUntil = filterUntil * 1000 - Date.now()
      untilTimer = maybeUnref(setTimeout(teardown, msUntil))
    }

    // Runs a reconnect gap fill for a single relay and returns a promise that resolves
    // when it completes. now is shared with the live sub so both use the same boundary.
    const runReconnectGapFill = (url, gapSince, now) => {
      const gapUntil = filterUntil !== null ? Math.min(now, filterUntil) : now
      const gapFilter = { ...baseFilter, since: gapSince, until: gapUntil }
      const gapGen = gapTimeoutAfterFirstEose !== null
        ? _gapAsapGenerator(gapFilter, [url], { timeout: gapTimeout, timeoutAfterFirstEose: gapTimeoutAfterFirstEose, signal: gapAc.signal })
        : _gapFetchGenerator(gapFilter, [url], { timeout: gapTimeout, signal: gapAc.signal })
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

        // Buffer live events while a reconnect gap fill is running so stored events
        // are yielded first. Flushed (with dedup) once gap fill completes.
        let liveBuffer = (gapSince !== null && gapSince > 0) ? [] : null

        // Open the live sub first so the relay starts buffering incoming events
        // before we scan its database for the reconnect gap fill.
        // Forward until to the relay so it can enforce the boundary server-side.
        const liveFilter = { ...baseFilter, since: now, limit: 0 }
        if (filterUntil !== null) liveFilter.until = filterUntil
        const liveSub = relay.subscribe([liveFilter], {
          onevent: (event) => {
            if (liveBuffer) liveBuffer.push(event)
            else pushEvent(event, url)
          },
          onclose: () => {
            liveSubs.delete(url)
            if (isDone) return
            const delay = reconnectDelay
            setTimeout(
              () => subscribeToRelay(url, lastSeenAt, Math.min(reconnectDelay * 2, 5 * 60_000)),
              delay
            )
          },
          oneose: () => { /* keep open for live events */ }
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
        if (isDone) return
        console.error(`Live subscription error at ${url}:`, err)
        const delay = reconnectDelay
        setTimeout(
          () => subscribeToRelay(url, lastSeenAt, Math.min(reconnectDelay * 2, 5 * 60_000)),
          delay
        )
      })
    }

    for (const url of relays) {
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
      for (const url of relays) this.#decrementLiveSub(url)
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
  // - live:false + timeoutAfterFirstEose set: one-shot fetch via getEventsAsapGenerator —
  //   short-circuits once the fastest relay with events has EOSEd.
  // - live:false + timeoutAfterFirstEose:null: one-shot fetch via getEventsGenerator —
  //   waits for all relays to EOSE naturally.
  //
  // All underlying generators are injectable for testing.
  async * getEventsFeedGenerator (filter, relays, {
    signal,
    live = true,
    timeout = 5000,
    timeoutAfterFirstEose = 500,
    _liveGenerator = (...args) => this.getLiveEventsGenerator(...args),
    _asapGenerator = (...args) => this.getEventsAsapGenerator(...args),
    _fetchGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}) {
    if (!live) {
      const gen = timeoutAfterFirstEose !== null
        ? _asapGenerator(filter, relays, { timeout, timeoutAfterFirstEose, signal })
        : _fetchGenerator(filter, relays, { timeout, signal })
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
      const fetchGen = timeoutAfterFirstEose !== null
        ? _asapGenerator(filter, relays, { timeout, timeoutAfterFirstEose, signal })
        : _fetchGenerator(filter, relays, { timeout, signal })

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

  // Returns after the first acknowledgement window. onRelayResult receives one
  // { relay, success, outcome, reason? } result per relay as it settles; outcome
  // is published, duplicate, muted, failed, or timed-out. getAuthEvent is used
  // only after auth-required or restricted publish failures, then retries once.
  // Await `promise` for the complete report, including every relay outcome.
  async sendEvent (event, relays, {
    firstFulfillmentTimeoutMs = SEND_FIRST_FULFILLMENT_TIMEOUT_MS,
    settlementTimeoutMs = SEND_SETTLEMENT_TIMEOUT_MS,
    getAuthEvent,
    onRelayResult
  } = {}) {
    const urls = relays || []
    if (!urls.length) {
      const promise = Promise.resolve(publishSummary([], urls, {
        result: null,
        includeSucceededRelays: true
      }))
      return { result: null, total: 0, success: false, promise }
    }

    const eventToSend = event.meta ? { ...event } : event
    if (eventToSend.meta) delete eventToSend.meta

    const sendPromises = urls.map(async (url) => {
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
    })

    // Resolves after every relay settles (or reaches the settlement timeout) as
    // { result: null, success, total, fulfilled, succeededRelays, errors },
    // where errors contains { relay, reason } entries for failed relays.
    const promise = Promise
      .all(sendPromises.map((promise, index) => settlePublishPromise(promise, settlementTimeoutMs, {
        onSettled: settlement => {
          notifyRelayResult(onRelayResult, relayResultForSettlement(urls[index], settlement))
        }
      })))
      .then(settlements => publishSummary(settlements, urls, {
        result: null,
        includeSucceededRelays: true
      }))
    const success = await firstFulfillment(sendPromises, firstFulfillmentTimeoutMs)

    return {
      result: null,
      total: urls.length,
      success,
      promise
    }
  }
}

// NIP-42 permits multiple pubkeys to authenticate on one connection, so callers
// can share a RelayPool without splitting connections by authenticated identity.
export const relayPool = new RelayPool()
