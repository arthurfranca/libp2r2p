import { ValidationError } from '../../error/index.js'
import { decodeHll, encodeHll, estimateHllCount, mergeHll } from '../helpers/hll.js'
import { createPublishSettlements, firstFulfillment, publishSummary } from '../helpers/publish.js'
import { ReadAdmission } from '../helpers/read-admission.js'
import { drainableStream } from '../helpers/drainable-stream.js'
import { maybeUnref } from '../helpers/timer.js'
import { categorizeRelayError } from '../helpers/error.js'
import { normalizeRelayUrl } from '../../url/index.js'
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

function asError (error) {
  return error instanceof Error ? error : new Error(String(error))
}

function assertReadOptions (filter, timeouts) {
  if (!filter || typeof filter !== 'object' || Array.isArray(filter)) throw new ValidationError('INVALID_FILTER')
  for (const value of Object.values(timeouts)) {
    if (value !== null && (!Number.isFinite(value) || value < 0)) throw new ValidationError('INVALID_RELAY_TIMEOUT')
  }
}

function assertBufferOptions (options) {
  for (const value of [options.maxBufferedLiveEvents === undefined ? 1000 : options.maxBufferedLiveEvents, options.maxBufferedLiveBytes === undefined ? 8 * 1024 * 1024 : options.maxBufferedLiveBytes]) {
    if (!Number.isSafeInteger(value) || value < 1) throw new ValidationError('INVALID_RELAY_BUFFER_CAPACITY')
  }
}

function normalizedRelayUrls (relays) {
  const urls = []
  const seen = new Set()

  for (const relay of relays || []) {
    const normalizedUrl = normalizeRelayUrl(relay)
    if (seen.has(normalizedUrl)) continue
    seen.add(normalizedUrl)
    // Keep the caller's first spelling in reports and envelopes while using the
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
    if (reason.category) this.category = reason.category
    if (reason.code !== undefined) this.code = reason.code
  }
}

// Interacts with Nostr relays
export class RelayPool {
  #relays = new Map()
  #relayTimeouts = new Map()
  #liveSubCounts = new Map() // url -> number of active live subscriptions
  #timeout = 30000 // 30 seconds
  #createRelay
  #admission

  constructor ({ _createRelay = url => new RelayConnection(url), ...capacity } = {}) {
    this.#createRelay = _createRelay
    this.#admission = new ReadAdmission(capacity)
  }

  #scheduleIdleDisconnect (url) {
    clearTimeout(this.#relayTimeouts.get(url))
    this.#relayTimeouts.set(url, maybeUnref(setTimeout(() => {
      if (this.#admission.states.get(url)?.active) this.#scheduleIdleDisconnect(url)
      else this.disconnect(url)
    }, this.#timeout)))
  }

  // Opens a normalized pooled connection. Failed connects are evicted so a later
  // retry creates a fresh RelayConnection instead of reusing broken socket state.
  async #getRelay (url) {
    const normalizedUrl = normalizeRelayUrl(url)
    let relay = this.#relays.get(normalizedUrl)
    if (!relay) {
      relay = this.#createRelay(normalizedUrl)
      this.#relays.set(normalizedUrl, relay)
    }

    try {
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
      throw categorizeRelayError(error, error?.category ?? 'connection')
    }

    // Only reset idle timeout when no live subscriptions are holding this relay open.
    if (!this.#liveSubCounts.get(normalizedUrl)) this.#scheduleIdleDisconnect(normalizedUrl)
    return relay
  }

  #incrementLiveSub (url) {
    const normalizedUrl = normalizeRelayUrl(url)
    this.#liveSubCounts.set(normalizedUrl, (this.#liveSubCounts.get(normalizedUrl) ?? 0) + 1)
    // Cancel any pending idle timeout — this relay must stay open
    clearTimeout(this.#relayTimeouts.get(normalizedUrl))
    this.#relayTimeouts.delete(normalizedUrl)
  }

  #decrementLiveSub (url) {
    const normalizedUrl = normalizeRelayUrl(url)
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
    const normalizedUrl = normalizeRelayUrl(url)
    this.#admission.cancelQueued(normalizedUrl)
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
    for (const url of new Set([...this.#relays.keys(), ...this.#admission.states.keys()])) {
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
      throw new ValidationError('COUNT_FILTER_REQUIRED')
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
  // the operation deadline. Disabling cross-relay deduplication still suppresses
  // repeated ids from the same relay; callbacks remain immediate in both modes.
  async getEvents (filter, relays, { timeout = 5000, timeoutAfterFirstEose = 500, callback, signal, deduplicateAcrossRelays = true, queueTimeout = 30000, _admissions } = {}) {
    assertReadOptions(filter, { timeout, timeoutAfterFirstEose, queueTimeout })
    if (typeof deduplicateAcrossRelays !== 'boolean') throw new ValidationError('INVALID_DEDUPLICATE_ACROSS_RELAYS')
    if (signal?.aborted) throw new Error('Aborted')
    const urls = normalizedRelayUrls(relays)
    const subscriptions = new Map()
    const outcomes = new Map()
    const errors = []
    const events = []
    const eventIds = deduplicateAcrossRelays ? new Set() : null
    let isResolved = false
    let eoseTimer = null
    const timers = new Map()
    const leases = new Map()
    const admissionAbort = new AbortController()
    const admissionSignal = signal ? AbortSignal.any([signal, admissionAbort.signal]) : admissionAbort.signal

    return await new Promise((resolve, reject) => {
      const cleanup = () => {
        admissionAbort.abort()
        for (const timer of timers.values()) clearTimeout(timer)
        clearTimeout(eoseTimer)
        signal?.removeEventListener('abort', onAbort)
        for (const sub of subscriptions.values()) sub.close()
        subscriptions.clear()
        for (const lease of leases.values()) lease.release()
        leases.clear()
      }
      const fail = error => {
        if (isResolved) return
        isResolved = true
        cleanup()
        reject(error)
      }
      const notify = item => {
        try { callback?.(item) } catch (error) { fail(error) }
      }
      const settleRelay = (relay, status, error) => {
        if (isResolved || outcomes.has(relay)) return
        outcomes.set(relay, { relay, status, ...(error ? { error } : {}) })
        clearTimeout(timers.get(relay))
        subscriptions.get(relay)?.close()
        subscriptions.delete(relay)
        leases.get(relay)?.release()
        leases.delete(relay)
        if (error) {
          errors.push({ relay, reason: error })
          notify({ type: 'error', relay, error })
        }
      }
      const finish = (pendingStatus = 'cutoff') => {
        if (isResolved) return
        for (const url of urls) {
          if (!outcomes.has(url)) settleRelay(url, pendingStatus, pendingStatus === 'timeout' ? getEventsTimeoutError() : undefined)
        }
        if (isResolved) return
        const relays = urls.map(url => outcomes.get(url))
        notify({ type: 'eose', relays })
        if (isResolved) return
        isResolved = true
        cleanup()
        resolve({
          result: events,
          errors,
          success: events.length > 0 || relays.some(({ status }) => ['eose', 'satisfied', 'closed'].includes(status)),
          relays
        })
      }
      const finishIfComplete = () => { if (outcomes.size === urls.length) finish() }
      const onAbort = () => fail(new Error('Aborted'))
      signal?.addEventListener('abort', onAbort, { once: true })
      if (!urls.length) { finish(); return }

      for (const url of urls) {
        const seenIds = eventIds ?? new Set()
        const reservation = _admissions?.get(url) ?? this.#admission.acquire(normalizeRelayUrl(url), { history: true, signal: admissionSignal, queueTimeout })
        reservation.then(async slots => {
          const lease = slots.history
          if (isResolved || outcomes.has(url)) { lease.release(); return }
          leases.set(url, lease)
          if (timeout !== null) {
            timers.set(url, maybeUnref(setTimeout(() => {
              settleRelay(url, 'timeout', getEventsTimeoutError())
              finishIfComplete()
            }, timeout)))
          }
          const relay = await this.#getRelay(url)
          if (isResolved || outcomes.has(url)) return
          let hasEvents = false
          // Subscription callbacks may run before subscribe() returns.
          // eslint-disable-next-line prefer-const
          let sub
          const complete = status => {
            if (isResolved || outcomes.has(url)) return
            settleRelay(url, status)
            sub?.close()
            subscriptions.delete(url)
            if (hasEvents && timeoutAfterFirstEose !== null && !eoseTimer && !isResolved) {
              eoseTimer = maybeUnref(setTimeout(() => finish('cutoff'), timeoutAfterFirstEose))
            }
            finishIfComplete()
          }
          const checkEarlyClose = makeEarlyCloseChecker(filter, () => complete('satisfied'))
          sub = relay.subscribe([filter], {
            onevent: event => {
              if (isResolved || outcomes.has(url)) return
              hasEvents = true
              if (!event?.id || !seenIds.has(event.id)) {
                if (event?.id) seenIds.add(event.id)
                events.push({ event, relay: url })
                notify({ type: 'event', event, relay: url })
              }
              checkEarlyClose(event)
            },
            oninvalidevent: () => {
              if (!isResolved && !outcomes.has(url)) checkEarlyClose()
            },
            onclose: error => {
              if (isResolved || outcomes.has(url)) return
              const reason = error === undefined ? undefined : asError(error)
              settleRelay(url, reason ? 'error' : 'closed', reason)
              subscriptions.delete(url)
              finishIfComplete()
            },
            oneose: () => complete('eose')
          })
          if (isResolved || outcomes.has(url)) sub.close()
          else subscriptions.set(url, sub)
        }).catch(error => {
          settleRelay(url, 'error', asError(error))
          finishIfComplete()
        })
      }
    })
  }

  getEventsGenerator (filter, relays, options = {}) {
    return drainableStream(options => this.#getEventsGenerator(filter, relays, options), options)
  }

  async * #getEventsGenerator (filter, relays, options = {}) {
    const queue = []
    let p = Promise.withResolvers()
    let isDone = false
    let failure
    const controller = new AbortController()
    const callback = item => {
      queue.push(item)
      p.resolve()
      p = Promise.withResolvers()
      options.callback?.(item)
    }
    const signal = AbortSignal.any([controller.signal, ...[options.signal, options._stopSignal].filter(Boolean)])
    // Attach rejection handling immediately, but propagate general failures to
    // the consumer after accepted deliveries instead of swallowing them.
    const methodPromise = this.getEvents(filter, relays, { ...options, signal, callback })
      .catch(error => { if (!signal.aborted) failure = error })
      .finally(() => { isDone = true; p.resolve() })
    try {
      // eslint-disable-next-line no-unmodified-loop-condition
      while (!isDone || queue.length > 0) {
        if (options.signal?.aborted) return
        if (queue.length > 0) yield queue.shift()
        else await p.promise
      }
      const report = await methodPromise
      if (failure) throw failure
      return report
    } finally {
      controller.abort()
      queue.length = 0
    }
  }

  // Returns a strictly-live stream. `ready` reports the first initial EOSE window,
  // while `readyRelays` follows relays that are currently past their own EOSE.
  getLiveEventsGenerator (filter, relays, options = {}) {
    assertBufferOptions(options)
    assertReadOptions(filter, { timeout: options.timeout === undefined ? 5000 : options.timeout, timeoutAfterFirstEose: options.timeoutAfterFirstEose === undefined ? 500 : options.timeoutAfterFirstEose, queueTimeout: options.queueTimeout === undefined ? 30000 : options.queueTimeout })
    const ready = Promise.withResolvers()
    const readyRelays = new Set()
    const stream = drainableStream(options => this.#getLiveEventsGenerator(filter, relays, options, { ready, readyRelays }), options)

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
    _stopSignal,
    timeout = 5000,
    timeoutAfterFirstEose = 500,
    queueTimeout = 30000,
    _admissions,
    maxBufferedLiveEvents = 1000,
    maxBufferedLiveBytes = 8 * 1024 * 1024,
    timeoutForReconnectGap = 5000,
    timeoutAfterFirstReconnectGapEose = 500,
    _gapEventsGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}, { ready, readyRelays }) {
    const urls = normalizedRelayUrls(relays)
    const queue = []
    let p = Promise.withResolvers()
    let isDone = false
    let draining = false
    let failure
    let queuedBytes = 0
    const encoder = new TextEncoder()
    const gapTasks = new Set()
    const liveSubs = new Map() // url → live sub
    const retryTimers = new Map()
    const initialPending = new Set(urls)
    const initialOutcomes = new Map()
    const readyTimeouts = new Map()
    const liveLeases = new Map()
    const initialAdmissions = new Set()
    let readyTimer = null
    let isReady = false

    // Stop recovery input on teardown, preserving its accepted history for a drain.
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
    const overflow = relay => {
      failure = Object.assign(new Error('RELAY_LIVE_BUFFER_FULL'), { code: 'RELAY_LIVE_BUFFER_FULL', relay, phase: 'live-buffer' })
      teardown()
    }
    const enqueue = item => {
      const bytes = encoder.encode(JSON.stringify(item)).byteLength
      if (queue.length >= maxBufferedLiveEvents || queuedBytes + bytes > maxBufferedLiveBytes) {
        overflow(item.relay)
        return
      }
      queuedBytes += bytes
      queue.push({ item, bytes })
      p.resolve()
      p = Promise.withResolvers()
    }
    const shift = () => {
      const { item, bytes } = queue.shift()
      queuedBytes -= bytes
      return item
    }
    const finishReady = (pendingStatus = 'cutoff', emit = true) => {
      if (isReady) return
      isReady = true
      clearTimeout(readyTimer)
      for (const timer of readyTimeouts.values()) clearTimeout(timer)
      for (const relay of initialPending) {
        const error = pendingStatus === 'timeout' ? getEventsTimeoutError() : undefined
        initialOutcomes.set(relay, { relay, status: pendingStatus, ...(error ? { error } : {}) })
        if (error && emit) enqueue({ type: 'error', relay, error })
      }
      initialPending.clear()
      const relays = urls.map(url => initialOutcomes.get(url))
      ready.resolve(Object.freeze({
        relays: Object.freeze([...readyRelays]),
        errors: Object.freeze(relays.filter(item => item.error).map(({ relay, error }) => ({ relay, reason: error })))
      }))
      if (emit) enqueue({ type: 'eose', relays })
    }

    const teardown = (drain = false) => {
      if (isDone && (drain || !draining)) return
      draining = drain
      isDone = true
      if (!drain) { queue.length = 0; queuedBytes = 0 }
      clearTimeout(untilTimer)
      finishReady('closed', false)
      gapAc.abort()
      for (const timer of retryTimers.values()) clearTimeout(timer)
      retryTimers.clear()
      liveSubs.forEach(sub => sub.close())
      liveSubs.clear()
      for (const lease of liveLeases.values()) lease.release()
      liveLeases.clear()
      p.resolve()
    }

    const pushEvent = (event, url, accepted = false) => {
      // Recovery queues accepted these events before input was stopped.
      if ((isDone && !(draining && accepted)) || (event.id && seenIds.has(event.id))) return
      if (event.id) {
        if (seenIds.size >= 500) seenIds.delete(seenIds.values().next().value) // evict oldest
        seenIds.add(event.id)
      }
      if (event.created_at > (lastSeenAt ?? 0)) lastSeenAt = event.created_at
      enqueue({ type: 'event', event, relay: url })
    }

    if (signal?.aborted || _stopSignal?.aborted) {
      finishReady('closed', false)
      return
    }
    const abort = () => teardown()
    const stop = () => teardown(true)
    signal?.addEventListener('abort', abort, { once: true })
    _stopSignal?.addEventListener('abort', stop, { once: true })

    const maybeFinishInitialReady = () => {
      if (initialPending.size === 0) finishReady()
    }

    const markInitialEose = (url) => {
      clearTimeout(readyTimeouts.get(url))
      readyRelays.add(url)
      if (isReady || !initialPending.delete(url)) return
      initialOutcomes.set(url, { relay: url, status: 'eose' })
      if (timeoutAfterFirstEose !== null && !readyTimer) {
        readyTimer = maybeUnref(setTimeout(() => finishReady('cutoff'), timeoutAfterFirstEose))
      }
      maybeFinishInitialReady()
    }

    const reportFailure = (url, error) => {
      if (isDone) return
      enqueue({ type: 'error', relay: url, error })
      if (!isReady && initialPending.delete(url)) {
        clearTimeout(readyTimeouts.get(url))
        initialOutcomes.set(url, { relay: url, status: 'error', error })
        maybeFinishInitialReady()
      }
    }
    const reportClosed = url => {
      if (!isReady && initialPending.delete(url)) {
        clearTimeout(readyTimeouts.get(url))
        initialOutcomes.set(url, { relay: url, status: 'closed' })
        maybeFinishInitialReady()
      }
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
      untilTimer = maybeUnref(setTimeout(() => { finishReady('closed'); teardown(true) }, Math.max(0, msUntil)))
    }

    // Runs a reconnect gap fill for a single relay and returns a promise that resolves
    // when it completes. now is shared with the live sub so both use the same boundary.
    const runReconnectGapFill = (url, gapSince, now, slots) => {
      const gapUntil = filterUntil !== null ? Math.min(now, filterUntil) : now
      const gapFilter = { ...baseFilter, since: gapSince, until: gapUntil }
      const gapGen = _gapEventsGenerator(gapFilter, [url], {
        _admissions: new Map([[url, Promise.resolve(slots)]]),
        queueTimeout,
        timeout: timeoutForReconnectGap,
        timeoutAfterFirstEose: timeoutAfterFirstReconnectGapEose,
        signal,
        _stopSignal: gapAc.signal
      })
      return (async () => {
        for await (const item of gapGen) {
          if (item?.type === 'event') pushEvent(item.event, url, true)
          else if (item?.type === 'error' && !isDone) enqueue(item)
        }
      })().catch(err => {
        reportFailure(url, asError(err))
      }).finally(() => slots.history.release())
    }

    const subscribeToRelay = (url, gapSince, reconnectDelay = 1000) => {
      let now = Math.floor(Date.now() / 1000)
      // Don't reconnect if we're past the until boundary
      if (filterUntil !== null && now >= filterUntil) return
      const reservation = !initialAdmissions.has(url) && _admissions?.get(url)
      const recovering = gapSince !== null && gapSince > 0
      let recoveryLease
      initialAdmissions.add(url)
      Promise.resolve(reservation || this.#admission.acquire(normalizeRelayUrl(url), { live: true, history: recovering, signal: gapAc.signal, queueTimeout })).then(async slots => {
        const lease = slots.live
        recoveryLease = recovering ? slots.history : null
        if (isDone) { lease.release(); recoveryLease?.release(); return }
        liveLeases.set(url, lease)
        if (timeout !== null && initialPending.has(url)) {
          readyTimeouts.set(url, maybeUnref(setTimeout(() => {
            if (!initialPending.delete(url)) return
            const error = getEventsTimeoutError()
            initialOutcomes.set(url, { relay: url, status: 'timeout', error })
            enqueue({ type: 'error', relay: url, error })
            maybeFinishInitialReady()
          }, timeout)))
        }
        const relay = await this.#getRelay(url)
        if (isDone) { recoveryLease?.release(); return }
        now = Math.floor(Date.now() / 1000)
        if (filterUntil !== null && now >= filterUntil) { lease.release(); recoveryLease?.release(); return }

        // Buffer post-EOSE live events while a reconnect gap fill is running so
        // the historical gap is yielded before newer socket events.
        let liveBuffer = (gapSince !== null && gapSince > 0) ? [] : null
        let liveEose = false
        let bufferedBytes = 0

        // Open the live sub first so the relay starts buffering incoming events
        // before we scan its database for the reconnect gap fill.
        // Forward until to the relay so it can enforce the boundary server-side.
        const liveFilter = { ...baseFilter, since: now, limit: 0 }
        if (filterUntil !== null) liveFilter.until = filterUntil
        const liveSub = relay.subscribe([liveFilter], {
          onevent: (event) => {
            // A limit:0 relay may still send retained events before EOSE. Do not
            // expose them from a strictly-live stream.
            if (isDone || liveSubs.get(url) !== liveSub || !liveEose) return
            if (liveBuffer) {
              const bytes = encoder.encode(JSON.stringify(event)).byteLength
              if (liveBuffer.length >= maxBufferedLiveEvents || bufferedBytes + bytes > maxBufferedLiveBytes) {
                overflow(url)
                return
              }
              bufferedBytes += bytes
              liveBuffer.push(event)
            } else pushEvent(event, url)
          },
          onclose: error => {
            if (liveSubs.get(url) === liveSub) liveSubs.delete(url)
            else if (liveSubs.has(url)) return
            lease.release()
            if (liveLeases.get(url) === lease) liveLeases.delete(url)
            readyRelays.delete(url)
            if (!isDone) {
              if (error !== undefined) reportFailure(url, asError(error))
              else if (!liveEose) reportClosed(url)
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
        if (isDone) { liveSub.close(); recoveryLease?.release(); return }
        liveSubs.set(url, liveSub)

        if (gapSince !== null && gapSince > 0) {
          const task = runReconnectGapFill(url, gapSince, now, slots).finally(() => {
            const buf = liveBuffer
            liveBuffer = null
            for (const event of buf) pushEvent(event, url, true)
            gapTasks.delete(task)
            p.resolve()
          })
          gapTasks.add(task)
        }
      }).catch(err => {
        recoveryLease?.release()
        liveLeases.get(url)?.release()
        liveLeases.delete(url)
        readyRelays.delete(url)
        const reason = err instanceof Error ? err : new Error(String(err))
        reportFailure(url, reason)
        if (isDone) return
        scheduleReconnect(url, reconnectDelay)
      })
    }

    if (!urls.length) {
      finishReady()
      try {
        if (failure) throw failure
        yield shift()
      } finally {
        teardown()
        signal?.removeEventListener('abort', abort)
        _stopSignal?.removeEventListener('abort', stop)
      }
      return
    }

    for (const url of urls) {
      this.#incrementLiveSub(url)
      subscribeToRelay(url, null) // no initial gap fill — that's getEventsFeedGenerator's job
    }

    try {
      // eslint-disable-next-line no-unmodified-loop-condition
      while (!isDone || (draining && gapTasks.size > 0) || queue.length > 0) {
        if (signal?.aborted) break
        if (queue.length > 0) yield shift()
        else { await p.promise; p = Promise.withResolvers() }
      }
      if (failure) throw failure
    } finally {
      signal?.removeEventListener('abort', abort)
      _stopSignal?.removeEventListener('abort', stop)
      for (const url of urls) this.#decrementLiveSub(url)
      teardown()
    }
  }

  // All-in-one event feed generator. For live:true, handles the full sequence:
  //
  // - live:true (default): unless filter.limit === 0, reserves live+history capacity,
  //   starts live input, then runs an initial one-shot fetch of stored events
  //   concurrently, yields stored events first, then flushes buffered live events (deduped
  //   against stored ones), then yields live events indefinitely. With limit:0 the relay
  //   sends no stored events, so the fetch is skipped and only the live sub runs.
  // - live:false: one-shot fetch via getEventsGenerator. timeoutAfterFirstEose
  //   short-circuits after the fastest relay with events EOSEs, or waits for all
  //   relays when null.
  //
  // snapshot:true captures a historical until after initial live readiness;
  // that bound is reported in the history marker and never stops live input.
  // All underlying generators are injectable for testing.
  getEventsFeedGenerator (filter, relays, options = {}) {
    if (options.snapshot !== undefined && typeof options.snapshot !== 'boolean') throw new ValidationError('INVALID_RELAY_SNAPSHOT')
    assertBufferOptions(options)
    assertReadOptions(filter, { timeout: options.timeout === undefined ? 5000 : options.timeout, timeoutAfterFirstEose: options.timeoutAfterFirstEose === undefined ? 500 : options.timeoutAfterFirstEose, queueTimeout: options.queueTimeout === undefined ? 30000 : options.queueTimeout })
    return drainableStream(options => this.#getEventsFeedGenerator(filter, relays, options), options)
  }

  async * #getEventsFeedGenerator (filter, relays, {
    signal,
    _stopSignal,
    live = true,
    snapshot = false,
    timeout = 5000,
    timeoutAfterFirstEose = 500,
    queueTimeout = 30000,
    maxBufferedLiveEvents = 1000,
    maxBufferedLiveBytes = 8 * 1024 * 1024,
    _liveGenerator = (...args) => this.getLiveEventsGenerator(...args),
    _eventsGenerator = (...args) => this.getEventsGenerator(...args)
  } = {}) {
    if (signal.aborted || _stopSignal.aborted) return
    const options = { timeout, timeoutAfterFirstEose, queueTimeout, signal, _stopSignal }
    if (!live) {
      const bounds = { since: filter.since ?? 0, until: Math.min(filter.until ?? Infinity, Math.floor(Date.now() / 1000)) }
      for await (const item of _eventsGenerator(snapshot ? { ...filter, ...bounds } : filter, relays, options)) {
        if (signal.aborted) return
        yield snapshot && item.type === 'eose' ? { ...item, snapshot: bounds } : item
      }
      return
    }

    // limit:0 is a live-only stream and reports that stream's initial window.
    if (filter.limit === 0) {
      yield * _liveGenerator(filter, relays, options)
      return
    }

    const urls = normalizedRelayUrls(relays)
    const inputAbort = new AbortController()
    const inputSignal = AbortSignal.any([signal, inputAbort.signal])
    const admissionSignal = AbortSignal.any([inputSignal, _stopSignal])
    const reservations = new Map()
    const leases = new Set()
    for (const url of urls) {
      const reservation = this.#admission.acquire(normalizeRelayUrl(url), { history: true, live: true, signal: admissionSignal, queueTimeout }).then(slots => {
        for (const lease of Object.values(slots)) {
          leases.add(lease)
          if (admissionSignal.aborted) lease.release()
        }
        return slots
      })
      // Both readers consume the same attempt, including admission failures.
      reservation.catch(() => {})
      reservations.set(url, reservation)
    }
    const readOptions = { ...options, signal: inputSignal, _admissions: reservations, maxBufferedLiveEvents, maxBufferedLiveBytes }
    const liveFilter = { ...filter }
    if (snapshot) delete liveFilter.until
    const liveGen = _liveGenerator(liveFilter, urls, readOptions)
    const liveBuffer = []
    const encoder = new TextEncoder()
    let bufferedBytes = 0
    let liveDone = false
    let liveFailure
    let liveWake = Promise.withResolvers()
    const initialReady = Promise.withResolvers()
    const shift = () => {
      const entry = liveBuffer.shift()
      bufferedBytes -= entry.bytes
      return entry.item
    }
    const bgLoop = (async () => {
      try {
        for await (const item of liveGen) {
          if (inputSignal.aborted) break
          if (item.type === 'eose') initialReady.resolve()
          else {
            const bytes = encoder.encode(JSON.stringify(item)).byteLength
            if (liveBuffer.length >= maxBufferedLiveEvents || bufferedBytes + bytes > maxBufferedLiveBytes) {
              throw Object.assign(new Error('RELAY_LIVE_BUFFER_FULL'), { code: 'RELAY_LIVE_BUFFER_FULL', relay: item.relay, phase: 'live-buffer' })
            }
            liveBuffer.push({ item, bytes })
            bufferedBytes += bytes
          }
          liveWake.resolve()
          liveWake = Promise.withResolvers()
        }
      } catch (error) {
        liveFailure = error
        if (error.code === 'RELAY_LIVE_BUFFER_FULL') inputAbort.abort()
      } finally {
        liveDone = true
        initialReady.resolve()
        liveWake.resolve()
      }
    })()

    try {
      if (snapshot) await initialReady.promise
      if (liveFailure?.code === 'RELAY_LIVE_BUFFER_FULL') throw liveFailure
      if (signal.aborted) return
      const bounds = { since: filter.since ?? 0, until: Math.min(filter.until ?? Infinity, Math.floor(Date.now() / 1000)) }
      const historyFilter = snapshot ? { ...filter, ...bounds } : filter
      const fetchGen = _eventsGenerator(historyFilter, urls, readOptions)
      const seenIds = new Set()
      for await (const item of fetchGen) {
        if (liveFailure?.code === 'RELAY_LIVE_BUFFER_FULL') throw liveFailure
        if (signal.aborted) return
        if (item?.type === 'event' && !seenIds.has(item.event.id)) {
          seenIds.add(item.event.id)
          yield item
        } else if (item?.type !== 'event') {
          yield snapshot && item.type === 'eose' ? { ...item, snapshot: bounds } : item
        }
      }
      if (liveFailure?.code === 'RELAY_LIVE_BUFFER_FULL') throw liveFailure

      while (liveBuffer.length > 0) {
        if (signal.aborted) return
        const item = shift()
        if (item.type !== 'event' || !seenIds.has(item.event.id)) {
          if (item.type === 'event') seenIds.add(item.event.id)
          yield item
        }
      }
      seenIds.clear()

      // eslint-disable-next-line no-unmodified-loop-condition
      while (!liveDone || liveBuffer.length > 0) {
        if (liveFailure?.code === 'RELAY_LIVE_BUFFER_FULL') throw liveFailure
        while (liveBuffer.length > 0) {
          if (signal.aborted) return
          yield shift()
        }
        if (!liveDone) await liveWake.promise
      }
      if (liveFailure) throw liveFailure
    } finally {
      inputAbort.abort()
      await liveGen.return()
      await bgLoop
      liveBuffer.length = 0
      for (const lease of leases) lease.release()
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
        if (settlement.reason?.category === 'timeout' && !settlement.reason.cause) {
          const relay = this.#relays.get(normalizeRelayUrl(urls[index]))
          if (relay?.lastTransportError) settlement.reason.cause = relay.lastTransportError
        }
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
