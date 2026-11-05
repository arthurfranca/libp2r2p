import { isValidEvent } from '../../event/index.js'
import { ValidationError } from '../../error/index.js'
import { maybeUnref } from '../helpers/timer.js'
import { categorizeRelayError, relayCloseError, relayTimeoutError, relayRejectionError } from '../helpers/error.js'
import { debugRateLimitRetry } from '../helpers/retry-debug.js'
import { DEFAULT_RATE_LIMIT_CAP_MS } from '../helpers/retry-advice.js'

const DEFAULT_CONNECT_TIMEOUT = 3000
const DEFAULT_OPERATION_TIMEOUT = 30000
const BULK_PACING_MS = 1000
const RATE_LIMIT_BACKOFF_MAX_MS = 30000
const DEFAULT_MAX_RATE_LIMIT_RETRY_WAIT_MS = 1500
const OPERATION_TYPES = ['EVENT', 'AUTH', 'REQ', 'COUNT']

function errorFrom (reason, fallback) {
  return reason instanceof Error ? reason : new Error(String(reason || fallback))
}

function retryAtFrom (value) {
  const pending = [value]
  const seen = new Set()
  let retryAt = 0
  while (pending.length) {
    const current = pending.pop()
    if (!current || typeof current !== 'object' || seen.has(current)) continue
    seen.add(current)
    if (Number.isSafeInteger(current.retryAt)) retryAt = Math.max(retryAt, current.retryAt)
    for (const key of ['cause', 'reason', 'error']) pending.push(current[key])
    for (const key of ['errors', 'reports', 'relays']) {
      if (Array.isArray(current[key])) pending.push(...current[key])
    }
  }
  return retryAt
}

function nextRateLimitBackoff (delay) {
  if (delay === BULK_PACING_MS) return 1500
  if (delay === 1500) return 2000
  return Math.min(RATE_LIMIT_BACKOFF_MAX_MS, delay * 2)
}

function retryWaitAllowed (retryAt, maxWaitMs, now = Date.now()) {
  return maxWaitMs === null || Math.max(0, retryAt - now) <= maxWaitMs
}

function waitForRetry (deadline, signal, retrySignal) {
  const combined = AbortSignal.any([signal, retrySignal].filter(Boolean))
  if (combined.aborted) return Promise.reject(combined.reason)
  return new Promise((resolve, reject) => {
    const timer = maybeUnref(setTimeout(() => {
      combined.removeEventListener('abort', onAbort)
      resolve()
    }, Math.max(0, deadline - Date.now())))
    const onAbort = () => {
      clearTimeout(timer)
      combined.removeEventListener('abort', onAbort)
      reject(combined.reason)
    }
    combined.addEventListener('abort', onAbort, { once: true })
  })
}

function hasMatchingPrefix (values, candidate) {
  return !values || values.some(value => typeof value === 'string' && candidate.startsWith(value))
}

function doesEventMatchFilter (filter, event) {
  if (filter.ids && !hasMatchingPrefix(filter.ids, event.id)) return false
  if (filter.authors && !hasMatchingPrefix(filter.authors, event.pubkey)) return false
  if (filter.kinds && !filter.kinds.includes(event.kind)) return false
  if (filter.since != null && event.created_at < filter.since) return false
  if (filter.until != null && event.created_at > filter.until) return false
  for (const [key, values] of Object.entries(filter)) {
    if (!key.startsWith('#') || !Array.isArray(values)) continue
    const name = key.slice(1)
    if (!event.tags.some(tag => tag[0] === name && values.includes(tag[1]))) return false
  }
  return true
}

function doesEventMatchAnyFilter (filters, event) {
  return filters.some(filter => doesEventMatchFilter(filter, event))
}

async function messageText (data) {
  if (typeof data === 'string') return data
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data)
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data)
  if (typeof data?.text === 'function') return await data.text()
  throw new ValidationError('INVALID_RELAY_MESSAGE')
}

export class RelayConnection {
  #WebSocket
  #connectPromise = null
  #challenge = null
  #serial = 0
  #lastTransportError = null
  #subscriptions = new Map()
  #publishes = new Map()
  #authentications = new Map()
  #counts = new Map()
  #retryAtByType = new Map()
  #cooldownWaiters = new Set()
  #bulkQueue = []
  #bulkActive = null
  #bulkTimer = null
  #bulkNextAt = 0
  #rateLimitBackoffMs = new Map(OPERATION_TYPES.map(type => [type, BULK_PACING_MS]))
  #getRateLimitCapMs
  #defaultMaxRateLimitRetryWaitMs

  constructor (url, {
    WebSocket: WebSocketImpl = globalThis.WebSocket,
    rateLimitCapMs = () => DEFAULT_RATE_LIMIT_CAP_MS,
    maxRateLimitRetryWaitMs = DEFAULT_MAX_RATE_LIMIT_RETRY_WAIT_MS
  } = {}) {
    this.url = url
    this.#WebSocket = WebSocketImpl
    if (typeof rateLimitCapMs !== 'function' && (!Number.isSafeInteger(rateLimitCapMs) || rateLimitCapMs < 1)) {
      throw new ValidationError('INVALID_RATE_LIMIT_CAP_MS')
    }
    this.#getRateLimitCapMs = typeof rateLimitCapMs === 'function'
      ? rateLimitCapMs
      : () => rateLimitCapMs
    if (maxRateLimitRetryWaitMs !== null &&
        (!Number.isSafeInteger(maxRateLimitRetryWaitMs) || maxRateLimitRetryWaitMs < 0)) {
      throw new ValidationError('INVALID_MAX_RATE_LIMIT_RETRY_WAIT_MS')
    }
    this.#defaultMaxRateLimitRetryWaitMs = maxRateLimitRetryWaitMs
    this.ws = null
    this.publishTimeout = DEFAULT_OPERATION_TIMEOUT
    this.onnotice = null
    this.onerror = null
    this.onclose = null
    this.onauth = null
  }

  // Exposes socket context to the pool's operation-wide publication deadline.
  get lastTransportError () { return this.#lastTransportError }

  setRateLimitCapMs (capMs, now = Date.now()) {
    if (!Number.isSafeInteger(capMs) || capMs < 1) throw new ValidationError('INVALID_RATE_LIMIT_CAP_MS')
    const limit = now + Math.min(capMs, Number.MAX_SAFE_INTEGER - now)
    let changed = false
    for (const [type, retryAt] of this.#retryAtByType) {
      if (retryAt <= limit) continue
      this.#retryAtByType.set(type, limit)
      changed = true
    }
    if (changed) for (const wake of [...this.#cooldownWaiters]) wake()
    this.#pumpBulkPublishes()
  }

  async connect ({ timeout = DEFAULT_CONNECT_TIMEOUT, signal } = {}) {
    if (this.ws?.readyState === 1) return
    if (this.#connectPromise) return await this.#connectPromise
    if (signal?.aborted) throw new Error('CONNECT_ABORTED')
    if (typeof this.#WebSocket !== 'function') throw categorizeRelayError(new Error('WEBSOCKET_UNAVAILABLE'), 'connection')

    this.#lastTransportError = null
    this.#connectPromise = new Promise((resolve, reject) => {
      let socket
      try { socket = new this.#WebSocket(this.url) } catch (error) {
        reject(categorizeRelayError(error, 'connection'))
        return
      }
      this.ws = socket
      let settled = false
      const finish = (reason) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
        if (reason) {
          try { socket.close() } catch {}
          reject(reason)
        } else resolve()
      }
      const onAbort = () => finish(new Error('CONNECT_ABORTED'))
      const timer = timeout === null ? null : maybeUnref(setTimeout(() => finish(relayTimeoutError('CONNECT_TIMEOUT')), timeout))
      signal?.addEventListener('abort', onAbort, { once: true })

      socket.onopen = () => finish()
      socket.onerror = event => {
        const reason = categorizeRelayError(event?.error, settled ? 'transport' : 'connection', 'CONNECTION_ERROR')
        if (!settled) finish(reason)
        else {
          this.#lastTransportError = reason
          this.onerror?.(reason)
        }
      }
      socket.onmessage = event => { this.#handleMessage(event).catch(reason => this.onerror?.(reason)) }
      socket.onclose = event => {
        if (!settled) finish(relayCloseError(event, 'connection'))
        if (this.ws === socket) this.ws = null
        this.#handleClose(event)
      }
    }).finally(() => { this.#connectPromise = null })
    return await this.#connectPromise
  }

  send (message) {
    if (this.ws?.readyState !== 1) throw relayCloseError(null, 'transport', this.#lastTransportError)
    try { this.ws.send(message) } catch (error) {
      this.#lastTransportError = categorizeRelayError(error, 'transport')
      throw this.#lastTransportError
    }
  }

  // New work respects relay cooldowns; cleanup uses send() directly. These
  // waits stay within the caller's existing deadline and are cancellable.
  #dispatchWork (type, send, fail, onDispatch) {
    let timer = null
    let cancelled = false
    const wake = () => {
      clearTimeout(timer)
      timer = null
      schedule()
    }
    const schedule = () => {
      if (cancelled) return
      const remaining = (this.#retryAtByType.get(type) ?? 0) - Date.now()
      if (remaining > 0) {
        timer = maybeUnref(setTimeout(wake, remaining))
        return
      }
      this.#cooldownWaiters.delete(wake)
      onDispatch?.()
      try { send() } catch (error) { fail(error) }
    }
    const dispatch = () => {
      this.#cooldownWaiters.add(wake)
      schedule()
    }
    dispatch()
    return () => {
      cancelled = true
      this.#cooldownWaiters.delete(wake)
      clearTimeout(timer)
    }
  }

  #rejection (type, reason, params, fallback) {
    const error = relayRejectionError(reason, params, fallback, { rateLimitCapMs: this.#getRateLimitCapMs() })
    if (error.message?.startsWith('rate-limited:')) {
      const now = Date.now()
      const hasAdvice = Number.isSafeInteger(error.retryAt)
      if (!hasAdvice) {
        const delay = this.#rateLimitBackoffMs.get(type) ?? BULK_PACING_MS
        error.retryAt = now + delay
        error.retryAfterMs = delay
      }
      const delay = this.#rateLimitBackoffMs.get(type) ?? BULK_PACING_MS
      this.#rateLimitBackoffMs.set(type, nextRateLimitBackoff(delay))
      this.#retryAtByType.set(type, Math.max(this.#retryAtByType.get(type) ?? 0, error.retryAt))
      Object.defineProperty(error, 'effectiveRetryAt', {
        value: Math.max(error.retryAt, this.#retryAtByType.get(type) ?? 0),
        enumerable: false,
        configurable: true
      })
    }
    return error
  }

  #recordSuccess (type) {
    this.#rateLimitBackoffMs.set(type, BULK_PACING_MS)
  }

  subscribe (filters, handlers = {}) {
    if (!Array.isArray(filters) || !filters.length) throw new ValidationError('SUBSCRIPTION_FILTERS_REQUIRED')
    const id = `p2r2p-sub:${++this.#serial}`
    let closed = false
    let sent = false
    const close = () => {
      if (closed) return
      closed = true
      const subscription = this.#subscriptions.get(id)
      if (!subscription) return
      this.#subscriptions.delete(id)
      subscription.cancelSend?.()
      if (sent) { try { this.send(JSON.stringify(['CLOSE', id])) } catch {} }
      handlers.onclose?.()
    }
    const subscription = { filters, handlers, close, cancelSend: null }
    this.#subscriptions.set(id, subscription)
    subscription.cancelSend = this.#dispatchWork('REQ', () => {
      this.send(JSON.stringify(['REQ', id, ...filters]))
      sent = true
    }, error => {
      this.#subscriptions.delete(id)
      handlers.onclose?.(error)
    })
    return { id, close }
  }

  publish (event, {
    signal,
    useBulkPacing = false,
    rateLimitRetries = 0,
    maxRateLimitRetryWaitMs = this.#defaultMaxRateLimitRetryWaitMs,
    retrySignal,
    deadlineAt = null
  } = {}) {
    if (!isValidEvent(event)) return Promise.reject(new Error('INVALID_EVENT'))
    if (typeof useBulkPacing !== 'boolean') return Promise.reject(new ValidationError('INVALID_USE_BULK_PACING'))
    if (!Number.isSafeInteger(rateLimitRetries) || rateLimitRetries < 0) return Promise.reject(new ValidationError('INVALID_RATE_LIMIT_RETRIES'))
    if (maxRateLimitRetryWaitMs !== null &&
        (!Number.isSafeInteger(maxRateLimitRetryWaitMs) || maxRateLimitRetryWaitMs < 0)) {
      return Promise.reject(new ValidationError('INVALID_MAX_RATE_LIMIT_RETRY_WAIT_MS'))
    }
    return this.#sendEventOperation('EVENT', event, this.#publishes, 'PUBLISH_TIMEOUT', signal, {
      useBulkPacing,
      rateLimitRetries,
      maxRateLimitRetryWaitMs,
      retrySignal,
      deadlineAt
    })
  }

  async authenticate (getAuthEvent, {
    signal,
    rateLimitRetries = 0,
    maxRateLimitRetryWaitMs = this.#defaultMaxRateLimitRetryWaitMs,
    deadlineAt = null,
    retrySignal
  } = {}) {
    if (!this.#challenge) throw new Error('AUTH_CHALLENGE_MISSING')
    if (!Number.isSafeInteger(rateLimitRetries) || rateLimitRetries < 0) throw new ValidationError('INVALID_RATE_LIMIT_RETRIES')
    if (maxRateLimitRetryWaitMs !== null &&
        (!Number.isSafeInteger(maxRateLimitRetryWaitMs) || maxRateLimitRetryWaitMs < 0)) {
      throw new ValidationError('INVALID_MAX_RATE_LIMIT_RETRY_WAIT_MS')
    }
    let lastRateLimitError = null
    for (let attempt = 0; ; attempt++) {
      if (signal?.aborted) throw signal.reason
      if (deadlineAt !== null && Date.now() >= deadlineAt) {
        throw lastRateLimitError ?? relayTimeoutError('AUTH_TIMEOUT', this.#lastTransportError)
      }
      const event = await getAuthEvent({ relay: this.url, challenge: this.#challenge })
      if (!isValidEvent(event)) throw new ValidationError('INVALID_AUTH_EVENT')
      try {
        return await this.#sendEventOperation('AUTH', event, this.#authentications, 'AUTH_TIMEOUT', signal, { deadlineAt })
      } catch (error) {
        if (!error?.message?.startsWith('rate-limited:') || attempt >= rateLimitRetries) throw error
        const retryAt = Math.max(
          retryAtFrom(error),
          this.#retryAtByType.get('AUTH') ?? 0
        )
        if (!retryAt || (deadlineAt !== null && retryAt >= deadlineAt)) throw error
        if (!retryWaitAllowed(retryAt, maxRateLimitRetryWaitMs)) throw error
        debugRateLimitRetry({
          relay: this.url,
          operation: 'AUTH',
          retryAttempt: attempt + 1,
          maxRetries: rateLimitRetries,
          retryAt,
          effectiveWaitMs: Math.max(0, retryAt - Date.now()),
          maxWaitMs: maxRateLimitRetryWaitMs
        })
        lastRateLimitError = error
        try {
          await waitForRetry(retryAt, signal, retrySignal)
        } catch (cause) {
          if (retrySignal?.aborted) throw error
          throw cause
        }
      }
    }
  }

  // A job owns physical transmission and scheduling. Its waiters retain
  // per-call cancellation, timeout, deadline and retry policy.
  #mapForType (type) {
    return type === 'EVENT' ? this.#publishes : this.#authentications
  }

  #hasJob (job) {
    return this.#mapForType(job.type).get(job.event.id)?.has(job) === true
  }

  #findJob (map, id, pacing) {
    for (const job of map.get(id) ?? []) {
      if (job.pacing === pacing) return job
    }
    return null
  }

  #selectJob (map, event, useBulkPacing) {
    const interactive = this.#findJob(map, event.id, 'interactive')
    if (!useBulkPacing) {
      if (interactive) return interactive
      // Interactive work may benefit from a bulk send already in progress,
      // but must never wait behind a bulk job that is still queued.
      const activeBulk = this.#findJob(map, event.id, 'bulk')
      return activeBulk?.state === 'active' ? activeBulk : null
    }
    if (interactive) return interactive
    return this.#findJob(map, event.id, 'bulk')
  }

  #addJob (job) {
    const map = this.#mapForType(job.type)
    let jobs = map.get(job.event.id)
    if (!jobs) {
      jobs = new Set()
      map.set(job.event.id, jobs)
    }
    jobs.add(job)
  }

  #allJobs (map) {
    const jobs = []
    for (const set of map.values()) jobs.push(...set)
    return jobs
  }

  #cleanupWaiter (job, waiter) {
    clearTimeout(waiter.timer)
    waiter.signal?.removeEventListener('abort', waiter.onAbort)
    waiter.retrySignal?.removeEventListener('abort', waiter.onRetryAbort)
    job.waiters.delete(waiter)
  }

  #detachJob (job) {
    if (job.detached) return
    job.detached = true
    job.awaitingAck = false
    job.retryScheduled = false
    const map = this.#mapForType(job.type)
    const jobs = map.get(job.event.id)
    if (jobs) {
      jobs.delete(job)
      if (!jobs.size) map.delete(job.event.id)
    }
    job.cancelSend?.()
    job.cancelSend = null
    const index = this.#bulkQueue.indexOf(job)
    if (index >= 0) this.#bulkQueue.splice(index, 1)
    const wasActive = this.#bulkActive === job
    if (wasActive) {
      this.#bulkActive = null
      this.#bulkNextAt = Math.max(this.#bulkNextAt, Date.now(), job.startedAt + BULK_PACING_MS)
    }
    if (!this.#bulkActive) this.#pumpBulkPublishes()
  }

  #settleJob (job, reason, value) {
    if (job.settled) return
    job.settled = true
    const waiters = [...job.waiters]
    for (const waiter of waiters) this.#cleanupWaiter(job, waiter)
    this.#detachJob(job)
    for (const waiter of waiters) {
      if (reason) waiter.reject(errorFrom(reason, 'OPERATION_REJECTED'))
      else waiter.resolve(value)
    }
  }

  #removeWaiter (job, waiter, reason) {
    if (!job.waiters.has(waiter)) return
    this.#cleanupWaiter(job, waiter)
    waiter.reject(errorFrom(reason, 'OPERATION_REJECTED'))
    if (!job.waiters.size) {
      job.settled = true
      this.#detachJob(job)
    }
  }

  #canShareScheduledRetry (waiter, retryAt) {
    return !waiter.retrySignal?.aborted &&
      (waiter.deadlineAt === null || retryAt < waiter.deadlineAt)
  }

  #canRetryAfterLimit (waiter, retryAt) {
    return waiter.observedRateLimitFailures <= waiter.rateLimitRetries &&
      retryWaitAllowed(retryAt, waiter.maxRateLimitRetryWaitMs) &&
      this.#canShareScheduledRetry(waiter, retryAt)
  }

  #pruneScheduledRetryWaiters (job) {
    if (!job.retryScheduled || !job.lastRateLimitError) return
    for (const waiter of [...job.waiters]) {
      if (!this.#canShareScheduledRetry(waiter, job.retryAt)) {
        this.#removeWaiter(job, waiter, job.lastRateLimitError)
      }
    }
  }

  #expireWaiter (job, waiter) {
    const reason = job.retryScheduled && job.lastRateLimitError
      ? job.lastRateLimitError
      : relayTimeoutError(job.timeoutCode, this.#lastTransportError)
    this.#removeWaiter(job, waiter, reason)
    this.#pruneScheduledRetryWaiters(job)
  }

  #abortWaiterRetry (job, waiter) {
    if (!job.retryScheduled || !job.lastRateLimitError) return
    this.#removeWaiter(job, waiter, job.lastRateLimitError)
    this.#pruneScheduledRetryWaiters(job)
  }

  #addWaiter (job, {
    signal,
    retrySignal,
    deadlineAt,
    rateLimitRetries,
    maxRateLimitRetryWaitMs
  }) {
    const deferred = Promise.withResolvers()
    const waiter = {
      ...deferred,
      signal,
      retrySignal,
      deadlineAt,
      rateLimitRetries,
      maxRateLimitRetryWaitMs,
      observedRateLimitFailures: 0,
      timer: null,
      onAbort: null,
      onRetryAbort: null
    }
    job.waiters.add(waiter)
    waiter.onAbort = () => this.#removeWaiter(job, waiter, signal.reason)
    waiter.onRetryAbort = () => this.#abortWaiterRetry(job, waiter)
    signal?.addEventListener('abort', waiter.onAbort, { once: true })
    retrySignal?.addEventListener('abort', waiter.onRetryAbort, { once: true })
    let timeout = this.publishTimeout
    if (!signal && deadlineAt !== null) timeout = Math.min(timeout, Math.max(0, deadlineAt - Date.now()))
    waiter.timer = maybeUnref(setTimeout(() => this.#expireWaiter(job, waiter), timeout))

    if (signal?.aborted) this.#removeWaiter(job, waiter, signal.reason)
    else if (job.retryScheduled && !this.#canShareScheduledRetry(waiter, job.retryAt)) {
      this.#removeWaiter(job, waiter, job.lastRateLimitError)
    }
    return deferred.promise
  }

  #clearBulkScheduler () {
    clearTimeout(this.#bulkTimer)
    this.#bulkTimer = null
    this.#bulkQueue.length = 0
    this.#bulkActive = null
    this.#bulkNextAt = 0
  }

  #pumpBulkPublishes () {
    if (this.#bulkActive || this.#bulkQueue.length === 0) return
    clearTimeout(this.#bulkTimer)
    this.#bulkTimer = null
    while (this.#bulkQueue.length) {
      const job = this.#bulkQueue[0]
      if (this.#hasJob(job) && job.waiters.size > 0) break
      this.#bulkQueue.shift()
    }
    if (!this.#bulkQueue.length) return

    const now = Date.now()
    const eligibleAt = Math.max(this.#bulkNextAt, this.#retryAtByType.get('EVENT') ?? 0)
    if (eligibleAt > now) {
      this.#bulkTimer = maybeUnref(setTimeout(() => this.#pumpBulkPublishes(), eligibleAt - now))
      return
    }

    const job = this.#bulkQueue.shift()
    const startedAt = now
    job.state = 'active'
    job.startedAt = startedAt
    this.#bulkActive = job
    this.#dispatchEventAttempt(job)
  }

  #dispatchEventAttempt (job) {
    const cancel = this.#dispatchWork(job.type, () => {
      if (!this.#hasJob(job)) return
      const retrying = job.retryScheduled
      for (const waiter of [...job.waiters]) {
        if (waiter.deadlineAt !== null && Date.now() >= waiter.deadlineAt) {
          this.#removeWaiter(job, waiter, retrying && job.lastRateLimitError
            ? job.lastRateLimitError
            : relayTimeoutError(job.timeoutCode, this.#lastTransportError))
        }
      }
      if (!job.waiters.size) return
      job.retryScheduled = false
      job.awaitingAck = true
      try {
        this.send(JSON.stringify([job.type, job.event]))
      } catch (error) {
        job.awaitingAck = false
        throw error
      }
    }, error => {
      job.awaitingAck = false
      this.#settleJob(job, error)
    })
    job.cancelSend = cancel
    if (!this.#hasJob(job)) cancel()
  }

  #applyRateLimit (job, reason) {
    job.awaitingAck = false
    job.lastRateLimitError = reason
    const retryAt = Math.max(
      retryAtFrom(reason),
      this.#retryAtByType.get(job.type) ?? 0
    )
    for (const waiter of [...job.waiters]) {
      waiter.observedRateLimitFailures++
      if (!retryAt || !this.#canRetryAfterLimit(waiter, retryAt)) {
        this.#removeWaiter(job, waiter, reason)
      }
    }
    if (!job.waiters.size) return
    job.retryAt = retryAt
    job.retryScheduled = true
    job.retryAttempts++
    const waitLimits = [...job.waiters].map(waiter => waiter.maxRateLimitRetryWaitMs)
    debugRateLimitRetry({
      relay: this.url,
      operation: job.type,
      retryAttempt: job.retryAttempts,
      maxRetries: Math.max(...[...job.waiters].map(waiter => waiter.rateLimitRetries)),
      retryAt,
      effectiveWaitMs: Math.max(0, retryAt - Date.now()),
      maxWaitMs: waitLimits.includes(null) ? null : Math.max(...waitLimits)
    })
    this.#dispatchEventAttempt(job)
  }

  #sendEventOperation (type, event, map, timeoutCode, signal, {
    useBulkPacing = false,
    rateLimitRetries = 0,
    maxRateLimitRetryWaitMs = this.#defaultMaxRateLimitRetryWaitMs,
    retrySignal,
    deadlineAt = null
  } = {}) {
    if (signal?.aborted) return Promise.reject(signal.reason)
    let job = this.#selectJob(map, event, useBulkPacing && type === 'EVENT')
    const created = !job
    if (created) {
      job = {
        waiters: new Set(),
        cancelSend: null,
        event,
        type,
        timeoutCode,
        pacing: useBulkPacing && type === 'EVENT' ? 'bulk' : 'interactive',
        state: useBulkPacing && type === 'EVENT' ? 'queued' : 'active',
        startedAt: 0,
        retryAttempts: 0,
        lastRateLimitError: null,
        retryAt: 0,
        retryScheduled: false,
        awaitingAck: false,
        detached: false,
        settled: false
      }
      this.#addJob(job)
    }
    const promise = this.#addWaiter(job, {
      signal,
      retrySignal,
      deadlineAt,
      rateLimitRetries,
      maxRateLimitRetryWaitMs
    })
    if (!created || !this.#hasJob(job)) return promise
    if (job.pacing === 'bulk') {
      this.#bulkQueue.push(job)
      this.#pumpBulkPublishes()
    } else this.#dispatchEventAttempt(job)
    return promise
  }

  countWithHll (filters, { signal } = {}) {
    if (signal?.aborted) return Promise.reject(new Error('COUNT_ABORTED'))
    const id = `p2r2p-count:${++this.#serial}`
    const deferred = Promise.withResolvers()
    const onAbort = () => this.#settleCount(id, null, new Error('COUNT_ABORTED'))
    const pending = { ...deferred, signal, onAbort, cancelSend: null }
    this.#counts.set(id, pending)
    signal?.addEventListener('abort', onAbort, { once: true })
    pending.cancelSend = this.#dispatchWork('COUNT', () => this.send(JSON.stringify(['COUNT', id, ...filters])), error => {
      this.#settleCount(id, null, error)
    })
    return deferred.promise
  }

  #settleJobsSuccess (jobs, value) {
    for (const job of [...jobs]) this.#settleJob(job, null, value)
  }

  #handleOkFailure (jobs, type, message, params) {
    const awaiting = [...jobs].filter(job => job.awaitingAck)
    if (!awaiting.length) return
    const reason = this.#rejection(type, message, params, 'EVENT_REJECTED')
    for (const job of awaiting) {
      job.awaitingAck = false
      if (reason.message?.startsWith('rate-limited:')) this.#applyRateLimit(job, reason)
      else this.#settleJob(job, reason)
    }
  }

  #settleCount (id, payload, reason) {
    const pending = this.#counts.get(id)
    if (!pending) return
    this.#counts.delete(id)
    if (!reason) this.#recordSuccess('COUNT')
    pending.cancelSend?.()
    pending.signal?.removeEventListener('abort', pending.onAbort)
    if (reason) pending.reject(errorFrom(reason, 'COUNT_REJECTED'))
    else pending.resolve(payload)
  }

  async #handleMessage (message) {
    let data
    try { data = JSON.parse(await messageText(message.data)) } catch (cause) {
      if (cause instanceof ValidationError) throw cause
      throw new ValidationError('INVALID_RELAY_MESSAGE', { cause })
    }
    if (!Array.isArray(data) || typeof data[0] !== 'string') throw new ValidationError('INVALID_RELAY_MESSAGE')

    if (data[0] === 'EVENT') {
      const subscription = this.#subscriptions.get(data[1])
      if (!subscription) return
      const event = data[2]
      if (!isValidEvent(event) || !doesEventMatchAnyFilter(subscription.filters, event)) subscription.handlers.oninvalidevent?.(event)
      else subscription.handlers.onevent?.(event)
      return
    }
    if (data[0] === 'EOSE') {
      const subscription = this.#subscriptions.get(data[1])
      if (subscription) {
        this.#recordSuccess('REQ')
        subscription.handlers.oneose?.()
      }
      return
    }
    if (data[0] === 'CLOSED') {
      const id = data[1]
      const subscription = this.#subscriptions.get(id)
      if (!subscription && !this.#counts.has(id)) return
      const reason = this.#rejection(subscription ? 'REQ' : 'COUNT', data[2], data[3], subscription ? 'SUBSCRIPTION_CLOSED' : 'COUNT_CLOSED')
      if (subscription) {
        this.#subscriptions.delete(id)
        subscription.cancelSend?.()
        subscription.handlers.onclose?.(reason)
      } else this.#settleCount(id, null, reason)
      return
    }
    if (data[0] === 'OK') {
      for (const [type, map] of [['EVENT', this.#publishes], ['AUTH', this.#authentications]]) {
        const jobs = map.get(data[1])
        if (!jobs || ![...jobs].some(job => job.awaitingAck)) continue
        if (data[2] === true) {
          this.#recordSuccess(type)
          // The same relay accepted this exact event. Settle every pacing job
          // locally, including copies that have not started transmission.
          this.#settleJobsSuccess(jobs, data[3])
        } else {
          this.#handleOkFailure(jobs, type, data[3], data[4])
        }
      }
      return
    }
    if (data[0] === 'AUTH' && typeof data[1] === 'string') {
      this.#challenge = data[1]
      this.onauth?.(data[1])
      return
    }
    if (data[0] === 'COUNT') {
      this.#settleCount(data[1], data[2])
      return
    }
    if (data[0] === 'NOTICE') this.onnotice?.(String(data[1] ?? ''))
  }

  #handleClose (event) {
    this.#challenge = null
    const reason = relayCloseError(event, 'transport', this.#lastTransportError)
    for (const [id, subscription] of this.#subscriptions) {
      this.#subscriptions.delete(id)
      subscription.cancelSend?.()
      subscription.handlers.onclose?.(reason)
    }
    const jobs = [...this.#allJobs(this.#publishes), ...this.#allJobs(this.#authentications)]
    this.#clearBulkScheduler()
    for (const job of jobs) this.#settleJob(job, reason)
    for (const id of [...this.#counts.keys()]) this.#settleCount(id, null, reason)
    this.onclose?.(reason)
  }

  async close () {
    const socket = this.ws
    this.ws = null
    this.#challenge = null
    const reason = relayCloseError(null, 'transport', this.#lastTransportError)
    for (const [id, subscription] of this.#subscriptions) {
      this.#subscriptions.delete(id)
      subscription.cancelSend?.()
      subscription.handlers.onclose?.()
    }
    const jobs = [...this.#allJobs(this.#publishes), ...this.#allJobs(this.#authentications)]
    this.#clearBulkScheduler()
    for (const job of jobs) this.#settleJob(job, reason)
    for (const id of [...this.#counts.keys()]) this.#settleCount(id, null, reason)
    if (socket && socket.readyState < 2) socket.close()
  }
}
