import { isValidEvent } from '../../event/index.js'
import { ValidationError } from '../../error/index.js'
import { maybeUnref } from '../helpers/timer.js'
import { categorizeRelayError, relayCloseError, relayTimeoutError, relayRejectionError } from '../helpers/error.js'
import { DEFAULT_RATE_LIMIT_CAP_MS } from '../helpers/retry-advice.js'

const DEFAULT_CONNECT_TIMEOUT = 3000
const DEFAULT_OPERATION_TIMEOUT = 30000
const BULK_PACING_MS = 1000
const BULK_BACKOFF_MAX_MS = 30000

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
  #bulkBackoffMs = BULK_PACING_MS
  #bulkNextAt = 0
  #getRateLimitCapMs

  constructor (url, {
    WebSocket: WebSocketImpl = globalThis.WebSocket,
    rateLimitCapMs = () => DEFAULT_RATE_LIMIT_CAP_MS
  } = {}) {
    this.url = url
    this.#WebSocket = WebSocketImpl
    if (typeof rateLimitCapMs !== 'function' && (!Number.isSafeInteger(rateLimitCapMs) || rateLimitCapMs < 1)) {
      throw new ValidationError('INVALID_RATE_LIMIT_CAP_MS')
    }
    this.#getRateLimitCapMs = typeof rateLimitCapMs === 'function'
      ? rateLimitCapMs
      : () => rateLimitCapMs
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
  #dispatchWork (type, send, fail) {
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
    if (error.retryAt !== undefined) this.#retryAtByType.set(type, Math.max(this.#retryAtByType.get(type) ?? 0, error.retryAt))
    return error
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

  publish (event, { signal, useBulkPacing = false } = {}) {
    if (!isValidEvent(event)) return Promise.reject(new Error('INVALID_EVENT'))
    if (typeof useBulkPacing !== 'boolean') return Promise.reject(new ValidationError('INVALID_USE_BULK_PACING'))
    return this.#sendEventOperation('EVENT', event, this.#publishes, 'PUBLISH_TIMEOUT', signal, { useBulkPacing })
  }

  async authenticate (getAuthEvent, { signal } = {}) {
    if (!this.#challenge) throw new Error('AUTH_CHALLENGE_MISSING')
    const event = await getAuthEvent({ relay: this.url, challenge: this.#challenge })
    if (!isValidEvent(event)) throw new ValidationError('INVALID_AUTH_EVENT')
    return await this.#sendEventOperation('AUTH', event, this.#authentications, 'AUTH_TIMEOUT', signal)
  }

  #removeBulkPublish (pending) {
    const index = this.#bulkQueue.indexOf(pending)
    if (index >= 0) this.#bulkQueue.splice(index, 1)
    if (this.#bulkActive === pending) {
      this.#bulkActive = null
      pending.onSettled = null
      this.#bulkNextAt = Math.max(this.#bulkNextAt, Date.now() + BULK_PACING_MS)
    }
    this.#pumpBulkPublishes()
  }

  #clearBulkScheduler () {
    clearTimeout(this.#bulkTimer)
    this.#bulkTimer = null
    this.#bulkQueue.length = 0
    this.#bulkActive = null
    this.#bulkNextAt = 0
    this.#bulkBackoffMs = BULK_PACING_MS
  }

  #pumpBulkPublishes () {
    if (this.#bulkActive || this.#bulkQueue.length === 0) return
    clearTimeout(this.#bulkTimer)
    this.#bulkTimer = null
    while (this.#bulkQueue.length) {
      const pending = this.#bulkQueue[0]
      if (this.#publishes.get(pending.event.id) === pending && pending.waiters.size > 0) break
      this.#bulkQueue.shift()
    }
    if (!this.#bulkQueue.length) return

    const now = Date.now()
    const eligibleAt = Math.max(this.#bulkNextAt, this.#retryAtByType.get('EVENT') ?? 0)
    if (eligibleAt > now) {
      this.#bulkTimer = maybeUnref(setTimeout(() => this.#pumpBulkPublishes(), eligibleAt - now))
      return
    }

    const pending = this.#bulkQueue.shift()
    const startedAt = now
    this.#bulkActive = pending
    pending.onSettled = (success, reason) => {
      if (this.#bulkActive !== pending) return
      this.#bulkActive = null
      pending.onSettled = null
      const completedAt = Date.now()
      const hintedRetryAt = retryAtFrom(reason)
      if (success) {
        this.#bulkBackoffMs = BULK_PACING_MS
        this.#bulkNextAt = Math.max(completedAt, startedAt + BULK_PACING_MS)
      } else if (hintedRetryAt) {
        // The relay advice overrides the local backoff for this retry.
        this.#bulkNextAt = Math.max(completedAt, startedAt + BULK_PACING_MS)
        this.#bulkBackoffMs = Math.min(BULK_BACKOFF_MAX_MS, this.#bulkBackoffMs * 2)
      } else {
        this.#bulkNextAt = Math.max(completedAt + this.#bulkBackoffMs, startedAt + BULK_PACING_MS)
        this.#bulkBackoffMs = Math.min(BULK_BACKOFF_MAX_MS, this.#bulkBackoffMs * 2)
      }
      this.#pumpBulkPublishes()
    }
    pending.cancelSend = this.#dispatchWork('EVENT', () => {
      this.send(JSON.stringify(['EVENT', pending.event]))
    }, error => this.#settleEvent(this.#publishes, pending.event.id, error))
  }

  #sendEventOperation (type, event, map, timeoutCode, signal, { useBulkPacing = false } = {}) {
    if (signal?.aborted) return Promise.reject(signal.reason)
    let pending = map.get(event.id)
    const created = !pending
    if (created) {
      const timer = maybeUnref(setTimeout(() => this.#settleEvent(map, event.id, relayTimeoutError(timeoutCode, this.#lastTransportError)), this.publishTimeout))
      pending = { timer, waiters: new Set(), cancelSend: null, event, type, timeoutCode, useBulkPacing, onSettled: null }
      map.set(event.id, pending)
    }
    const deferred = Promise.withResolvers()
    const onAbort = () => {
      pending.waiters.delete(waiter)
      signal.removeEventListener('abort', onAbort)
      deferred.reject(signal.reason)
      if (!pending.waiters.size && map.get(event.id) === pending) {
        map.delete(event.id)
        pending.cancelSend?.()
        clearTimeout(pending.timer)
        if (this.#bulkActive === pending) {
          this.#bulkActive = null
          pending.onSettled = null
          this.#bulkNextAt = Math.max(this.#bulkNextAt, Date.now() + BULK_PACING_MS)
          this.#pumpBulkPublishes()
        }
      }
    }
    const waiter = { ...deferred, stopAbort: () => signal?.removeEventListener('abort', onAbort) }
    pending.waiters.add(waiter)
    signal?.addEventListener('abort', onAbort, { once: true })
    if (created && useBulkPacing && type === 'EVENT') {
      pending.cancelSend = () => this.#removeBulkPublish(pending)
      this.#bulkQueue.push(pending)
      this.#pumpBulkPublishes()
    } else if (created) {
      pending.cancelSend = this.#dispatchWork(type, () => {
        this.send(JSON.stringify([type, event]))
      }, error => this.#settleEvent(map, event.id, error))
    }
    return deferred.promise
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

  #settleEvent (map, id, reason, value) {
    const pending = map.get(id)
    if (!pending) return
    map.delete(id)
    pending.cancelSend?.()
    clearTimeout(pending.timer)
    pending.onSettled?.(!reason, reason)
    for (const waiter of pending.waiters) {
      waiter.stopAbort()
      if (reason) waiter.reject(errorFrom(reason, 'OPERATION_REJECTED'))
      else waiter.resolve(value)
    }
    pending.waiters.clear()
  }

  #settleCount (id, payload, reason) {
    const pending = this.#counts.get(id)
    if (!pending) return
    this.#counts.delete(id)
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
      this.#subscriptions.get(data[1])?.handlers.oneose?.()
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
      for (const [type, pending] of [['EVENT', this.#publishes], ['AUTH', this.#authentications]]) {
        if (!pending.has(data[1])) continue
        const reason = data[2] === true ? null : this.#rejection(type, data[3], data[4], 'EVENT_REJECTED')
        this.#settleEvent(pending, data[1], reason, data[3])
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
    for (const id of [...this.#publishes.keys()]) this.#settleEvent(this.#publishes, id, reason)
    for (const id of [...this.#authentications.keys()]) this.#settleEvent(this.#authentications, id, reason)
    for (const id of [...this.#counts.keys()]) this.#settleCount(id, null, reason)
    this.#clearBulkScheduler()
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
    for (const id of [...this.#publishes.keys()]) this.#settleEvent(this.#publishes, id, reason)
    for (const id of [...this.#authentications.keys()]) this.#settleEvent(this.#authentications, id, reason)
    for (const id of [...this.#counts.keys()]) this.#settleCount(id, null, reason)
    this.#clearBulkScheduler()
    if (socket && socket.readyState < 2) socket.close()
  }
}
