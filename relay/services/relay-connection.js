import { isValidEvent } from '../../event/index.js'
import { ValidationError } from '../../error/index.js'
import { maybeUnref } from '../helpers/timer.js'
import { categorizeRelayError, relayCloseError, relayTimeoutError, relayRejectionError } from '../helpers/error.js'

const DEFAULT_CONNECT_TIMEOUT = 3000
const DEFAULT_OPERATION_TIMEOUT = 30000

function errorFrom (reason, fallback) {
  return reason instanceof Error ? reason : new Error(String(reason || fallback))
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
  #retryAt = 0

  constructor (url, { WebSocket: WebSocketImpl = globalThis.WebSocket } = {}) {
    this.url = url
    this.#WebSocket = WebSocketImpl
    this.ws = null
    this.publishTimeout = DEFAULT_OPERATION_TIMEOUT
    this.onnotice = null
    this.onerror = null
    this.onclose = null
    this.onauth = null
  }

  // Exposes socket context to the pool's operation-wide publication deadline.
  get lastTransportError () { return this.#lastTransportError }

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
  #dispatchWork (send, fail) {
    let timer = null
    let cancelled = false
    const dispatch = () => {
      if (cancelled) return
      const remaining = this.#retryAt - Date.now()
      if (remaining > 0) {
        timer = maybeUnref(setTimeout(dispatch, remaining))
        return
      }
      try { send() } catch (error) { fail(error) }
    }
    dispatch()
    return () => { cancelled = true; clearTimeout(timer) }
  }

  #rejection (reason, extra, fallback) {
    const error = relayRejectionError(reason, extra, fallback)
    this.#retryAt = Math.max(this.#retryAt, error.retryAt || 0)
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
    subscription.cancelSend = this.#dispatchWork(() => {
      this.send(JSON.stringify(['REQ', id, ...filters]))
      sent = true
    }, error => {
      this.#subscriptions.delete(id)
      handlers.onclose?.(error)
    })
    return { id, close }
  }

  publish (event, { signal } = {}) {
    if (!isValidEvent(event)) return Promise.reject(new Error('INVALID_EVENT'))
    return this.#sendEventOperation('EVENT', event, this.#publishes, 'PUBLISH_TIMEOUT', signal)
  }

  async authenticate (getAuthEvent, { signal } = {}) {
    if (!this.#challenge) throw new Error('AUTH_CHALLENGE_MISSING')
    const event = await getAuthEvent({ relay: this.url, challenge: this.#challenge })
    if (!isValidEvent(event)) throw new ValidationError('INVALID_AUTH_EVENT')
    return await this.#sendEventOperation('AUTH', event, this.#authentications, 'AUTH_TIMEOUT', signal)
  }

  #sendEventOperation (type, event, map, timeoutCode, signal) {
    if (signal?.aborted) return Promise.reject(signal.reason || relayTimeoutError(timeoutCode))
    if (map.has(event.id)) return map.get(event.id).promise
    const deferred = Promise.withResolvers()
    const timer = maybeUnref(setTimeout(() => this.#settleEvent(map, event.id, relayTimeoutError(timeoutCode, this.#lastTransportError)), this.publishTimeout))
    const onAbort = () => this.#settleEvent(map, event.id, signal.reason || relayTimeoutError(timeoutCode))
    const stopAbort = () => signal?.removeEventListener('abort', onAbort)
    const pending = { ...deferred, timer, promise: deferred.promise, cancelSend: null, stopAbort }
    map.set(event.id, pending)
    signal?.addEventListener('abort', onAbort, { once: true })
    pending.cancelSend = this.#dispatchWork(() => {
      // Once transmitted, keep the existing ACK/report semantics. Before that,
      // an operation-wide deadline must cancel the deferred publication.
      stopAbort()
      this.send(JSON.stringify([type, event]))
    }, error => {
      this.#settleEvent(map, event.id, error)
    })
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
    pending.cancelSend = this.#dispatchWork(() => this.send(JSON.stringify(['COUNT', id, ...filters])), error => {
      this.#settleCount(id, null, error)
    })
    return deferred.promise
  }

  #settleEvent (map, id, reason, value) {
    const pending = map.get(id)
    if (!pending) return
    map.delete(id)
    pending.cancelSend?.()
    pending.stopAbort?.()
    clearTimeout(pending.timer)
    if (reason) pending.reject(errorFrom(reason, 'OPERATION_REJECTED'))
    else pending.resolve(value)
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
      const reason = this.#rejection(data[2], data[3], subscription ? 'SUBSCRIPTION_CLOSED' : 'COUNT_CLOSED')
      if (subscription) {
        this.#subscriptions.delete(id)
        subscription.cancelSend?.()
        subscription.handlers.onclose?.(reason)
      } else this.#settleCount(id, null, reason)
      return
    }
    if (data[0] === 'OK') {
      const reason = data[2] === true ? null : this.#rejection(data[3], data[4], 'EVENT_REJECTED')
      this.#settleEvent(this.#publishes, data[1], reason, data[3])
      this.#settleEvent(this.#authentications, data[1], reason, data[3])
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
    if (socket && socket.readyState < 2) socket.close()
  }
}
