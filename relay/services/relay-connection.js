import { verifyEvent } from '../../event/index.js'
import { maybeUnref } from '../helpers/timer.js'

const DEFAULT_CONNECT_TIMEOUT = 3000
const DEFAULT_OPERATION_TIMEOUT = 30000

function errorFrom (reason, fallback) {
  return reason instanceof Error ? reason : new Error(String(reason || fallback))
}

function prefixMatches (values, candidate) {
  return !values || values.some(value => typeof value === 'string' && candidate.startsWith(value))
}

function matchFilter (filter, event) {
  if (filter.ids && !prefixMatches(filter.ids, event.id)) return false
  if (filter.authors && !prefixMatches(filter.authors, event.pubkey)) return false
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

function matchFilters (filters, event) {
  return filters.some(filter => matchFilter(filter, event))
}

async function messageText (data) {
  if (typeof data === 'string') return data
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data)
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data)
  if (typeof data?.text === 'function') return await data.text()
  throw new Error('INVALID_RELAY_MESSAGE')
}

export class RelayConnection {
  #WebSocket
  #connectPromise = null
  #challenge = null
  #serial = 0
  #subscriptions = new Map()
  #publishes = new Map()
  #authentications = new Map()
  #counts = new Map()

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

  async connect ({ timeout = DEFAULT_CONNECT_TIMEOUT, signal } = {}) {
    if (this.ws?.readyState === 1) return
    if (this.#connectPromise) return await this.#connectPromise
    if (signal?.aborted) throw new Error('CONNECT_ABORTED')
    if (typeof this.#WebSocket !== 'function') throw new Error('WEBSOCKET_UNAVAILABLE')

    this.#connectPromise = new Promise((resolve, reject) => {
      const socket = new this.#WebSocket(this.url)
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
      const timer = timeout === null ? null : maybeUnref(setTimeout(() => finish(new Error('CONNECT_TIMEOUT')), timeout))
      signal?.addEventListener('abort', onAbort, { once: true })

      socket.onopen = () => finish()
      socket.onerror = event => {
        const reason = errorFrom(event?.error, 'CONNECTION_ERROR')
        if (!settled) finish(reason)
        else this.onerror?.(reason)
      }
      socket.onmessage = event => { this.#handleMessage(event).catch(reason => this.onerror?.(reason)) }
      socket.onclose = event => {
        if (!settled) finish(new Error('CONNECTION_CLOSED'))
        if (this.ws === socket) this.ws = null
        this.#handleClose(event)
      }
    }).finally(() => { this.#connectPromise = null })
    return await this.#connectPromise
  }

  send (message) {
    if (this.ws?.readyState !== 1) throw new Error('CONNECTION_CLOSED')
    this.ws.send(message)
  }

  subscribe (filters, handlers = {}) {
    if (!Array.isArray(filters) || !filters.length) throw new Error('SUBSCRIPTION_FILTERS_REQUIRED')
    const id = `p2r2p-sub:${++this.#serial}`
    let closed = false
    const close = () => {
      if (closed) return
      closed = true
      const subscription = this.#subscriptions.get(id)
      if (!subscription) return
      this.#subscriptions.delete(id)
      try { this.send(JSON.stringify(['CLOSE', id])) } catch {}
      handlers.onclose?.()
    }
    this.#subscriptions.set(id, { filters, handlers, close })
    try { this.send(JSON.stringify(['REQ', id, ...filters])) } catch (error) {
      this.#subscriptions.delete(id)
      throw error
    }
    return { id, close }
  }

  publish (event) {
    if (!verifyEvent(event)) return Promise.reject(new Error('INVALID_EVENT'))
    return this.#sendEventOperation('EVENT', event, this.#publishes, 'PUBLISH_TIMEOUT')
  }

  async authenticate (getAuthEvent) {
    if (!this.#challenge) throw new Error('AUTH_CHALLENGE_MISSING')
    const event = await getAuthEvent({ relay: this.url, challenge: this.#challenge })
    if (!verifyEvent(event)) throw new Error('INVALID_AUTH_EVENT')
    return await this.#sendEventOperation('AUTH', event, this.#authentications, 'AUTH_TIMEOUT')
  }

  #sendEventOperation (type, event, map, timeoutCode) {
    if (map.has(event.id)) return map.get(event.id).promise
    const deferred = Promise.withResolvers()
    const timer = maybeUnref(setTimeout(() => this.#settleEvent(map, event.id, new Error(timeoutCode)), this.publishTimeout))
    map.set(event.id, { ...deferred, timer, promise: deferred.promise })
    try { this.send(JSON.stringify([type, event])) } catch (error) {
      this.#settleEvent(map, event.id, error)
    }
    return deferred.promise
  }

  countWithHll (filters, { signal } = {}) {
    if (signal?.aborted) return Promise.reject(new Error('COUNT_ABORTED'))
    const id = `p2r2p-count:${++this.#serial}`
    const deferred = Promise.withResolvers()
    const onAbort = () => this.#settleCount(id, null, new Error('COUNT_ABORTED'))
    this.#counts.set(id, { ...deferred, signal, onAbort })
    signal?.addEventListener('abort', onAbort, { once: true })
    try { this.send(JSON.stringify(['COUNT', id, ...filters])) } catch (error) {
      this.#settleCount(id, null, error)
    }
    return deferred.promise
  }

  #settleEvent (map, id, reason, value) {
    const pending = map.get(id)
    if (!pending) return
    map.delete(id)
    clearTimeout(pending.timer)
    if (reason) pending.reject(errorFrom(reason, 'OPERATION_REJECTED'))
    else pending.resolve(value)
  }

  #settleCount (id, payload, reason) {
    const pending = this.#counts.get(id)
    if (!pending) return
    this.#counts.delete(id)
    pending.signal?.removeEventListener('abort', pending.onAbort)
    if (reason) pending.reject(errorFrom(reason, 'COUNT_REJECTED'))
    else pending.resolve(payload)
  }

  async #handleMessage (message) {
    let data
    try { data = JSON.parse(await messageText(message.data)) } catch { throw new Error('INVALID_RELAY_MESSAGE') }
    if (!Array.isArray(data) || typeof data[0] !== 'string') throw new Error('INVALID_RELAY_MESSAGE')

    if (data[0] === 'EVENT') {
      const subscription = this.#subscriptions.get(data[1])
      if (!subscription) return
      const event = data[2]
      if (!verifyEvent(event) || !matchFilters(subscription.filters, event)) subscription.handlers.oninvalidevent?.(event)
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
      if (subscription) {
        this.#subscriptions.delete(id)
        subscription.handlers.onclose?.(errorFrom(data[2], 'SUBSCRIPTION_CLOSED'))
      } else this.#settleCount(id, null, errorFrom(data[2], 'COUNT_CLOSED'))
      return
    }
    if (data[0] === 'OK') {
      const reason = data[2] === true ? null : errorFrom(data[3], 'EVENT_REJECTED')
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
    const reason = errorFrom(event?.reason, 'CONNECTION_CLOSED')
    for (const [id, subscription] of this.#subscriptions) {
      this.#subscriptions.delete(id)
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
    const reason = new Error('CONNECTION_CLOSED')
    for (const [id, subscription] of this.#subscriptions) {
      this.#subscriptions.delete(id)
      subscription.handlers.onclose?.()
    }
    for (const id of [...this.#publishes.keys()]) this.#settleEvent(this.#publishes, id, reason)
    for (const id of [...this.#authentications.keys()]) this.#settleEvent(this.#authentications, id, reason)
    for (const id of [...this.#counts.keys()]) this.#settleCount(id, null, reason)
    if (socket && socket.readyState < 2) socket.close()
  }
}
