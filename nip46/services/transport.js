import { ValidationError } from '../../error/index.js'
import { getPublicKey } from '../../key/index.js'
import { relayPool as defaultRelayPool } from '../../relay/index.js'
import { DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_AFTER_FIRST_EOSE } from '../constants/index.js'
import { createNip46Event, requestError } from '../helpers/frame.js'

const RETIRE_CONTEXT_AFTER_MS = 5000
const SEEN_EVENT_LIMIT = 500

function requestId () {
  const bytes = new Uint8Array(8)
  globalThis.crypto.getRandomValues(bytes)
  return [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('')
}

function requestExtension (value) {
  if (value === undefined || value === null) return null
  if (typeof value !== 'object' || Array.isArray(value)) throw new ValidationError('NIP46_REQUEST_EXTENSION_REQUIRED')
  for (const key of ['id', 'method', 'params']) {
    if (Object.hasOwn(value, key)) throw new ValidationError(`NIP46_REQUEST_EXTENSION_CANNOT_SET_${key.toUpperCase()}`)
  }
  return value
}

export function waitForNip46 (promise, { timeout = null, signal, label = 'NIP46_TIMEOUT' } = {}) {
  if (signal?.aborted) return Promise.reject(new Error('Aborted'))
  if (timeout === null && !signal) return Promise.resolve(promise)

  return new Promise((resolve, reject) => {
    let timer = null
    let settled = false
    const finish = (fn, value) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      fn(value)
    }
    const onAbort = () => finish(reject, new Error('Aborted'))

    if (timeout !== null) timer = setTimeout(() => finish(reject, new Error(label)), timeout)
    signal?.addEventListener('abort', onAbort, { once: true })
    Promise.resolve(promise).then(
      value => finish(resolve, value),
      error => finish(reject, error)
    )
  })
}

export function areRelaySetsEqual (left, right) {
  if (left.length !== right.length) return false
  const values = new Set(left)
  return right.every(value => values.has(value))
}

// Owns the shared encrypted RPC mechanics while client/server classes enforce
// their different connection and authorization rules.
export class Nip46Transport {
  #secretKey
  #pubkey
  #relayPool
  #networkTimeout
  #timeoutAfterFirstEose
  #onError
  #contexts = new Set()
  #activeContext = null
  #retireTimers = new Set()
  #pending = new Map()
  #seenEventIds = new Set()
  #closed = false

  constructor (secretKey, {
    relayPool = defaultRelayPool,
    networkTimeout = DEFAULT_TIMEOUT,
    timeoutAfterFirstEose = DEFAULT_TIMEOUT_AFTER_FIRST_EOSE,
    onError
  } = {}) {
    if (!(secretKey instanceof Uint8Array)) throw new ValidationError('NIP46_SECRET_KEY_REQUIRED')
    if (!relayPool?.getLiveEventsGenerator || !relayPool?.sendEvent) throw new ValidationError('RELAY_POOL_REQUIRED')
    this.#secretKey = secretKey
    this.#pubkey = getPublicKey(secretKey)
    this.#relayPool = relayPool
    this.#networkTimeout = networkTimeout
    this.#timeoutAfterFirstEose = timeoutAfterFirstEose
    this.#onError = onError
  }

  get pubkey () { return this.#pubkey }
  get closed () { return this.#closed }
  get activeContext () { return this.#activeContext }
  get readyRelays () { return this.#activeContext?.stream.readyRelays || Object.freeze([]) }

  openContext ({ filter, relays, onEvent }) {
    if (this.#closed) throw new Error('NIP46_CLOSED')
    const controller = new AbortController()
    const stream = this.#relayPool.getLiveEventsGenerator(filter, relays, {
      signal: controller.signal,
      timeoutAfterFirstEose: this.#timeoutAfterFirstEose
    })
    const context = { controller, stream, relays: [...relays], consume: null }
    this.#contexts.add(context)
    context.consume = (async () => {
      try {
        for await (const event of stream) {
          Promise.resolve(onEvent(event)).catch(error => this.#reportError(error))
        }
      } catch (error) {
        if (!this.#closed && error?.message !== 'Aborted') this.#reportError(error)
      }
    })()
    return context
  }

  async awaitContextReady (context, { timeout = this.#networkTimeout, signal } = {}) {
    const report = await waitForNip46(context.stream.ready, {
      timeout,
      signal,
      label: 'NIP46_LISTENER_TIMEOUT'
    })
    if (!context.stream.readyRelays.length) {
      throw requestError(report.errors?.[0]?.reason || 'NIP46_NO_READY_RELAYS')
    }
    return context.stream.readyRelays
  }

  activateContext (context, { retirePrevious = true } = {}) {
    const previous = this.#activeContext
    this.#activeContext = context
    if (retirePrevious && previous && previous !== context) this.retireContext(previous)
    return previous
  }

  retireContext (context, delay = RETIRE_CONTEXT_AFTER_MS) {
    if (!context || !this.#contexts.has(context)) return
    const timer = setTimeout(() => {
      this.#retireTimers.delete(timer)
      this.closeContext(context)
    }, delay)
    this.#retireTimers.add(timer)
  }

  async closeContext (context) {
    if (!context || !this.#contexts.delete(context)) return
    context.controller.abort()
    try {
      await context.consume
    } catch {
      // Unexpected stream failures are reported by the consume loop.
    }
  }

  isNewEvent (event) {
    if (!event?.id) return true
    if (this.#seenEventIds.has(event.id)) return false
    if (this.#seenEventIds.size >= SEEN_EVENT_LIMIT) {
      this.#seenEventIds.delete(this.#seenEventIds.values().next().value)
    }
    this.#seenEventIds.add(event.id)
    return true
  }

  async sendRequest (peerPubkey, method, params = [], {
    timeout = null,
    signal,
    extension
  } = {}) {
    if (this.#closed) throw new Error('NIP46_CLOSED')
    if (typeof method !== 'string' || !method) throw new ValidationError('NIP46_METHOD_REQUIRED')
    if (!Array.isArray(params) || !params.every(param => typeof param === 'string')) {
      throw new ValidationError('NIP46_PARAMS_REQUIRED')
    }
    if (signal?.aborted) throw new Error('Aborted')
    const context = this.#activeContext
    if (!context) throw new Error('NIP46_LISTENER_UNAVAILABLE')
    await this.awaitContextReady(context, { signal })

    const id = requestId()
    const response = Promise.withResolvers()
    const extra = requestExtension(extension)
    this.#pending.set(id, { ...response, peerPubkey })

    try {
      await this.publish(peerPubkey, { id, method, params, ...(extra || {}) }, { context })
      return await waitForNip46(response.promise, {
        timeout,
        signal,
        label: 'NIP46_REQUEST_TIMEOUT'
      })
    } finally {
      const pending = this.#pending.get(id)
      if (pending?.promise === response.promise) this.#pending.delete(id)
    }
  }

  receiveResponse (peerPubkey, response, { onAuthUrl } = {}) {
    if (!response || typeof response.id !== 'string') return false
    const pending = this.#pending.get(response.id)
    if (!pending || pending.peerPubkey !== peerPubkey) return false

    if (response.result === 'auth_url') {
      try {
        onAuthUrl?.(typeof response.error === 'string' ? response.error : '')
      } catch (error) {
        this.#reportError(error)
      }
      return true
    }

    this.#pending.delete(response.id)
    if (Object.hasOwn(response, 'error') && response.error !== null && response.error !== undefined) {
      pending.reject(requestError(response.error))
    } else if (Object.hasOwn(response, 'result')) {
      pending.resolve(response.result)
    } else {
      pending.reject(new Error('NIP46_INVALID_RESPONSE'))
    }
    return true
  }

  reply (peerPubkey, id, result, error = null, options) {
    return this.publish(peerPubkey, { id, result, error }, options)
  }

  async publish (peerPubkey, payload, { context = this.#activeContext } = {}) {
    if (this.#closed) throw new Error('NIP46_CLOSED')
    if (!context) throw new Error('NIP46_LISTENER_UNAVAILABLE')
    await this.awaitContextReady(context)
    const relays = context.stream.readyRelays
    if (!relays.length) throw new Error('NIP46_NO_READY_RELAYS')
    const event = createNip46Event({ secretKey: this.#secretKey, recipientPubkey: peerPubkey, payload })
    const published = await this.#relayPool.sendEvent(event, relays, {
      timeout: this.#networkTimeout,
      timeoutUntilFirstFulfillment: null
    })
    if (!published.success) {
      const report = await published.promise
      throw requestError(report.errors?.[0]?.reason || 'NIP46_PUBLISH_FAILED')
    }
    return event
  }

  async close () {
    if (this.#closed) return
    this.#closed = true
    for (const timer of this.#retireTimers) clearTimeout(timer)
    this.#retireTimers.clear()
    const contexts = [...this.#contexts]
    for (const context of contexts) context.controller.abort()
    await Promise.all(contexts.map(context => this.closeContext(context)))
    this.#activeContext = null
    for (const pending of this.#pending.values()) pending.reject(new Error('NIP46_CLOSED'))
    this.#pending.clear()
  }

  #reportError (error) {
    try {
      if (this.#onError) {
        Promise.resolve(this.#onError(error)).catch(callbackError => {
          console.error('NIP46 onError failed:', callbackError)
        })
      } else console.error('NIP46 stream failed:', error)
    } catch (callbackError) {
      console.error('NIP46 onError failed:', callbackError)
    }
  }
}
