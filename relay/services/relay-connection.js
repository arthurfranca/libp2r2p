import { Relay } from 'nostr-tools/relay'
import { maybeUnref } from '../helpers/timer.js'

function relayError (reason, fallback) {
  return reason instanceof Error ? reason : new Error(reason || fallback)
}

// Relay.auth() caches one authentication promise per connection. NIP-42 permits
// several pubkeys on one connection, so this adapter tracks AUTH replies by
// signed auth-event id instead of using Relay.auth().
export class RelayConnection extends Relay {
  #challenge = null
  #pendingAuths = new Map()
  #countSerial = 0
  #pendingCounts = new Map()

  constructor (url) {
    super(url)
    this.onclose = () => {
      this.#challenge = null
      this.#rejectPendingAuths(new Error('AUTH_CONNECTION_CLOSED'))
      this.#rejectPendingCounts(new Error('COUNT_CONNECTION_CLOSED'))
    }
  }

  #settleAuth (eventId, { success, reason }) {
    const pending = this.#pendingAuths.get(eventId)
    if (!pending) return

    this.#pendingAuths.delete(eventId)
    clearTimeout(pending.timer)
    if (success) pending.resolve(reason)
    else pending.reject(relayError(reason, 'AUTH_REJECTED'))
  }

  #rejectPendingAuths (reason) {
    for (const eventId of [...this.#pendingAuths.keys()]) {
      this.#settleAuth(eventId, { success: false, reason })
    }
  }

  #settleCount (id, { payload, reason }) {
    const pending = this.#pendingCounts.get(id)
    if (!pending) return

    this.#pendingCounts.delete(id)
    pending.signal?.removeEventListener('abort', pending.onAbort)
    if (reason !== undefined) pending.reject(relayError(reason, 'COUNT_REJECTED'))
    else pending.resolve(payload)
  }

  #rejectPendingCounts (reason) {
    for (const id of [...this.#pendingCounts.keys()]) {
      this.#settleCount(id, { reason })
    }
  }

  // Sends a caller-signed AUTH event and waits for the matching NIP-42 OK.
  // Each auth event is independent, so different pubkeys can share this socket.
  async authenticate (getAuthEvent) {
    if (!this.#challenge) throw new Error('AUTH_CHALLENGE_MISSING')

    const event = await getAuthEvent({
      relay: this.url,
      challenge: this.#challenge
    })
    if (typeof event?.id !== 'string' || !event.id) throw new Error('AUTH_EVENT_ID_REQUIRED')
    const existing = this.#pendingAuths.get(event.id)
    if (existing) return existing.promise

    let resolveAuth
    let rejectAuth
    const promise = new Promise((resolve, reject) => {
      resolveAuth = resolve
      rejectAuth = reject
    })
    const timer = maybeUnref(setTimeout(() => {
      this.#settleAuth(event.id, { success: false, reason: new Error('AUTH_TIMEOUT') })
    }, this.publishTimeout))
    this.#pendingAuths.set(event.id, {
      resolve: resolveAuth,
      reject: rejectAuth,
      timer,
      promise
    })

    Promise.resolve(this.send(JSON.stringify(['AUTH', event]))).catch(reason => {
      this.#settleAuth(event.id, { success: false, reason })
    })
    return promise
  }

  // nostr-tools@2.23.5 exposes only Relay.count(), which discards the NIP-45
  // HLL payload. Keep these requests separate so callers can aggregate it.
  countWithHll (filters, { signal } = {}) {
    if (signal?.aborted) return Promise.reject(new Error('COUNT_ABORTED'))

    const id = `p2r2p-count:${++this.#countSerial}`
    let resolveCount
    let rejectCount
    const promise = new Promise((resolve, reject) => {
      resolveCount = resolve
      rejectCount = reject
    })
    const onAbort = () => {
      this.#settleCount(id, { reason: new Error('COUNT_ABORTED') })
    }
    this.#pendingCounts.set(id, {
      resolve: resolveCount,
      reject: rejectCount,
      signal,
      onAbort
    })
    signal?.addEventListener('abort', onAbort, { once: true })

    let message
    try {
      message = JSON.stringify(['COUNT', id, ...filters])
    } catch (error) {
      this.#settleCount(id, { reason: error })
      return promise
    }
    Promise.resolve(this.send(message)).catch(reason => {
      this.#settleCount(id, { reason })
    })
    return promise
  }

  // Relay assigns this method to its WebSocket handler while connecting. Capture
  // AUTH and raw COUNT replies before Relay's parser discards COUNT HLL payloads.
  _onmessage (message) {
    try {
      const data = JSON.parse(message.data)
      if (data[0] === 'AUTH' && typeof data[1] === 'string') {
        this.#challenge = data[1]
      } else if (data[0] === 'OK' && typeof data[1] === 'string') {
        this.#settleAuth(data[1], { success: data[2] === true, reason: data[3] })
      } else if (data[0] === 'COUNT' && typeof data[1] === 'string') {
        this.#settleCount(data[1], { payload: data[2] })
      } else if (data[0] === 'CLOSED' && typeof data[1] === 'string') {
        this.#settleCount(data[1], { reason: data[2] || 'COUNT_CLOSED' })
      }
    } catch {
      // Relay's own parser below reports malformed relay messages.
    }

    return super._onmessage(message)
  }

  close () {
    this.#challenge = null
    this.#rejectPendingAuths(new Error('AUTH_CONNECTION_CLOSED'))
    this.#rejectPendingCounts(new Error('COUNT_CONNECTION_CLOSED'))
    return super.close()
  }
}
