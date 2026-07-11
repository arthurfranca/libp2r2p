import { Relay } from 'nostr-tools/relay'
import { maybeUnref } from '../helpers/timer.js'

function authError (reason, fallback) {
  return reason instanceof Error ? reason : new Error(reason || fallback)
}

// Relay.auth() caches one authentication promise per connection. NIP-42 permits
// several pubkeys on one connection, so this adapter tracks AUTH replies by
// signed auth-event id instead of using Relay.auth().
export class Nip42Relay extends Relay {
  #challenge = null
  #pendingAuths = new Map()

  constructor (url) {
    super(url)
    this.onclose = () => {
      this.#challenge = null
      this.#rejectPendingAuths(new Error('AUTH_CONNECTION_CLOSED'))
    }
  }

  #settleAuth (eventId, { success, reason }) {
    const pending = this.#pendingAuths.get(eventId)
    if (!pending) return

    this.#pendingAuths.delete(eventId)
    clearTimeout(pending.timer)
    if (success) pending.resolve(reason)
    else pending.reject(authError(reason, 'AUTH_REJECTED'))
  }

  #rejectPendingAuths (reason) {
    for (const eventId of [...this.#pendingAuths.keys()]) {
      this.#settleAuth(eventId, { success: false, reason })
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

  // Relay assigns this method to its WebSocket handler while connecting. Capture
  // the NIP-42 control messages, then let Relay handle normal protocol state.
  _onmessage (message) {
    try {
      const data = JSON.parse(message.data)
      if (data[0] === 'AUTH' && typeof data[1] === 'string') {
        this.#challenge = data[1]
      } else if (data[0] === 'OK' && typeof data[1] === 'string') {
        this.#settleAuth(data[1], { success: data[2] === true, reason: data[3] })
      }
    } catch {
      // Relay's own parser below reports malformed relay messages.
    }

    return super._onmessage(message)
  }

  close () {
    this.#challenge = null
    this.#rejectPendingAuths(new Error('AUTH_CONNECTION_CLOSED'))
    return super.close()
  }
}
