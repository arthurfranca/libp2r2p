import { ValidationError } from '../../error/index.js'
import { relayPool as defaultRelayPool } from '../../relay/index.js'
import { DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_AFTER_FIRST_EOSE, NIP46_KIND } from '../constants/index.js'
import {
  decodeNip46Frame,
  isNip46EventFor,
  isValidRequestFrame
} from '../helpers/frame.js'
import { areRelaySetsEqual, Nip46Transport } from './transport.js'

function cleanRelays (relays) {
  return [...new Set((Array.isArray(relays) ? relays : [])
    .filter(relay => typeof relay === 'string' && relay))]
}

function parseClientMetadata (value) {
  if (!value) return null
  try {
    const metadata = JSON.parse(value)
    return metadata && typeof metadata === 'object' && !Array.isArray(metadata) ? metadata : null
  } catch {
    return null
  }
}

// A one-secret, single-client NIP-46 server session.
export class Nip46ServerSession {
  #secretKey
  #secret
  #secretConsumed = false
  #transport
  #relays
  #clientPubkey = null
  #connectingPubkey = null
  #pendingContext = null
  #onConnect
  #onRequest
  #onLogout

  constructor (serverSecretKey, {
    relays,
    secret = '',
    relayPool = defaultRelayPool,
    onConnect,
    onRequest,
    onLogout,
    onError,
    timeout = DEFAULT_TIMEOUT,
    timeoutAfterFirstEose = DEFAULT_TIMEOUT_AFTER_FIRST_EOSE
  } = {}) {
    this.#relays = cleanRelays(relays)
    if (!this.#relays.length) throw new ValidationError('NIP46_RELAYS_REQUIRED')
    if (typeof secret !== 'string') throw new ValidationError('NIP46_SECRET_REQUIRED')
    this.#secretKey = serverSecretKey
    this.#secret = secret
    this.#onConnect = onConnect
    this.#onRequest = onRequest
    this.#onLogout = onLogout
    this.#transport = new Nip46Transport(serverSecretKey, {
      relayPool,
      networkTimeout: timeout,
      timeoutAfterFirstEose,
      onError
    })
  }

  get serverPubkey () { return this.#transport.pubkey }
  get clientPubkey () { return this.#clientPubkey }
  get relays () { return Object.freeze([...this.#relays]) }
  get readyRelays () { return this.#transport.readyRelays }

  // Opens the live request listener and waits until at least one relay is ready.
  start (options) {
    if (!this.#transport.activeContext) {
      this.#transport.activateContext(this.#openContext(this.#relays), { retirePrevious: false })
    }
    return this.#transport.awaitContextReady(this.#transport.activeContext, options)
  }

  // Sends an application-defined request to the connected client.
  sendRequest (method, params = [], options = {}) {
    if (!this.#clientPubkey) return Promise.reject(new Error('NIP46_NOT_CONNECTED'))
    return this.#transport.sendRequest(this.#clientPubkey, method, params, options)
  }

  // Opens replacements now and switches only after the client requests them.
  async updateRelays (relays) {
    const nextRelays = cleanRelays(relays)
    if (!nextRelays.length) throw new ValidationError('NIP46_RELAYS_REQUIRED')
    if (areRelaySetsEqual(nextRelays, this.#relays)) return false
    if (!this.#transport.activeContext) {
      this.#relays = nextRelays
      return true
    }

    const nextContext = this.#openContext(nextRelays)
    try {
      await this.#transport.awaitContextReady(nextContext)
    } catch (error) {
      await this.#transport.closeContext(nextContext)
      throw error
    }
    if (this.#pendingContext) await this.#transport.closeContext(this.#pendingContext)
    this.#relays = nextRelays
    this.#pendingContext = nextContext
    return true
  }

  close () {
    this.#pendingContext = null
    return this.#transport.close()
  }

  #openContext (relays) {
    const context = this.#transport.openContext({
      filter: {
        kinds: [NIP46_KIND],
        '#p': [this.#transport.pubkey],
        limit: 0
      },
      relays,
      onEvent: event => this.#receiveEvent(event, context)
    })
    return context
  }

  #receiveEvent (event, context) {
    if (!isNip46EventFor(event, this.#transport.pubkey) || !this.#transport.isNewEvent(event)) return
    const frame = decodeNip46Frame(event, this.#secretKey)
    if (!frame) return

    if (!Object.hasOwn(frame, 'method')) {
      if (event.pubkey === this.#clientPubkey) this.#transport.receiveResponse(event.pubkey, frame)
      return
    }
    return this.#handleRequest(event.pubkey, frame, context)
  }

  async #handleRequest (peerPubkey, request, context) {
    if (!isValidRequestFrame(request)) {
      if (typeof request?.id === 'string') {
        await this.#transport.reply(peerPubkey, request.id, null, 'NIP46_INVALID_REQUEST', { context })
      }
      return
    }

    if (request.method === 'connect') {
      await this.#handleConnect(peerPubkey, request, context)
      return
    }
    if (!this.#clientPubkey || peerPubkey !== this.#clientPubkey) {
      await this.#transport.reply(peerPubkey, request.id, null, 'NIP46_NOT_CONNECTED', { context })
      return
    }

    try {
      if (request.method === 'ping') {
        await this.#transport.reply(peerPubkey, request.id, 'pong', null, { context })
        return
      }
      if (request.method === 'switch_relays') {
        await this.#transport.reply(peerPubkey, request.id, JSON.stringify(this.#relays), null, { context })
        if (this.#pendingContext) {
          const nextContext = this.#pendingContext
          this.#pendingContext = null
          const previous = this.#transport.activateContext(nextContext, { retirePrevious: false })
          if (previous && previous !== nextContext) this.#transport.retireContext(previous)
        }
        return
      }
      if (request.method === 'logout') {
        await this.#transport.reply(peerPubkey, request.id, 'ack', null, { context })
        await this.#onLogout?.({ clientPubkey: peerPubkey })
        await this.close()
        return
      }
      if (!this.#onRequest) throw new Error('NIP46_METHOD_NOT_SUPPORTED')
      const result = await this.#onRequest({
        method: request.method,
        params: [...request.params],
        clientPubkey: peerPubkey
      })
      if (typeof result !== 'string') throw new Error('NIP46_RESULT_REQUIRED')
      await this.#transport.reply(peerPubkey, request.id, result, null, { context })
    } catch (error) {
      await this.#transport.reply(peerPubkey, request.id, null, error?.message || 'NIP46_REQUEST_REJECTED', { context })
    }
  }

  async #handleConnect (peerPubkey, request, context) {
    if (this.#clientPubkey || this.#connectingPubkey || this.#secretConsumed) {
      await this.#transport.reply(peerPubkey, request.id, null, 'NIP46_ALREADY_CONNECTED', { context })
      return
    }
    if (request.params[0] !== this.#transport.pubkey || request.params[1] !== this.#secret) {
      await this.#transport.reply(peerPubkey, request.id, null, 'NIP46_INVALID_SECRET', { context })
      return
    }

    const requestedPermissions = request.params[2]
      ? request.params[2].split(',').filter(Boolean)
      : []
    const clientMetadata = parseClientMetadata(request.params[3])
    this.#connectingPubkey = peerPubkey
    try {
      await this.#onConnect?.({ peerPubkey, requestedPermissions, clientMetadata })
    } catch (error) {
      this.#connectingPubkey = null
      await this.#transport.reply(peerPubkey, request.id, null, error?.message || 'NIP46_CONNECT_REJECTED', { context })
      return
    }

    this.#clientPubkey = peerPubkey
    this.#secretConsumed = true
    try {
      await this.#transport.reply(peerPubkey, request.id, 'ack', null, { context })
    } catch (error) {
      this.#clientPubkey = null
      this.#secretConsumed = false
      throw error
    } finally {
      this.#connectingPubkey = null
    }
  }
}
