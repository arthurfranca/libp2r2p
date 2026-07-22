import { getPublicKey } from '../../key/index.js'
import { relayPool as defaultRelayPool } from '../../relay/index.js'
import {
  DEFAULT_TIMEOUT,
  DEFAULT_TIMEOUT_AFTER_FIRST_EOSE,
  NIP46_KIND,
  RELAY_SWITCH_WAIT_TIMEOUT
} from '../constants/index.js'
import {
  decodeNip46Frame,
  isNip46EventFor,
  requestError,
  validRequestFrame
} from '../helpers/frame.js'
import { normalizeBunkerPointer, parseNostrConnectURI } from '../helpers/url.js'
import { Nip46Transport, sameRelays, waitForNip46 } from './transport.js'

function cleanClientMetadata (value) {
  if (!value || typeof value !== 'object') return null
  const metadata = {}
  for (const key of ['name', 'url', 'image']) {
    if (typeof value[key] === 'string' && value[key]) metadata[key] = value[key]
  }
  return Object.keys(metadata).length ? metadata : null
}

// A reusable NIP-46 client for standard or application-defined commands.
export class Nip46Client {
  #secretKey
  #transport
  #pointer
  #onAuthUrl
  #onRequest

  constructor (clientSecretKey, pointer, {
    relayPool = defaultRelayPool,
    onAuthUrl,
    onRequest,
    onError,
    timeout = DEFAULT_TIMEOUT,
    timeoutAfterFirstEose = DEFAULT_TIMEOUT_AFTER_FIRST_EOSE
  } = {}) {
    const normalized = normalizeBunkerPointer(pointer)
    if (!normalized) throw new Error('INVALID_BUNKER_POINTER')
    this.#secretKey = clientSecretKey
    this.#pointer = normalized
    this.#onAuthUrl = onAuthUrl
    this.#onRequest = onRequest
    this.#transport = new Nip46Transport(clientSecretKey, {
      relayPool,
      networkTimeout: timeout,
      timeoutAfterFirstEose,
      onError
    })
    this.#transport.activateContext(this.#openResponseContext(normalized), { retirePrevious: false })
  }

  // Creates a client for a parsed direct `bunker://` pointer.
  static fromBunker (clientSecretKey, pointer, options = {}) {
    return new this(clientSecretKey, pointer, options)
  }

  // Waits for a signer response to a client-created `nostrconnect://` URI.
  static async fromURI (clientSecretKey, uri, options = {}) {
    const parsed = parseNostrConnectURI(uri)
    const clientPubkey = getPublicKey(clientSecretKey)
    if (!parsed || clientPubkey !== parsed.clientPubkey) throw new Error('INVALID_NOSTRCONNECT_URI')

    const relayPool = options.relayPool || defaultRelayPool
    const controller = new AbortController()
    const stream = relayPool.getLiveEventsGenerator({
      kinds: [NIP46_KIND],
      '#p': [clientPubkey],
      limit: 0
    }, parsed.relays, {
      signal: controller.signal,
      timeoutAfterFirstEose: options.timeoutAfterFirstEose ?? DEFAULT_TIMEOUT_AFTER_FIRST_EOSE
    })
    const found = Promise.withResolvers()
    const consume = (async () => {
      try {
        for await (const event of stream) {
          if (!isNip46EventFor(event, clientPubkey)) continue
          const response = decodeNip46Frame(event, clientSecretKey)
          if (response?.result === parsed.secret) {
            found.resolve({
              remoteSignerPubkey: event.pubkey,
              relays: parsed.relays,
              secret: parsed.secret
            })
          }
        }
      } catch (error) {
        if (error?.message !== 'Aborted') found.reject(error)
      }
    })()

    try {
      const timeout = options.timeout ?? DEFAULT_TIMEOUT
      const report = await waitForNip46(stream.ready, {
        timeout,
        signal: options.signal,
        label: 'NIP46_LISTENER_TIMEOUT'
      })
      if (!stream.readyRelays.length) {
        throw requestError(report.errors?.[0]?.reason || 'NIP46_NO_READY_RELAYS')
      }
      const pointer = await waitForNip46(found.promise, {
        timeout,
        signal: options.signal,
        label: 'NIP46_CONNECTION_TIMEOUT'
      })
      controller.abort()
      await consume

      const client = new this(clientSecretKey, pointer, options)
      await client.#awaitReady({ timeout, signal: options.signal })
      await client.#switchRelaysAfterConnect()
      return client
    } catch (error) {
      controller.abort()
      await consume.catch(() => {})
      throw error
    }
  }

  get clientPubkey () { return this.#transport.pubkey }
  get pointer () { return { ...this.#pointer, relays: [...this.#pointer.relays] } }

  // Connects and immediately asks the remote signer for its preferred relays.
  async connect ({ requestedPermissions = [], clientMetadata, timeout = null, signal } = {}) {
    const permissions = Array.isArray(requestedPermissions)
      ? requestedPermissions.filter(permission => typeof permission === 'string' && permission).join(',')
      : ''
    const metadata = cleanClientMetadata(clientMetadata)
    const params = [this.#pointer.remoteSignerPubkey, this.#pointer.secret || '']
    if (permissions || metadata) params.push(permissions)
    if (metadata) params.push(JSON.stringify(metadata))
    await this.sendRequest('connect', params, { timeout, signal })
    return this.#switchRelaysAfterConnect()
  }

  // Sends a positional-string request after the response listener is ready.
  sendRequest (method, params = [], options = {}) {
    return this.#transport.sendRequest(this.#pointer.remoteSignerPubkey, method, params, options)
  }

  async ping (options) {
    const response = await this.sendRequest('ping', [], options)
    if (response !== 'pong') throw new Error(`NIP46_PING_FAILED:${response}`)
  }

  async logout (options) {
    const response = await this.sendRequest('logout', [], options)
    if (response !== 'ack') throw new Error(`NIP46_LOGOUT_FAILED:${response}`)
    await this.close()
  }

  // Moves to the relay list returned by the remote signer, if it changed.
  async switchRelays (options) {
    const response = await this.sendRequest('switch_relays', [], options)
    if (response === null) return false
    let relays
    try {
      relays = JSON.parse(response)
    } catch {
      return false
    }
    if (relays === null) return false

    const nextPointer = normalizeBunkerPointer({ ...this.#pointer, relays })
    if (!nextPointer || sameRelays(nextPointer.relays, this.#pointer.relays)) return false

    const nextContext = this.#openResponseContext(nextPointer)
    try {
      await this.#transport.awaitContextReady(nextContext)
    } catch {
      await this.#transport.closeContext(nextContext)
      return false
    }

    this.#pointer = nextPointer
    this.#transport.activateContext(nextContext)
    return true
  }

  close () {
    return this.#transport.close()
  }

  async #switchRelaysAfterConnect () {
    try {
      return await this.switchRelays({ timeout: RELAY_SWITCH_WAIT_TIMEOUT })
    } catch {
      return false
    }
  }

  #openResponseContext (pointer) {
    let context
    context = this.#transport.openContext({
      filter: {
        kinds: [NIP46_KIND],
        authors: [pointer.remoteSignerPubkey],
        '#p': [this.#transport.pubkey],
        limit: 0
      },
      relays: pointer.relays,
      onEvent: event => this.#receiveEvent(event, context)
    })
    return context
  }

  #awaitReady (options) {
    return this.#transport.awaitContextReady(this.#transport.activeContext, options)
  }

  #receiveEvent (event, context) {
    if (event?.pubkey !== this.#pointer.remoteSignerPubkey ||
        !isNip46EventFor(event, this.#transport.pubkey) ||
        !this.#transport.isNewEvent(event)) return
    const frame = decodeNip46Frame(event, this.#secretKey)
    if (!frame) return

    if (Object.hasOwn(frame, 'method')) {
      return this.#handleRequest(event.pubkey, frame, context)
    }
    this.#transport.receiveResponse(event.pubkey, frame, { onAuthUrl: this.#onAuthUrl })
  }

  async #handleRequest (peerPubkey, request, context) {
    if (!validRequestFrame(request)) {
      if (typeof request?.id === 'string') {
        await this.#transport.reply(peerPubkey, request.id, null, 'NIP46_INVALID_REQUEST', { context })
      }
      return
    }

    try {
      if (!this.#onRequest) throw new Error('NIP46_METHOD_NOT_SUPPORTED')
      const result = await this.#onRequest({
        method: request.method,
        params: [...request.params],
        peerPubkey
      })
      if (typeof result !== 'string') throw new Error('NIP46_RESULT_REQUIRED')
      await this.#transport.reply(peerPubkey, request.id, result, null, { context })
    } catch (error) {
      await this.#transport.reply(peerPubkey, request.id, null, error?.message || 'NIP46_REQUEST_REJECTED', { context })
    }
  }
}
