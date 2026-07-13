import { finalizeEvent, getPublicKey, verifyEvent } from 'nostr-tools'
import { decrypt, encrypt, getConversationKey } from 'nostr-tools/nip44'
import { relayPool as defaultRelayPool } from '../../relay/index.js'
import {
  DEFAULT_TIMEOUT,
  DEFAULT_TIMEOUT_AFTER_FIRST_EOSE,
  NIP46_KIND,
  RELAY_SWITCH_WAIT_TIMEOUT
} from '../constants/index.js'
import { normalizeBunkerPointer, parseNostrConnectURI } from '../helpers/url.js'

const BUNKER_CREATE = Symbol('BunkerSigner-create')
const PUBKEY = /^[0-9a-f]{64}$/

function hasPTag (event, pubkey) {
  return Array.isArray(event?.tags) && event.tags.some(tag => tag?.[0] === 'p' && tag?.[1] === pubkey)
}

function sameRelays (left, right) {
  if (left.length !== right.length) return false
  const values = new Set(left)
  return right.every(value => values.has(value))
}

function requestId () {
  const bytes = new Uint8Array(8)
  globalThis.crypto.getRandomValues(bytes)
  return [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('')
}

function timeoutError (label = 'NIP46_TIMEOUT') {
  return new Error(label)
}

function waitFor (promise, { timeout, signal, label = 'NIP46_TIMEOUT' } = {}) {
  if (signal?.aborted) return Promise.reject(new Error('Aborted'))
  if (timeout === null && !signal) return Promise.resolve(promise)

  return new Promise((resolve, reject) => {
    let timer = null
    const finish = (fn, value) => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      fn(value)
    }
    const onAbort = () => finish(reject, new Error('Aborted'))

    if (timeout !== null) timer = setTimeout(() => finish(reject, timeoutError(label)), timeout)
    signal?.addEventListener('abort', onAbort, { once: true })
    Promise.resolve(promise).then(
      value => finish(resolve, value),
      error => finish(reject, error)
    )
  })
}

function requestError (reason = 'NIP46_REQUEST_REJECTED') {
  return reason instanceof Error ? reason : new Error(typeof reason === 'string' && reason ? reason : 'NIP46_REQUEST_REJECTED')
}

function requestExtension (value) {
  if (value === undefined || value === null) return null
  if (typeof value !== 'object' || Array.isArray(value)) throw new Error('NIP46_REQUEST_EXTENSION_REQUIRED')
  for (const key of ['id', 'method', 'params']) {
    if (Object.hasOwn(value, key)) throw new Error(`NIP46_REQUEST_EXTENSION_CANNOT_SET_${key.toUpperCase()}`)
  }
  return value
}

function validPubkey (value) {
  return typeof value === 'string' && PUBKEY.test(value)
}

function clientMetadata (value) {
  if (!value || typeof value !== 'object') return null
  const metadata = {}
  for (const key of ['name', 'url', 'image']) {
    if (typeof value[key] === 'string' && value[key]) metadata[key] = value[key]
  }
  return Object.keys(metadata).length ? metadata : null
}

/** A NIP-46 remote signer client backed by libp2r2p's RelayPool. */
export class BunkerSigner {
  #clientSecretKey
  #clientPubkey
  #relayPool
  #onAuthUrl
  #timeout
  #pointer = null
  #activeContext = null
  #contexts = new Set()
  #retireTimers = new Set()
  #pending = new Map()
  #cachedPubkey = null
  #closed = false

  constructor (token, clientSecretKey, { relayPool = defaultRelayPool, onAuthUrl, timeout = DEFAULT_TIMEOUT } = {}) {
    if (token !== BUNKER_CREATE) throw new Error('USE_BunkerSigner.fromBunker_OR_fromURI')
    if (!(clientSecretKey instanceof Uint8Array)) throw new Error('CLIENT_SECRET_KEY_REQUIRED')
    if (!relayPool?.getLiveEventsGenerator || !relayPool?.sendEvent) throw new Error('RELAY_POOL_REQUIRED')
    this.#clientSecretKey = clientSecretKey
    this.#clientPubkey = getPublicKey(clientSecretKey)
    this.#relayPool = relayPool
    this.#onAuthUrl = onAuthUrl
    this.#timeout = timeout
    Object.preventExtensions(this)
  }

  /** Creates a signer for a parsed direct `bunker://` pointer. */
  static fromBunker (clientSecretKey, pointer, options = {}) {
    const normalized = normalizeBunkerPointer(pointer)
    if (!normalized) throw new Error('INVALID_BUNKER_POINTER')
    const signer = new BunkerSigner(BUNKER_CREATE, clientSecretKey, options)
    signer.#pointer = normalized
    signer.#activeContext = signer.#openResponseContext(normalized)
    return signer
  }

  /** Waits for a remote signer to accept a client-created `nostrconnect://` URI. */
  static async fromURI (clientSecretKey, uri, options = {}) {
    const parsed = parseNostrConnectURI(uri)
    if (!parsed || getPublicKey(clientSecretKey) !== parsed.clientPubkey) throw new Error('INVALID_NOSTRCONNECT_URI')

    const signer = new BunkerSigner(BUNKER_CREATE, clientSecretKey, options)
    const found = Promise.withResolvers()
    const discovery = signer.#openContext({
      filter: { kinds: [NIP46_KIND], '#p': [signer.#clientPubkey], limit: 0 },
      relays: parsed.relays,
      onEvent: event => {
        if (!signer.#isEventForClient(event)) return
        try {
          const content = JSON.parse(decrypt(event.content, getConversationKey(signer.#clientSecretKey, event.pubkey)))
          if (content?.result === parsed.secret) {
            found.resolve({
              remoteSignerPubkey: event.pubkey,
              relays: parsed.relays,
              secret: parsed.secret
            })
          }
        } catch {
          // Ignore malformed or unrelated discovery responses.
        }
      }
    })

    try {
      await signer.#awaitContextReady(discovery, options)
      const pointer = await waitFor(found.promise, {
        timeout: signer.#timeout,
        signal: options.signal,
        label: 'NIP46_CONNECTION_TIMEOUT'
      })
      await signer.#closeContext(discovery)
      signer.#pointer = pointer
      signer.#activeContext = signer.#openResponseContext(pointer)
      await signer.#awaitContextReady(signer.#activeContext, options)
      if (!options.skipRelaySwitch) {
        // A bunker that omits switch_relays is still connected. Give it a brief
        // chance to replace the relay set without delaying the completed URI flow.
        await waitFor(signer.switchRelays(), {
          timeout: RELAY_SWITCH_WAIT_TIMEOUT,
          label: 'NIP46_RELAY_SWITCH_TIMEOUT'
        }).catch(() => false)
      }
      return signer
    } catch (error) {
      await signer.close()
      throw error
    }
  }

  get clientPubkey () {
    return this.#clientPubkey
  }

  get pointer () {
    return this.#pointer && { ...this.#pointer, relays: [...this.#pointer.relays] }
  }

  /** Establishes a direct bunker session, optionally identifying this client. */
  async connect ({ requestedPermissions = [], clientMetadata: metadata } = {}) {
    const permissions = Array.isArray(requestedPermissions)
      ? requestedPermissions.filter(permission => typeof permission === 'string' && permission).join(',')
      : ''
    const cleanMetadata = clientMetadata(metadata)
    const params = [this.#pointer.remoteSignerPubkey, this.#pointer.secret || '']
    if (permissions || cleanMetadata) params.push(permissions)
    if (cleanMetadata) params.push(JSON.stringify(cleanMetadata))
    await this.sendRequest('connect', params)
  }

  /**
   * Sends one NIP-46 request. `extension` can add application-defined request
   * fields, but cannot replace the protocol's id, method, or params fields.
   */
  async sendRequest (method, params = [], options = {}) {
    if (this.#closed || !this.#pointer) throw new Error('NIP46_CLOSED')
    if (typeof method !== 'string' || !method) throw new Error('NIP46_METHOD_REQUIRED')
    if (!Array.isArray(params)) throw new Error('NIP46_PARAMS_REQUIRED')
    const extra = requestExtension(options?.extension)

    const id = requestId()
    const response = Promise.withResolvers()
    this.#pending.set(id, response)

    try {
      const relays = await this.#readyRelays()
      const event = finalizeEvent({
        kind: NIP46_KIND,
        created_at: Math.floor(Date.now() / 1000),
        tags: [['p', this.#pointer.remoteSignerPubkey]],
        content: encrypt(JSON.stringify({ id, method, params, ...(extra || {}) }), getConversationKey(this.#clientSecretKey, this.#pointer.remoteSignerPubkey))
      }, this.#clientSecretKey)
      const published = await this.#relayPool.sendEvent(event, relays, {
        timeout: this.#timeout,
        timeoutUntilFirstFulfillment: null
      })
      if (!published.success) {
        const report = await published.promise
        throw requestError(report.errors?.[0]?.reason || 'NIP46_PUBLISH_FAILED')
      }
    } catch (error) {
      if (this.#pending.get(id) === response) {
        this.#pending.delete(id)
      }
      throw error
    }

    return response.promise
  }

  async ping () {
    const response = await this.sendRequest('ping', [])
    if (response !== 'pong') throw new Error(`NIP46_PING_FAILED:${response}`)
  }

  async logout () {
    const response = await this.sendRequest('logout', [])
    if (response !== 'ack') throw new Error(`NIP46_LOGOUT_FAILED:${response}`)
    await this.close()
  }

  async getPublicKey (options) {
    if (!options?.extension && this.#cachedPubkey) return this.#cachedPubkey
    const pubkey = await this.sendRequest('get_public_key', [], options)
    if (!validPubkey(pubkey)) throw new Error('NIP46_INVALID_PUBLIC_KEY')
    if (!options?.extension) this.#cachedPubkey = pubkey
    return pubkey
  }

  async signEvent (event, options) {
    const response = await this.sendRequest('sign_event', [JSON.stringify(event)], options)
    let signed
    try {
      signed = JSON.parse(response)
    } catch {
      throw new Error('NIP46_INVALID_SIGNED_EVENT')
    }
    if (!verifyEvent(signed)) throw new Error('NIP46_INVALID_SIGNED_EVENT')
    return signed
  }

  nip04Encrypt (pubkey, plaintext, options) { return this.sendRequest('nip04_encrypt', [pubkey, plaintext], options) }
  nip04Decrypt (pubkey, ciphertext, options) { return this.sendRequest('nip04_decrypt', [pubkey, ciphertext], options) }
  nip44Encrypt (pubkey, plaintext, options) { return this.sendRequest('nip44_encrypt', [pubkey, plaintext], options) }
  nip44Decrypt (pubkey, ciphertext, options) { return this.sendRequest('nip44_decrypt', [pubkey, ciphertext], options) }

  async switchRelays () {
    const response = await this.sendRequest('switch_relays', [])
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
      await this.#awaitContextReady(nextContext)
    } catch {
      await this.#closeContext(nextContext)
      return false
    }

    const previous = this.#activeContext
    this.#pointer = nextPointer
    this.#activeContext = nextContext
    if (previous) {
      const timer = setTimeout(() => {
        this.#retireTimers.delete(timer)
        this.#closeContext(previous)
      }, 5000)
      this.#retireTimers.add(timer)
    }
    return true
  }

  async close () {
    if (this.#closed) return
    this.#closed = true
    for (const timer of this.#retireTimers) clearTimeout(timer)
    this.#retireTimers.clear()
    const contexts = [...this.#contexts]
    await Promise.all(contexts.map(context => this.#closeContext(context)))
    this.#activeContext = null
    for (const pending of this.#pending.values()) pending.reject(new Error('NIP46_CLOSED'))
    this.#pending.clear()
  }

  #isEventForClient (event) {
    return event?.kind === NIP46_KIND &&
      typeof event.pubkey === 'string' &&
      hasPTag(event, this.#clientPubkey) &&
      verifyEvent(event)
  }

  #openResponseContext (pointer) {
    return this.#openContext({
      filter: {
        kinds: [NIP46_KIND],
        authors: [pointer.remoteSignerPubkey],
        '#p': [this.#clientPubkey],
        limit: 0
      },
      relays: pointer.relays,
      onEvent: event => this.#receiveResponse(event)
    })
  }

  #openContext ({ filter, relays, onEvent }) {
    const controller = new AbortController()
    const stream = this.#relayPool.getLiveEventsGenerator(filter, relays, {
      signal: controller.signal,
      timeoutAfterFirstEose: DEFAULT_TIMEOUT_AFTER_FIRST_EOSE
    })
    const context = { controller, stream, consume: null }
    this.#contexts.add(context)
    context.consume = (async () => {
      try {
        for await (const event of stream) onEvent(event)
      } catch (error) {
        if (!this.#closed) console.error('NIP46 response stream failed:', error)
      }
    })()
    return context
  }

  async #closeContext (context) {
    if (!context || !this.#contexts.delete(context)) return
    context.controller.abort()
    try {
      await context.consume
    } catch {
      // The consume loop reports unexpected stream errors itself.
    }
  }

  async #awaitContextReady (context, { timeout = this.#timeout, signal } = {}) {
    const report = await waitFor(context.stream.ready, { timeout, signal, label: 'NIP46_LISTENER_TIMEOUT' })
    if (!context.stream.readyRelays.length) {
      throw requestError(report.errors?.[0]?.reason || 'NIP46_NO_READY_RELAYS')
    }
    return context.stream.readyRelays
  }

  async #readyRelays () {
    const context = this.#activeContext
    if (!context) throw new Error('NIP46_LISTENER_UNAVAILABLE')
    await this.#awaitContextReady(context)
    const relays = context.stream.readyRelays
    if (!relays.length) throw new Error('NIP46_NO_READY_RELAYS')
    return relays
  }

  #receiveResponse (event) {
    if (!this.#pointer || event?.pubkey !== this.#pointer.remoteSignerPubkey || !this.#isEventForClient(event)) return
    let response
    try {
      response = JSON.parse(decrypt(event.content, getConversationKey(this.#clientSecretKey, event.pubkey)))
    } catch {
      return
    }
    if (!response || typeof response.id !== 'string') return
    const pending = this.#pending.get(response.id)
    if (!pending) return

    if (response.result === 'auth_url') {
      try {
        this.#onAuthUrl?.(typeof response.error === 'string' ? response.error : '')
      } catch (error) {
        console.error('NIP46 onAuthUrl failed:', error)
      }
      return
    }

    this.#pending.delete(response.id)
    if (Object.hasOwn(response, 'error') && response.error !== null && response.error !== undefined) {
      pending.reject(requestError(response.error))
    } else if (Object.hasOwn(response, 'result')) {
      pending.resolve(response.result)
    } else {
      pending.reject(new Error('NIP46_INVALID_RESPONSE'))
    }
  }
}
