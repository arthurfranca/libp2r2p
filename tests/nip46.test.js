import { test } from 'node:test'
import assert from 'node:assert/strict'
import { finalizeEvent } from '../event/index.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'
import { decrypt, encrypt, getConversationKey } from '../nip44/index.js'
import {
  BunkerSigner,
  createNostrConnectURI,
  Nip46Client,
  NIP46_KIND,
  Nip46ServerSession,
  parseBunkerUrl,
  toBunkerUrl
} from '../nip46/index.js'

const RELAYS = ['wss://one.example', 'wss://two.example']

const tick = () => new Promise(resolve => setImmediate(resolve))

class FakeLiveStream {
  #events = []
  #waiters = []
  #done = false
  #ready = Promise.withResolvers()
  #readyRelays = []

  constructor (relays) {
    this.relays = relays
    this.ready = this.#ready.promise
  }

  get readyRelays () {
    return Object.freeze([...this.#readyRelays])
  }

  setReady (relays = this.relays, errors = []) {
    this.#readyRelays = [...relays]
    this.#ready.resolve({ relays: Object.freeze([...relays]), errors: Object.freeze([...errors]) })
  }

  setReadyRelays (relays) {
    this.#readyRelays = [...relays]
  }

  emit (event) {
    const waiter = this.#waiters.shift()
    if (waiter) waiter({ value: event, done: false })
    else this.#events.push(event)
  }

  close () {
    if (this.#done) return
    this.#done = true
    for (const waiter of this.#waiters.splice(0)) waiter({ value: undefined, done: true })
  }

  async next () {
    if (this.#events.length) return { value: this.#events.shift(), done: false }
    if (this.#done) return { value: undefined, done: true }
    return new Promise(resolve => this.#waiters.push(resolve))
  }

  async return () {
    this.close()
    return { value: undefined, done: true }
  }

  [Symbol.asyncIterator] () {
    return this
  }
}

class FakeRelayPool {
  constructor ({ autoReady = true } = {}) {
    this.autoReady = autoReady
    this.streams = []
    this.sent = []
  }

  getLiveEventsGenerator (filter, relays, options) {
    const stream = new FakeLiveStream(relays)
    this.streams.push({ filter, relays, options, stream })
    options.signal?.addEventListener('abort', () => stream.close(), { once: true })
    if (this.autoReady) queueMicrotask(() => stream.setReady(relays))
    return stream
  }

  async sendEvent (event, relays, options) {
    this.sent.push({ event, relays, options })
    return {
      success: true,
      promise: Promise.resolve({ success: true, errors: [] })
    }
  }
}

function matchesFilter (event, filter) {
  if (filter.kinds?.length && !filter.kinds.includes(event.kind)) return false
  if (filter.authors?.length && !filter.authors.includes(event.pubkey)) return false
  for (const [key, values] of Object.entries(filter)) {
    if (!key.startsWith('#')) continue
    if (!event.tags.some(tag => tag[0] === key.slice(1) && values.includes(tag[1]))) return false
  }
  return true
}

class LoopbackRelayPool extends FakeRelayPool {
  async sendEvent (event, relays, options) {
    this.sent.push({ event, relays, options })
    queueMicrotask(() => {
      for (const entry of this.streams) {
        if (!entry.relays.some(relay => relays.includes(relay))) continue
        if (matchesFilter(event, entry.filter)) entry.stream.emit(event)
      }
    })
    return {
      success: true,
      promise: Promise.resolve({ success: true, errors: [] })
    }
  }
}

function pointer (remoteSignerPubkey) {
  return { remoteSignerPubkey, relays: RELAYS, secret: 'one-use-secret' }
}

function responseEvent ({ remoteSecretKey, clientPubkey, response }) {
  return finalizeEvent({
    kind: NIP46_KIND,
    created_at: Math.floor(Date.now() / 1000),
    tags: [['p', clientPubkey]],
    content: encrypt(JSON.stringify(response), getConversationKey(remoteSecretKey, clientPubkey))
  }, remoteSecretKey)
}

function requestPayload ({ sent, remotePubkey, clientSecretKey }) {
  return JSON.parse(decrypt(sent.event.content, getConversationKey(clientSecretKey, remotePubkey)))
}

test('parses and builds direct bunker URLs without NIP-05 lookup', () => {
  const remoteSignerPubkey = 'a'.repeat(64)
  const value = `bunker://${remoteSignerPubkey}?relay=wss%3A%2F%2Fone.example&relay=wss%3A%2F%2Ftwo.example&secret=once`
  const parsed = parseBunkerUrl(value)

  assert.deepEqual(parsed, {
    remoteSignerPubkey,
    relays: RELAYS,
    secret: 'once'
  })
  assert.deepEqual(parseBunkerUrl(toBunkerUrl(parsed)), parsed)
  assert.equal(parseBunkerUrl('alice@example.com'), null)
  assert.equal(parseBunkerUrl(`bunker://${remoteSignerPubkey}`), null)

  const uri = createNostrConnectURI({
    clientPubkey: remoteSignerPubkey,
    relays: RELAYS,
    secret: 'connect-secret',
    perms: ['sign_event:1', 'nip44_encrypt'],
    name: 'Test Client',
    url: 'https://client.example',
    image: 'https://client.example/icon.png'
  })
  const params = new URL(uri).searchParams
  assert.equal(params.get('perms'), 'sign_event:1,nip44_encrypt')
  assert.equal(params.get('name'), 'Test Client')
  assert.equal(params.get('url'), 'https://client.example')
  assert.equal(params.get('image'), 'https://client.example/icon.png')
  assert.throws(() => createNostrConnectURI({ clientPubkey: remoteSignerPubkey, relays: RELAYS }), /INVALID_NOSTRCONNECT_URI/)
})

test('waits for the live listener before publishing a connect request', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool({ autoReady: false })
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool, timeout: 100 })
  const connecting = signer.connect({
    requestedPermissions: ['sign_event:1'],
    clientMetadata: { name: 'Test Vault', url: 'https://vault.example' }
  })

  await tick()
  assert.equal(relayPool.sent.length, 0)

  relayPool.streams[0].stream.setReady(RELAYS)
  await tick()
  assert.equal(relayPool.sent.length, 1)
  assert.deepEqual(relayPool.sent[0].relays, RELAYS)

  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  assert.equal(request.method, 'connect')
  assert.deepEqual(request.params, [
    remoteSignerPubkey,
    'one-use-secret',
    'sign_event:1',
    JSON.stringify({ name: 'Test Vault', url: 'https://vault.example' })
  ])

  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'ack' }
  }))
  await tick()
  const switchRequest = requestPayload({
    sent: relayPool.sent[1],
    remotePubkey: remoteSignerPubkey,
    clientSecretKey
  })
  assert.equal(switchRequest.method, 'switch_relays')
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: switchRequest.id, result: 'null' }
  }))
  await connecting
  await signer.close()
})

test('connects to a bunker pointer without a one-use secret', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, {
    remoteSignerPubkey,
    relays: RELAYS,
    secret: null
  }, { relayPool })

  const connecting = signer.connect()
  await tick()
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  assert.equal(request.method, 'connect')
  assert.deepEqual(request.params, [remoteSignerPubkey, ''])

  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'ack' }
  }))
  await tick()
  const switchRequest = requestPayload({
    sent: relayPool.sent[1],
    remotePubkey: remoteSignerPubkey,
    clientSecretKey
  })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: switchRequest.id, result: 'null' }
  }))

  await connecting
  await signer.close()
})

test('connect uses the configurable client timeout when the signer never replies', async () => {
  const signer = BunkerSigner.fromBunker(generateSecretKey(), pointer(getPublicKey(generateSecretKey())), {
    relayPool: new FakeRelayPool(),
    timeout: 5
  })

  await assert.rejects(signer.connect(), /NIP46_REQUEST_TIMEOUT/)
  await signer.close()
})

test('keeps a request pending across an auth URL and ignores another signer', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const wrongRemoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const authUrls = []
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), {
    relayPool,
    onAuthUrl: url => authUrls.push(url)
  })
  const pending = signer.sendRequest('ping', [])

  await tick()
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  const invalidSignature = JSON.parse(JSON.stringify(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'forged' }
  })))
  invalidSignature.sig = '0'.repeat(invalidSignature.sig.length)
  let settled = false
  pending.finally(() => { settled = true }).catch(() => {})
  relayPool.streams[0].stream.emit(invalidSignature)
  await tick()
  assert.equal(settled, false)

  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey: wrongRemoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'wrong' }
  }))
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'auth_url', error: 'https://auth.example' }
  }))
  await tick()
  assert.deepEqual(authUrls, ['https://auth.example'])

  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'pong' }
  }))
  assert.equal(await pending, 'pong')
  await signer.close()
})

test('preserves application request extensions without allowing protocol fields to be replaced', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })
  const pending = signer.sendRequest('obfuscate', ['value'], {
    extension: { tweak: ['withSharedKey', 'peer', 'scope'] }
  })

  await tick()
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  assert.deepEqual(request.tweak, ['withSharedKey', 'peer', 'scope'])

  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: 'ok' }
  }))
  assert.equal(await pending, 'ok')
  await assert.rejects(
    signer.sendRequest('ping', [], { extension: { id: 'not-allowed' } }),
    /NIP46_REQUEST_EXTENSION_CANNOT_SET_ID/
  )
  await signer.close()
})

test('adds a relay to later requests after it becomes ready', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool({ autoReady: false })
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })

  const first = signer.sendRequest('ping', [])
  await tick()
  relayPool.streams[0].stream.setReady([RELAYS[0]])
  await tick()
  assert.deepEqual(relayPool.sent[0].relays, [RELAYS[0]])
  const firstRequest = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: firstRequest.id, result: 'pong' }
  }))
  assert.equal(await first, 'pong')

  relayPool.streams[0].stream.setReadyRelays(RELAYS)
  const second = signer.sendRequest('ping', [])
  await tick()
  assert.deepEqual(relayPool.sent[1].relays, RELAYS)
  const secondRequest = requestPayload({ sent: relayPool.sent[1], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: secondRequest.id, result: 'pong' }
  }))
  assert.equal(await second, 'pong')
  await signer.close()
})

test('rejects NIP-46 error responses', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })
  const pending = signer.sendRequest('ping', [])

  await tick()
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, error: 'not allowed' }
  }))
  await assert.rejects(pending, /not allowed/)
  await signer.close()
})

test('verifies signed events returned by the bunker', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })
  const signing = signer.signEvent({ kind: 1, created_at: 10, tags: [], content: 'hello' })

  await tick()
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  const signed = finalizeEvent({ kind: 1, created_at: 10, tags: [], content: 'hello' }, remoteSecretKey)
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: request.id, result: JSON.stringify(signed) }
  }))

  assert.deepEqual(await signing, signed)
  await signer.close()
})

test('validates the remote public key and closes after logout', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const userPubkey = getPublicKey(generateSecretKey())
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })

  const publicKey = signer.getPublicKey()
  await tick()
  const publicKeyRequest = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: publicKeyRequest.id, result: userPubkey }
  }))
  assert.equal(await publicKey, userPubkey)

  const loggingOut = signer.logout()
  await tick()
  const logoutRequest = requestPayload({ sent: relayPool.sent[1], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: logoutRequest.id, result: 'ack' }
  }))
  await loggingOut
  await assert.rejects(signer.ping(), /NIP46_CLOSED/)
})

test('switches relay listeners before sending later requests to the new set', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(remoteSignerPubkey), { relayPool })
  const switching = signer.switchRelays()

  await tick()
  const switchRequest = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: switchRequest.id, result: JSON.stringify(['wss://new.example']) }
  }))

  assert.equal(await switching, true)
  assert.deepEqual(signer.pointer.relays, ['wss://new.example'])
  assert.deepEqual(relayPool.streams[1].relays, ['wss://new.example'])

  const pending = signer.sendRequest('ping', [])
  await tick()
  assert.deepEqual(relayPool.sent[1].relays, ['wss://new.example'])
  const pingRequest = requestPayload({ sent: relayPool.sent[1], remotePubkey: remoteSignerPubkey, clientSecretKey })
  relayPool.streams[1].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey: getPublicKey(clientSecretKey),
    response: { id: pingRequest.id, result: 'pong' }
  }))
  assert.equal(await pending, 'pong')
  await signer.close()
})

test('discovers and validates a client-initiated connection URI', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const clientPubkey = getPublicKey(clientSecretKey)
  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const relayPool = new FakeRelayPool()
  const uri = createNostrConnectURI({ clientPubkey, relays: RELAYS, secret: 'connection-secret' })
  const creating = BunkerSigner.fromURI(clientSecretKey, uri, {
    relayPool,
    timeout: 100
  })

  await tick()
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey,
    response: { id: 'ignored', result: 'wrong-secret' }
  }))
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey,
    response: { id: 'connected', result: 'connection-secret' }
  }))

  await tick()
  const switchRequest = requestPayload({
    sent: relayPool.sent[0],
    remotePubkey: remoteSignerPubkey,
    clientSecretKey
  })
  relayPool.streams[1].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey,
    response: { id: switchRequest.id, result: 'null' }
  }))

  const signer = await creating
  assert.equal(signer.pointer.remoteSignerPubkey, remoteSignerPubkey)
  assert.equal(relayPool.streams.length, 2)
  await signer.close()
})

test('briefly attempts relay switching after a client-initiated connection', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const clientPubkey = getPublicKey(clientSecretKey)
  const relayPool = new FakeRelayPool()
  const uri = createNostrConnectURI({ clientPubkey, relays: RELAYS, secret: 'connection-secret' })
  const creating = BunkerSigner.fromURI(clientSecretKey, uri, { relayPool, timeout: 100 })

  await tick()
  relayPool.streams[0].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey,
    response: { id: 'connected', result: 'connection-secret' }
  }))
  await tick()

  const remoteSignerPubkey = getPublicKey(remoteSecretKey)
  const request = requestPayload({ sent: relayPool.sent[0], remotePubkey: remoteSignerPubkey, clientSecretKey })
  assert.equal(request.method, 'switch_relays')
  relayPool.streams[1].stream.emit(responseEvent({
    remoteSecretKey,
    clientPubkey,
    response: { id: request.id, result: 'null' }
  }))

  const signer = await creating
  await signer.close()
})

test('times out or aborts a client-initiated connection that has no matching secret', async () => {
  const clientSecretKey = generateSecretKey()
  const uri = createNostrConnectURI({
    clientPubkey: getPublicKey(clientSecretKey),
    relays: RELAYS,
    secret: 'connection-secret'
  })

  await assert.rejects(
    BunkerSigner.fromURI(clientSecretKey, uri, {
      relayPool: new FakeRelayPool(),
      timeout: 10
    }),
    /NIP46_CONNECTION_TIMEOUT/
  )

  const controller = new AbortController()
  const pending = BunkerSigner.fromURI(clientSecretKey, uri, {
    relayPool: new FakeRelayPool(),
    timeout: null,
    signal: controller.signal
  })
  await tick()
  controller.abort()
  await assert.rejects(pending, /Aborted/)
})

test('close rejects a pending NIP-46 request', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSecretKey = generateSecretKey()
  const signer = BunkerSigner.fromBunker(clientSecretKey, pointer(getPublicKey(remoteSecretKey)), {
    relayPool: new FakeRelayPool()
  })
  const pending = signer.sendRequest('ping', [])
  await tick()
  await signer.close()
  await assert.rejects(pending, /NIP46_CLOSED/)
})

test('public client and server roles support bidirectional custom RPC and relay switching', async () => {
  const relayPool = new LoopbackRelayPool()
  const serverSecretKey = generateSecretKey()
  const clientSecretKey = generateSecretKey()
  const serverPubkey = getPublicKey(serverSecretKey)
  const connected = []
  const serverRequests = []
  const clientRequests = []
  const server = new Nip46ServerSession(serverSecretKey, {
    relays: RELAYS,
    secret: 'one-use-secret',
    relayPool,
    onConnect: value => connected.push(value),
    onRequest: ({ method, params }) => {
      serverRequests.push({ method, params })
      if (method !== 'server_echo') throw new Error('unsupported custom method')
      return JSON.stringify(params)
    }
  })
  await server.start({ timeout: 100 })

  const client = new Nip46Client(clientSecretKey, pointer(serverPubkey), {
    relayPool,
    onRequest: ({ method, params }) => {
      clientRequests.push({ method, params })
      if (method !== 'client_echo') throw new Error('unsupported custom method')
      return params[0]
    }
  })
  await client.connect({
    requestedPermissions: ['server_echo'],
    clientMetadata: { name: 'Pairing client' },
    timeout: 100
  })

  assert.equal(server.clientPubkey, client.clientPubkey)
  assert.deepEqual(connected, [{
    peerPubkey: client.clientPubkey,
    requestedPermissions: ['server_echo'],
    clientMetadata: { name: 'Pairing client' }
  }])
  await client.ping({ timeout: 100 })
  assert.equal(
    await client.sendRequest('server_echo', ['one', 'two'], { timeout: 100 }),
    JSON.stringify(['one', 'two'])
  )
  assert.equal(await server.sendRequest('client_echo', ['back'], { timeout: 100 }), 'back')
  assert.deepEqual(serverRequests, [{ method: 'server_echo', params: ['one', 'two'] }])
  assert.deepEqual(clientRequests, [{ method: 'client_echo', params: ['back'] }])

  await server.updateRelays(['wss://new.example'])
  assert.equal(await server.sendRequest('client_echo', ['before-switch'], { timeout: 100 }), 'before-switch')
  assert.equal(await client.switchRelays({ timeout: 100 }), true)
  assert.deepEqual(client.pointer.relays, ['wss://new.example'])
  assert.deepEqual(server.relays, ['wss://new.example'])
  assert.equal(await server.sendRequest('client_echo', ['after-switch'], { timeout: 100 }), 'after-switch')
  assert.deepEqual(clientRequests, [
    { method: 'client_echo', params: ['back'] },
    { method: 'client_echo', params: ['before-switch'] },
    { method: 'client_echo', params: ['after-switch'] }
  ])

  await client.logout({ timeout: 100 })
  await server.close()
})

test('client and server reply with errors for unsupported methods', async () => {
  const relayPool = new LoopbackRelayPool()
  const serverSecretKey = generateSecretKey()
  const clientSecretKey = generateSecretKey()
  const server = new Nip46ServerSession(serverSecretKey, {
    relays: RELAYS,
    secret: 'one-use-secret',
    relayPool
  })
  await server.start({ timeout: 100 })

  const client = new Nip46Client(clientSecretKey, pointer(getPublicKey(serverSecretKey)), {
    relayPool
  })
  await client.connect({ timeout: 100 })

  await assert.rejects(
    client.sendRequest('unknown_server_method', [], { timeout: 100 }),
    /NIP46_METHOD_NOT_SUPPORTED/
  )
  await assert.rejects(
    server.sendRequest('unknown_client_method', [], { timeout: 100 }),
    /NIP46_METHOD_NOT_SUPPORTED/
  )

  await client.close()
  await server.close()
})

test('server session keeps an invalid secret from consuming its single client slot', async () => {
  const relayPool = new LoopbackRelayPool()
  const serverSecretKey = generateSecretKey()
  const serverPubkey = getPublicKey(serverSecretKey)
  const server = new Nip46ServerSession(serverSecretKey, {
    relays: RELAYS,
    secret: 'correct-secret',
    relayPool
  })
  await server.start({ timeout: 100 })

  const invalid = new Nip46Client(generateSecretKey(), {
    remoteSignerPubkey: serverPubkey,
    relays: RELAYS,
    secret: 'wrong-secret'
  }, { relayPool })
  await assert.rejects(invalid.connect({ timeout: 100 }), /NIP46_INVALID_SECRET/)
  assert.equal(server.clientPubkey, null)
  await invalid.close()

  const accepted = new Nip46Client(generateSecretKey(), {
    remoteSignerPubkey: serverPubkey,
    relays: RELAYS,
    secret: 'correct-secret'
  }, { relayPool })
  await accepted.connect({ timeout: 100 })

  const rejected = new Nip46Client(generateSecretKey(), {
    remoteSignerPubkey: serverPubkey,
    relays: RELAYS,
    secret: 'correct-secret'
  }, { relayPool })
  await assert.rejects(rejected.connect({ timeout: 100 }), /NIP46_ALREADY_CONNECTED/)

  await rejected.close()
  await accepted.close()
  await server.close()
})

test('generic client removes pending requests after timeout and abort', async () => {
  const clientSecretKey = generateSecretKey()
  const remoteSignerPubkey = getPublicKey(generateSecretKey())
  const client = new Nip46Client(clientSecretKey, pointer(remoteSignerPubkey), {
    relayPool: new FakeRelayPool(),
    timeout: 5
  })

  await assert.rejects(
    client.sendRequest('never_replies', []),
    /NIP46_REQUEST_TIMEOUT/
  )

  await assert.rejects(
    client.sendRequest('override_never_replies', [], { timeout: 1 }),
    /NIP46_REQUEST_TIMEOUT/
  )

  const controller = new AbortController()
  const pending = client.sendRequest('also_never_replies', [], {
    timeout: null,
    signal: controller.signal
  })
  await tick()
  controller.abort()
  await assert.rejects(pending, /Aborted/)
  await client.close()
})
