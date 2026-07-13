import { test } from 'node:test'
import assert from 'node:assert/strict'
import { finalizeEvent, generateSecretKey, getPublicKey } from 'nostr-tools'
import { decrypt, encrypt, getConversationKey } from 'nostr-tools/nip44'
import {
  BunkerSigner,
  createNostrConnectURI,
  NIP46_KIND,
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
  await connecting
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
    skipRelaySwitch: true,
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
      skipRelaySwitch: true,
      timeout: 10
    }),
    /NIP46_CONNECTION_TIMEOUT/
  )

  const controller = new AbortController()
  const pending = BunkerSigner.fromURI(clientSecretKey, uri, {
    relayPool: new FakeRelayPool(),
    skipRelaySwitch: true,
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
