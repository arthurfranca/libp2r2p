import assert from 'node:assert/strict'
import { test } from 'node:test'
import { IDBFactory } from 'fake-indexeddb'
import { RelayPool } from '../relay/index.js'
import { RelayConnection } from '../relay/services/relay-connection.js'
import { bytesToHex } from '../base16/index.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'
import { Nip46Client, Nip46ServerSession } from '../nip46/index.js'
import { fetch, subscribe, wrapEvent } from '../private-channel/index.js'
import NsecSigner from './helpers/test-signer.js'

const relay = 'wss://controlled.example'
const tick = () => new Promise(resolve => setImmediate(resolve))

// Only transport is simulated: the pool, connection, filtering, NIP-46 frames,
// private-channel encryption and subscription lifecycle are production code.
function network (t) {
  const sockets = new Set()
  const history = []
  class Socket {
    constructor () {
      this.readyState = 0
      this.subs = new Map()
      sockets.add(this)
      queueMicrotask(() => { this.readyState = 1; this.onopen?.() })
    }
    receive (frame) {
      queueMicrotask(() => { if (this.readyState === 1) this.onmessage?.({ data: JSON.stringify(frame) }) })
    }
    send (text) {
      const [type, id, ...filters] = JSON.parse(text)
      if (type === 'REQ') {
        this.subs.set(id, filters)
        if (filters[0].limit !== 0) for (const event of history) this.receive(['EVENT', id, event])
        this.receive(['EOSE', id])
      } else if (type === 'CLOSE') this.subs.delete(id)
      else if (type === 'EVENT') {
        history.push(id)
        this.receive(['OK', id.id, true, ''])
        for (const socket of sockets) {
          for (const subId of socket.subs.keys()) socket.receive(['EVENT', subId, id])
        }
      }
    }
    close () {
      this.readyState = 3
      sockets.delete(this)
      queueMicrotask(() => this.onclose?.({ code: 1000, reason: '', wasClean: true }))
    }
  }
  const pool = new RelayPool({ _createRelay: url => new RelayConnection(url, { WebSocket: Socket }) })
  let controlsDelivered = 0
  // The real transport still supplies events; insert unfamiliar controls at the
  // pool boundary to verify that higher-level protocols do not interpret them.
  const originalLive = pool.getLiveEventsGenerator.bind(pool)
  pool.getLiveEventsGenerator = (...args) => {
    const source = originalLive(...args)
    const pending = []
    return {
      get ready () { return source.ready },
      get readyRelays () { return source.readyRelays },
      [Symbol.asyncIterator] () { return this },
      async next () {
        if (pending.length) return pending.shift()
        const result = await source.next()
        if (!result.done && result.value.type === 'event') {
          controlsDelivered += 2
          pending.push({ value: { type: 'future-control', relay }, done: false }, result)
          return { value: { type: 'live-progress', relay, epoch: 1, since: 0, until: 1 }, done: false }
        }
        return result
      },
      return () { pending.length = 0; return source.return() },
      stopAndDrain () { source.stopAndDrain() }
    }
  }
  t.after(() => pool.disconnectAll())
  return { pool, history, controlsDelivered: () => controlsDelivered }
}

test('NIP-46 client and server exchange RPC through typed real pool streams', { timeout: 5000 }, async t => {
  const { pool, controlsDelivered } = network(t)
  const secret = generateSecretKey()
  const server = new Nip46ServerSession(secret, {
    relays: [relay], secret: 'test-secret', relayPool: pool,
    onRequest: ({ method, params }) => {
      assert.equal(method, 'echo')
      return params[0]
    }
  })
  t.after(() => server.close())
  await server.start({ timeout: 1000 })
  const client = new Nip46Client(generateSecretKey(), {
    remoteSignerPubkey: getPublicKey(secret), relays: [relay], secret: 'test-secret'
  }, { relayPool: pool })
  t.after(() => client.close())
  await client.connect({ timeout: 1000 })
  await client.ping({ timeout: 1000 })
  assert.equal(await client.sendRequest('echo', ['envelopes'], { timeout: 1000 }), 'envelopes')
  assert.ok(controlsDelivered() > 0)
  await client.close()
  await server.close()
})

test('private-channel reads history and live events through real pool envelopes', { timeout: 5000 }, async t => {
  const { pool, history, controlsDelivered } = network(t)
  const alice = NsecSigner.getOrCreate(bytesToHex(generateSecretKey()))
  const bob = NsecSigner.getOrCreate(bytesToHex(generateSecretKey()))
  const owner = await alice.getPublicKey()
  const receiver = await bob.getPublicKey()
  const data = new Map()
  const temporaryStorageArea = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value), removeItem: key => data.delete(key) }
  const makeOuter = async content => {
    const [outer] = await wrapEvent({
      senderSigner: alice, receivers: [receiver], temporaryStorageArea,
      event: { kind: 9, created_at: Math.floor(Date.now() / 1000), tags: [], content },
      _getIykcProofs: async () => ({})
    })
    return outer
  }
  const stored = await makeOuter('stored')
  history.push(stored)
  const options = {
    receiverSigner: bob, privateChannelSigner: alice, privateChannelPubkey: owner, receiverPubkey: receiver,
    relays: [relay], receivedChunkIndexedDB: new IDBFactory(),
    onError: error => { throw error }
  }
  const read = []
  const fetched = await fetch({ ...options, onEvent: event => read.push(event), _getEvents: (...args) => pool.getEvents(...args) })
  assert.equal(fetched[0].id, stored.id)
  assert.equal(read[0].content, 'stored')
  const delivered = Promise.withResolvers()
  let liveStream
  const subscription = subscribe({
    ...options, liveOnly: true, onEvent: event => delivered.resolve(event),
    _liveEventsGenerator: (...args) => { liveStream = pool.getLiveEventsGenerator(...args); return liveStream }
  })
  t.after(() => subscription.close())
  await tick()
  await liveStream.ready
  const published = await pool.sendEvent(await makeOuter('live'), [relay])
  assert.equal(published.success, true)
  assert.equal((await delivered.promise).content, 'live')
  assert.ok(controlsDelivered() > 0)
  await subscription.close()
})
