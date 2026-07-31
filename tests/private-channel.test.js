import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { finalizeEvent, getEventHash } from '../event/index.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'
import NsecSigner, { deriveSharedKey } from './helpers/test-signer.js'
import { deriveDoubleDhConversationKey } from '../double-dh/index.js'
import {
  EXPIRATION_SECONDS,
  eventFromNymCarriers,
  fetch,
  getJsonlChunkByteSize,
  getNymCarrierChunkSize,
  MAX_EVENT_BYTES,
  NYM_CARRIER_KIND,
  PRIVATE_BROADCAST_KIND,
  publish,
  publishNymEvent,
  ROUTER_KIND,
  subscribe,
  unwrapEvent,
  wrapEvent,
  wrapEvents,
  wrapNymEvent
} from '../private-channel/index.js'
import { createReceivedChunkStore, DEFAULT_RECEIVED_CHUNK_MAX_BYTES } from '../private-channel/services/received-chunks.js'
import { isValidContentKeyProof, makeContentKeyEvent, parseContentKeyEvent } from '../content-key/event/index.js'
import { TEMPORARY_STORAGE_KEYS_KEY } from '../temporary-storage/index.js'
import { bytesToBase64, base64ToBytes } from '../base64/index.js'
import { bytesToHex, hexToBytes } from '../base16/index.js'
import { relayPool } from '../relay/index.js'

const originalEventsFeedGenerator = relayPool.getEventsFeedGenerator
const originalLiveEventsGenerator = relayPool.getLiveEventsGenerator
let mockedSubscribeMany = null

// Adapts the old callback fixture style to the async-generator RelayPool API.
// Resolving onevent waits until private-channel has consumed that event.
function createMockEventsGenerator (subscribeMany) {
  return (filter, relays, { signal } = {}) => {
    const pending = []
    let wake = Promise.withResolvers()
    let current = null
    let closed = false
    // eslint-disable-next-line prefer-const
    let subscription

    const close = () => {
      if (closed) return
      closed = true
      current?.resolve()
      current = null
      for (const entry of pending.splice(0)) entry.resolve()
      subscription?.close?.()
      wake.resolve()
    }
    const handlers = {
      onevent: event => new Promise(resolve => {
        if (closed) return resolve()
        pending.push({ event, resolve })
        wake.resolve()
        wake = Promise.withResolvers()
      })
    }
    subscription = subscribeMany(relays, filter, handlers)
    signal?.addEventListener('abort', close, { once: true })

    return {
      async next () {
        current?.resolve()
        current = null
        // eslint-disable-next-line no-unmodified-loop-condition
        while (!closed && pending.length === 0) await wake.promise
        if (closed) return { done: true }
        current = pending.shift()
        return { value: current.event, done: false }
      },
      async return () {
        close()
        return { done: true }
      },
      [Symbol.asyncIterator] () { return this }
    }
  }
}

const pool = {
  get subscribeMany () {
    return mockedSubscribeMany
  },
  set subscribeMany (subscribeMany) {
    mockedSubscribeMany = subscribeMany
    relayPool.getEventsFeedGenerator = subscribeMany
      ? createMockEventsGenerator(subscribeMany)
      : originalEventsFeedGenerator
    relayPool.getLiveEventsGenerator = subscribeMany
      ? createMockEventsGenerator(subscribeMany)
      : originalLiveEventsGenerator
  }
}

if (!globalThis.localStorage) {
  const data = new Map()
  globalThis.localStorage = {
    clear: () => data.clear(),
    getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
    key: index => [...data.keys()][index] || null,
    removeItem: key => { data.delete(String(key)) },
    setItem: (key, value) => { data.set(String(key), String(value)) },
    get length () { return data.size }
  }
}

if (!globalThis.sessionStorage) {
  const data = new Map()
  globalThis.sessionStorage = {
    clear: () => data.clear(),
    getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
    key: index => [...data.keys()][index] || null,
    removeItem: key => { data.delete(String(key)) },
    setItem: (key, value) => { data.set(String(key), String(value)) },
    get length () { return data.size }
  }
}

if (!globalThis.crypto) globalThis.crypto = crypto
if (!globalThis.btoa) globalThis.btoa = s => Buffer.from(s, 'binary').toString('base64')
if (!globalThis.atob) globalThis.atob = s => Buffer.from(s, 'base64').toString('binary')
globalThis.indexedDB = new IDBFactory()
globalThis.IDBKeyRange = IDBKeyRange

afterEach(() => {
  pool.subscribeMany = null
  NsecSigner.releaseAll()
  globalThis.localStorage.clear()
  globalThis.sessionStorage.clear()
  globalThis.indexedDB = new IDBFactory()
})

function signer () {
  return NsecSigner.getOrCreate(bytesToHex(generateSecretKey()))
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function textToBase64 (text) {
  return bytesToBase64(encoder.encode(text))
}

async function nip44v3EncryptText (signer, peerPubkey, kind, plaintext) {
  return signer.nip44v3Encrypt(peerPubkey, kind, '', textToBase64(plaintext))
}

async function nip44v3DecryptText (signer, peerPubkey, kind, ciphertext) {
  return decoder.decode(base64ToBytes(await signer.nip44v3Decrypt(peerPubkey, kind, '', ciphertext)))
}

async function decryptPrivateBroadcast (signer, peerPubkey, ciphertext) {
  return JSON.parse(await nip44v3DecryptText(signer, peerPubkey, PRIVATE_BROADCAST_KIND, ciphertext))
}

async function encryptPrivateBroadcast (signer, peerPubkey, value) {
  return nip44v3EncryptText(signer, peerPubkey, PRIVATE_BROADCAST_KIND, JSON.stringify(value))
}

async function signerWithInternalContentKey (identitySigner, contentSigner) {
  const identityPubkey = await identitySigner.getPublicKey()
  NsecSigner.setContentSigners(identitySigner, [contentSigner])
  return {
    getPublicKey: () => identityPubkey,
    signEvent: event => identitySigner.signEvent(event),
    nip44Encrypt: (peerPubkey, plaintext) => identitySigner.nip44Encrypt(peerPubkey, plaintext),
    nip44Decrypt: (peerPubkey, ciphertext) => identitySigner.nip44Decrypt(peerPubkey, ciphertext),
    nip44v3Encrypt: (peerPubkey, kind, scope, plaintextB64) => identitySigner.nip44v3Encrypt(peerPubkey, kind, scope, plaintextB64),
    nip44v3Decrypt: (peerPubkey, kind, scope, ciphertext) => identitySigner.nip44v3Decrypt(peerPubkey, kind, scope, ciphertext),
    nip44EncryptDoubleDH: (...params) => identitySigner.nip44EncryptDoubleDH(...params),
    nip44DecryptDoubleDH: (...params) => identitySigner.nip44DecryptDoubleDH(...params)
  }
}

function eventFixture (content = 'hello') {
  return { kind: 1, created_at: 1, tags: [], content }
}

function unwrappedFixture (event, pubkey) {
  const unwrapped = { ...event, pubkey }
  return { ...unwrapped, id: getEventHash(unwrapped) }
}

async function noContentKeys () {
  return {}
}

function routerJsonlRows (router) {
  return routerJsonlLines(router).map(line => JSON.parse(line))
}

function routerJsonlLines (router) {
  return new TextDecoder()
    .decode(Buffer.from(router.content, 'base64'))
    .trim()
    .split('\n')
}

function routerRecipientRows (router) {
  return routerJsonlRows(router).filter(row => row.length !== 1)
}

test('shared-key signer derives matching deniable key from either side', async () => {
  const alice = signer()
  const bob = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const aliceShared = alice.withSharedKey(bobPubkey)
  const bobShared = bob.withSharedKey(alicePubkey)

  assert.equal(await aliceShared.getPublicKey(), await bobShared.getPublicKey())
  const sharedPubkey = await aliceShared.getPublicKey()
  const ciphertext = await aliceShared.nip44Encrypt(sharedPubkey, 'secret')
  assert.equal(await bobShared.nip44Decrypt(sharedPubkey, ciphertext), 'secret')

  const syncShared = alice.withSharedKey(bobPubkey, 'trusted-signer-sync-v1')
  assert.notEqual(await syncShared.getPublicKey(), sharedPubkey)
})

test('shared-key derivation matches the 48-byte reduction vector', async () => {
  const aliceSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000001')
  const bobSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000002')
  const alicePubkey = getPublicKey(aliceSecret)
  const bobPubkey = getPublicKey(bobSecret)

  const aliceShared = await deriveSharedKey(aliceSecret, bobPubkey, 'trusted-signer-sync-v1')
  const bobShared = await deriveSharedKey(bobSecret, alicePubkey, 'trusted-signer-sync-v1')

  assert.equal(bytesToHex(aliceShared), '10228022a480c7db02f2d68796e30f029dd77853dca392174e623ea9fc275de0')
  assert.equal(bytesToHex(bobShared), bytesToHex(aliceShared))
})

test('double-DH signer round-trips every content-key mode', async () => {
  const alice = signer()
  const bob = signer()
  const aliceContent = signer()
  const bobContent = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const aliceContentPubkey = await aliceContent.getPublicKey()
  const bobContentPubkey = await bobContent.getPublicKey()

  const cases = [
    { mode: 'identity' },
    { mode: 'sender-content', aliceContent },
    { mode: 'receiver-content', bobContent },
    { mode: 'both-content', aliceContent, bobContent }
  ]

  for (const c of cases) {
    NsecSigner.setContentSigners(alice, c.aliceContent ? [aliceContent] : [])
    NsecSigner.setContentSigners(bob, c.bobContent ? [bobContent] : [])
    const encrypted = await alice.nip44EncryptDoubleDH(
      bobPubkey,
      ROUTER_KIND,
      '',
      textToBase64(c.mode),
      c.bobContent ? bobContentPubkey : ''
    )
    const decrypted = await bob.nip44DecryptDoubleDH(
      alicePubkey,
      ROUTER_KIND,
      '',
      encrypted[0],
      encrypted[1],
      c.bobContent ? bobContentPubkey : ''
    )

    assert.equal(encrypted[1], c.aliceContent ? aliceContentPubkey : '')
    assert.equal(decoder.decode(base64ToBytes(decrypted)), c.mode)
    if (c.mode === 'both-content') {
      await assert.rejects(
        () => bob.nip44DecryptDoubleDH(alicePubkey, ROUTER_KIND + 1, '', encrypted[0], aliceContentPubkey, bobContentPubkey),
        /kind mismatch/
      )
    }
  }
})

test('double-DH conversation key matches the fixed salt vector', () => {
  const aliceSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000001')
  const bobSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000002')
  const aliceContentSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000003')
  const bobContentSecret = hexToBytes('0000000000000000000000000000000000000000000000000000000000000004')
  const alicePubkey = getPublicKey(aliceSecret)
  const bobPubkey = getPublicKey(bobSecret)
  const aliceContentPubkey = getPublicKey(aliceContentSecret)
  const bobContentPubkey = getPublicKey(bobContentSecret)
  const { mode, conversationKey } = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: aliceSecret,
    identityPubkey: alicePubkey,
    contentSecretKey: aliceContentSecret,
    contentPubkey: aliceContentPubkey,
    peerIdentityPubkey: bobPubkey,
    peerContentPubkey: bobContentPubkey,
    kind: ROUTER_KIND,
    scope: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
  })

  assert.equal(mode, 'both-content')
  assert.equal(bytesToHex(conversationKey), '995fec3e81d662f029fc4218bb3cf52305957c849c7b8356bd5d71852f4629b0')
})

test('double-DH conversation key is pair-oriented, not direction-oriented', () => {
  const aliceSecret = generateSecretKey()
  const bobSecret = generateSecretKey()
  const aliceContentSecret = generateSecretKey()
  const bobContentSecret = generateSecretKey()
  const alicePubkey = getPublicKey(aliceSecret)
  const bobPubkey = getPublicKey(bobSecret)
  const aliceContentPubkey = getPublicKey(aliceContentSecret)
  const bobContentPubkey = getPublicKey(bobContentSecret)
  const aliceToBob = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: aliceSecret,
    identityPubkey: alicePubkey,
    contentSecretKey: aliceContentSecret,
    contentPubkey: aliceContentPubkey,
    peerIdentityPubkey: bobPubkey,
    peerContentPubkey: bobContentPubkey,
    kind: ROUTER_KIND,
    scope: ''
  })
  const bobReceiving = deriveDoubleDhConversationKey({
    role: 'receiver',
    identitySecretKey: bobSecret,
    identityPubkey: bobPubkey,
    contentSecretKey: bobContentSecret,
    contentPubkey: bobContentPubkey,
    peerIdentityPubkey: alicePubkey,
    peerContentPubkey: aliceContentPubkey,
    kind: ROUTER_KIND,
    scope: ''
  })
  const bobToAlice = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: bobSecret,
    identityPubkey: bobPubkey,
    contentSecretKey: bobContentSecret,
    contentPubkey: bobContentPubkey,
    peerIdentityPubkey: alicePubkey,
    peerContentPubkey: aliceContentPubkey,
    kind: ROUTER_KIND,
    scope: ''
  })
  const aliceToBobInChannel = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: aliceSecret,
    identityPubkey: alicePubkey,
    contentSecretKey: aliceContentSecret,
    contentPubkey: aliceContentPubkey,
    peerIdentityPubkey: bobPubkey,
    peerContentPubkey: bobContentPubkey,
    kind: ROUTER_KIND,
    scope: '1'.repeat(64)
  })
  const bobReceivingInChannel = deriveDoubleDhConversationKey({
    role: 'receiver',
    identitySecretKey: bobSecret,
    identityPubkey: bobPubkey,
    contentSecretKey: bobContentSecret,
    contentPubkey: bobContentPubkey,
    peerIdentityPubkey: alicePubkey,
    peerContentPubkey: aliceContentPubkey,
    kind: ROUTER_KIND,
    scope: '1'.repeat(64)
  })
  const aliceToBobInOtherChannel = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: aliceSecret,
    identityPubkey: alicePubkey,
    contentSecretKey: aliceContentSecret,
    contentPubkey: aliceContentPubkey,
    peerIdentityPubkey: bobPubkey,
    peerContentPubkey: bobContentPubkey,
    kind: ROUTER_KIND,
    scope: '2'.repeat(64)
  })
  const aliceToBobInOtherKind = deriveDoubleDhConversationKey({
    role: 'sender',
    identitySecretKey: aliceSecret,
    identityPubkey: alicePubkey,
    contentSecretKey: aliceContentSecret,
    contentPubkey: aliceContentPubkey,
    peerIdentityPubkey: bobPubkey,
    peerContentPubkey: bobContentPubkey,
    kind: ROUTER_KIND + 1,
    scope: '1'.repeat(64)
  })

  assert.equal(bytesToHex(aliceToBob.conversationKey), bytesToHex(bobReceiving.conversationKey))
  assert.equal(bytesToHex(aliceToBob.conversationKey), bytesToHex(bobToAlice.conversationKey))
  assert.equal(bytesToHex(aliceToBobInChannel.conversationKey), bytesToHex(bobReceivingInChannel.conversationKey))
  assert.notEqual(bytesToHex(aliceToBob.conversationKey), bytesToHex(aliceToBobInChannel.conversationKey))
  assert.notEqual(bytesToHex(aliceToBobInChannel.conversationKey), bytesToHex(aliceToBobInOtherChannel.conversationKey))
  assert.notEqual(bytesToHex(aliceToBobInChannel.conversationKey), bytesToHex(aliceToBobInOtherKind.conversationKey))
})

test('double-DH self-encryption with a content key requires both secrets', async () => {
  const alice = signer()
  const aliceContent = signer()
  const wrongIdentity = signer()
  const alicePubkey = await alice.getPublicKey()
  const aliceContentPubkey = await aliceContent.getPublicKey()
  NsecSigner.setContentSigners(alice, [aliceContent])

  const encrypted = await alice.nip44EncryptDoubleDH(alicePubkey, ROUTER_KIND, '', textToBase64('note to self'), aliceContentPubkey)

  assert.equal(encrypted[1], aliceContentPubkey)
  assert.equal(decoder.decode(base64ToBytes(await alice.nip44DecryptDoubleDH(
    alicePubkey,
    ROUTER_KIND,
    '',
    encrypted[0],
    aliceContentPubkey,
    aliceContentPubkey
  ))), 'note to self')

  await assert.rejects(
    () => alice.nip44DecryptDoubleDH(alicePubkey, ROUTER_KIND, '', encrypted[0], aliceContentPubkey, ''),
    /RECEIVER_CONTENT_KEY_REQUIRED/
  )

  NsecSigner.setContentSigners(wrongIdentity, [aliceContent])
  await assert.rejects(
    () => wrongIdentity.nip44DecryptDoubleDH(alicePubkey, ROUTER_KIND, '', encrypted[0], aliceContentPubkey, aliceContentPubkey),
    /invalid MAC/
  )
})

test('wrapEvent creates private broadcast events under relay event size limit', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const before = Math.floor(Date.now() / 1000)
  const wrapped = await wrapEvent({ senderSigner: alice, receivers: [bobPubkey], event: eventFixture('hello bob'), _getIykcProofs: noContentKeys })

  assert.equal(wrapped.length, 1)
  assert.equal(wrapped[0].kind, PRIVATE_BROADCAST_KIND)
  assert.equal(wrapped[0].pubkey, await alice.getPublicKey())
  assert.equal(wrapped[0].tags.some(tag => tag[0] === 's'), false)
  assert.ok(Number(wrapped[0].tags[0][1]) >= before + EXPIRATION_SECONDS)
  assert.ok(new TextEncoder().encode(JSON.stringify(wrapped[0])).length <= MAX_EVENT_BYTES)
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('private broadcast wrappers share and normalize one deletion pubkey per logical message', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const deletionPubkey = 'A'.repeat(64)
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    deletionPubkey,
    event: eventFixture('deletable'),
    _getIykcProofs: noContentKeys
  })

  assert.deepEqual(wrapped.tags.find(tag => tag[0] === 's'), ['s', deletionPubkey.toLowerCase()])
  assert.ok(Number(wrapped.tags.find(tag => tag[0] === 'expiration')?.[1]) > 0)
  assert.deepEqual(
    await unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }),
    unwrappedFixture(eventFixture('deletable'), await alice.getPublicKey())
  )

  for (const invalidDeletionPubkey of ['', 'not-a-pubkey']) {
    await assert.rejects(
      () => wrapEvent({
        senderSigner: alice,
        receivers: [bobPubkey],
        deletionPubkey: invalidDeletionPubkey,
        event: eventFixture('invalid'),
        _getIykcProofs: noContentKeys
      }),
      /INVALID_DELETION_PUBKEY/
    )
  }
})

test('publish splits relay-targeted routers by receiver subset with separate router pubkeys', async () => {
  const alice = signer()
  const bob = signer()
  const carol = signer()
  const dave = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const carolPubkey = await carol.getPublicKey()
  const davePubkey = await dave.getPublicKey()
  const event = eventFixture('split publish')
  const published = []
  const temporaryWrites = []
  const temporaryStorageArea = {
    getItem: key => globalThis.localStorage.getItem(key),
    removeItem: key => globalThis.localStorage.removeItem(key),
    setItem: (key, value) => {
      temporaryWrites.push(key)
      globalThis.localStorage.setItem(key, value)
    }
  }

  const reports = await publish({
    senderSigner: alice,
    receivers: [bobPubkey, carolPubkey, davePubkey],
    relayToReceivers: new Map([
      ['wss://one.example', [bobPubkey, carolPubkey]],
      ['wss://two.example', [carolPubkey, bobPubkey]],
      ['wss://three.example', [carolPubkey, davePubkey]]
    ]),
    recoveryRelays: ['wss://seed.example', 'wss://one.example'],
    deletionPubkey: 'd'.repeat(64),
    event,
    temporaryStorageArea,
    _getIykcProofs: noContentKeys,
    _publish: async (outer, relays) => {
      published.push({ outer, relays })
      return { success: true }
    }
  })

  assert.equal(published.length, 2)
  assert.deepEqual(published.map(({ outer }) => outer.tags.find(tag => tag[0] === 's')), [
    ['s', 'd'.repeat(64)],
    ['s', 'd'.repeat(64)]
  ])
  assert.deepEqual(reports, [{ success: true }, { success: true }])
  assert.deepEqual(published[0].relays, ['wss://one.example', 'wss://two.example', 'wss://seed.example'])
  assert.deepEqual(published[1].relays, ['wss://three.example', 'wss://seed.example', 'wss://one.example'])

  const routers = []
  for (const { outer } of published) {
    routers.push(await decryptPrivateBroadcast(alice, alicePubkey, outer.content))
  }
  assert.deepEqual(routers.map(router => router.tags.some(tag => tag[0] === 'id')), [false, false])
  assert.notEqual(routers[0].pubkey, routers[1].pubkey)
  assert.deepEqual(
    routerRecipientRows(routers[0]).map(row => row[0]).sort(),
    [bobPubkey, carolPubkey].sort()
  )
  assert.deepEqual(
    routerRecipientRows(routers[1]).map(row => row[0]),
    [carolPubkey, davePubkey]
  )
  const firstLines = routerJsonlLines(routers[0])
  const secondLines = routerJsonlLines(routers[1])
  assert.equal(firstLines[0], secondLines[0])
  assert.equal(
    firstLines.find(line => JSON.parse(line)[0] === carolPubkey),
    secondLines.find(line => JSON.parse(line)[0] === carolPubkey)
  )
  assert.ok(temporaryWrites.includes(TEMPORARY_STORAGE_KEYS_KEY))
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('publish sends wrapped events through RelayPool.sendEvent', async () => {
  const originalSendEvent = relayPool.sendEvent
  const published = []
  relayPool.sendEvent = async (outer, relays) => {
    published.push({ outer, relays })
    return { success: true, total: relays.length, promise: Promise.resolve({ success: true }) }
  }

  try {
    const alice = signer()
    const bob = signer()
    await publish({
      senderSigner: alice,
      receivers: [await bob.getPublicKey()],
      event: eventFixture('relay pool publish'),
      relays: ['wss://relay.example'],
      _getIykcProofs: noContentKeys
    })
  } finally {
    relayPool.sendEvent = originalSendEvent
  }

  assert.equal(published.length, 1)
  assert.equal(published[0].outer.kind, PRIVATE_BROADCAST_KIND)
  assert.deepEqual(published[0].relays, ['wss://relay.example'])
})

test('fetch uses complete RelayPool reads for private-channel recovery', async () => {
  let call
  const events = await fetch({
    relays: ['wss://relay.example'],
    _getEvents: async (filter, relays, options) => {
      call = { filter, relays, options }
      return { result: [] }
    }
  })

  assert.deepEqual(events, [])
  assert.deepEqual(call.relays, ['wss://relay.example'])
  assert.deepEqual(call.options, { timeout: 5000, timeoutAfterFirstEose: null })
  assert.deepEqual(call.filter.kinds, [PRIVATE_BROADCAST_KIND])
})

test('publish cleans prepared rows when grouped publishing fails', async () => {
  const alice = signer()
  const bob = signer()
  const carol = signer()
  const bobPubkey = await bob.getPublicKey()
  const carolPubkey = await carol.getPublicKey()

  await assert.rejects(
    publish({
      senderSigner: alice,
      receivers: [bobPubkey, carolPubkey],
      relayToReceivers: new Map([
        ['wss://one.example', [bobPubkey]],
        ['wss://two.example', [carolPubkey]]
      ]),
      event: eventFixture('publish fail'),
      _getIykcProofs: noContentKeys,
      _publish: async () => { throw new Error('relay offline') }
    }),
    /relay offline/
  )

  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('publishNymEvent mirrors carrier chunks to recovery relays', async () => {
  const channel = signer()
  const nym = signer()
  const published = []

  const reports = await publishNymEvent({
    nymSigner: nym,
    privateChannelSigner: channel,
    event: eventFixture('nym mirror'),
    relays: ['wss://receiver.example'],
    recoveryRelays: ['wss://seed.example', 'wss://receiver.example'],
    deletionPubkey: 'e'.repeat(64),
    _publish: async (outer, relays) => {
      published.push({ outer, relays })
      return { success: true }
    }
  })

  assert.equal(published.length, 1)
  assert.deepEqual(published[0].relays, ['wss://receiver.example', 'wss://seed.example'])
  assert.deepEqual(published[0].outer.tags.find(tag => tag[0] === 's'), ['s', 'e'.repeat(64)])
  assert.deepEqual(reports, [{ success: true }])
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('received chunk default cap is 16 MiB', () => {
  assert.equal(getJsonlChunkByteSize(), 30159)
  assert.equal(DEFAULT_RECEIVED_CHUNK_MAX_BYTES, 16 * 1024 * 1024)
})

test('wrapEvent supports overriding the outer expiration window', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const before = Math.floor(Date.now() / 1000)
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: eventFixture('hello bob'),
    expirationSeconds: 7 * 24 * 60 * 60,
    _getIykcProofs: noContentKeys
  })

  assert.ok(Number(wrapped.tags[0][1]) >= before + 7 * 24 * 60 * 60)
})

test('unwrapEvent returns the addressed receiver event or null', async () => {
  const alice = signer()
  const bob = signer()
  const carol = signer()
  const bobPubkey = await bob.getPublicKey()
  const carolPubkey = await carol.getPublicKey()
  const alicePubkey = await alice.getPublicKey()
  const original = { ...eventFixture('private'), pubkey: '0'.repeat(64), id: 'f'.repeat(64) }
  const [wrapped] = await wrapEvent({ senderSigner: alice, receivers: [bobPubkey, carolPubkey], event: original, _getIykcProofs: noContentKeys })

  assert.deepEqual(await unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }), unwrappedFixture(original, alicePubkey))
  assert.deepEqual(await unwrapEvent({ receiverSigner: carol, privateChannelSigner: alice, event: wrapped, receiverPubkey: carolPubkey }), unwrappedFixture(original, alicePubkey))
  assert.equal(await unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: await signer().getPublicKey() }), null)
})

test('wrapEvent can encrypt the outer router to a separate reader key', async () => {
  const sender = signer()
  const channel = signer()
  const reader = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const readerPubkey = await reader.getPublicKey()
  const channelPubkey = await channel.getPublicKey()
  const original = eventFixture('reader decrypts channel')
  const [wrapped] = await wrapEvent({
    senderSigner: sender,
    privateChannelSigner: channel,
    privateChannelReaderPubkey: readerPubkey,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: noContentKeys
  })

  assert.equal(wrapped.pubkey, channelPubkey)
  const router = await decryptPrivateBroadcast(reader, channelPubkey, wrapped.content)
  assert.equal(router.kind, ROUTER_KIND)
  assert.deepEqual(await unwrapEvent({
    receiverSigner: bob,
    privateChannelSigner: channel,
    privateChannelReaderSigner: reader,
    event: wrapped,
    receiverPubkey: bobPubkey
  }), unwrappedFixture(original, await sender.getPublicKey()))
  assert.deepEqual(await unwrapEvent({
    receiverSigner: bob,
    privateChannelSigner: channel,
    privateChannelReaderPubkey: readerPubkey,
    event: wrapped,
    receiverPubkey: bobPubkey
  }), unwrappedFixture(original, await sender.getPublicKey()))
})

test('subscribe can read channel events with only a reader signer', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const sender = signer()
    const channel = signer()
    const reader = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const channelPubkey = await channel.getPublicKey()
    const original = eventFixture('reader-only subscribe')
    const [wrapped] = await wrapEvent({
      senderSigner: sender,
      privateChannelSigner: channel,
      privateChannelReaderPubkey: await reader.getPublicKey(),
      receivers: [bobPubkey],
      event: original,
      _getIykcProofs: noContentKeys
    })
    const events = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: null,
      privateChannelReaderSigner: reader,
      privateChannelPubkey: channelPubkey,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      onEvent: event => events.push(event)
    })
    await handlers.onevent(wrapped)

    assert.deepEqual(events, [unwrappedFixture(original, await sender.getPublicKey())])
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('live-only subscribe closes its RelayPool stream', async () => {
  const sender = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const channelPubkey = await sender.getPublicKey()
  const original = eventFixture('live subscription')
  const [wrapped] = await wrapEvent({
    senderSigner: sender,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: noContentKeys
  })
  const received = []
  let aborted = false
  let waitingForAbort
  const reachedAbortWait = new Promise(resolve => { waitingForAbort = resolve })

  const subscription = subscribe({
    receiverSigner: bob,
    privateChannelSigner: sender,
    privateChannelPubkey: channelPubkey,
    receiverPubkey: bobPubkey,
    relays: ['wss://relay.example'],
    liveOnly: true,
    onEvent: event => received.push(event),
    _liveEventsGenerator: (_filter, _relays, { signal }) => (async function * () {
      yield wrapped
      waitingForAbort()
      if (signal.aborted) {
        aborted = true
        return
      }
      await new Promise(resolve => signal.addEventListener('abort', () => {
        aborted = true
        resolve()
      }, { once: true }))
    })()
  })

  for (let attempt = 0; attempt < 100 && received.length === 0; attempt++) {
    await new Promise(resolve => setImmediate(resolve))
  }
  assert.deepEqual(received, [unwrappedFixture(original, await sender.getPublicKey())])

  await reachedAbortWait
  subscription.close()
  // eslint-disable-next-line no-unmodified-loop-condition
  for (let attempt = 0; attempt < 20 && !aborted; attempt++) {
    await new Promise(resolve => setImmediate(resolve))
  }
  assert.ok(aborted)
})

test('subscribe can read reader-targeted channel events with the writer signer', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const sender = signer()
    const channel = signer()
    const reader = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const readerPubkey = await reader.getPublicKey()
    const channelPubkey = await channel.getPublicKey()
    const original = eventFixture('writer reads reader-targeted router')
    const [wrapped] = await wrapEvent({
      senderSigner: sender,
      privateChannelSigner: channel,
      privateChannelReaderPubkey: readerPubkey,
      receivers: [bobPubkey],
      event: original,
      _getIykcProofs: noContentKeys
    })
    const events = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: channel,
      privateChannelReaderPubkey: readerPubkey,
      privateChannelPubkey: channelPubkey,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      onEvent: event => events.push(event)
    })
    await handlers.onevent(wrapped)

    assert.deepEqual(events, [unwrappedFixture(original, await sender.getPublicKey())])
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('unwrapEvent preserves valid signed inner events', async () => {
  const alice = signer()
  const bob = signer()
  const authorSecret = generateSecretKey()
  const bobPubkey = await bob.getPublicKey()
  const signed = finalizeEvent({ kind: 9002, created_at: 2, tags: [['x', '1']], content: 'signed' }, authorSecret)
  const [wrapped] = await wrapEvent({ senderSigner: alice, receivers: [bobPubkey], event: signed, _getIykcProofs: noContentKeys })

  assert.deepEqual(await unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }), signed)
})

test('unwrapEvent rejects invalid signed inner events', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const signed = finalizeEvent({ kind: 9002, created_at: 2, tags: [], content: 'signed' }, generateSecretKey())
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: { ...signed, content: 'tampered' },
    _getIykcProofs: noContentKeys
  })

  await assert.rejects(
    () => unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }),
    /INVALID_SIGNED_INNER_EVENT/
  )
})

test('unwrapEvent rejects malformed signed inner events', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: { id: 'e'.repeat(64), pubkey: 'a'.repeat(64), kind: 9002, created_at: 2, tags: [], content: 'signed', sig: 42 },
    _getIykcProofs: noContentKeys
  })

  await assert.rejects(
    () => unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }),
    /INVALID_SIGNED_INNER_EVENT/
  )
})

test('wrapNymEvent signs nym carriers and reconstructs nym rumors', async () => {
  const channel = signer()
  const reader = signer()
  const nym = signer()
  const channelPubkey = await channel.getPublicKey()
  const readerPubkey = await reader.getPublicKey()
  const nymPubkey = await nym.getPublicKey()
  const original = eventFixture('nym rumor')
  const [wrapped] = await wrapNymEvent({
    nymSigner: nym,
    privateChannelSigner: channel,
    privateChannelReaderPubkey: readerPubkey,
    event: original
  })

  assert.equal(wrapped.kind, PRIVATE_BROADCAST_KIND)
  assert.equal(wrapped.pubkey, channelPubkey)
  assert.ok(new TextEncoder().encode(JSON.stringify(wrapped)).length <= MAX_EVENT_BYTES)
  const carrier = await decryptPrivateBroadcast(reader, channelPubkey, wrapped.content)
  const expected = unwrappedFixture(original, nymPubkey)

  assert.equal(carrier.kind, NYM_CARRIER_KIND)
  assert.equal(carrier.pubkey, nymPubkey)
  assert.equal(carrier.tags.find(tag => tag[0] === 'id')?.[1], expected.id)
  assert.deepEqual(carrier.tags.find(tag => tag[0] === 'c'), ['c', '0', '1'])
  assert.deepEqual(eventFromNymCarriers([carrier]), expected)
})

test('wrapNymEvent preserves signed inner event authors distinct from carrier nym', async () => {
  const channel = signer()
  const nym = signer()
  const signed = finalizeEvent({ kind: 9002, created_at: 23, tags: [['x', 'signed']], content: 'signed by someone else' }, generateSecretKey())
  const [wrapped] = await wrapNymEvent({
    nymSigner: nym,
    privateChannelSigner: channel,
    event: signed
  })
  const carrier = await decryptPrivateBroadcast(channel, await channel.getPublicKey(), wrapped.content)

  assert.notEqual(carrier.pubkey, signed.pubkey)
  assert.equal(carrier.tags.find(tag => tag[0] === 'id')?.[1], signed.id)
  assert.deepEqual(eventFromNymCarriers([carrier]), signed)
})

test('subscribe buffers out-of-order nym carrier chunks by nym pubkey and inner id', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const channel = signer()
    const nym = signer()
    const nymPubkey = await nym.getPublicKey()
    const original = eventFixture('x'.repeat(getNymCarrierChunkSize()))
    const wrapped = await wrapNymEvent({
      nymSigner: nym,
      privateChannelSigner: channel,
      deletionPubkey: 'e'.repeat(64),
      event: original
    })
    const routedEvents = []
    const nymEvents = []
    const seeds = []

    assert.ok(wrapped.length > 1)
    for (const event of wrapped) assert.ok(new TextEncoder().encode(JSON.stringify(event)).length <= MAX_EVENT_BYTES)
    assert.deepEqual(new Set(wrapped.map(event => event.tags.find(tag => tag[0] === 's')?.[1])), new Set(['e'.repeat(64)]))

    subscribe({
      privateChannelSigner: channel,
      privateChannelPubkey: await channel.getPublicKey(),
      relays: ['wss://relay.example'],
      mode: 'seeder',
      onEvent: event => routedEvents.push(event),
      onNymEvent: event => nymEvents.push(event),
      onSeedEvent: seed => seeds.push(seed)
    })
    for (const event of [...wrapped].reverse()) await handlers.onevent(event)

    assert.deepEqual(routedEvents, [])
    assert.deepEqual(nymEvents, [unwrappedFixture(original, nymPubkey)])
    assert.equal(seeds.length, 1)
    assert.equal(seeds[0].recordType, 'nymCarrier_v1')
    assert.equal(seeds[0].carriers.length, wrapped.length)
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('subscribe separates nym carrier groups that use different chunk totals', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const channel = signer()
    const nym = signer()
    const channelPubkey = await channel.getPublicKey()
    const nymPubkey = await nym.getPublicKey()
    const original = eventFixture('same inner event, different chunking')
    const expected = unwrappedFixture(original, nymPubkey)
    const encoded = Buffer.from(JSON.stringify(original)).toString('base64')
    const midpoint = Math.ceil(encoded.length / 2)
    const nymEvents = []
    const errors = []

    async function carrier (index, total, content) {
      return nym.signEvent({
        kind: NYM_CARRIER_KIND,
        created_at: 9,
        tags: [['id', expected.id], ['c', String(index), String(total)]],
        content
      })
    }

    async function outer (carrier) {
      return channel.signEvent({
        kind: PRIVATE_BROADCAST_KIND,
        created_at: 10,
        tags: [],
        content: await encryptPrivateBroadcast(channel, channelPubkey, carrier)
      })
    }

    const twoChunkFirst = await outer(await carrier(0, 2, encoded.slice(0, midpoint)))
    const oneChunk = await outer(await carrier(0, 1, encoded))
    const twoChunkLast = await outer(await carrier(1, 2, encoded.slice(midpoint)))

    subscribe({
      privateChannelSigner: channel,
      privateChannelPubkey: channelPubkey,
      relays: ['wss://relay.example'],
      onNymEvent: event => nymEvents.push(event),
      onError: err => errors.push(err)
    })
    await handlers.onevent(twoChunkFirst)
    await handlers.onevent(oneChunk)
    await handlers.onevent(twoChunkLast)

    assert.deepEqual(errors, [])
    assert.deepEqual(nymEvents, [expected, expected])
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('unwrapEvent uses imkc tag as the row encryption pubkey', async () => {
  const alice = signer()
  const imkc = signer()
  const bob = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const imkcPubkey = await imkc.getPublicKey()
  const original = eventFixture('private')
  NsecSigner.setContentSigners(alice, [imkc])
  const [wrapped] = await wrapEvent({ senderSigner: alice, imkcSigner: imkc, receivers: [bobPubkey], event: original, _getIykcProofs: noContentKeys })
  const router = await decryptPrivateBroadcast(alice, await alice.getPublicKey(), wrapped.content)
  const imkcTag = router.tags.find(t => t[0] === 'imkc')

  assert.equal(router.tags.find(t => t[0] === 'f')?.[1], alicePubkey)
  assert.equal(imkcTag?.[1], imkcPubkey)
  assert.equal(isValidContentKeyProof({ ownerPubkey: alicePubkey, contentPubkey: imkcPubkey, proof: imkcTag?.[2] }), true)
  assert.deepEqual(
    await unwrapEvent({
      receiverSigner: bob,
      privateChannelSigner: alice,
      event: wrapped,
      receiverPubkey: bobPubkey
    }),
    unwrappedFixture(original, alicePubkey)
  )
})

test('wrapEvent can add imkc from senderSigner Double-DH without direct content signer access', async () => {
  const alice = signer()
  const aliceContent = signer()
  const bob = signer()
  const aliceProxy = await signerWithInternalContentKey(alice, aliceContent)
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const contentPubkey = await aliceContent.getPublicKey()
  const original = eventFixture('private')
  const [wrapped] = await wrapEvent({
    senderSigner: aliceProxy,
    privateChannelSigner: alice,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: noContentKeys
  })
  const router = await decryptPrivateBroadcast(alice, alicePubkey, wrapped.content)
  const imkcTag = router.tags.find(t => t[0] === 'imkc')

  assert.equal(imkcTag?.[1], contentPubkey)
  assert.equal(isValidContentKeyProof({ ownerPubkey: alicePubkey, contentPubkey, proof: imkcTag?.[2] }), true)
  assert.deepEqual(
    await unwrapEvent({
      receiverSigner: bob,
      privateChannelSigner: alice,
      event: wrapped,
      receiverPubkey: bobPubkey
    }),
    unwrappedFixture(original, alicePubkey)
  )
})

test('wrapEvent uses Double-DH returned own content pubkey for imkc tag', async () => {
  const alice = signer()
  const oldContent = signer()
  const actualContent = signer()
  const bob = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const actualContentPubkey = await actualContent.getPublicKey()
  const senderSigner = {
    getPublicKey: () => alice.getPublicKey(),
    signEvent: event => alice.signEvent(event),
    nip44Encrypt: (peerPubkey, plaintext) => alice.nip44Encrypt(peerPubkey, plaintext),
    nip44v3Encrypt: (peerPubkey, kind, scope, plaintextB64) => alice.nip44v3Encrypt(peerPubkey, kind, scope, plaintextB64),
    nip44EncryptDoubleDH: (...params) => {
      NsecSigner.setContentSigners(alice, [actualContent])
      return alice.nip44EncryptDoubleDH(...params)
    }
  }
  const [wrapped] = await wrapEvent({
    senderSigner,
    imkcSigner: oldContent,
    privateChannelSigner: alice,
    receivers: [bobPubkey],
    event: eventFixture('private'),
    _getIykcProofs: noContentKeys
  })
  const router = await decryptPrivateBroadcast(alice, alicePubkey, wrapped.content)
  const imkcTag = router.tags.find(t => t[0] === 'imkc')

  assert.equal(imkcTag?.[1], actualContentPubkey)
  assert.equal(isValidContentKeyProof({ ownerPubkey: alicePubkey, contentPubkey: actualContentPubkey, proof: imkcTag?.[2] }), true)
})

test('wrapEvent retries once when sender content key rotates while writing chunks', async () => {
  const alice = signer()
  const oldContent = signer()
  const actualContent = signer()
  const bob = signer()
  const carol = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const carolPubkey = await carol.getPublicKey()
  const actualContentPubkey = await actualContent.getPublicKey()
  let encryptCalls = 0
  const senderSigner = {
    getPublicKey: () => alice.getPublicKey(),
    signEvent: event => alice.signEvent(event),
    nip44Encrypt: (peerPubkey, plaintext) => alice.nip44Encrypt(peerPubkey, plaintext),
    nip44v3Encrypt: (peerPubkey, kind, scope, plaintextB64) => alice.nip44v3Encrypt(peerPubkey, kind, scope, plaintextB64),
    nip44EncryptDoubleDH: (...params) => {
      encryptCalls++
      NsecSigner.setContentSigners(alice, [encryptCalls === 1 ? oldContent : actualContent])
      return alice.nip44EncryptDoubleDH(...params)
    }
  }
  const original = eventFixture('private after rotation')
  const [wrapped] = await wrapEvent({
    senderSigner,
    imkcSigner: oldContent,
    privateChannelSigner: alice,
    receivers: [bobPubkey, carolPubkey],
    event: original,
    _getIykcProofs: noContentKeys
  })
  const router = await decryptPrivateBroadcast(alice, alicePubkey, wrapped.content)
  const imkcTag = router.tags.find(t => t[0] === 'imkc')

  assert.equal(encryptCalls, 4)
  assert.equal(imkcTag?.[1], actualContentPubkey)
  assert.deepEqual(
    await unwrapEvent({
      receiverSigner: bob,
      privateChannelSigner: alice,
      event: wrapped,
      receiverPubkey: bobPubkey
    }),
    unwrappedFixture(original, alicePubkey)
  )
})

test('unwrapEvent rejects sender imkc keys that do not match the ciphertext', async () => {
  const alice = signer()
  const oldImkc = signer()
  const newImkc = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  NsecSigner.setContentSigners(alice, [oldImkc])
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    imkcSigner: oldImkc,
    receivers: [bobPubkey],
    event: eventFixture('private'),
    _getIykcProofs: noContentKeys
  })
  const channelPubkey = await alice.getPublicKey()
  const router = await decryptPrivateBroadcast(alice, channelPubkey, wrapped.content)
  const newImkcPubkey = await newImkc.getPublicKey()
  const newImkcProof = parseContentKeyEvent(await makeContentKeyEvent({ userSigner: alice, contentKeySigner: newImkc, createdAt: 7 })).iykcProof
  router.tags = router.tags.map(tag => tag[0] === 'imkc' ? ['imkc', newImkcPubkey, newImkcProof] : tag)
  const tampered = await alice.signEvent({
    kind: PRIVATE_BROADCAST_KIND,
    created_at: wrapped.created_at,
    tags: wrapped.tags,
    content: await encryptPrivateBroadcast(alice, channelPubkey, router)
  })

  await assert.rejects(
    () => unwrapEvent({
      receiverSigner: bob,
      privateChannelSigner: alice,
      event: tampered,
      receiverPubkey: bobPubkey
    }),
    /invalid MAC/
  )
})

test('unwrapEvent rejects sender imkc rows without valid proofs', async () => {
  const alice = signer()
  const imkc = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  NsecSigner.setContentSigners(alice, [imkc])
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    imkcSigner: imkc,
    receivers: [bobPubkey],
    event: eventFixture('private'),
    _getIykcProofs: noContentKeys
  })
  const channelPubkey = await alice.getPublicKey()
  const router = await decryptPrivateBroadcast(alice, channelPubkey, wrapped.content)
  router.tags = router.tags.map(tag => tag[0] === 'imkc' ? ['imkc', tag[1]] : tag)
  const tampered = await alice.signEvent({
    kind: PRIVATE_BROADCAST_KIND,
    created_at: wrapped.created_at,
    tags: wrapped.tags,
    content: await encryptPrivateBroadcast(alice, channelPubkey, router)
  })

  await assert.rejects(
    () => unwrapEvent({
      receiverSigner: bob,
      privateChannelSigner: alice,
      event: tampered,
      receiverPubkey: bobPubkey
    }),
    /INVALID_IMKC_PROOF/
  )
})

test('subscribe emits content key usage for own sent direct messages', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const [wrapped] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      receiverTag: bobPubkey,
      event: eventFixture('private'),
      _getIykcProofs: noContentKeys
    })
    const usages = []
    const delivered = []

    subscribe({
      receiverSigner: alice,
      privateChannelSigner: alice,
      receiverPubkey: await alice.getPublicKey(),
      relays: ['wss://relay.example'],
      onContentKeyUsage: usage => usages.push(usage),
      onEvent: event => delivered.push(event)
    })
    await handlers.onevent(wrapped)

    assert.equal(usages.length, 1)
    assert.equal(usages[0].direction, 'sent')
    assert.equal(usages[0].senderPubkey, await alice.getPublicKey())
    assert.equal(usages[0].receiverPubkey, bobPubkey)
    assert.equal(usages[0].contentKeyPubkey, '')
    assert.equal(delivered.length, 0)
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('subscribe treats watchtower mode as recovery seed storage', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const original = eventFixture('watchtower payload')
    const [wrapped] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      event: original,
      _getIykcProofs: noContentKeys
    })
    const events = []
    const seeds = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      mode: 'watchtower',
      onEvent: event => events.push(event),
      onSeedEvent: seed => seeds.push(seed)
    })
    await handlers.onevent(wrapped)

    assert.deepEqual(events, [unwrappedFixture(original, await alice.getPublicKey())])
    assert.equal(seeds.length, 1)
    assert.equal(seeds[0].channelPubkey, await alice.getPublicKey())
    assert.ok(seeds[0].router.content)
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('wrapEvent uses receiver content key rows when iykc is advertised', async () => {
  const alice = signer()
  const bob = signer()
  const bobContent = signer()
  const alicePubkey = await alice.getPublicKey()
  const bobPubkey = await bob.getPublicKey()
  const contentKeyEvent = await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobContent, createdAt: 7 })
  const contentKey = parseContentKeyEvent(contentKeyEvent)
  const original = eventFixture('private')
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: async () => ({
      [bobPubkey]: contentKey
    })
  })
  const router = await decryptPrivateBroadcast(alice, alicePubkey, wrapped.content)
  const line = routerRecipientRows(router)[0]

  assert.deepEqual(line.slice(0, 1), [bobPubkey])
  assert.deepEqual(line.slice(2), [contentKey.iykcPubkey, contentKey.iykcProof])
  await assert.rejects(
    () => unwrapEvent({ receiverSigner: bob, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }),
    /RECEIVER_CONTENT_KEY_REQUIRED/
  )
  NsecSigner.setContentSigners(bob, [bobContent])
  await assert.rejects(
    () => bob.nip44DecryptDoubleDH(alicePubkey, ROUTER_KIND, '', line[1], router.tags.find(t => t[0] === 'imkc')?.[1] || '', contentKey.iykcPubkey),
    /scope mismatch/
  )
  assert.deepEqual(await unwrapEvent({ receiverSigner: bob, iykcSigner: bobContent, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }), unwrappedFixture(original, alicePubkey))
})

test('unwrapEvent can decrypt iykc rows through receiverSigner without direct content signer access', async () => {
  const alice = signer()
  const bob = signer()
  const bobContent = signer()
  const bobProxy = await signerWithInternalContentKey(bob, bobContent)
  const bobPubkey = await bob.getPublicKey()
  const contentKeyEvent = await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobContent, createdAt: 7 })
  const contentKey = parseContentKeyEvent(contentKeyEvent)
  const original = eventFixture('private')
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: async () => ({
      [bobPubkey]: contentKey
    })
  })

  assert.deepEqual(
    await unwrapEvent({
      receiverSigner: bobProxy,
      privateChannelSigner: alice,
      event: wrapped,
      receiverPubkey: bobPubkey
    }),
    unwrappedFixture(original, await alice.getPublicKey())
  )
})

test('unwrapEvent lets receiverSigner resolve older iykc when current content signer differs', async () => {
  const alice = signer()
  const bob = signer()
  const bobOldContent = signer()
  const bobLatestContent = signer()
  const bobProxy = await signerWithInternalContentKey(bob, bobOldContent)
  const bobPubkey = await bob.getPublicKey()
  const oldContentKey = parseContentKeyEvent(await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobOldContent, createdAt: 7 }))
  const original = eventFixture('older key private')
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: original,
    _getIykcProofs: async () => ({
      [bobPubkey]: oldContentKey
    })
  })

  assert.deepEqual(
    await unwrapEvent({
      receiverSigner: bobProxy,
      iykcSigner: bobLatestContent,
      privateChannelSigner: alice,
      event: wrapped,
      receiverPubkey: bobPubkey
    }),
    unwrappedFixture(original, await alice.getPublicKey())
  )
})

test('wrapEvent accepts explicit receiver content keys with valid proofs', async () => {
  const alice = signer()
  const bob = signer()
  const bobContent = signer()
  const bobPubkey = await bob.getPublicKey()
  const contentKey = parseContentKeyEvent(await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobContent, createdAt: 7 }))
  const original = eventFixture('private')
  NsecSigner.setContentSigners(bob, [bobContent])
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [[bobPubkey, contentKey.iykcPubkey, contentKey.iykcProof]],
    event: original,
    _getIykcProofs: noContentKeys
  })
  const router = await decryptPrivateBroadcast(alice, await alice.getPublicKey(), wrapped.content)
  const line = routerRecipientRows(router)[0]

  assert.equal(line.length, 4)
  assert.deepEqual(await unwrapEvent({ receiverSigner: bob, iykcSigner: bobContent, privateChannelSigner: alice, event: wrapped, receiverPubkey: bobPubkey }), unwrappedFixture(original, await alice.getPublicKey()))
})

test('wrapEvent rejects explicit receiver content keys without valid proofs', async () => {
  const alice = signer()
  const bob = signer()
  const bobContent = signer()
  const bobPubkey = await bob.getPublicKey()
  const bobContentPubkey = await bobContent.getPublicKey()

  await assert.rejects(
    () => wrapEvent({
      senderSigner: alice,
      receivers: [[bobPubkey, bobContentPubkey]],
      event: eventFixture('private'),
      _getIykcProofs: noContentKeys
    }),
    /INVALID_IYKC_PROOF/
  )
})

test('unwrapEvent rejects receiver iykc rows without valid proofs', async () => {
  const alice = signer()
  const bob = signer()
  const bobContent = signer()
  const bobPubkey = await bob.getPublicKey()
  const contentKey = parseContentKeyEvent(await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobContent, createdAt: 7 }))
  const original = eventFixture('private')
  const [wrapped] = await wrapEvent({
    senderSigner: alice,
    receivers: [[bobPubkey, contentKey.iykcPubkey, contentKey.iykcProof]],
    event: original,
    _getIykcProofs: noContentKeys
  })
  const channelPubkey = await alice.getPublicKey()
  const router = await decryptPrivateBroadcast(alice, channelPubkey, wrapped.content)
  const rows = routerJsonlRows(router)
  const [receiverPubkey, ciphertext, iykcPubkey] = rows.find(row => row.length !== 1)
  router.content = Buffer.from(`${JSON.stringify(rows[0])}\n${JSON.stringify([receiverPubkey, ciphertext, iykcPubkey, '7:bad'])}\n`).toString('base64')
  const tampered = await alice.signEvent({
    kind: PRIVATE_BROADCAST_KIND,
    created_at: wrapped.created_at,
    tags: wrapped.tags,
    content: await encryptPrivateBroadcast(alice, channelPubkey, router)
  })

  await assert.rejects(
    () => unwrapEvent({ receiverSigner: bob, iykcSigner: bobContent, privateChannelSigner: alice, event: tampered, receiverPubkey: bobPubkey }),
    /INVALID_IYKC_PROOF/
  )
})

test('subscribe only emits receiver content key usage after iykc proof validation', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobContent = signer()
    const bobPubkey = await bob.getPublicKey()
    const contentKey = parseContentKeyEvent(await makeContentKeyEvent({ userSigner: bob, contentKeySigner: bobContent, createdAt: 7 }))
    const [wrapped] = await wrapEvent({
      senderSigner: alice,
      receivers: [[bobPubkey, contentKey.iykcPubkey, contentKey.iykcProof]],
      event: eventFixture('private'),
      _getIykcProofs: noContentKeys
    })
    const channelPubkey = await alice.getPublicKey()
    const router = await decryptPrivateBroadcast(alice, channelPubkey, wrapped.content)
    const rows = routerJsonlRows(router)
    const [receiverPubkey, ciphertext, iykcPubkey] = rows.find(row => row.length !== 1)
    router.content = Buffer.from(`${JSON.stringify(rows[0])}\n${JSON.stringify([receiverPubkey, ciphertext, iykcPubkey, '7:bad'])}\n`).toString('base64')
    const tampered = await alice.signEvent({
      kind: PRIVATE_BROADCAST_KIND,
      created_at: wrapped.created_at,
      tags: wrapped.tags,
      content: await encryptPrivateBroadcast(alice, channelPubkey, router)
    })
    const usages = []
    const errors = []

    subscribe({
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      onContentKeyUsage: usage => usages.push(usage),
      onError: err => errors.push(err)
    })

    await handlers.onevent(tampered)

    assert.deepEqual(usages, [])
    assert.equal(errors.length, 1)
    assert.equal(errors[0].message, 'INVALID_IYKC_PROOF')
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('wrapEvent chunks large jsonl without oversize events and unwraps reassembled bytes', async () => {
  const alice = signer()
  const imkc = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const imkcPubkey = await imkc.getPublicKey()
  const original = eventFixture('x'.repeat(getJsonlChunkByteSize()))
  NsecSigner.setContentSigners(alice, [imkc])
  const wrapped = await wrapEvent({
    senderSigner: alice,
    imkcSigner: imkc,
    receivers: [bobPubkey],
    deletionPubkey: 'd'.repeat(64),
    event: original,
    _getIykcProofs: noContentKeys
  })

  assert.ok(wrapped.length > 1)
  const deletionPubkeys = new Set()
  for (const event of wrapped) {
    assert.ok(new TextEncoder().encode(JSON.stringify(event)).length <= MAX_EVENT_BYTES)
    deletionPubkeys.add(event.tags.find(tag => tag[0] === 's')?.[1])
  }
  assert.deepEqual(deletionPubkeys, new Set(['d'.repeat(64)]))

  const routers = []
  for (const event of wrapped) {
    routers.push(await decryptPrivateBroadcast(alice, await alice.getPublicKey(), event.content))
  }
  assert.equal(routers[0].kind, ROUTER_KIND)
  assert.equal(routers[0].tags.find(t => t[0] === 'r')?.[1], bobPubkey)
  assert.equal(routers[0].tags.find(t => t[0] === 'imkc')?.[1], imkcPubkey)
  assert.ok(routers[0].tags.find(t => t[0] === 'imkc')?.[2])
  assert.equal(routers.length, Number(routers[0].tags.find(t => t[0] === 'c')[2]))
})

test('wrapEvents cleans temporary chunks when the stream is stopped early', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const original = eventFixture('x'.repeat(getJsonlChunkByteSize()))
  const stream = wrapEvents({ senderSigner: alice, receivers: [bobPubkey], event: original, _getIykcProofs: noContentKeys })

  const first = await stream.next()
  assert.equal(first.done, false)
  assert.notEqual(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)

  await stream.return()

  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('wrapEvents uses a caller-supplied temporary storage area', async () => {
  const alice = signer()
  const bob = signer()
  const bobPubkey = await bob.getPublicKey()
  const original = eventFixture('x'.repeat(getJsonlChunkByteSize()))
  const stream = wrapEvents({
    senderSigner: alice,
    receivers: [bobPubkey],
    event: original,
    temporaryStorageArea: globalThis.localStorage,
    _getIykcProofs: noContentKeys
  })

  await stream.next()

  assert.notEqual(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)

  await stream.return()

  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})

test('subscribe buffers out-of-order chunks and unwraps when the missing chunk arrives', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const original = eventFixture('x'.repeat(getJsonlChunkByteSize()))
    const wrapped = await wrapEvent({ senderSigner: alice, receivers: [bobPubkey], event: original, _getIykcProofs: noContentKeys })
    const events = []
    const chunks = []

    assert.ok(wrapped.length > 1)
    subscribe({
      receiverSigner: bob,
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      onChunk: chunk => chunks.push(chunk),
      onEvent: event => events.push(event),
      _getIykcProofs: noContentKeys
    })

    for (const event of wrapped.slice(1)) await handlers.onevent(event)

    assert.equal(events.length, 0)
    assert.ok(chunks.at(-1).missing.includes(0))

    await handlers.onevent(wrapped[0])

    assert.deepEqual(events, [unwrappedFixture(original, await alice.getPublicKey())])
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('subscribe drops invalid signed inner events and emits an error', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const signed = finalizeEvent({ kind: 9002, created_at: 2, tags: [], content: 'signed' }, generateSecretKey())
    const [wrapped] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      event: { ...signed, content: 'tampered' },
      _getIykcProofs: noContentKeys
    })
    const events = []
    const errors = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      onEvent: event => events.push(event),
      onError: err => errors.push(err),
      _getIykcProofs: noContentKeys
    })

    await handlers.onevent(wrapped)
    await handlers.onevent(wrapped)

    assert.equal(events.length, 0)
    assert.equal(errors.length, 1)
    assert.equal(errors[0].message, 'INVALID_SIGNED_INNER_EVENT')
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('subscribe forgets ignored groups after their ttl', async () => {
  const originalSubscribeMany = pool.subscribeMany
  const originalNow = Date.now
  let handlers = null
  let now = 1000
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }
  Date.now = () => now

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const signed = finalizeEvent({ kind: 9002, created_at: 2, tags: [], content: 'signed' }, generateSecretKey())
    const [wrapped] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      event: { ...signed, content: 'tampered' },
      _getIykcProofs: noContentKeys
    })
    const errors = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      ignoredGroupTtlMs: 5,
      onError: err => errors.push(err),
      _getIykcProofs: noContentKeys
    })

    await handlers.onevent(wrapped)
    now += 4
    await handlers.onevent(wrapped)
    now += 2
    await handlers.onevent(wrapped)

    assert.equal(errors.length, 2)
    assert.equal(errors[0].message, 'INVALID_SIGNED_INNER_EVENT')
    assert.equal(errors[1].message, 'INVALID_SIGNED_INNER_EVENT')
  } finally {
    Date.now = originalNow
    pool.subscribeMany = originalSubscribeMany
  }
})

test('subscribe evicts old ignored groups when the tombstone cache is full', async () => {
  const originalSubscribeMany = pool.subscribeMany
  let handlers = null
  pool.subscribeMany = (_relays, _filter, nextHandlers) => {
    handlers = nextHandlers
    return { close: () => {} }
  }

  try {
    const alice = signer()
    const bob = signer()
    const bobPubkey = await bob.getPublicKey()
    const signedA = finalizeEvent({ kind: 9002, created_at: 2, tags: [], content: 'a' }, generateSecretKey())
    const signedB = finalizeEvent({ kind: 9002, created_at: 3, tags: [], content: 'b' }, generateSecretKey())
    const [wrappedA] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      event: { ...signedA, content: 'tampered-a' },
      _getIykcProofs: noContentKeys
    })
    const [wrappedB] = await wrapEvent({
      senderSigner: alice,
      receivers: [bobPubkey],
      event: { ...signedB, content: 'tampered-b' },
      _getIykcProofs: noContentKeys
    })
    const errors = []

    subscribe({
      receiverSigner: bob,
      privateChannelSigner: alice,
      receiverPubkey: bobPubkey,
      relays: ['wss://relay.example'],
      ignoredGroupMaxEntries: 1,
      onError: err => errors.push(err),
      _getIykcProofs: noContentKeys
    })

    await handlers.onevent(wrappedA)
    await handlers.onevent(wrappedA)
    await handlers.onevent(wrappedB)
    await handlers.onevent(wrappedA)

    assert.equal(errors.length, 3)
    assert.ok(errors.every(err => err.message === 'INVALID_SIGNED_INNER_EVENT'))
  } finally {
    pool.subscribeMany = originalSubscribeMany
  }
})

test('received chunk store purges stale incomplete groups', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1000
  Date.now = () => now

  try {
    const store = createReceivedChunkStore({
      prefix: 'test:received-chunks',
      indexedDB,
      ttlMs: 5
    })
    const meta = await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'router',
      index: 1,
      total: 2,
      contentBytes: encoder.encode('abc')
    })

    assert.equal((await store.status(meta)).received, 1)

    now += 6
    await store.cleanupStale()

    assert.deepEqual(await store.status('channel:router'), { received: 0, missing: [] })
  } finally {
    Date.now = originalNow
  }
})

test('received chunk store does not revive an expired partial group', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1000
  Date.now = () => now

  try {
    const store = createReceivedChunkStore({
      prefix: 'test:received-chunks:expired-arrival',
      indexedDB,
      ttlMs: 5
    })
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'router',
      index: 0,
      total: 2,
      contentBytes: encoder.encode('old')
    })

    now += 6
    const meta = await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'router',
      index: 1,
      total: 2,
      contentBytes: encoder.encode('new')
    })

    assert.deepEqual(await store.status(meta), { received: 1, missing: [0] })
  } finally {
    Date.now = originalNow
  }
})

test('received chunk groups retain the TTL chosen by their original caller', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1000
  Date.now = () => now

  try {
    const firstStore = createReceivedChunkStore({
      prefix: 'test:received-chunks:caller-ttl',
      indexedDB,
      ttlMs: 100
    })
    await firstStore.put({
      channelPubkey: 'channel',
      routerPubkey: 'router',
      index: 0,
      total: 2,
      contentBytes: encoder.encode('kept')
    })
    firstStore.close()

    const secondStore = createReceivedChunkStore({
      prefix: 'test:received-chunks:caller-ttl',
      indexedDB,
      ttlMs: 5
    })
    now += 6
    await secondStore.cleanupStale(now)
    assert.equal((await secondStore.status('channel:router')).received, 1)

    now += 100
    await secondStore.cleanupStale(now)
    assert.deepEqual(await secondStore.status('channel:router'), { received: 0, missing: [] })
    secondStore.close()
  } finally {
    Date.now = originalNow
  }
})

test('received chunk groups use per-group TTL overrides without changing existing groups', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1000
  Date.now = () => now

  try {
    const store = createReceivedChunkStore({
      prefix: 'test:received-chunks:per-group-ttl',
      indexedDB,
      ttlMs: 1000
    })
    await store.put({
      channelPubkey: 'short',
      routerPubkey: 'router',
      index: 0,
      total: 2,
      contentBytes: encoder.encode('short'),
      ttlMs: 5
    })
    await store.put({
      channelPubkey: 'long',
      routerPubkey: 'router',
      index: 0,
      total: 3,
      contentBytes: encoder.encode('long'),
      ttlMs: 100
    })

    now += 6
    await store.put({
      channelPubkey: 'long',
      routerPubkey: 'router',
      index: 1,
      total: 3,
      contentBytes: encoder.encode('still long'),
      ttlMs: 5
    })
    await store.cleanupStale(now)
    assert.deepEqual(await store.status('short:router'), { received: 0, missing: [] })
    assert.equal((await store.status('long:router')).received, 2)

    now += 100
    await store.cleanupStale(now)
    assert.deepEqual(await store.status('long:router'), { received: 0, missing: [] })
    store.close()
  } finally {
    Date.now = originalNow
  }
})

test('received chunk store evicts the least-recently-used group atomically', async () => {
  const originalNow = Date.now
  const indexedDB = new IDBFactory()
  let now = 1000
  Date.now = () => now

  try {
    const store = createReceivedChunkStore({
      prefix: 'test:received-chunks:lru',
      indexedDB,
      maxBytes: 6
    })
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'recent',
      index: 0,
      total: 1,
      contentBytes: new Uint8Array([1, 2])
    })
    now++
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'old',
      index: 0,
      total: 2,
      contentBytes: new Uint8Array([3])
    })
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'old',
      index: 1,
      total: 2,
      contentBytes: new Uint8Array([4])
    })

    // A duplicate does not consume bytes, but does make the group recent.
    now++
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'recent',
      index: 0,
      total: 1,
      contentBytes: new Uint8Array([1, 2])
    })
    now++
    await store.put({
      channelPubkey: 'channel',
      routerPubkey: 'incoming',
      index: 0,
      total: 1,
      contentBytes: new Uint8Array([5, 6, 7, 8])
    })

    assert.deepEqual(await store.status('channel:old'), { received: 0, missing: [] })
    assert.equal((await store.status('channel:recent')).received, 1)
    assert.equal((await store.status('channel:incoming')).received, 1)
  } finally {
    Date.now = originalNow
  }
})

test('received chunk store rejects a group that cannot fit without evicting unrelated data', async () => {
  const store = createReceivedChunkStore({
    prefix: 'test:received-chunks:too-large',
    indexedDB: new IDBFactory(),
    maxBytes: 3
  })
  await store.put({
    channelPubkey: 'channel',
    routerPubkey: 'kept',
    index: 0,
    total: 1,
    contentBytes: new Uint8Array([1, 2])
  })

  await assert.rejects(store.put({
    channelPubkey: 'channel',
    routerPubkey: 'too-large',
    index: 0,
    total: 1,
    contentBytes: new Uint8Array([3, 4, 5, 6])
  }), /RECEIVED_CHUNK_GROUP_TOO_LARGE/)

  assert.equal((await store.status('channel:kept')).received, 1)
  assert.deepEqual(await store.status('channel:too-large'), { received: 0, missing: [] })
})

test('received chunk store resumes incremental parsing after reload', async () => {
  const indexedDB = new IDBFactory()
  const line = `${JSON.stringify(['alice', 'ciphertext'])}\n`
  const first = line.slice(0, 12)
  const second = line.slice(12)
  const firstStore = createReceivedChunkStore({
    prefix: 'test:received-chunks:reload',
    indexedDB
  })
  const lines = []

  await firstStore.put({
    channelPubkey: 'channel',
    routerPubkey: 'router',
    index: 0,
    total: 2,
    contentBytes: encoder.encode(first)
  })

  const firstDrain = await firstStore.drainAvailable('channel:router', { onLine: line => lines.push(line) })

  assert.equal(firstDrain.complete, false)
  assert.equal(firstDrain.meta.nextIndex, 1)
  assert.equal(firstDrain.meta.carry, first)
  assert.deepEqual(lines, [])

  const secondStore = createReceivedChunkStore({
    prefix: 'test:received-chunks:reload',
    indexedDB
  })
  await secondStore.put({
    channelPubkey: 'channel',
    routerPubkey: 'router',
    index: 1,
    total: 2,
    contentBytes: encoder.encode(second)
  })

  const secondDrain = await secondStore.drainAvailable('channel:router', { onLine: line => lines.push(line) })

  assert.equal(secondDrain.complete, true)
  assert.deepEqual(lines, [line.trim()])
})
