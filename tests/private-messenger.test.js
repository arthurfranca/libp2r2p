import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { ASK_KIND, REPLY_KIND, TELL_KIND } from '../private-message/index.js'
import {
  createEventReplyPacker,
  createMissingMessageReplyPacker,
  MISSING_MESSAGES_ASK_CODE,
  MISSING_MESSAGES_REPLY_CODE,
  NYM_CARRIER_SEED_RECORD_TYPE,
  PrivateMessenger,
  ROUTER_SEED_RECORD_TYPE,
  SEEDER_PRESENCE_CODE
} from '../private-messenger/index.js'
import { TEMPORARY_STORAGE_KEYS_KEY } from '../temporary-storage/index.js'
import { createChannelStateStore } from '../private-messenger/services/channel-state.js'

const data = new Map()
const sessionData = new Map()
globalThis.localStorage = {
  clear: () => data.clear(),
  getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
  removeItem: key => { data.delete(String(key)) },
  setItem: (key, value) => { data.set(String(key), String(value)) }
}
globalThis.sessionStorage = {
  clear: () => sessionData.clear(),
  getItem: key => sessionData.has(String(key)) ? sessionData.get(String(key)) : null,
  removeItem: key => { sessionData.delete(String(key)) },
  setItem: (key, value) => { sessionData.set(String(key), String(value)) }
}

function resetIndexedDb () {
  globalThis.indexedDB = new IDBFactory()
  globalThis.IDBKeyRange = IDBKeyRange
}

resetIndexedDb()

afterEach(() => {
  globalThis.localStorage.clear()
  globalThis.sessionStorage.clear()
  resetIndexedDb()
})

function signer (pubkey) {
  return {
    getPublicKey: () => pubkey,
    withSharedKey: () => ({})
  }
}

async function seedMessengerState (channels) {
  const state = await createChannelStateStore({
    prefix: 'libp2r2p:private-messenger:user',
    indexedDB: globalThis.indexedDB
  })
  await state.replace(channels)
}

function jsonlContent (...rows) {
  return Buffer.from(`${rows.join('\n')}\n`).toString('base64')
}

function payloadRow (value = 'payload-ciphertext') {
  return JSON.stringify([value])
}

function fakePrivateMessage () {
  const watchCalls = []
  const stopped = []
  const sent = []
  const cleared = []
  return {
    watchCalls,
    stopped,
    sent,
    cleared,
    ASK_KIND,
    REPLY_KIND,
    TELL_KIND,
    watch: async options => {
      watchCalls.push(options)
      return () => stopped.push(options.channels[0])
    },
    ask: async options => {
      sent.push({ method: 'ask', options })
      return { question: { id: 'question-id', kind: ASK_KIND, pubkey: 'user' }, delivery: { reports: [] } }
    },
    reply: async options => {
      sent.push({ method: 'reply', options })
      return { reply: { id: 'reply-id', kind: REPLY_KIND }, delivery: { reports: [] } }
    },
    tell: async options => {
      sent.push({ method: 'tell', options })
      return { tell: { id: 'tell-id', kind: TELL_KIND }, delivery: { reports: [] } }
    },
    yell: async options => {
      sent.push({ method: 'yell', options })
      return { yell: { id: 'yell-id', kind: TELL_KIND }, delivery: { reports: [] } }
    },
    broadcastRumor: async options => {
      sent.push({ method: 'broadcastRumor', options })
      return { rumor: { id: 'raw-id', kind: 9001 }, delivery: { reports: [] } }
    },
    broadcastEvent: async options => {
      sent.push({ method: 'broadcastEvent', options })
      return { event: options.event, delivery: { reports: [] } }
    },
    broadcastNymRumor: async options => {
      sent.push({ method: 'broadcastNymRumor', options })
      return { rumor: { id: 'nym-raw-id', kind: 9003, pubkey: options.nymSigner.getPublicKey() }, delivery: { reports: [] } }
    },
    broadcastNymEvent: async options => {
      sent.push({ method: 'broadcastNymEvent', options })
      return { event: options.event, delivery: { reports: [] } }
    },
    unwatch: channels => stopped.push(channels),
    clearChannelState: channel => cleared.push(channel)
  }
}

test('private messenger maintenance uses session storage by default and configured storage on init', async () => {
  globalThis.sessionStorage.setItem('tmp.session', 'encrypted')
  globalThis.sessionStorage.setItem(TEMPORARY_STORAGE_KEYS_KEY, JSON.stringify(['tmp.session']))
  globalThis.localStorage.setItem('permanent', 'keep')

  await PrivateMessenger.maintainStorage({ indexedDB: globalThis.indexedDB })

  assert.equal(globalThis.sessionStorage.getItem('tmp.session'), null)
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  assert.equal(globalThis.localStorage.getItem('permanent'), 'keep')

  globalThis.localStorage.setItem('tmp.local', 'encrypted')
  globalThis.localStorage.setItem(TEMPORARY_STORAGE_KEYS_KEY, JSON.stringify(['tmp.local']))
  await PrivateMessenger.maintainStorage({
    indexedDB: globalThis.indexedDB,
    temporaryStorageArea: globalThis.localStorage
  })

  assert.equal(globalThis.localStorage.getItem('tmp.local'), null)
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)

  globalThis.localStorage.setItem('tmp.local', 'encrypted')
  globalThis.localStorage.setItem(TEMPORARY_STORAGE_KEYS_KEY, JSON.stringify(['tmp.local']))
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    temporaryStorageArea: globalThis.localStorage
  }).init({
    userSigner: signer('user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(globalThis.localStorage.getItem('tmp.local'), null)
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  await messenger.close()
})

test('private messenger defaults to larger bounded IndexedDB queues', () => {
  const messenger = new PrivateMessenger({ _privateMessage: fakePrivateMessage() })

  assert.equal(messenger.messageQueueMaxBytes, 16 * 1024 * 1024)
  assert.equal(messenger.seedQueueMaxBytes, 64 * 1024 * 1024)
})

test('private messenger persists queued messages in IndexedDB across instances', async () => {
  const indexedDB = new IDBFactory()
  const firstPrivateMessage = fakePrivateMessage()
  const first = await new PrivateMessenger({
    _privateMessage: firstPrivateMessage,
    _indexedDB: indexedDB
  }).init({
    userSigner: signer('durable-user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await firstPrivateMessage.watchCalls[0].onTell({
    event: { id: 'durable-id', kind: TELL_KIND, pubkey: 'alice', created_at: 10, tags: [['r', 'durable-user']], content: 'hi' },
    outer: { id: 'durable-outer', created_at: 10 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' }
  })
  await first.close()

  const second = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  }).init({
    userSigner: signer('durable-user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal((await second.nextMessage()).event.id, 'durable-id')
  assert.equal(await second.nextMessage(), null)
})

test('private messenger persists per-channel recovery state in IndexedDB', async () => {
  const indexedDB = new IDBFactory()
  const first = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  }).init({
    userSigner: signer('state-user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })
  first.updateChannelState('channel', { lastSeenAt: 123 })
  await first.flushStateWrites()
  await first.close()

  const second = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _indexedDB: indexedDB
  }).init({
    userSigner: signer('state-user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(second.readState().channels.channel.lastSeenAt, 123)
})

test('private messenger leaves legacy localStorage queue records untouched', async () => {
  const prefix = 'libp2r2p:private-messenger:legacy-user'
  const stateKey = `${prefix}:queue`
  const itemKey = `${prefix}:queue:item:0`
  const seedStateKey = `${prefix}:seeds:queue`
  const seedItemKey = `${prefix}:seeds:queue:item:0`
  const oldState = JSON.stringify({ head: 0, tail: 1, usedBytes: 42 })
  const oldItem = JSON.stringify({ id: 0, value: 'manual-cleanup' })
  globalThis.localStorage.setItem(stateKey, oldState)
  globalThis.localStorage.setItem(itemKey, oldItem)
  globalThis.localStorage.setItem(seedStateKey, oldState)
  globalThis.localStorage.setItem(seedItemKey, oldItem)

  await new PrivateMessenger({ _privateMessage: fakePrivateMessage() }).init({
    userSigner: signer('legacy-user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(globalThis.localStorage.getItem(stateKey), oldState)
  assert.equal(globalThis.localStorage.getItem(itemKey), oldItem)
  assert.equal(globalThis.localStorage.getItem(seedStateKey), oldState)
  assert.equal(globalThis.localStorage.getItem(seedItemKey), oldItem)
})

test('private messenger rejects initialization when IndexedDB is unavailable', async () => {
  await assert.rejects(
    new PrivateMessenger({ _privateMessage: fakePrivateMessage(), _indexedDB: null }).init({
      userSigner: signer('no-idb-user'),
      channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
    }),
    /IDB_UNAVAILABLE/
  )
})

function fakeRelayListUpdates () {
  const subscriptions = []
  return {
    subscriptions,
    subscribe: (pubkeys, options) => {
      const subscription = {
        pubkeys,
        options,
        closed: false,
        emit: update => Promise.resolve(options.onChange?.(update))
      }
      subscriptions.push(subscription)
      return () => { subscription.closed = true }
    }
  }
}

test('private messenger watches channels and queues received leecher rumors', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(pm.watchCalls.length, 1)
  assert.deepEqual(pm.watchCalls[0].channels, ['channel'])
  assert.deepEqual(pm.watchCalls[0].relays, ['wss://relay.example'])
  assert.equal(pm.watchCalls[0].mode, 'leecher')
  assert.equal(pm.watchCalls[0].receivedChunkTtlMs, 7 * 24 * 60 * 60 * 1000)

  pm.watchCalls[0].onTell({
    event: { id: 'tell-id', kind: TELL_KIND, pubkey: 'alice', created_at: 10, tags: [['r', 'user']], content: 'hi' },
    outer: { id: 'outer-id', created_at: 11 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    tell: { id: 'tell-id' }
  })

  const item = (await messenger.nextMessage())
  assert.equal(item.type, 'tell')
  assert.equal(item.channelPubkey, 'channel')
  assert.equal(item.event.id, 'tell-id')
  assert.deepEqual(item.payload, { payload: 'hi' })
  assert.equal(messenger.readState().channels.channel.lastSeenAt, 11)

  pm.watchCalls[0].onReply({
    event: { id: 'reply-id', kind: REPLY_KIND, pubkey: 'alice', created_at: 12, tags: [['q', 'question-id']], content: 'pong' },
    outer: { id: 'outer-reply-id', created_at: 13 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'pong' },
    questionId: 'question-id',
    reply: { id: 'reply-id' }
  })

  const reply = (await messenger.nextMessage())
  assert.equal(reply.type, 'reply')
  assert.equal(reply.question, null)
  assert.equal(reply.questionId, 'question-id')
  assert.equal(reply.event.id, 'reply-id')

  pm.watchCalls[0].onMessage({
    event: { id: 'raw-id', kind: 9001, pubkey: 'alice', created_at: 14, tags: [], content: JSON.stringify(['raw-payload', 'not-a-private-message-code']) },
    outer: { id: 'outer-raw-id', created_at: 15 },
    meta: { channelPubkey: 'channel' },
    payload: ['raw-payload', 'not-a-private-message-code']
  })

  const raw = (await messenger.nextMessage())
  assert.equal(raw.type, 'message')
  assert.equal(raw.event.id, 'raw-id')
  assert.deepEqual(raw.payload, ['raw-payload', 'not-a-private-message-code'])
})

test('private messenger pauses live watches offline, restarts them before durable recovery, and keeps presence running', async () => {
  const originalWindow = globalThis.window
  const originalDateNow = Date.now
  const events = new EventTarget()
  const pm = fakePrivateMessage()
  const order = []
  const clearedIntervals = []
  const originalWatch = pm.watch
  let now = 1_000_000

  globalThis.window = {
    addEventListener: (...args) => events.addEventListener(...args),
    removeEventListener: (...args) => events.removeEventListener(...args)
  }
  Date.now = () => now
  pm.watch = async options => {
    order.push(`watch:${options.channels[0]}`)
    return originalWatch(options)
  }

  let messenger
  try {
    messenger = await new PrivateMessenger({
      _privateMessage: pm,
      _privateChannel: {
        fetch: async () => {
          order.push('recover')
          return []
        }
      },
      _setInterval: () => 'presence-timer',
      _clearInterval: timer => clearedIntervals.push(timer)
    }).init({
      userSigner: signer('user'),
      channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder' }]
    })
    order.length = 0

    events.dispatchEvent(new Event('offline'))
    assert.deepEqual(pm.stopped, ['channel'])
    assert.equal(messenger.stopByChannel.size, 0)
    assert.equal(messenger.presenceTimers.get('channel'), 'presence-timer')
    assert.deepEqual(clearedIntervals, [])
    assert.ok(messenger.readState().channels.channel.openOfflineStart)

    now += 1_000
    events.dispatchEvent(new Event('online'))
    for (let attempt = 0; attempt < 20 && !order.includes('recover'); attempt++) {
      await new Promise(resolve => setImmediate(resolve))
    }
    assert.deepEqual(order, ['watch:channel', 'recover'])

    const message = {
      event: { id: 'offline-duplicate', kind: TELL_KIND, pubkey: 'alice', created_at: 1001, tags: [['r', 'user']], content: 'hi' },
      outer: { id: 'offline-outer', created_at: 1001 },
      meta: { channelPubkey: 'channel' },
      payload: { payload: 'hi' },
      tell: { id: 'offline-duplicate' }
    }
    await pm.watchCalls[0].onTell(message)
    await pm.watchCalls[1].onTell(message)
    assert.equal((await messenger.nextMessage()).event.id, 'offline-duplicate')
    assert.equal(await messenger.nextMessage(), null)
  } finally {
    await messenger?.close()
    Date.now = originalDateNow
    if (originalWindow === undefined) delete globalThis.window
    else globalThis.window = originalWindow
  }
})

test('private messenger forwards watch errors to the configured error handler', async () => {
  const pm = fakePrivateMessage()
  const errors = []
  await new PrivateMessenger({ _privateMessage: pm, onError: err => errors.push(err) }).init({
    userSigner: signer('user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  pm.watchCalls[0].onError(new Error('RECEIVER_DOUBLE_DH_UNSUPPORTED'))

  assert.equal(errors.length, 1)
  assert.equal(errors[0].message, 'RECEIVER_DOUBLE_DH_UNSUPPORTED')
})

test('private messenger queues nym messages without dispatching helper kinds', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  pm.watchCalls[0].onNym({
    event: { id: 'nym-ask-id', kind: ASK_KIND, pubkey: 'nym', created_at: 10, tags: [['r', 'user']], content: 'hi' },
    outer: { id: 'outer-id', created_at: 11 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    nym: { id: 'nym-ask-id' }
  })

  const item = (await messenger.nextMessage())
  assert.equal(item.type, 'nym')
  assert.equal(item.event.kind, ASK_KIND)
  assert.equal(item.event.pubkey, 'nym')
  assert.equal((await messenger.nextMessage()), null)
})

test('private messenger skips duplicate pending app messages by channel type and event id', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })
  const message = {
    event: { id: 'tell-id', kind: TELL_KIND, pubkey: 'alice', created_at: 10, tags: [['r', 'user']], content: 'hi' },
    outer: { id: 'outer-id', created_at: 11 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    tell: { id: 'tell-id' }
  }

  pm.watchCalls[0].onTell(message)
  pm.watchCalls[0].onTell({ ...message, outer: { id: 'outer-duplicate-id', created_at: 12 } })

  const queued = await messenger.nextMessage()
  assert.equal(queued.event.id, 'tell-id')
  assert.equal(Object.hasOwn(queued, '__p2r2pMessageDedupeKey'), false)
  assert.equal(Object.hasOwn(queued, 'id'), false)
  assert.equal((await messenger.nextMessage()), null)
  assert.equal(messenger.readState().channels.channel.lastSeenAt, 12)
})

test('private messenger reports content key usage changes for sent and received messages', async () => {
  const pm = fakePrivateMessage()
  const changes = []
  const messenger = await new PrivateMessenger({ _privateMessage: pm, onContentKeyChange: event => changes.push(event) }).init({
    userSigner: signer('user'),
    contentKeySigner: signer('content'),
    channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })
  const base = {
    channelPubkey: 'channel',
    outer: { id: 'outer-id', created_at: 20 },
    router: { pubkey: 'router-id', created_at: 19 },
    senderPubkey: 'user',
    receiverPubkeys: ['alice']
  }

  pm.watchCalls[0].onContentKeyUsage({
    ...base,
    direction: 'sent',
    keyRole: 'sender',
    receiverPubkey: 'alice',
    contentKeyPubkey: '',
    isBroadcast: false
  })
  pm.watchCalls[0].onContentKeyUsage({
    ...base,
    direction: 'sent',
    keyRole: 'sender',
    receiverPubkey: 'alice',
    contentKeyPubkey: '',
    isBroadcast: false
  })
  pm.watchCalls[0].onContentKeyUsage({
    ...base,
    direction: 'sent',
    keyRole: 'sender',
    receiverPubkey: '',
    receiverPubkeys: ['alice', 'bob'],
    contentKeyPubkey: 'unknown-content',
    isBroadcast: true
  })
  pm.watchCalls[0].onContentKeyUsage({
    ...base,
    direction: 'received',
    keyRole: 'receiver',
    senderPubkey: 'alice',
    receiverPubkey: 'user',
    contentKeyPubkey: 'content',
    isBroadcast: false
  })

  assert.equal(changes.length, 3)
  assert.equal(changes[0].direction, 'sent')
  assert.equal(changes[0].contentKeyStatus, 'none')
  assert.equal(changes[0].counterpartyPubkey, 'alice')
  assert.equal(changes[1].direction, 'sent')
  assert.equal(changes[1].contentKeyStatus, 'unknown')
  assert.equal(changes[1].previousContentKeyPubkey, '')
  assert.equal(changes[1].isBroadcast, true)
  assert.deepEqual(changes[1].receiverPubkeys, ['alice', 'bob'])
  assert.equal(changes[2].direction, 'received')
  assert.equal(changes[2].contentKeyStatus, 'known')
  assert.equal(changes[2].counterpartyPubkey, 'alice')
  assert.equal(messenger.readState().channels.channel.contentKeyUsage.sent.contentKeyPubkey, 'unknown-content')
  assert.equal(messenger.readState().channels.channel.contentKeyUsage.received.contentKeyPubkey, 'content')
})

test('private messenger delegates send helpers with scoped signers and relays', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    temporaryStorageArea: globalThis.localStorage
  }).init({
    userSigner: signer('user'),
    contentKeySigner: signer('content'),
    nymSigner: signer('global-nym'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await messenger.ask({ receiverPubkey: 'alice', payload: 'ping' })
  await messenger.reply({ question: { id: 'q', pubkey: 'alice' }, payload: 'pong' })
  await messenger.tell({ receiverPubkey: 'alice', payload: 'note' })
  await messenger.yell({ receiverPubkeys: ['alice', 'bob'], payload: 'news' })
  await messenger.broadcastRumor({ receiverPubkeys: ['alice', 'bob'], rumor: { kind: 9001, created_at: 1, tags: [], content: 'raw' } })
  await messenger.broadcastEvent({ receiverPubkeys: ['alice', 'bob'], event: { id: 'signed-id', kind: 9002, pubkey: 'author', created_at: 2, tags: [], content: 'signed', sig: 'sig' } })
  await messenger.broadcastNymRumor({ rumor: { kind: 9003, created_at: 3, tags: [], content: 'nym raw' } })
  await messenger.broadcastNymEvent({ event: { id: 'nym-signed-id', kind: 9004, pubkey: 'author', created_at: 4, tags: [], content: 'nym signed', sig: 'sig' } })

  assert.deepEqual(pm.sent.map(s => s.method), ['ask', 'reply', 'tell', 'yell', 'broadcastRumor', 'broadcastEvent', 'broadcastNymRumor', 'broadcastNymEvent'])
  for (const sent of pm.sent.slice(0, 6)) {
    assert.equal(sent.options.senderSigner.getPublicKey(), 'user')
    assert.equal(sent.options.imkcSigner.getPublicKey(), 'content')
    assert.equal(sent.options.privateChannelSigner.getPublicKey(), 'channel')
    assert.deepEqual(sent.options.relays, ['wss://relay.example'])
    assert.equal(sent.options.expirationSeconds, 7 * 24 * 60 * 60)
    assert.equal(sent.options.temporaryStorageArea, globalThis.localStorage)
    assert.equal(sent.options.autoDeletionCapability, true)
  }
  assert.equal(pm.sent[5].options.event.id, 'signed-id')
  assert.equal(pm.sent[6].options.nymSigner.getPublicKey(), 'global-nym')
  assert.equal(pm.sent[6].options.privateChannelSigner.getPublicKey(), 'channel')
  assert.deepEqual(pm.sent[6].options.relays, ['wss://relay.example'])
  assert.equal(pm.sent[6].options.expirationSeconds, 7 * 24 * 60 * 60)
  assert.equal(pm.sent[6].options.autoDeletionCapability, true)
  assert.equal(pm.sent[7].options.nymSigner.getPublicKey(), 'global-nym')
  assert.equal(pm.sent[7].options.event.pubkey, 'author')
  assert.equal(pm.sent[7].options.autoDeletionCapability, true)
})

test('private messenger configures automatic deletion capabilities and forwards caller pubkeys', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, autoDeletionCapability: true }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'inherited', signer: signer('inherited'), relays: ['wss://relay.example'] },
      { pubkey: 'disabled', signer: signer('disabled'), relays: ['wss://relay.example'], autoDeletionCapability: false }
    ]
  })

  await messenger.tell({ channelPubkey: 'inherited', receiverPubkey: 'alice', payload: 'default' })
  await messenger.tell({ channelPubkey: 'disabled', receiverPubkey: 'alice', payload: 'disabled' })
  await messenger.tell({ channelPubkey: 'disabled', receiverPubkey: 'alice', payload: 'caller-managed', deletionPubkey: 'a'.repeat(64) })

  assert.equal(pm.sent[0].options.autoDeletionCapability, true)
  assert.equal(pm.sent[1].options.autoDeletionCapability, false)
  assert.equal(pm.sent[2].options.autoDeletionCapability, false)
  assert.equal(pm.sent[2].options.deletionPubkey, 'a'.repeat(64))

  const enabledPm = fakePrivateMessage()
  const enabledMessenger = await new PrivateMessenger({ _privateMessage: enabledPm, autoDeletionCapability: false }).init({
    userSigner: signer('other-user'),
    channels: [{ pubkey: 'enabled', signer: signer('enabled'), relays: ['wss://relay.example'], autoDeletionCapability: true }]
  })
  await enabledMessenger.tell({ receiverPubkey: 'alice', payload: 'enabled' })
  assert.equal(enabledPm.sent[0].options.autoDeletionCapability, true)

  await messenger.tell({
    channelPubkey: 'inherited',
    receiverPubkey: 'alice',
    payload: 'ignored fields',
    deletionSeckey: 'b'.repeat(64),
    autoDeletionCapability: false
  })
  assert.equal(pm.sent[3].options.deletionPubkey, undefined)
  assert.equal(pm.sent[3].options.autoDeletionCapability, true)
})

test('private messenger update accepts only same-user replacement signers', async () => {
  const pm = fakePrivateMessage()
  const originalUser = signer('user')
  const replacementUser = signer('user')
  const otherUser = signer('other-user')
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: originalUser,
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await messenger.update({ userSigner: replacementUser })

  assert.equal(messenger.userSigner, replacementUser)
  assert.equal(messenger.userPubkey, 'user')
  await assert.rejects(
    () => messenger.update({ userSigner: otherUser }),
    /USER_SIGNER_MISMATCH/
  )
  assert.equal(messenger.userSigner, replacementUser)
  assert.equal(messenger.userPubkey, 'user')
})

test('private messenger falls back to recipient read relays when no relay set is configured', async () => {
  const pm = fakePrivateMessage()
  const relayLookups = []
  const readRelays = pubkey => [
    `wss://${pubkey}.read-one.example`,
    `wss://${pubkey}.read-two.example`,
    `wss://${pubkey}.read-three.example`
  ]
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _getRelaysByPubkey: async pubkeys => {
      relayLookups.push(pubkeys)
      return Object.fromEntries(pubkeys.map(pubkey => [pubkey, {
        read: readRelays(pubkey),
        write: [`wss://${pubkey}.write.example`]
      }]))
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel') }]
  })

  assert.deepEqual(pm.watchCalls[0].relays, readRelays('user'))

  await messenger.tell({ receiverPubkey: 'alice', payload: 'note' })
  await messenger.broadcastNymRumor({
    receiverPubkeys: ['bob'],
    nymSigner: signer('nym'),
    rumor: { kind: 9001, created_at: 1, tags: [], content: 'nym rumor' }
  })
  await messenger.broadcastNymEvent({
    receiverPubkeys: ['carol'],
    nymSigner: signer('nym'),
    event: { id: 'nym-signed-id', kind: 9002, pubkey: 'author', created_at: 2, tags: [], content: 'nym event', sig: 'sig' }
  })

  assert.deepEqual(relayLookups, [['user'], ['alice'], ['bob'], ['carol']])
  assert.equal(pm.sent[0].options.relays, undefined)
  assert.deepEqual([...pm.sent[0].options.relayToReceivers.entries()], [
    ['wss://alice.read-one.example', ['alice']],
    ['wss://alice.read-two.example', ['alice']]
  ])
  assert.equal(pm.sent[1].options.relays, undefined)
  assert.deepEqual([...pm.sent[1].options.relayToReceivers.entries()], [
    ['wss://bob.read-one.example', ['bob']],
    ['wss://bob.read-two.example', ['bob']]
  ])
  assert.equal(Object.prototype.hasOwnProperty.call(pm.sent[1].options, 'receiverPubkeys'), false)
  assert.equal(pm.sent[2].options.relays, undefined)
  assert.deepEqual([...pm.sent[2].options.relayToReceivers.entries()], [
    ['wss://carol.read-one.example', ['carol']],
    ['wss://carol.read-two.example', ['carol']]
  ])
  assert.equal(Object.prototype.hasOwnProperty.call(pm.sent[2].options, 'receiverPubkeys'), false)
})

test('private messenger reload-gap fetch uses all local read relays when channel relays are absent', async () => {
  const pm = fakePrivateMessage()
  const fetches = []
  let scheduled = null
  const now = Math.floor(Date.now() / 1000)
  const userReadRelays = [
    'wss://user.read-one.example',
    'wss://user.read-two.example',
    'wss://user.read-three.example'
  ]
  await seedMessengerState({
    channel: { lastSeenAt: now - 10, lastWatchedAt: now - 10 }
  })

  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      fetch: async options => {
        fetches.push(options)
        return []
      }
    },
    _getRelaysByPubkey: async pubkeys => Object.fromEntries(pubkeys.map(pubkey => [pubkey, {
      read: pubkey === 'user' ? userReadRelays : [`wss://${pubkey}.read.example`],
      write: []
    }])),
    _setTimeout: fn => { scheduled = fn }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel') }]
  })

  assert.deepEqual(pm.watchCalls[0].relays, userReadRelays)

  await scheduled()

  assert.deepEqual(fetches[0].relays, userReadRelays)
  assert.equal((await messenger.nextMessage()), null)
})

test('private messenger refreshes NIP-65-derived watch relays from relay-list updates', async () => {
  const pm = fakePrivateMessage()
  const relayUpdates = fakeRelayListUpdates()
  const fetches = []
  const now = Math.floor(Date.now() / 1000)
  let userReadRelays = ['wss://user.old-one.example', 'wss://user.old-two.example']
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      fetch: async options => {
        fetches.push(options)
        options.onEvent({
          id: 'missed-id',
          kind: TELL_KIND,
          pubkey: 'alice',
          created_at: now - 5,
          tags: [['r', 'user']],
          content: 'missed'
        }, { id: 'outer-id', created_at: now - 5 }, { channelPubkey: 'derived' })
        return []
      }
    },
    _getRelaysByPubkey: async pubkeys => Object.fromEntries(pubkeys.map(pubkey => [pubkey, {
      read: pubkey === 'user' ? userReadRelays : [`wss://${pubkey}.read.example`],
      write: []
    }])),
    _subscribeRelayListUpdates: relayUpdates.subscribe
  }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'derived', signer: signer('derived') },
      { pubkey: 'explicit', signer: signer('explicit'), relays: ['wss://explicit.example'] }
    ]
  })
  messenger.updateChannelState('derived', { lastSeenAt: now - 20 })
  messenger.updateChannelState('explicit', { lastSeenAt: now - 20 })

  assert.equal(relayUpdates.subscriptions.length, 1)
  assert.deepEqual(relayUpdates.subscriptions[0].pubkeys, ['user'])
  assert.equal(relayUpdates.subscriptions[0].options.relayType, 'read')
  assert.deepEqual(pm.watchCalls[0].channels, ['derived'])
  assert.deepEqual(pm.watchCalls[0].relays, ['wss://user.old-one.example', 'wss://user.old-two.example'])
  assert.deepEqual(pm.watchCalls[1].channels, ['explicit'])
  assert.deepEqual(pm.watchCalls[1].relays, ['wss://explicit.example'])

  userReadRelays = ['wss://user.old-two.example', 'wss://user.new.example']
  await relayUpdates.subscriptions[0].emit({ pubkey: 'user' })

  assert.equal(pm.watchCalls.length, 3)
  assert.deepEqual(pm.watchCalls[2].channels, ['derived'])
  assert.deepEqual(pm.watchCalls[2].relays, ['wss://user.old-two.example', 'wss://user.new.example'])
  assert.deepEqual(pm.stopped, [])
  assert.equal(fetches.length, 1)
  assert.deepEqual(fetches[0].privateChannelPubkeys, ['derived'])
  assert.deepEqual(fetches[0].relays, ['wss://user.old-two.example', 'wss://user.new.example'])
  assert.ok(fetches[0].since <= now - 20)
  assert.ok(fetches[0].until >= now)
  assert.equal((await messenger.nextMessage()).event.id, 'missed-id')
  assert.deepEqual(messenger.readState().channels.derived.relays, ['wss://user.old-two.example', 'wss://user.new.example'])
  assert.deepEqual(messenger.readState().channels.explicit.relays, ['wss://explicit.example'])
})

test('private messenger does not subscribe to relay-list updates for explicit-only channels', async () => {
  const pm = fakePrivateMessage()
  const relayUpdates = fakeRelayListUpdates()
  await new PrivateMessenger({
    _privateMessage: pm,
    _subscribeRelayListUpdates: relayUpdates.subscribe
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'explicit', signer: signer('explicit'), relays: ['wss://explicit.example'] }]
  })

  assert.equal(relayUpdates.subscriptions.length, 0)
  assert.deepEqual(pm.watchCalls[0].relays, ['wss://explicit.example'])
})

test('private messenger prefers explicit relay receiver maps over channel relays', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://channel.example'] }]
  })
  const relayToReceivers = new Map([
    ['wss://alice.example', ['alice']],
    ['wss://bob.example', ['bob']]
  ])

  await messenger.broadcastRumor({
    receiverPubkeys: ['alice', 'bob'],
    relayToReceivers,
    rumor: { kind: 9001, created_at: 1, tags: [], content: 'raw' }
  })

  assert.equal(pm.sent[0].options.relays, undefined)
  assert.equal(pm.sent[0].options.relayToReceivers, relayToReceivers)
})

test('private messenger uses channel sendRelays after per-call routing overrides', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{
      pubkey: 'channel',
      signer: signer('channel'),
      relays: ['wss://watch-one.example', 'wss://watch-two.example', 'wss://watch-three.example'],
      sendRelays: ['wss://send-one.example', 'wss://send-two.example']
    }]
  })
  const relayToReceivers = new Map([['wss://mapped.example', ['alice']]])

  await messenger.tell({ receiverPubkey: 'alice', payload: 'default send relays' })
  await messenger.tell({ receiverPubkey: 'alice', relays: ['wss://per-call.example'], payload: 'per call relays' })
  await messenger.tell({ receiverPubkey: 'alice', relayToReceivers, payload: 'mapped relays' })

  assert.deepEqual(pm.watchCalls[0].relays, ['wss://watch-one.example', 'wss://watch-two.example', 'wss://watch-three.example'])
  assert.deepEqual(pm.sent[0].options.relays, ['wss://send-one.example', 'wss://send-two.example'])
  assert.deepEqual(pm.sent[1].options.relays, ['wss://per-call.example'])
  assert.equal(pm.sent[2].options.relays, undefined)
  assert.equal(pm.sent[2].options.relayToReceivers, relayToReceivers)
})

test('private messenger mirrors routed and nym sends to recovery seeder relays', async () => {
  const pm = fakePrivateMessage()
  const relayLookups = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _getRelaysByPubkey: async pubkeys => {
      relayLookups.push(pubkeys)
      return Object.fromEntries(pubkeys.map(pubkey => [pubkey, {
        read: [`wss://${pubkey}.read.example`],
        write: [`wss://${pubkey}.write.example`]
      }]))
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), seeders: ['seed1'] }]
  })

  await messenger.tell({ receiverPubkey: 'alice', payload: 'note' })
  await messenger.broadcastNymRumor({
    receiverPubkeys: ['bob'],
    nymSigner: signer('nym'),
    rumor: { kind: 9001, created_at: 1, tags: [], content: 'nym rumor' }
  })
  await messenger.broadcastRumor({
    receiverPubkeys: ['carol'],
    relayToReceivers: new Map([['wss://carol.custom.example', ['carol']]]),
    rumor: { kind: 9002, created_at: 2, tags: [], content: 'explicit map' }
  })

  assert.deepEqual(relayLookups, [['user'], ['seed1'], ['alice'], ['seed1'], ['bob'], ['seed1']])
  assert.deepEqual(pm.sent[0].options.recoveryRelays, ['wss://seed1.read.example'])
  assert.deepEqual([...pm.sent[0].options.relayToReceivers.entries()], [['wss://alice.read.example', ['alice']]])
  assert.deepEqual(pm.sent[1].options.recoveryRelays, ['wss://seed1.read.example'])
  assert.deepEqual([...pm.sent[1].options.relayToReceivers.entries()], [['wss://bob.read.example', ['bob']]])
  assert.deepEqual(pm.sent[2].options.recoveryRelays, ['wss://seed1.read.example'])
  assert.deepEqual([...pm.sent[2].options.relayToReceivers.entries()], [['wss://carol.custom.example', ['carol']]])
})

test('private messenger reader-only channels watch and drain but reject sends', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', readerSigner: signer('reader'), relays: ['wss://relay.example'] }]
  })

  assert.equal(pm.watchCalls[0].privateChannelSigner, null)
  assert.equal(pm.watchCalls[0].privateChannelReaderSigner.getPublicKey(), 'reader')

  pm.watchCalls[0].onTell({
    event: { id: 'tell-id', kind: TELL_KIND, pubkey: 'alice', created_at: 10, tags: [['r', 'user']], content: 'hi' },
    outer: { id: 'outer-id', created_at: 11 },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    tell: { id: 'tell-id' }
  })

  assert.equal((await messenger.nextMessage()).event.id, 'tell-id')
  await assert.rejects(
    () => messenger.tell({ channelPubkey: 'channel', receiverPubkey: 'alice', payload: 'note' }),
    /PRIVATE_CHANNEL_WRITER_REQUIRED/
  )
  await assert.rejects(
    () => messenger.broadcastNymRumor({ channelPubkey: 'channel', nymSigner: signer('nym'), rumor: { kind: 1, tags: [], content: 'note' } }),
    /PRIVATE_CHANNEL_WRITER_REQUIRED/
  )
})

test('private messenger resolves nym signers by method channel then global order', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    nymSigner: signer('global-nym'),
    channels: [
      { pubkey: 'global-channel', signer: signer('global-channel'), relays: ['wss://relay.example'] },
      { pubkey: 'channel-nym', signer: signer('channel-nym'), nymSigner: signer('channel-nym-signer'), relays: ['wss://relay.example'] }
    ]
  })

  await messenger.broadcastNymRumor({ channelPubkey: 'global-channel', rumor: { kind: 1, tags: [], content: 'global' } })
  await messenger.broadcastNymRumor({ channelPubkey: 'channel-nym', rumor: { kind: 1, tags: [], content: 'channel' } })
  await messenger.broadcastNymRumor({ channelPubkey: 'channel-nym', nymSigner: signer('method-nym'), rumor: { kind: 1, tags: [], content: 'method' } })

  assert.deepEqual(
    pm.sent.filter(sent => sent.method === 'broadcastNymRumor').map(sent => sent.options.nymSigner.getPublicKey()),
    ['global-nym', 'channel-nym-signer', 'method-nym']
  )
})

test('private messenger writer channels can encrypt to a reader key', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{
      pubkey: 'channel',
      signer: signer('channel'),
      readerSigner: signer('reader'),
      relays: ['wss://relay.example']
    }]
  })

  await messenger.tell({ channelPubkey: 'channel', receiverPubkey: 'alice', payload: 'note' })

  assert.equal(pm.watchCalls[0].privateChannelSigner.getPublicKey(), 'channel')
  assert.equal(pm.watchCalls[0].privateChannelReaderSigner.getPublicKey(), 'reader')
  assert.equal(pm.watchCalls[0].privateChannelReaderPubkey, 'reader')
  assert.equal(pm.sent[0].options.privateChannelSigner.getPublicKey(), 'channel')
  assert.equal(pm.sent[0].options.privateChannelReaderPubkey, 'reader')
})

test('private messenger writer channels can read with only a reader pubkey', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{
      pubkey: 'channel',
      signer: signer('channel'),
      readerPubkey: 'reader',
      relays: ['wss://relay.example']
    }]
  })

  await messenger.tell({ channelPubkey: 'channel', receiverPubkey: 'alice', payload: 'note' })

  assert.equal(pm.watchCalls[0].privateChannelSigner.getPublicKey(), 'channel')
  assert.equal(pm.watchCalls[0].privateChannelReaderSigner.getPublicKey(), 'channel')
  assert.equal(pm.watchCalls[0].privateChannelReaderPubkey, 'reader')
  assert.equal(pm.sent[0].options.privateChannelReaderPubkey, 'reader')
})

test('reader-only channels cannot use recovery seed modes', async () => {
  const pm = fakePrivateMessage()
  await assert.rejects(
    () => new PrivateMessenger({ _privateMessage: pm }).init({
      userSigner: signer('user'),
      channels: [{ pubkey: 'channel', readerSigner: signer('reader'), relays: ['wss://relay.example'], mode: 'seeder' }]
    }),
    /PRIVATE_CHANNEL_WRITER_REQUIRED/
  )
  await assert.rejects(
    () => new PrivateMessenger({ _privateMessage: pm }).init({
      userSigner: signer('user'),
      channels: [{ pubkey: 'channel', readerSigner: signer('reader'), relays: ['wss://relay.example'], mode: 'watchtower' }]
    }),
    /PRIVATE_CHANNEL_WRITER_REQUIRED/
  )
})

test('private messenger debug reports send and enqueue events without payload secrets', async () => {
  const pm = fakePrivateMessage()
  const debugEvents = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    onDebug: event => debugEvents.push(event)
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await messenger.yell({
    receiverPubkeys: ['alice', 'bob'],
    code: 'contentKeys_reply_v1',
    payload: { keys: [{ pubkey: 'pubkey', seckey: 'sent-secret' }] }
  })
  await pm.watchCalls[0].onReply({
    event: { id: 'reply-id', kind: REPLY_KIND, pubkey: 'alice', created_at: 12, tags: [['q', 'question-id']], content: 'pong' },
    outer: { id: 'outer-reply-id', created_at: 13 },
    meta: { channelPubkey: 'channel' },
    payload: { code: 'contentKeys_reply_v1', payload: { keys: [{ pubkey: 'pubkey', seckey: 'received-secret' }] } },
    questionId: 'question-id',
    reply: { id: 'reply-id' }
  })

  const send = debugEvents.find(event => event.action === 'send' && event.method === 'yell')
  const enqueue = debugEvents.find(event => event.action === 'enqueue' && event.type === 'reply')
  assert.ok(debugEvents.some(event => event.action === 'watch'))
  assert.equal(send.code, 'contentKeys_reply_v1')
  assert.deepEqual(send.receiverPubkeys, ['alice', 'bob'])
  assert.equal(send.receiverCount, 2)
  assert.equal(enqueue.code, 'contentKeys_reply_v1')
  assert.equal(enqueue.channelPubkey, 'channel')
  assert.equal(enqueue.senderPubkey, 'alice')
  assert.equal(JSON.stringify(debugEvents).includes('sent-secret'), false)
  assert.equal(JSON.stringify(debugEvents).includes('received-secret'), false)
})

test('private messenger can disable receiver content-key lookup for identity-only traffic', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, useContentKeys: false }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await messenger.tell({ receiverPubkey: 'alice', payload: 'identity only' })

  assert.equal(typeof pm.sent[0].options._getIykcProofs, 'function')
  assert.deepEqual(await pm.sent[0].options._getIykcProofs(['alice']), {})
})

test('clearChannel removes queued items and channel state without clearing other channels', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'one', signer: signer('one'), relays: ['wss://relay.example'] },
      { pubkey: 'two', signer: signer('two'), relays: ['wss://relay.example'] }
    ]
  })
  messenger.queue.enqueue({ type: 'tell', channelPubkey: 'one', event: { id: 'one' } })
  messenger.queue.enqueue({ type: 'tell', channelPubkey: 'two', event: { id: 'two' } })

  await messenger.clearChannel('one')

  const item = (await messenger.nextMessage())
  assert.equal(item.channelPubkey, 'two')
  assert.equal((await messenger.nextMessage()), null)
  assert.equal(messenger.channels.has('one'), false)
  assert.equal(messenger.readState().channels.one, undefined)
  assert.ok(messenger.readState().channels.two)
  assert.deepEqual(pm.cleared, ['one'])
})

test('watch schedules reload-gap recovery and fetches missing channel window', async () => {
  const pm = fakePrivateMessage()
  const fetches = []
  let scheduled = null
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({
    channel: { lastSeenAt: now - 10, lastWatchedAt: now - 10 }
  })
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      fetch: async options => {
        fetches.push(options)
        options.onEvent({
          id: 'ask-id',
          kind: ASK_KIND,
          pubkey: 'alice',
          created_at: now - 5,
          tags: [['r', 'user']],
          content: 'missed'
        }, { id: 'outer-id', created_at: now - 5 }, { channelPubkey: 'channel' })
      }
    },
    _setTimeout: fn => { scheduled = fn }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  await scheduled()

  assert.equal(fetches.length, 1)
  assert.equal(fetches[0].privateChannelPubkeys[0], 'channel')
  assert.ok(fetches[0].since <= now - 10)
  assert.ok(fetches[0].until >= now)
  assert.equal(fetches[0].receivedChunkTtlMs, 7 * 24 * 60 * 60 * 1000)
  assert.equal((await messenger.nextMessage()).event.id, 'ask-id')
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
})

test('reader-only channels fetch reload gaps with the reader signer', async () => {
  const pm = fakePrivateMessage()
  const fetches = []
  let scheduled = null
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({
    channel: { lastSeenAt: now - 10, lastWatchedAt: now - 10 }
  })
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      fetch: async options => {
        fetches.push(options)
        options.onEvent({
          id: 'missed-id',
          kind: TELL_KIND,
          pubkey: 'alice',
          created_at: now - 5,
          tags: [['r', 'user']],
          content: 'missed'
        }, { id: 'outer-id', created_at: now - 5 }, { channelPubkey: 'channel' })
      }
    },
    _setTimeout: fn => { scheduled = fn }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', readerSigner: signer('reader'), relays: ['wss://relay.example'] }]
  })

  await scheduled()

  assert.equal(fetches.length, 1)
  assert.equal(fetches[0].privateChannelSigner, null)
  assert.equal(fetches[0].privateChannelReaderSigner.getPublicKey(), 'reader')
  assert.equal((await messenger.nextMessage()).event.id, 'missed-id')
})

test('seeder channels publish presence immediately and on interval', async () => {
  const pm = fakePrivateMessage()
  const intervals = []
  const cleared = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _setInterval: (fn, ms) => {
      const timer = { fn, ms }
      intervals.push(timer)
      return timer
    },
    _clearInterval: timer => cleared.push(timer)
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder', seeders: ['alice'], autoDeletionCapability: false }]
  })

  assert.equal(pm.sent[0].method, 'yell')
  assert.equal(pm.sent[0].options.code, SEEDER_PRESENCE_CODE)
  assert.deepEqual(pm.sent[0].options.receiverPubkeys, ['alice', 'user'])
  assert.equal(pm.sent[0].options.autoDeletionCapability, false)
  assert.equal(intervals[0].ms, 10 * 60 * 1000)

  await intervals[0].fn()

  assert.equal(pm.sent[1].method, 'yell')
  assert.equal(pm.sent[1].options.code, SEEDER_PRESENCE_CODE)
  assert.equal(pm.sent[1].options.autoDeletionCapability, false)

  await messenger.close()
  assert.deepEqual(cleared, intervals)
})

test('seeder channels store router seeds separately, consume messages, and answer missing-message asks', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('seeder'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder' }]
  })
  const userRow = JSON.stringify(['user', 'ciphertext'])
  const otherRow = JSON.stringify(['other', 'ciphertext'])

  await pm.watchCalls[0].onSeed({
    channelPubkey: 'channel',
    outer: { id: 'outer-id', kind: 3560, pubkey: 'channel', created_at: now },
    router: {
      kind: 26300,
      pubkey: 'router',
      created_at: now,
      tags: [['f', 'alice'], ['c', '0', '1']],
      content: jsonlContent(payloadRow(), userRow, otherRow)
    }
  })
  await pm.watchCalls[0].onSeed({
    channelPubkey: 'channel',
    outer: { id: 'outer-duplicate-id', kind: 3560, pubkey: 'channel', created_at: now + 100 },
    router: {
      kind: 26300,
      pubkey: 'router-duplicate',
      created_at: now + 100,
      tags: [['f', 'alice'], ['c', '0', '1']],
      content: jsonlContent(payloadRow(), userRow, otherRow)
    }
  })

  const storedSeeds = []
  for await (const seed of messenger.seedQueue.storedItems()) storedSeeds.push(seed)
  const routerRows = storedSeeds.filter(seed => seed.recordType === ROUTER_SEED_RECORD_TYPE)
  assert.equal(routerRows.length, 2)
  assert.equal(routerRows.find(seed => seed.receiverPubkey === 'user').firstSeenAt, now)
  assert.equal(routerRows.find(seed => seed.receiverPubkey === 'user').lastSeenAt, now + 100)

  pm.watchCalls[0].onTell({
    event: { id: 'tell-id', kind: TELL_KIND, pubkey: 'alice', created_at: now, tags: [['r', 'seeder']], content: 'hi' },
    outer: { id: 'tell-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    tell: { id: 'tell-id' }
  })

  const item = (await messenger.nextMessage())
  assert.equal(item.type, 'tell')
  assert.equal(item.event.id, 'tell-id')
  assert.equal((await messenger.nextMessage()), null)

  await pm.watchCalls[0].onAsk({
    event: {
      id: 'question-id',
      kind: ASK_KIND,
      pubkey: 'user',
      created_at: now,
      tags: [['r', 'seeder'], ['h', MISSING_MESSAGES_ASK_CODE]],
      content: JSON.stringify({ since: now + 50, until: now + 60 })
    },
    outer: { id: 'ask-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_ASK_CODE, payload: { since: now + 50, until: now + 60 } },
    question: { id: 'question-id' }
  })

  const reply = pm.sent.find(sent => sent.method === 'reply' && sent.options.code === MISSING_MESSAGES_REPLY_CODE)
  assert.equal(reply.options.receiverPubkey, 'user')
  assert.equal(reply.options.payload.isLast, true)
  const records = reply.options.payload.jsonl.trim().split('\n').map(line => JSON.parse(line))
  assert.equal(records.length, 1)
  assert.equal(records[0].recordType, ROUTER_SEED_RECORD_TYPE)
  assert.equal(records[0].router.kind, 26300)
  assert.equal(Buffer.from(records[0].router.content, 'base64').toString(), `${payloadRow()}\n${userRow}\n`)
  assert.deepEqual(records[0].router.tags, [['f', 'alice'], ['c', '0', '1']])
  assert.equal((await messenger.nextMessage()), null)
})

test('router seed rows dedupe by proven inner id without content-key pubkey', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder' }]
  })
  const oldContentRow = JSON.stringify(['user', 'old-ciphertext', 'old-content-key', 'old-proof'])
  const newContentRow = JSON.stringify(['user', 'new-ciphertext', 'new-content-key', 'new-proof'])

  await pm.watchCalls[0].onSeed({
    channelPubkey: 'channel',
    outer: { id: 'outer-id', kind: 3560, pubkey: 'channel', created_at: now },
    router: {
      kind: 26300,
      pubkey: 'router',
      created_at: now,
      tags: [['f', 'alice'], ['c', '0', '1']],
      content: jsonlContent(payloadRow(), oldContentRow, newContentRow)
    },
    innerEventIdsByRowIndex: { 1: 'same-inner-id', 2: 'same-inner-id' }
  })

  const storedSeeds = []
  for await (const seed of messenger.seedQueue.storedItems()) storedSeeds.push(seed)

  assert.equal(storedSeeds.filter(seed => seed.recordType === ROUTER_SEED_RECORD_TYPE).length, 1)
  assert.equal(storedSeeds[0].receiverPubkey, 'user')
  assert.equal(storedSeeds[0].innerEventId, 'same-inner-id')
  assert.equal(storedSeeds[0].iykcPubkey, 'new-content-key')
  assert.equal(storedSeeds[0].row, newContentRow)
})

test('watchtower channels store router seeds without consuming normal messages', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('watchtower'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'watchtower' }]
  })
  const userRow = JSON.stringify(['user', 'ciphertext'])

  assert.equal(pm.watchCalls[0].mode, 'watchtower')

  pm.watchCalls[0].onSeed({
    channelPubkey: 'channel',
    outer: { id: 'outer-id', kind: 3560, pubkey: 'channel', created_at: now },
    router: {
      kind: 26300,
      pubkey: 'router',
      created_at: now,
      tags: [['f', 'alice'], ['c', '0', '1']],
      content: jsonlContent(payloadRow(), userRow)
    }
  })

  pm.watchCalls[0].onTell({
    event: { id: 'tell-id', kind: TELL_KIND, pubkey: 'alice', created_at: now, tags: [['r', 'watchtower']], content: 'hi' },
    outer: { id: 'tell-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { payload: 'hi' },
    tell: { id: 'tell-id' }
  })

  assert.equal((await messenger.nextMessage()), null)

  await pm.watchCalls[0].onAsk({
    event: {
      id: 'question-id',
      kind: ASK_KIND,
      pubkey: 'user',
      created_at: now,
      tags: [['r', 'watchtower'], ['h', MISSING_MESSAGES_ASK_CODE]],
      content: JSON.stringify({ since: now - 5, until: now + 5 })
    },
    outer: { id: 'ask-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_ASK_CODE, payload: { since: now - 5, until: now + 5 } },
    question: { id: 'question-id' }
  })

  const reply = pm.sent.find(sent => sent.method === 'reply' && sent.options.code === MISSING_MESSAGES_REPLY_CODE)
  assert.equal(reply.options.receiverPubkey, 'user')
  assert.equal(reply.options.payload.isLast, true)
  const records = reply.options.payload.jsonl.trim().split('\n').map(line => JSON.parse(line))
  assert.equal(records.length, 1)
  assert.equal(records[0].recordType, ROUTER_SEED_RECORD_TYPE)
  assert.equal(Buffer.from(records[0].router.content, 'base64').toString(), `${payloadRow()}\n${userRow}\n`)
  assert.equal((await messenger.nextMessage()), null)
})

test('missing-message asks without stored seeds do not send empty replies', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('seeder'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder' }]
  })

  await pm.watchCalls[0].onAsk({
    event: {
      id: 'question-id',
      kind: ASK_KIND,
      pubkey: 'user',
      created_at: now,
      tags: [['r', 'seeder'], ['h', MISSING_MESSAGES_ASK_CODE]],
      content: JSON.stringify({ since: now - 5, until: now + 5 })
    },
    outer: { id: 'ask-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_ASK_CODE, payload: { since: now - 5, until: now + 5 } },
    question: { id: 'question-id' }
  })

  assert.equal(pm.sent.some(sent => sent.method === 'reply' && sent.options.code === MISSING_MESSAGES_REPLY_CODE), false)
})

test('recovery asks online seeders for the relay-uncovered left edge', async () => {
  const pm = fakePrivateMessage()
  const fetches = []
  let scheduled = null
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({
    channel: { lastSeenAt: now - 20, lastWatchedAt: now - 20 }
  })
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      fetch: async options => {
        fetches.push(options)
        options.onEvent({
          id: 'relay-id',
          kind: TELL_KIND,
          pubkey: 'alice',
          created_at: now - 5,
          tags: [['r', 'user']],
          content: 'relay'
        }, { id: 'outer-id', created_at: now - 5 }, { channelPubkey: 'channel' })
        return [{ id: 'outer-id', created_at: now - 5 }]
      }
    },
    _setTimeout: fn => { scheduled = fn }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], seeders: ['seeder'] }]
  })

  pm.watchCalls[0].onYell({
    event: { id: 'presence-id', kind: TELL_KIND, pubkey: 'seeder', created_at: now - 2, tags: [['h', SEEDER_PRESENCE_CODE]], content: '{}' },
    outer: { id: 'presence-outer-id', created_at: now - 2 },
    meta: { channelPubkey: 'channel' },
    payload: { code: SEEDER_PRESENCE_CODE, payload: {} },
    yell: { id: 'presence-id' }
  })

  assert.equal((await messenger.nextMessage()), null)

  await scheduled()

  const ask = pm.sent.find(sent => sent.method === 'ask' && sent.options.code === MISSING_MESSAGES_ASK_CODE)
  assert.equal(fetches.length, 1)
  assert.equal(ask.options.receiverPubkey, 'seeder')
  assert.ok(ask.options.payload.since <= now - 20)
  assert.equal(ask.options.payload.until, now - 5)
  assert.equal((await messenger.nextMessage()).event.id, 'relay-id')
})

test('recovery asks all configured seeders but caps discovered seeders', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const configuredSeeders = Array.from({ length: 10 }, (_v, index) => `configured-${index}`)
  const discoveredSeeders = Array.from({ length: 12 }, (_v, index) => `discovered-${index}`)
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'configured', signer: signer('configured'), relays: ['wss://relay.example'], seeders: configuredSeeders },
      { pubkey: 'discovered', signer: signer('discovered'), relays: ['wss://relay.example'] }
    ]
  })

  for (const [index, seeder] of discoveredSeeders.entries()) {
    messenger.markSeederActive('discovered', seeder, { at: now - index })
  }

  await messenger.askSeedersForMissingRange('configured', now - 20, now - 10)
  await messenger.askSeedersForMissingRange('discovered', now - 20, now - 10)

  const configuredAsks = pm.sent.filter(sent => sent.method === 'ask' && sent.options.privateChannelSigner.getPublicKey() === 'configured')
  const discoveredAsks = pm.sent.filter(sent => sent.method === 'ask' && sent.options.privateChannelSigner.getPublicKey() === 'discovered')

  assert.deepEqual(configuredAsks.map(sent => sent.options.receiverPubkey), configuredSeeders)
  assert.deepEqual(discoveredAsks.map(sent => sent.options.receiverPubkey), discoveredSeeders.slice(0, 8))
})

test('missing-message replies ignore raw event rows', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], seeders: ['seeder'] }]
  })
  const jsonl = `${JSON.stringify({
    id: 'missed-id',
    kind: TELL_KIND,
    pubkey: 'alice',
    created_at: 1,
    tags: [['r', 'user']],
    content: 'old'
  })}\n`
  await pm.watchCalls[0].onReply({
    event: { id: 'reply-id', kind: REPLY_KIND, pubkey: 'seeder', created_at: 2, tags: [['q', 'question-id']], content: '' },
    outer: { id: 'reply-outer-id', created_at: 3 },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_REPLY_CODE, payload: { index: 0, isLast: true, jsonl } },
    questionId: 'question-id',
    reply: { id: 'reply-id' }
  })

  assert.equal((await messenger.nextMessage()), null)
})

test('missing-message replies can recover router-only seed records', async () => {
  const pm = fakePrivateMessage()
  let unwrapCall = null
  let encryptedTo = null
  let encryptedKind = null
  let encryptedScope = null
  const channel = {
    ...signer('channel'),
    nip44v3Encrypt: async (pubkey, kind, scope, content) => {
      encryptedTo = pubkey
      encryptedKind = kind
      encryptedScope = scope
      return content
    }
  }
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      unwrapEvent: async options => {
        unwrapCall = options
        return {
          id: 'missed-id',
          kind: TELL_KIND,
          pubkey: 'alice',
          created_at: 1,
          tags: [['r', 'user']],
          content: 'old'
        }
      }
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: channel, readerPubkey: 'reader', relays: ['wss://relay.example'], seeders: ['seeder'] }]
  })
  const userRow = JSON.stringify(['user', 'ciphertext'])
  const jsonl = `${JSON.stringify({
    recordType: ROUTER_SEED_RECORD_TYPE,
    router: {
      kind: 26300,
      pubkey: 'router',
      created_at: 1,
      tags: [['f', 'alice'], ['c', '0', '1']],
      content: jsonlContent(payloadRow(), userRow)
    }
  })}\n`
  await pm.watchCalls[0].onReply({
    event: { id: 'reply-id', kind: REPLY_KIND, pubkey: 'seeder', created_at: 2, tags: [['q', 'question-id']], content: '' },
    outer: { id: 'reply-outer-id', created_at: 3 },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_REPLY_CODE, payload: { index: 0, isLast: true, jsonl } },
    questionId: 'question-id',
    reply: { id: 'reply-id' }
  })

  const syntheticRouter = JSON.parse(Buffer.from(unwrapCall.event.content, 'base64').toString())
  assert.equal(encryptedTo, 'reader')
  assert.equal(encryptedKind, 3560)
  assert.equal(encryptedScope, '')
  assert.equal(unwrapCall.privateChannelReaderPubkey, 'reader')
  assert.equal(syntheticRouter.content, jsonlContent(payloadRow(), userRow))
  assert.deepEqual(syntheticRouter.tags, [['f', 'alice'], ['c', '0', '1']])
  assert.equal((await messenger.nextMessage()).event.id, 'missed-id')
  assert.equal((await messenger.nextMessage()), null)
})

test('nym carrier seeds are replied to and recovered as nym queue items', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const carriers = [
    {
      id: 'carrier-id',
      kind: 26400,
      pubkey: 'nym',
      created_at: now,
      tags: [['id', 'inner-id'], ['c', '0', '1']],
      content: 'payload',
      sig: 'sig'
    }
  ]
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: {
      eventFromNymCarriers: input => {
        assert.deepEqual(input, carriers)
        return { id: 'inner-id', kind: ASK_KIND, pubkey: 'inner-author', created_at: now, tags: [['r', 'user']], content: 'nym ask' }
      }
    }
  }).init({
    userSigner: signer('seeder'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], mode: 'seeder' }]
  })

  pm.watchCalls[0].onSeed({
    recordType: NYM_CARRIER_SEED_RECORD_TYPE,
    channelPubkey: 'channel',
    outer: { id: 'outer-id', kind: 3560, pubkey: 'channel', created_at: now },
    carriers
  })
  pm.watchCalls[0].onSeed({
    recordType: NYM_CARRIER_SEED_RECORD_TYPE,
    channelPubkey: 'channel',
    outer: { id: 'outer-duplicate-id', kind: 3560, pubkey: 'channel', created_at: now },
    carriers
  })

  await pm.watchCalls[0].onAsk({
    event: {
      id: 'question-id',
      kind: ASK_KIND,
      pubkey: 'user',
      created_at: now,
      tags: [['r', 'seeder'], ['h', MISSING_MESSAGES_ASK_CODE]],
      content: JSON.stringify({ since: now - 5, until: now + 5 })
    },
    outer: { id: 'ask-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_ASK_CODE, payload: { since: now - 5, until: now + 5 } },
    question: { id: 'question-id' }
  })

  const reply = pm.sent.find(sent => sent.method === 'reply' && sent.options.code === MISSING_MESSAGES_REPLY_CODE)
  const records = reply.options.payload.jsonl.trim().split('\n').map(line => JSON.parse(line))
  assert.equal(records.length, 1)
  assert.equal(records[0].recordType, NYM_CARRIER_SEED_RECORD_TYPE)
  assert.deepEqual(records[0].carriers, carriers)

  await pm.watchCalls[0].onReply({
    event: { id: 'reply-id', kind: REPLY_KIND, pubkey: 'seeder', created_at: now, tags: [['q', 'question-id']], content: '' },
    outer: { id: 'reply-outer-id', created_at: now },
    meta: { channelPubkey: 'channel' },
    payload: { code: MISSING_MESSAGES_REPLY_CODE, payload: { index: 0, isLast: true, jsonl: reply.options.payload.jsonl } },
    questionId: 'question-id',
    reply: { id: 'reply-id' }
  })

  const item = (await messenger.nextMessage())
  assert.equal(item.type, 'nym')
  assert.equal(item.event.kind, ASK_KIND)
  assert.equal(item.meta.recoveredFromSeeder, 'seeder')
  assert.equal((await messenger.nextMessage()), null)
})

test('missing-message reply packer streams compact seed routers only', async () => {
  const replies = []
  const question = {
    id: 'question-id',
    pubkey: 'user',
    tags: [['h', MISSING_MESSAGES_ASK_CODE]],
    content: JSON.stringify({ since: 5, until: 20 })
  }
  const packer = createMissingMessageReplyPacker({
    messenger: { reply: async options => replies.push(options) },
    channelPubkey: 'channel',
    question,
    eventsPerChunk: 1
  })
  const userRow = JSON.stringify(['user', 'ciphertext'])

  await packer.update({
    id: 'event-id',
    kind: TELL_KIND,
    pubkey: 'alice',
    created_at: 6,
    tags: [['r', 'user']],
    content: 'first'
  })
  await packer.finalize({
    type: 'seed',
    recordType: ROUTER_SEED_RECORD_TYPE,
    channelPubkey: 'channel',
    router: {
      kind: 26300,
      pubkey: 'router',
      created_at: 10,
      tags: [['f', 'sender'], ['c', '0', '1']],
      content: ''
    },
    receiverPubkey: 'user',
    iykcPubkey: '',
    innerEventId: 'seeded-id',
    payloadRow: payloadRow(),
    row: userRow,
    firstSeenAt: 10,
    lastSeenAt: 10
  })

  assert.equal(replies.length, 1)
  assert.equal(replies[0].code, MISSING_MESSAGES_REPLY_CODE)
  assert.equal(replies[0].receiverPubkey, 'user')
  assert.equal(replies[0].payload.since, 5)
  assert.equal(replies[0].payload.until, 20)
  assert.equal(replies[0].payload.isLast, true)
  const lines = replies[0].payload.jsonl.trim().split('\n')
  assert.equal(lines.length, 1)
  const record = JSON.parse(lines[0])
  assert.equal(record.recordType, ROUTER_SEED_RECORD_TYPE)
  assert.equal(record.router.kind, 26300)
  assert.equal(Buffer.from(record.router.content, 'base64').toString(), `${payloadRow()}\n${userRow}\n`)
  assert.deepEqual(record.router.tags, [['f', 'sender'], ['c', '0', '1']])
})

test('missing-message reply packer skips empty replies by default', async () => {
  const replies = []
  const question = {
    id: 'question-id',
    pubkey: 'user',
    tags: [['h', MISSING_MESSAGES_ASK_CODE]],
    content: JSON.stringify({ since: 5, until: 20 })
  }
  const packer = createMissingMessageReplyPacker({
    messenger: { reply: async options => replies.push(options) },
    channelPubkey: 'channel',
    question
  })

  await packer.finalize()

  assert.deepEqual(replies, [])
})

test('event reply packer streams regular event lists', async () => {
  const replies = []
  const question = { id: 'question-id', pubkey: 'peer', content: '' }
  const packer = createEventReplyPacker({
    messenger: { reply: async options => replies.push(options) },
    channelPubkey: 'channel',
    question,
    code: 'eventSync_test',
    payload: { collection: 'local-db' },
    eventsPerChunk: 2
  })

  await packer.update({ id: 'event-1', kind: 1, pubkey: 'alice', created_at: 1, tags: [], content: 'one' })
  await packer.update({ id: 'event-2', kind: 1, pubkey: 'alice', created_at: 2, tags: [], content: 'two' })
  await packer.finalize({ id: 'event-3', kind: 1, pubkey: 'alice', created_at: 3, tags: [], content: 'three' })

  assert.equal(replies.length, 2)
  assert.equal(replies[0].code, 'eventSync_test')
  assert.equal(replies[0].receiverPubkey, 'peer')
  assert.deepEqual(replies[0].payload.collection, 'local-db')
  assert.equal(replies[0].payload.index, 0)
  assert.equal(replies[0].payload.isLast, false)
  assert.deepEqual(replies[0].payload.jsonl.trim().split('\n').map(line => JSON.parse(line).id), ['event-1', 'event-2'])

  assert.equal(replies[1].payload.index, 1)
  assert.equal(replies[1].payload.isLast, true)
  assert.deepEqual(replies[1].payload.jsonl.trim().split('\n').map(line => JSON.parse(line).id), ['event-3'])
})

test('event reply packer can send configured empty replies', async () => {
  const replies = []
  const question = { id: 'question-id', pubkey: 'peer', content: '' }
  const packer = createEventReplyPacker({
    messenger: { reply: async options => replies.push(options) },
    channelPubkey: 'channel',
    question,
    code: 'eventSync_empty',
    sendEmptyReply: true
  })

  await packer.finalize()

  assert.equal(replies.length, 1)
  assert.equal(replies[0].payload.index, 0)
  assert.equal(replies[0].payload.isLast, true)
  assert.equal(replies[0].payload.jsonl, '')
})

test('event reply packer still sends an empty final marker after prior chunks', async () => {
  const replies = []
  const question = { id: 'question-id', pubkey: 'peer', content: '' }
  const packer = createEventReplyPacker({
    messenger: { reply: async options => replies.push(options) },
    channelPubkey: 'channel',
    question,
    code: 'eventSync_marker',
    eventsPerChunk: 1
  })

  await packer.update({ id: 'event-1', kind: 1, pubkey: 'alice', created_at: 1, tags: [], content: 'one' })
  await packer.finalize()

  assert.equal(replies.length, 2)
  assert.equal(replies[0].payload.isLast, false)
  assert.equal(JSON.parse(replies[0].payload.jsonl).id, 'event-1')
  assert.equal(replies[1].payload.index, 1)
  assert.equal(replies[1].payload.isLast, true)
  assert.equal(replies[1].payload.jsonl, '')
})
