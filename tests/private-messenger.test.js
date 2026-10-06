import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { ValidationError } from '../error/index.js'
import { ASK_KIND, REPLY_KIND, TELL_KIND } from '../private-message/index.js'
import { EXPIRATION_SECONDS } from '../private-channel/index.js'
import { readHistory } from '../private-channel/helpers/history.js'
import {
  createEventReplyPacker,
  createMissingMessageReplyPacker,
  MISSING_MESSAGES_ASK_CODE,
  MISSING_MESSAGES_REPLY_CODE,
  NYM_CARRIER_SEED_RECORD_TYPE,
  PrivateMessenger as RealPrivateMessenger,
  RECOVERY_RELAY_RETRY_LIMITS,
  ROUTER_SEED_RECORD_TYPE,
  SEEDER_PRESENCE_CODE
} from '../private-messenger/index.js'
import { TEMPORARY_STORAGE_KEYS_KEY } from '../temporary-storage/index.js'
import { createChannelStateStore } from '../private-messenger/services/channel-state.js'

// Unit fixtures never contact public relays, even when a restart schedules a gap.
const instances = new Set()
class PrivateMessenger extends RealPrivateMessenger {
  constructor (options = {}) {
    // Existing fixtures describe outer-event arrays; production history now
    // returns a bounded summary instead of retaining all those outer events.
    const channel = options._privateChannel ?? { fetchHistory: async () => [] }
    super({
      ...options, _privateChannel: {
        ...channel, fetchHistory: async args => {
          const result = await channel.fetchHistory?.(args) ?? []
          if (!Array.isArray(result)) return result
          return { oldestCreatedAt: result.length ? Math.min(...result.map(event => event.created_at)) : null, receivedEventCount: result.length, relays: [] }
        }
      }
    })
    instances.add(this)
  }
}

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

afterEach(async () => {
  await Promise.all([...instances].map(instance => instance.close()))
  instances.clear()
  globalThis.localStorage.clear()
  globalThis.sessionStorage.clear()
  resetIndexedDb()
})

async function takeMessage (messenger) {
  const delivery = await messenger.nextMessage()
  if (!delivery) return null
  await delivery.ack()
  return delivery.message
}

function signer (pubkey) {
  return {
    getPublicKey: () => pubkey,
    withSharedKey: () => ({})
  }
}

async function seedMessengerState (channels, userPubkey = 'user') {
  const state = await createChannelStateStore({
    prefix: `libp2r2p:private-messenger:${userPubkey}`,
    indexedDB: globalThis.indexedDB
  })
  await state.update(channels)
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
      return { question: { id: 'question-id', kind: ASK_KIND, pubkey: 'user' }, delivery: { reports: [{ success: true }] } }
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

  assert.equal((await takeMessage(second)).event.id, 'durable-id')
  assert.equal(await takeMessage(second), null)
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

  const item = (await takeMessage(messenger))
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

  const reply = (await takeMessage(messenger))
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

  const raw = (await takeMessage(messenger))
  assert.equal(raw.type, 'message')
  assert.equal(raw.event.id, 'raw-id')
  assert.deepEqual(raw.payload, ['raw-payload', 'not-a-private-message-code'])
})

test('private messenger validates and applies offline recovery defaults and channel overrides', async () => {
  for (const invalid of [-1, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER]) {
    assert.throws(
      () => new PrivateMessenger({ _privateMessage: fakePrivateMessage(), offlineRecoverySeconds: invalid }),
      error => error instanceof ValidationError && error.code === 'INVALID_OFFLINE_RECOVERY_SECONDS'
    )
  }

  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    offlineRecoverySeconds: 2 * 60 * 60
  }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'inherited', signer: signer('inherited'), relays: ['wss://relay.example'] },
      { pubkey: 'custom', signer: signer('custom'), relays: ['wss://relay.example'], offlineRecoverySeconds: 30 * 60 }
    ]
  })

  assert.equal(pm.watchCalls[0].receivedChunkTtlMs, 2 * 60 * 60 * 1000)
  assert.equal(pm.watchCalls[1].receivedChunkTtlMs, 30 * 60 * 1000)
  assert.equal(messenger.readState().channels.inherited.offlineRecoverySeconds, 2 * 60 * 60)
  assert.equal(messenger.readState().channels.custom.offlineRecoverySeconds, 30 * 60)

  await messenger.tell({ channelPubkey: 'inherited', receiverPubkey: 'alice', payload: 'default' })
  await messenger.tell({ channelPubkey: 'custom', receiverPubkey: 'alice', payload: 'custom' })
  assert.equal(pm.sent[0].options.expirationSeconds, 2 * 60 * 60)
  assert.equal(pm.sent[1].options.expirationSeconds, 30 * 60)

  await assert.rejects(
    () => messenger.update({
      channels: [{ pubkey: 'custom', signer: signer('custom'), relays: ['wss://relay.example'], offlineRecoverySeconds: -1 }]
    }),
    /INVALID_OFFLINE_RECOVERY_SECONDS/
  )
  await messenger.close()
})

test('private messenger validates identity and stale-channel retention policies', async () => {
  for (const invalid of [-1, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER]) {
    assert.throws(
      () => new PrivateMessenger({ _privateMessage: fakePrivateMessage(), staleChannelSeconds: invalid }),
      /INVALID_STALE_CHANNEL_SECONDS/
    )
    assert.throws(
      () => new PrivateMessenger({ _privateMessage: fakePrivateMessage(), identityStorageRetentionSeconds: invalid }),
      /INVALID_IDENTITY_STORAGE_RETENTION_SECONDS/
    )
  }

  const messenger = await new PrivateMessenger({ _privateMessage: fakePrivateMessage() }).init({
    userSigner: signer('retention-validation-user'),
    channels: []
  })
  await assert.rejects(
    messenger.update({ staleChannelSeconds: -1 }),
    /INVALID_STALE_CHANNEL_SECONDS/
  )
  await assert.rejects(
    messenger.update({ identityStorageRetentionSeconds: 0.5 }),
    /INVALID_IDENTITY_STORAGE_RETENTION_SECONDS/
  )
  await messenger.close()
})

test('updating a maintained channel keeps its watch available to concurrent asks', async () => {
  const pm = fakePrivateMessage()
  const watched = new Set()
  let releaseRewatch
  let rewatchStarted
  const rewatchGate = new Promise(resolve => { releaseRewatch = resolve })
  const rewatchStartedPromise = new Promise(resolve => { rewatchStarted = resolve })
  pm.watch = async options => {
    pm.watchCalls.push(options)
    if (pm.watchCalls.length > 1) {
      rewatchStarted()
      await rewatchGate
    }
    const pubkey = options.channels[0]
    watched.add(pubkey)
    return () => {
      watched.delete(pubkey)
      pm.stopped.push(pubkey)
    }
  }
  pm.ask = async options => {
    const pubkey = await options.privateChannelSigner.getPublicKey()
    if (!watched.has(pubkey)) throw new Error('PRIVATE_MESSAGE_NOT_WATCHING')
    pm.sent.push({ method: 'ask', options })
    return { question: { id: 'question-id' }, delivery: { reports: [{ success: true }] } }
  }

  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  const updating = messenger.update({
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay-two.example'] }]
  })
  await rewatchStartedPromise

  await messenger.ask({ channelPubkey: 'channel', receiverPubkey: 'peer', payload: 'during-update' })

  releaseRewatch()
  await updating
  assert.equal(pm.stopped.includes('channel'), false)
  await messenger.close()
})

test('identity and stale-channel retention silently cap recovery without replacing its requested value', async () => {
  const originalDateNow = Date.now
  const now = 2_000_000_000
  Date.now = () => now * 1000
  const pm = fakePrivateMessage()
  let messenger
  try {
    messenger = await new PrivateMessenger({
      _privateMessage: pm,
      offlineRecoverySeconds: 1000,
      staleChannelSeconds: 100,
      identityStorageRetentionSeconds: 60
    }).init({
      userSigner: signer('capped-recovery-user'),
      channels: [{
        pubkey: 'channel',
        signer: signer('channel'),
        relays: ['wss://relay.example'],
        offlineRecoverySeconds: 500
      }]
    })

    assert.equal(messenger.readState().channels.channel.offlineRecoverySeconds, 500)
    assert.equal(messenger.offlineRecoverySecondsFor('channel'), 60)
    assert.equal(pm.watchCalls.at(-1).receivedChunkTtlMs, 60 * 1000)
    await messenger.tell({ channelPubkey: 'channel', receiverPubkey: 'alice', payload: 'capped' })
    assert.equal(pm.sent.at(-1).options.expirationSeconds, 60)

    await messenger.seedQueue.enqueue({
      type: 'seed',
      channelPubkey: 'channel',
      receivedAt: now - 40,
      __p2r2pSeedKey: 'capped-seed',
      __p2r2pSeedTime: now - 40
    })
    messenger.updateChannelState('channel', {
      offlineRanges: [{ start: now - 50, end: now - 10 }]
    })
    await messenger.flushStateWrites()

    await messenger.update({ identityStorageRetentionSeconds: 30 })
    assert.equal(messenger.readState().channels.channel.offlineRecoverySeconds, 500)
    assert.equal(messenger.offlineRecoverySecondsFor('channel'), 30)
    assert.equal(await messenger.seedQueue.someBy('bySeedKey', 'capped-seed'), false)
    assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [{ start: now - 30, end: now }])
    assert.equal(pm.watchCalls.at(-1).receivedChunkTtlMs, 30 * 1000)

    await messenger.update({ identityStorageRetentionSeconds: 120 })
    assert.equal(messenger.offlineRecoverySecondsFor('channel'), 100)
    assert.equal(await messenger.seedQueue.someBy('bySeedKey', 'capped-seed'), false)
  } finally {
    await messenger?.close()
    Date.now = originalDateNow
  }
})

test('zero global retention disables durable recovery but preserves live technical deadlines', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    identityStorageRetentionSeconds: 0
  }).init({
    userSigner: signer('zero-global-recovery-user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(messenger.offlineRecoverySecondsFor('channel'), 0)
  assert.equal(pm.watchCalls.at(-1).receivedChunkTtlMs, 60 * 60 * 1000)
  await messenger.tell({ channelPubkey: 'channel', receiverPubkey: 'alice', payload: 'live' })
  assert.equal(pm.sent.at(-1).options.expirationSeconds, EXPIRATION_SECONDS)
  await messenger.close()
})

test('private messenger applies reduced channel recovery policies immediately without affecting other channels', async () => {
  const originalDateNow = Date.now
  const now = 2_000_000_000
  Date.now = () => now * 1000
  const pm = fakePrivateMessage()
  let messenger
  try {
    messenger = await new PrivateMessenger({
      _privateMessage: pm,
      offlineRecoverySeconds: 1000,
      _setTimeout: () => null
    }).init({
      userSigner: signer('user'),
      channels: [
        { pubkey: 'short', signer: signer('short'), relays: ['wss://relay.example'] },
        { pubkey: 'long', signer: signer('long'), relays: ['wss://relay.example'] }
      ]
    })

    await messenger.seedQueue.enqueue({
      type: 'seed',
      channelPubkey: 'short',
      receivedAt: now - 100,
      __p2r2pSeedKey: 'short-seed',
      __p2r2pSeedTime: now - 100
    })
    await messenger.seedQueue.enqueue({
      type: 'seed',
      channelPubkey: 'long',
      receivedAt: now - 100,
      __p2r2pSeedKey: 'long-seed',
      __p2r2pSeedTime: now - 100
    })
    messenger.updateChannelState('short', {
      openOfflineStart: now - 500,
      offlineRanges: [
        { start: now - 500, end: now - 400 },
        { start: now - 100, end: now - 10 }
      ]
    })
    await messenger.flushStateWrites()

    await messenger.update({
      channels: [
        { pubkey: 'short', signer: signer('short'), relays: ['wss://relay.example'], offlineRecoverySeconds: 60 },
        { pubkey: 'long', signer: signer('long'), relays: ['wss://relay.example'], offlineRecoverySeconds: 200 }
      ]
    })

    assert.equal(await messenger.seedQueue.someBy('bySeedKey', 'short-seed'), false)
    assert.equal(await messenger.seedQueue.someBy('bySeedKey', 'long-seed'), true)
    assert.deepEqual(messenger.readState().channels.short.offlineRanges, [{ start: now - 60, end: now }])
    assert.equal(messenger.readState().channels.short.openOfflineStart, undefined)

    await messenger.update({
      channels: [
        { pubkey: 'short', signer: signer('short'), relays: ['wss://relay.example'], offlineRecoverySeconds: 500 },
        { pubkey: 'long', signer: signer('long'), relays: ['wss://relay.example'], offlineRecoverySeconds: 200 }
      ]
    })
    assert.equal(await messenger.seedQueue.someBy('bySeedKey', 'short-seed'), false)
  } finally {
    await messenger?.close()
    Date.now = originalDateNow
  }
})

test('private messenger applies the current channel policy before startup pruning', async () => {
  const originalDateNow = Date.now
  const indexedDB = new IDBFactory()
  const now = 2_000_000_000
  Date.now = () => now * 1000
  let first
  let second
  try {
    first = await new PrivateMessenger({
      _privateMessage: fakePrivateMessage(),
      _indexedDB: indexedDB,
      offlineRecoverySeconds: 1000
    }).init({
      userSigner: signer('restart-user'),
      channels: [{
        pubkey: 'channel',
        signer: signer('channel'),
        relays: ['wss://relay.example'],
        offlineRecoverySeconds: 60
      }]
    })
    await first.seedQueue.enqueue({
      type: 'seed',
      channelPubkey: 'channel',
      receivedAt: now - 100,
      __p2r2pSeedKey: 'policy-increase-seed',
      __p2r2pSeedTime: now - 100
    })
    await first.close()
    first = null

    second = await new PrivateMessenger({
      _privateMessage: fakePrivateMessage(),
      _indexedDB: indexedDB,
      offlineRecoverySeconds: 1000
    }).init({
      userSigner: signer('restart-user'),
      channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
    })

    assert.equal(second.readState().channels.channel.offlineRecoverySeconds, 1000)
    assert.equal(await second.seedQueue.someBy('bySeedKey', 'policy-increase-seed'), true)
  } finally {
    await first?.close()
    await second?.close()
    Date.now = originalDateNow
  }
})

test('offline recovery zero disables durable recovery while preserving live technical deadlines', async () => {
  const pm = fakePrivateMessage()
  const intervals = []
  const scheduled = []
  const contentKeyChanges = []
  const fetches = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: { fetchHistory: async options => { fetches.push(options); return [] } },
    _setInterval: (fn, ms) => { const timer = { fn, ms }; intervals.push(timer); return timer },
    _setTimeout: fn => { scheduled.push(fn); return fn },
    onContentKeyChange: event => contentKeyChanges.push(event)
  }).init({
    userSigner: signer('user'),
    channels: [
      { pubkey: 'leecher', signer: signer('leecher'), relays: ['wss://relay.example'], seeders: ['remote'], offlineRecoverySeconds: 0 },
      { pubkey: 'seeder', signer: signer('seeder'), relays: ['wss://relay.example'], mode: 'seeder', offlineRecoverySeconds: 0 },
      { pubkey: 'watchtower', signer: signer('watchtower'), relays: ['wss://relay.example'], mode: 'watchtower', offlineRecoverySeconds: 0 }
    ]
  })

  assert.deepEqual(pm.watchCalls.map(call => call.receivedChunkTtlMs), [60 * 60 * 1000, 60 * 60 * 1000, 60 * 60 * 1000])
  assert.equal(intervals.length, 0)
  assert.equal(scheduled.length, 0)
  assert.deepEqual(messenger.readState().channels.leecher.offlineRanges, [])

  messenger.addOfflineRange('leecher', 1, 2)
  await messenger.recoverOfflineRanges(['leecher'])
  assert.deepEqual(messenger.readState().channels.leecher.offlineRanges, [])
  assert.equal(fetches.length, 0)

  await messenger.enqueueSeed('seeder', { channelPubkey: 'seeder' })
  assert.equal(await messenger.seedQueue.some(() => true), false)
  assert.deepEqual(await messenger.askSeedersForMissingRange('leecher', 1, 2), [])
  assert.equal(await messenger.publishSeederPresence('seeder'), null)

  await messenger.tell({ channelPubkey: 'leecher', receiverPubkey: 'alice', payload: 'live' })
  assert.equal(pm.sent[0].options.expirationSeconds, EXPIRATION_SECONDS)
  assert.deepEqual(pm.sent[0].options.recoveryRelays, [])

  const now = Math.floor(Date.now() / 1000)
  await pm.watchCalls[1].onAsk({
    event: { id: 'question-id', kind: ASK_KIND, pubkey: 'alice', created_at: now, tags: [], content: '' },
    outer: { id: 'outer-id', created_at: now },
    meta: { channelPubkey: 'seeder' },
    payload: { code: MISSING_MESSAGES_ASK_CODE, payload: { since: now - 10, until: now } }
  })
  const emptyReply = pm.sent.find(sent => sent.method === 'reply' && sent.options.code === MISSING_MESSAGES_REPLY_CODE)
  assert.equal(emptyReply.options.payload.jsonl, '')
  assert.equal(emptyReply.options.payload.isLast, true)
  assert.equal(emptyReply.options.expirationSeconds, EXPIRATION_SECONDS)

  pm.watchCalls[2].onContentKeyUsage({
    direction: 'received',
    senderPubkey: 'alice',
    receiverPubkey: 'user',
    contentKeyPubkey: 'content-key'
  })
  assert.equal(contentKeyChanges.length, 1)
  assert.equal(contentKeyChanges[0].channelPubkey, 'watchtower')
  await messenger.close()
})

test('private messenger pauses live watches offline, restarts them before durable recovery, and suspends presence publishing', async () => {
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
      _isOnline: async () => true,
      _onOnline: () => () => {},
      _privateChannel: {
        fetchHistory: async () => {
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
    assert.equal(messenger.presenceTimers.has('channel'), false)
    assert.deepEqual(clearedIntervals, ['presence-timer'])
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
    assert.equal((await takeMessage(messenger)).event.id, 'offline-duplicate')
    assert.equal(await takeMessage(messenger), null)
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

  const item = (await takeMessage(messenger))
  assert.equal(item.type, 'nym')
  assert.equal(item.event.kind, ASK_KIND)
  assert.equal(item.event.pubkey, 'nym')
  assert.equal((await takeMessage(messenger)), null)
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

  const queued = await takeMessage(messenger)
  assert.equal(queued.event.id, 'tell-id')
  assert.equal(Object.hasOwn(queued, '__p2r2pMessageDedupeKey'), false)
  assert.equal(Object.hasOwn(queued, 'id'), false)
  assert.equal((await takeMessage(messenger)), null)
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
      fetchHistory: async options => {
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
  assert.equal((await takeMessage(messenger)), null)
})

test('private messenger refreshes NIP-65-derived watch relays from relay-list updates while retaining configured fallbacks', async () => {
  const pm = fakePrivateMessage()
  const relayUpdates = fakeRelayListUpdates()
  const fetches = []
  const now = Math.floor(Date.now() / 1000)
  let userReadRelays = ['wss://user.old-one.example', 'wss://user.old-two.example']
  const messenger = await new PrivateMessenger({
    fallbackRelays: ['wss://fallback.example/', 'wss://user.old-two.example', 'wss://fallback.example'],
    _privateMessage: pm,
    _privateChannel: {
      fetchHistory: async options => {
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
  assert.deepEqual(pm.watchCalls[0].relays, ['wss://user.old-one.example', 'wss://user.old-two.example', 'wss://fallback.example'])
  assert.deepEqual(pm.watchCalls[1].channels, ['explicit'])
  assert.deepEqual(pm.watchCalls[1].relays, ['wss://explicit.example', 'wss://fallback.example', 'wss://user.old-two.example'])

  userReadRelays = ['wss://user.old-two.example', 'wss://user.new.example']
  await relayUpdates.subscriptions[0].emit({ pubkey: 'user' })

  assert.equal(pm.watchCalls.length, 3)
  assert.deepEqual(pm.watchCalls[2].channels, ['derived'])
  assert.deepEqual(pm.watchCalls[2].relays, ['wss://user.old-two.example', 'wss://user.new.example', 'wss://fallback.example'])
  assert.deepEqual(pm.stopped, [])
  assert.equal(fetches.length, 1)
  assert.deepEqual(fetches[0].privateChannelPubkeys, ['derived'])
  assert.deepEqual(fetches[0].relays, ['wss://user.old-two.example', 'wss://user.new.example', 'wss://fallback.example'])
  assert.ok(fetches[0].since <= now - 20)
  assert.ok(fetches[0].until >= now)
  assert.equal((await takeMessage(messenger)).event.id, 'missed-id')
  assert.deepEqual(messenger.readState().channels.derived.relays, ['wss://user.old-two.example', 'wss://user.new.example', 'wss://fallback.example'])
  assert.deepEqual(messenger.readState().channels.explicit.relays, ['wss://explicit.example', 'wss://fallback.example', 'wss://user.old-two.example'])
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
  const messenger = await new PrivateMessenger({ _privateMessage: pm, fallbackRelays: ['wss://fallback.example'] }).init({
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

  assert.deepEqual(pm.watchCalls[0].relays, ['wss://watch-one.example', 'wss://watch-two.example', 'wss://watch-three.example', 'wss://fallback.example'])
  assert.equal(typeof pm.sent[0].options._publish, 'function')
  assert.equal(typeof pm.sent[1].options._publish, 'function')
  assert.deepEqual(pm.sent[0].options.relays, ['wss://send-one.example', 'wss://send-two.example'])
  assert.deepEqual(pm.sent[1].options.relays, ['wss://per-call.example'])
  assert.equal(pm.sent[2].options.relays, undefined)
  assert.equal(pm.sent[2].options.relayToReceivers, relayToReceivers)
  assert.equal(typeof pm.sent[2].options._publish, 'function')
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

  assert.equal((await takeMessage(messenger)).event.id, 'tell-id')
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

  const item = (await takeMessage(messenger))
  assert.equal(item.channelPubkey, 'two')
  assert.equal((await takeMessage(messenger)), null)
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
      fetchHistory: async options => {
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
  assert.equal((await takeMessage(messenger)).event.id, 'ask-id')
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
})

test('stale reload-gap timers do not run after rewatch, unwatch, or close', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  const fetches = []
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({ channel: { lastSeenAt: now - 10, lastWatchedAt: now - 10 } })
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _privateChannel: { fetchHistory: async options => { fetches.push(options); return [] } },
    _setTimeout: fn => {
      const timer = { fn, cleared: false }
      timers.push(timer)
      return timer
    },
    _clearTimeout: timer => { timer.cleared = true }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  const initialTimer = timers.at(-1)
  await messenger.update({
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay-two.example'] }]
  })
  assert.equal(initialTimer.cleared, true)
  await initialTimer.fn()

  const rewatchTimer = timers.at(-1)
  await messenger.unwatch('channel')
  assert.equal(rewatchTimer.cleared, true)
  await rewatchTimer.fn()

  await messenger.watch(['channel'])
  const closeTimer = timers.at(-1)
  await messenger.close()
  assert.equal(closeTimer.cleared, true)
  await closeTimer.fn()

  assert.equal(fetches.length, 0)
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
      fetchHistory: async options => {
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
  assert.equal((await takeMessage(messenger)).event.id, 'missed-id')
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

  const item = (await takeMessage(messenger))
  assert.equal(item.type, 'tell')
  assert.equal(item.event.id, 'tell-id')
  assert.equal((await takeMessage(messenger)), null)

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
  assert.equal((await takeMessage(messenger)), null)
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

  assert.equal((await takeMessage(messenger)), null)

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
  assert.equal((await takeMessage(messenger)), null)
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
      fetchHistory: async options => {
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

  assert.equal((await takeMessage(messenger)), null)

  await scheduled()

  const ask = pm.sent.find(sent => sent.method === 'ask' && sent.options.code === MISSING_MESSAGES_ASK_CODE)
  assert.equal(fetches.length, 1)
  assert.equal(ask.options.receiverPubkey, 'seeder')
  assert.ok(ask.options.payload.since <= now - 20)
  assert.equal(ask.options.payload.until, now - 5)
  assert.equal((await takeMessage(messenger)).event.id, 'relay-id')
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

test('recovery retains the full range until every seeder ask is delivered', async t => {
  const now = Math.floor(Date.now() / 1000)
  const range = { start: now - 20, end: now - 10 }
  const cases = [
    {
      name: 'local ask failure',
      seeders: ['one'],
      ask: async () => { throw new Error('LOCAL_FAILURE') },
      retained: true
    },
    {
      name: 'delivery failure',
      seeders: ['one'],
      ask: async () => ({ question: { id: 'one' }, delivery: { reports: [{ success: false }] } }),
      retained: true
    },
    {
      name: 'one failed seeder among several',
      seeders: ['one', 'two'],
      ask: async options => ({
        question: { id: options.receiverPubkey },
        delivery: { reports: [{ success: options.receiverPubkey === 'one' }] }
      }),
      retained: true
    },
    {
      name: 'all seeders delivered',
      seeders: ['one', 'two'],
      ask: async options => ({
        question: { id: options.receiverPubkey },
        delivery: { reports: [{ success: true }] }
      }),
      retained: false
    }
  ]

  for (const [index, scenario] of cases.entries()) {
    await t.test(scenario.name, async () => {
      const userPubkey = `recovery-user-${index}`
      await seedMessengerState({ channel: { offlineRanges: [range] } }, userPubkey)
      const pm = fakePrivateMessage()
      pm.ask = scenario.ask
      const messenger = await new PrivateMessenger({
        _privateMessage: pm,
        _privateChannel: { fetchHistory: async () => [] }
      }).init({
        userSigner: signer(userPubkey),
        channels: [{
          pubkey: 'channel',
          signer: signer('channel'),
          relays: ['wss://relay.example'],
          seeders: scenario.seeders
        }]
      })

      const pendingRanges = messenger.readState().channels.channel.offlineRanges
      await messenger.recoverOfflineRanges(['channel'])

      assert.deepEqual(
        messenger.readState().channels.channel.offlineRanges,
        scenario.retained ? pendingRanges : []
      )
      await messenger.close()
    })
  }
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

  assert.equal((await takeMessage(messenger)), null)
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
  assert.equal((await takeMessage(messenger)).event.id, 'missed-id')
  assert.equal((await takeMessage(messenger)), null)
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

  const item = (await takeMessage(messenger))
  assert.equal(item.type, 'nym')
  assert.equal(item.event.kind, ASK_KIND)
  assert.equal(item.meta.recoveredFromSeeder, 'seeder')
  assert.equal((await takeMessage(messenger)), null)
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

test('unacknowledged deliveries survive close, nack and iterator cancellation', async () => {
  const pm = fakePrivateMessage()
  const init = { userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] }
  const first = await new PrivateMessenger({ _privateMessage: pm }).init(init)
  const message = { event: { kind: 9, id: 'one', pubkey: 'peer', tags: [], created_at: 1, content: 'hello' }, senderPubkey: 'peer', outer: { created_at: 1 } }
  await pm.watchCalls[0].onMessage(message)
  const delivery = await first.nextMessage()
  assert.equal(delivery.message.provenance, 'direct')
  assert.equal(await first.nextMessage(), null)
  await first.close()
  const second = await new PrivateMessenger({ _privateMessage: fakePrivateMessage() }).init(init)
  try {
    const stream = second.messages()
    const received = (await stream.next()).value
    assert.equal(received.message.event.id, 'one')
    await stream.return()
    assert.equal(await received.ack(), false)
    const again = await second.nextMessage()
    assert.equal(again.message.event.id, 'one')
    assert.equal(await again.ack(), true)
    assert.equal(await again.ack(), true)
    const waiting = second.messages()
    const pending = waiting.next()
    await waiting.return()
    assert.equal((await pending).done, true)
  } finally { await second.close() }
})

test('unwatch intent survives online, update and independent pause reasons', async () => {
  const pm = fakePrivateMessage()
  const channel = { signer: signer('channel'), relays: ['wss://relay.example'] }
  const messenger = await new PrivateMessenger({ _privateMessage: pm, _privateChannel: { fetchHistory: async () => [] } }).init({ userSigner: signer('owner'), channels: [channel] })
  try {
    await messenger.unwatch('channel')
    await messenger.pause('network')
    await messenger.resume('network')
    await messenger.update({ channels: [channel] })
    assert.equal(pm.watchCalls.length, 1)
    await messenger.pause('vault')
    await messenger.pause('network')
    await messenger.watch(['channel'])
    assert.ok(messenger.readState().channels.channel.openOfflineStart)
    await messenger.resume('network')
    assert.equal(pm.watchCalls.length, 1)
    await messenger.resume('vault')
    assert.equal(pm.watchCalls.length, 2)
    assert.equal(messenger.pauseReasons.size, 0)
  } finally { await messenger.close() }
})

test('capacity pauses ingestion without eviction and ack resumes durable recovery', async () => {
  const pm = fakePrivateMessage()
  let recovered = 0
  const errors = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm, messageQueueMaxBytes: 850, onError: err => errors.push(err),
    _privateChannel: { fetchHistory: async () => { recovered++; return [] } }
  }).init({
    userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }]
  })
  try {
    const now = Math.floor(Date.now() / 1000)
    const message = id => ({ event: { kind: 9, id, pubkey: 'peer', tags: [], created_at: now, content: 'a'.repeat(250) }, outer: { created_at: now }, senderPubkey: 'peer' })
    await pm.watchCalls[0].onMessage(message('one'))
    await assert.rejects(pm.watchCalls[0].onMessage(message('two')), /QUEUE_CAPACITY_EXCEEDED/)
    assert.equal(messenger.pauseReasons.has('capacity'), true)
    assert.ok(messenger.readState().channels.channel.openOfflineStart)
    const first = await messenger.nextMessage()
    assert.equal(first.message.event.id, 'one')
    await first.ack()
    if (messenger.capacityCheck) await messenger.capacityCheck
    if (messenger.resumeWork) await messenger.resumeWork
    assert.equal(messenger.pauseReasons.size, 0)
    assert.equal(recovered, 1)
    assert.equal(await messenger.nextMessage(), null)
    assert.ok(errors.some(err => err.message === 'QUEUE_CAPACITY_EXCEEDED'))
  } finally { await messenger.close() }
})

test('failed recovery ingestion retains its range and does not advance lastSeenAt', async () => {
  const pm = fakePrivateMessage()
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({
    _privateMessage: pm, messageQueueMaxBytes: 100, onError: () => {},
    _privateChannel: {
      fetchHistory: async options => {
        await options.onEvent({ kind: 9, id: 'large', pubkey: 'peer', tags: [], created_at: now, content: 'x'.repeat(300) }, { created_at: now }, { senderPubkey: 'peer' })
        return []
      }
    }
  }).init({ userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  try {
    messenger.addOfflineRange('channel', now - 10, now)
    await messenger.recoverOfflineRanges()
    const state = messenger.readState().channels.channel
    assert.equal(state.lastSeenAt, undefined)
    assert.ok(state.offlineRanges.length)
    assert.equal(messenger.pauseReasons.has('storage'), true)
  } finally { await messenger.close() }
})

test('failed resume remains paused and can be retried', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, _privateChannel: { fetchHistory: async () => [] } }).init({ userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  try {
    await messenger.pause('vault')
    const watch = pm.watch
    pm.watch = async () => { throw new Error('SIGNER_LOCKED') }
    await assert.rejects(messenger.resume('vault'), /SIGNER_LOCKED/)
    assert.equal(messenger.pauseReasons.has('vault'), true)
    pm.watch = watch
    await messenger.resume('vault')
    assert.equal(messenger.pauseReasons.size, 0)
    assert.equal(messenger.stopByChannel.size, 1)
  } finally { await messenger.close() }
})

test('capacity monitor resumes after another instance acknowledges a delivery', { timeout: 5000 }, async () => {
  const pm = fakePrivateMessage()
  const resumed = Promise.withResolvers()
  const watch = pm.watch
  pm.watch = async options => { const stop = await watch(options); if (pm.watchCalls.length > 1) resumed.resolve(); return stop }
  const init = { userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] }
  const first = await new PrivateMessenger({ _privateMessage: pm, messageQueueMaxBytes: 850, onError: () => {} }).init(init)
  const second = await new PrivateMessenger({ _privateMessage: fakePrivateMessage(), messageQueueMaxBytes: 850 }).init(init)
  try {
    const now = Math.floor(Date.now() / 1000)
    const message = id => ({ event: { kind: 9, id, pubkey: 'peer', tags: [], created_at: now, content: 'a'.repeat(250) }, outer: { created_at: now }, senderPubkey: 'peer' })
    await pm.watchCalls[0].onMessage(message('one'))
    await assert.rejects(pm.watchCalls[0].onMessage(message('two')), /QUEUE_CAPACITY_EXCEEDED/)
    await (await second.nextMessage()).ack()
    // Keep the test process alive while the production polling timer is unref'ed.
    const keepAlive = setTimeout(() => resumed.reject(new Error('capacity did not resume')), 3000)
    try { await resumed.promise } finally { clearTimeout(keepAlive) }
    if (first.capacityCheck) await first.capacityCheck
    assert.equal(first.pauseReasons.has('capacity'), false)
  } finally { await first.close(); await second.close() }
})

test('pause reports state persistence errors and flush repairs the interruption', async () => {
  const messenger = await new PrivateMessenger({ _privateMessage: fakePrivateMessage(), onError: () => {} }).init({ userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  const update = messenger.stateStore.update.bind(messenger.stateStore)
  try {
    messenger.stateStore.update = async () => { throw new Error('DISK_UNAVAILABLE') }
    await assert.rejects(messenger.pause('vault'), /DISK_UNAVAILABLE/)
    assert.equal(messenger.pauseReasons.has('vault'), true)
    messenger.stateStore.update = update
    await messenger.flushStateWrites()
    const stored = await messenger.stateStore.load()
    assert.ok(stored.channel.openOfflineStart)
  } finally { messenger.stateStore.update = update; await messenger.close() }
})

test('a pending hearsay does not suppress a direct original of the same event', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({ userSigner: signer('owner'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  try {
    const event = { id: 'same', kind: 9, pubkey: 'author', tags: [], content: 'quote', created_at: 1 }
    await pm.watchCalls[0].onMessage({ event, senderPubkey: 'forwarder' })
    await pm.watchCalls[0].onMessage({ event, senderPubkey: 'author' })
    await pm.watchCalls[0].onMessage({ event, senderPubkey: 'author' })
    assert.equal((await takeMessage(messenger)).provenance, 'hearsay')
    assert.equal((await takeMessage(messenger)).provenance, 'direct')
    assert.equal(await messenger.nextMessage(), null)
  } finally { await messenger.close() }
})

test('first watch commits the bounded initial window before live delivery', async t => {
  const now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  const pm = fakePrivateMessage()
  const watch = pm.watch
  const fetches = []
  let scheduled
  pm.watch = async options => {
    const store = await createChannelStateStore({ prefix: 'libp2r2p:private-messenger:user' })
    try {
      const state = await store.load()
      assert.deepEqual(state.channel.offlineRanges, [{ start: now - 7 * 86400, end: now }])
    } finally { await store.close() }
    const stop = await watch(options)
    await options.onMessage({
      event: { id: 'live', kind: 9, pubkey: 'peer', created_at: now, tags: [], content: 'new' },
      outer: { created_at: now }, senderPubkey: 'peer'
    })
    return stop
  }
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    _setTimeout: fn => { scheduled = fn },
    _privateChannel: { fetchHistory: async options => { fetches.push(options); return [] } }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  await scheduled()
  assert.equal(fetches[0].since, now - 7 * 86400)
  assert.equal(fetches[0].until, now)
  assert.equal(messenger.readState().channels.channel.lastSeenAt, now)
  assert.equal(messenger.readState().channels.channel.recoveredThrough, now)
})

test('abrupt restart of an empty channel uses its completed scan, not its heartbeat', async t => {
  let now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  let scheduled
  const init = { userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] }
  const first = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(), _setTimeout: fn => { scheduled = fn }
  }).init(init)
  await scheduled()
  now += 3600
  await first.runStorageHeartbeat()
  // Copy exactly the committed state at termination, without invoking close/unwatch.
  const snapshot = await first.stateStore.load()
  assert.equal(snapshot.channel.lastSeenAt, undefined)
  assert.equal(snapshot.channel.openOfflineStart, undefined)
  assert.equal(snapshot.channel.recoveredThrough, now - 3600)
  assert.equal(snapshot.channel.lastWatchedAt, now)
  const restartedDb = new IDBFactory()
  const store = await createChannelStateStore({ prefix: first.prefix, indexedDB: restartedDb })
  await store.update(snapshot)
  await store.close()
  now += 3600
  const fetches = []
  const second = await new PrivateMessenger({
    _indexedDB: restartedDb, _privateMessage: fakePrivateMessage(),
    _setTimeout: fn => { scheduled = fn },
    _privateChannel: { fetchHistory: async options => { fetches.push(options); return [] } }
  }).init(init)
  await scheduled()
  assert.equal(fetches[0].since, now - 7200 - second.offlineSkewSeconds)
  assert.equal(fetches[0].until, now)
  assert.equal((await second.stateStore.load()).channel.recoveredThrough, now)
})

test('failed initial recovery survives restart despite newer live progress', async t => {
  let now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  let scheduled
  const pm = fakePrivateMessage()
  const init = { userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] }
  const first = await new PrivateMessenger({
    _privateMessage: pm, _setTimeout: fn => { scheduled = fn }, onError: () => {},
    _privateChannel: { fetchHistory: async () => { throw new Error('offline') } }
  }).init(init)
  await scheduled()
  await pm.watchCalls[0].onMessage({
    event: { id: 'recent', kind: 9, pubkey: 'peer', created_at: now, tags: [], content: '' },
    outer: { created_at: now }, senderPubkey: 'peer'
  })
  await first.flushStateWrites()
  const snapshot = await first.stateStore.load()
  assert.equal(snapshot.channel.recoveredThrough, 0)
  assert.deepEqual(snapshot.channel.offlineRanges, [{ start: now - 7 * 86400, end: now }])
  const restartedDb = new IDBFactory()
  const store = await createChannelStateStore({ prefix: first.prefix, indexedDB: restartedDb })
  await store.update(snapshot)
  await store.close()
  now += 60
  const fetches = []
  await new PrivateMessenger({
    _indexedDB: restartedDb, _privateMessage: fakePrivateMessage(),
    _setTimeout: fn => { scheduled = fn },
    _privateChannel: { fetchHistory: async options => { fetches.push(options); return [] } }
  }).init(init)
  await scheduled()
  assert.equal(fetches[0].since, now - 7 * 86400)
  assert.equal(fetches[0].until, now)
})

test('ack by A does not prevent first-open historical delivery to B on the same channel', async t => {
  const now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  const event = { id: 'shared', kind: 9, pubkey: 'peer', created_at: now - 100, tags: [], content: 'hello' }
  const seenReceivers = []
  const timers = []
  const create = owner => new PrivateMessenger({
    _privateMessage: fakePrivateMessage(), _setTimeout: fn => { timers.push(fn) },
    offlineRecoverySeconds: 600,
    _privateChannel: {
      fetchHistory: async options => {
        assert.equal(options.since, now - 600)
        seenReceivers.push(options.receiverPubkey)
        await options.onEvent(event, { created_at: event.created_at }, { channelPubkey: 'channel', senderPubkey: 'peer' })
        return []
      }
    }
  }).init({ userSigner: signer(owner), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  const a = await create('A')
  await timers.shift()()
  const deliveryA = await a.nextMessage()
  assert.equal(await deliveryA.ack(), true)
  const b = await create('B')
  await timers.shift()()
  const deliveryB = await b.nextMessage()
  assert.equal(deliveryB.message.event.id, event.id)
  assert.equal(await a.nextMessage(), null)
  assert.deepEqual(seenReceivers, ['A', 'B'])
  assert.equal(await deliveryB.ack(), true)
})

test('incomplete fetch reaches onError unchanged and remains pending until a successful retry', async t => {
  const now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  let scheduled
  let fail = true
  const reason = new Error('GET_EVENTS_TIMEOUT')
  const failure = Object.assign(new AggregateError([reason], 'PRIVATE_CHANNEL_FETCH_INCOMPLETE: wss://relay.example [timeout]'), {
    code: 'PRIVATE_CHANNEL_FETCH_INCOMPLETE', relays: [{ relay: 'wss://relay.example', status: 'timeout', error: reason }]
  })
  const errors = []
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(), _setTimeout: fn => { scheduled = fn },
    onError: error => errors.push(error),
    _privateChannel: { fetchHistory: async () => { if (fail) throw failure; return [] } }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  await scheduled()
  assert.equal(errors[0], failure)
  let state = (await messenger.stateStore.load()).channel
  assert.equal(state.recoveredThrough, 0)
  assert.deepEqual(state.offlineRanges, [{ start: now - 7 * 86400, end: now }])
  fail = false
  await messenger.recoverOfflineRanges(['channel'])
  state = (await messenger.stateStore.load()).channel
  assert.equal(state.recoveredThrough, now)
  assert.deepEqual(state.offlineRanges, [])
})

test('failed reload-gap recovery schedules an automatic retry', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  let fail = true
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    onError: () => {},
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: { fetchHistory: async () => { if (fail) throw Object.assign(new Error('relay down'), { category: 'transport' }); return [] } }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })

  const first = timers.at(-1)
  await first.fn()
  assert.ok(messenger.readState().channels.channel.offlineRanges.length)
  const retry = timers.at(-1)
  assert.notEqual(retry, first)
  assert.ok(retry.delay > 0)

  fail = false
  await retry.fn()
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
})

test('prioritized ranges resolve defaults from persisted recovery ranges', async () => {
  const channel = '11'.repeat(32)
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({ [channel]: { offlineRanges: [{ start: now - 1000, end: now - 100 }] } })
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _privateChannel: { fetchHistory: async () => [] }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(await messenger.prioritizeRange(channel, { since: now - 500 }), true)
  const [unread] = messenger.recoveryPriorities(channel)
  assert.equal(unread.type, 'unread-page')
  assert.equal(unread.start, now - 500)
  assert.ok(unread.end >= now - 500 && unread.end <= Math.floor(Date.now() / 1000) + 1)

  assert.equal(await messenger.prioritizeRange(channel, { type: 'tail' }), true)
  const tail = messenger.recoveryPriorities(channel).find(entry => entry.type === 'tail')
  assert.equal(tail.start, tail.end - 6 * 3600)
})

test('prioritized range input is validated and requires persisted ranges', async () => {
  const channel = '11'.repeat(32)
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _privateChannel: { fetchHistory: async () => [] }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: ['wss://relay.example'], offlineRecoverySeconds: 0 }]
  })

  await assert.rejects(() => messenger.prioritizeRange('nope', { since: 1 }), /INVALID_CHANNEL_PUBKEY/)
  await assert.rejects(() => messenger.prioritizeRange(channel, { since: 1, type: 'kind' }), /INVALID_PRIORITY_TYPE/)
  await assert.rejects(() => messenger.prioritizeRange(channel, { type: 'unread-page' }), /PRIORITY_SINCE_REQUIRED/)
  await assert.rejects(() => messenger.prioritizeRange(channel, { since: 1.5 }), /INVALID_PRIORITY_RANGE/)
  assert.equal(await messenger.prioritizeRange(channel, { since: 1 }), false)
})

test('prioritized ranges expire by ttl and clear with recovery state', async t => {
  const channel = '11'.repeat(32)
  const now = Math.floor(Date.now() / 1000)
  let nowMs = now * 1000
  t.mock.method(Date, 'now', () => nowMs)
  await seedMessengerState({ [channel]: { offlineRanges: [{ start: now - 100, end: now - 50 }] } })
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _privateChannel: { fetchHistory: async () => [] }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: ['wss://relay.example'] }]
  })

  assert.equal(await messenger.prioritizeRange(channel, { since: now - 60, ttlMs: 1000 }), true)
  assert.equal(messenger.recoveryPriorities(channel).length, 1)
  nowMs += 2000
  assert.deepEqual(messenger.recoveryPriorities(channel), [])

  assert.equal(await messenger.prioritizeRange(channel, { since: now - 60, ttlMs: 60000 }), true)
  await messenger.unwatch([channel])
  assert.deepEqual(messenger.recoveryPriorities(channel), [])
})

test('priority lane fetches the requested window before older history', async () => {
  const channel = '11'.repeat(32)
  const relay = 'wss://relay.example'
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({ [channel]: { offlineRanges: [{ start: now - 1000, end: now - 100 }] } })
  const fetches = []
  const partial = args => {
    const url = args.relays[0]
    const intervals = args.resume?.[url] || [{ start: args.since, end: args.until }]
    return {
      oldestCreatedAt: intervals[0]?.start ?? args.since,
      receivedEventCount: 1,
      elapsedMs: 5,
      relays: [{ relay: url, status: 'eose', covered: intervals, pending: [], events: 1 }],
      pendingByRelay: {},
      anyEose: true,
      anyEoseWithEvents: true,
      allFailed: false
    }
  }
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _privateChannel: { fetchHistory: async args => { fetches.push(args); return partial(args) } }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: [relay] }]
  })

  const range = messenger.readState().channels[channel].offlineRanges[0]
  const since = range.end - 100
  assert.equal(await messenger.prioritizeRange(channel, { since, ttlMs: 60000 }), true)
  await messenger.runPriorityLane(channel, messenger.channels.get(channel), [range])

  const record = [...messenger.recoveryRanges.values()].find(entry => entry.channelPubkey === channel)
  assert.ok(record.covered.length)
  assert.ok([...record.relays.values()].every(state => state.attempts === 0))
  assert.equal(fetches[0].since, since)
  const priorityCoverage = record.covered[0]

  fetches.length = 0
  await messenger.recoverOfflineRanges([channel])
  assert.ok(fetches.length > 0)
  const overlapsPriority = fetches.some(args => {
    const url = args.relays[0]
    const intervals = args.resume?.[url] || [{ start: args.since, end: args.until }]
    return intervals.some(interval => interval.start <= priorityCoverage.end && interval.end >= priorityCoverage.start)
  })
  assert.equal(overlapsPriority, false)
  assert.deepEqual(messenger.readState().channels[channel].offlineRanges, [])
})

test('priority hedge starts the next relay after the delay and aborts the loser', async () => {
  const channel = '11'.repeat(32)
  const [first, second] = ['wss://a.example', 'wss://b.example']
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({ [channel]: { offlineRanges: [{ start: now - 1000, end: now - 100 }] } })
  const calls = []
  let aborted = false
  const partial = (url, intervals) => ({
    oldestCreatedAt: intervals[0]?.start ?? 0,
    receivedEventCount: 1,
    elapsedMs: 5,
    relays: [{ relay: url, status: 'eose', covered: intervals, pending: [], events: 1 }],
    pendingByRelay: {},
    anyEose: true,
    anyEoseWithEvents: true,
    allFailed: false
  })
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    priorityHedgeDelayMs: 5,
    _privateChannel: {
      fetchHistory: async args => {
        const url = args.relays[0]
        calls.push(url)
        const intervals = args.resume?.[url] || [{ start: args.since, end: args.until }]
        if (url === first && calls.filter(value => value === first).length === 1) {
          return await new Promise((resolve, reject) => {
            args.signal.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')) }, { once: true })
          })
        }
        return partial(url, intervals)
      }
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: [first, second] }]
  })

  const range = messenger.readState().channels[channel].offlineRanges[0]
  assert.equal(await messenger.prioritizeRange(channel, { since: range.end - 100, ttlMs: 60000 }), true)
  await messenger.runPriorityLane(channel, messenger.channels.get(channel), [range])

  assert.deepEqual(calls.slice(0, 2), [first, second])
  assert.equal(aborted, true)
})

test('priority seeder fallback requires recent presence', async () => {
  const channel = '11'.repeat(32)
  const seeder = '22'.repeat(32)
  const now = Math.floor(Date.now() / 1000)
  const failingFetch = async args => {
    const url = args.relays[0]
    const pending = args.resume?.[url] || [{ start: args.since, end: args.until }]
    return {
      oldestCreatedAt: null,
      receivedEventCount: 0,
      elapsedMs: 5,
      relays: [{ relay: url, status: 'timeout', covered: [], pending, events: 0 }],
      pendingByRelay: { [url]: pending },
      anyEose: false,
      anyEoseWithEvents: false,
      allFailed: true
    }
  }
  const run = async lastActiveAt => {
    await seedMessengerState({
      [channel]: {
        offlineRanges: [{ start: now - 1000, end: now - 100 }],
        seederActivity: { [seeder]: { lastActiveAt, announcedAt: lastActiveAt } }
      }
    })
    const pm = fakePrivateMessage()
    const messenger = await new PrivateMessenger({
      _privateMessage: pm,
      _privateChannel: { fetchHistory: failingFetch }
    }).init({
      userSigner: signer('user'),
      channels: [{ pubkey: channel, signer: signer('channel'), relays: ['wss://relay.example'], seeders: [seeder] }]
    })
    const range = messenger.readState().channels[channel].offlineRanges[0]
    await messenger.prioritizeRange(channel, { since: range.end - 100, ttlMs: 60000 })
    await messenger.runPriorityLane(channel, messenger.channels.get(channel), [range])
    return {
      asks: pm.sent.filter(entry => entry.method === 'ask'),
      record: [...messenger.recoveryRanges.values()].find(entry => entry.channelPubkey === channel)
    }
  }

  assert.equal((await run(now - 3600)).asks.length, 0)
  const recent = await run(now)
  assert.equal(recent.asks.length, 1)
  assert.ok(recent.asks[0].options.payload.since <= now && recent.asks[0].options.payload.until >= recent.asks[0].options.payload.since)
  assert.ok(recent.record.claimed.length)
})

test('recovery relay ordering prefers the most reliable relay', async () => {
  const channel = '11'.repeat(32)
  const [slow, fast] = ['wss://slow.example', 'wss://fast.example']
  const now = Math.floor(Date.now() / 1000)
  await seedMessengerState({ [channel]: { offlineRanges: [{ start: now - 1000, end: now - 100 }] } })
  const calls = []
  const partial = args => {
    const results = args.relays.map(relay => {
      const pending = args.resume?.[relay] || [{ start: args.since, end: args.until }]
      return { relay, status: 'timeout', covered: [], pending, events: 0, elapsedMs: relay === slow ? 5000 : 20 }
    })
    return {
      oldestCreatedAt: null,
      receivedEventCount: 0,
      elapsedMs: results.reduce((sum, entry) => sum + entry.elapsedMs, 0),
      relays: results,
      pendingByRelay: Object.fromEntries(results.map(entry => [entry.relay, entry.pending])),
      anyEose: false,
      anyEoseWithEvents: false,
      allFailed: true
    }
  }
  const messenger = await new PrivateMessenger({
    _privateMessage: fakePrivateMessage(),
    _privateChannel: {
      fetchHistory: async args => {
        calls.push(args.relays[0])
        return partial(args)
      }
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: channel, signer: signer('channel'), relays: [slow, fast] }]
  })

  await messenger.recoverOfflineRanges([channel])
  assert.equal(calls[0], slow)
  calls.length = 0
  await messenger.recoverOfflineRanges([channel])
  assert.equal(calls[0], fast)
})

test('partial recovery retries the failing relay then hands the left edge to seeders', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  const fetches = []
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    onError: () => {},
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: {
      fetchHistory: async args => {
        fetches.push(args)
        const results = []
        const healthy = args.relays.find(relay => relay.includes('a.example'))
        const failing = args.relays.find(relay => relay.includes('b.example'))
        if (healthy) results.push({ relay: healthy, status: 'eose', covered: [{ start: args.since, end: args.until }], pending: [], events: 1 })
        if (failing) results.push({ relay: failing, status: 'timeout', covered: [], pending: [{ start: args.since, end: args.until }], events: 0 })
        const anyEose = results.some(entry => entry.status === 'eose')
        return {
          oldestCreatedAt: now - 5,
          receivedEventCount: results.reduce((sum, entry) => sum + entry.events, 0),
          elapsedMs: 1,
          relays: results,
          pendingByRelay: Object.fromEntries(results.filter(entry => entry.pending.length).map(entry => [entry.relay, entry.pending])),
          anyEose,
          anyEoseWithEvents: results.some(entry => entry.status === 'eose' && entry.events > 0),
          allFailed: !anyEose
        }
      }
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://a.example', 'wss://b.example'], seeders: ['seeder'] }]
  })

  const callback = pm.watchCalls[0].onSubscriptionState
  await callback({ state: 'interrupted', relay: 'wss://a.example', since: now - 10 })
  await callback({ state: 'ready', relay: 'wss://a.example', until: now })

  const limit = RECOVERY_RELAY_RETRY_LIMITS.partial
  let timer = timers.at(-1)
  for (let attempt = 0; attempt < limit; attempt++) {
    await timer.fn()
    if (attempt < limit - 1) timer = timers.at(-1)
  }

  assert.equal(fetches.length, limit)
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
  assert.equal(messenger.recoveryRanges.size, 0)
  const asks = pm.sent.filter(entry => entry.method === 'ask')
  assert.equal(asks.length, 1)
  assert.equal(asks[0].options.payload.until, now - 5)
  assert.ok(asks[0].options.payload.since < now - 5)
})

test('all-failed relay attempts exhaust the larger budget before the seeder handoff', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  const fetches = []
  const now = Math.floor(Date.now() / 1000)
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    onError: () => {},
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: {
      fetchHistory: async args => {
        fetches.push(args)
        const relay = args.relays[0]
        const pending = [{ start: args.since, end: args.until }]
        return {
          oldestCreatedAt: null,
          receivedEventCount: 0,
          elapsedMs: 1,
          relays: [{ relay, status: 'timeout', covered: [], pending, events: 0 }],
          pendingByRelay: { [relay]: pending },
          anyEose: false,
          anyEoseWithEvents: false,
          allFailed: true
        }
      }
    }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], seeders: ['seeder'] }]
  })

  const callback = pm.watchCalls[0].onSubscriptionState
  await callback({ state: 'interrupted', relay: 'wss://relay.example', since: now - 10 })
  await callback({ state: 'ready', relay: 'wss://relay.example', until: now })
  const range = messenger.readState().channels.channel.offlineRanges[0]

  const limit = RECOVERY_RELAY_RETRY_LIMITS.allFailed
  let timer = timers.at(-1)
  for (let attempt = 0; attempt < limit; attempt++) {
    await timer.fn()
    if (attempt < limit - 1) timer = timers.at(-1)
  }

  assert.equal(fetches.length, limit)
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
  const asks = pm.sent.filter(entry => entry.method === 'ask')
  assert.equal(asks.length, 1)
  assert.deepEqual(asks[0].options.payload, { since: range.start, until: range.end })
})

test('offline recovery leaves ranges and budgets untouched', async () => {
  const now = Math.floor(Date.now() / 1000)
  const range = { start: now - 100, end: now - 50 }
  await seedMessengerState({ channel: { offlineRanges: [range] } })
  const pm = fakePrivateMessage()
  const fetches = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm,
    onError: () => {},
    _isOnline: async () => false,
    _privateChannel: { fetchHistory: async args => { fetches.push(args); return [] } }
  }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://relay.example'], seeders: ['seeder'] }]
  })

  const pending = messenger.readState().channels.channel.offlineRanges
  await messenger.recoverOfflineRanges(['channel'])
  assert.equal(fetches.length, 0)
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, pending)
  assert.equal(pm.sent.filter(entry => entry.method === 'ask').length, 0)
})

test('outgoing seeder capture precedes publication and does not require a network echo', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm }).init({
    userSigner: signer('user'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://recipient-only.example'], mode: 'seeder' }]
  })
  const seed = { channelPubkey: 'channel', router: { kind: 26300, pubkey: 'router', created_at: Math.floor(Date.now() / 1000), tags: [['f', 'user'], ['p', 'recipient']], content: jsonlContent(payloadRow(), JSON.stringify(['recipient', 'ciphertext'])) } }
  pm.broadcastRumor = async options => {
    await options.onPreparedSeed(seed)
    assert.equal(await messenger.seedQueue.some(item => item.receiverPubkey === 'recipient'), true)
    return { delivery: { reports: [{ success: true }] } }
  }
  await messenger.broadcastRumor({ receiverPubkeys: ['recipient'], rumor: { kind: 9, tags: [], content: 'hello' } })
  await messenger.enqueueSeed('channel', seed)
  assert.equal((await Array.fromAsync(messenger.seedQueue.storedItems())).length, 1, 'echo deduplicates against the local ciphertext')
  await messenger.close()
})

test('fallback relay configuration validates and snapshots normalized URLs without fixing channels', async () => {
  for (const value of [null, 'wss://fallback.example', {}]) assert.throws(() => new PrivateMessenger({ fallbackRelays: value }), { code: 'INVALID_FALLBACK_RELAYS' })
  assert.throws(() => new PrivateMessenger({ fallbackRelays: Array(1) }), { code: 'INVALID_RELAY_URL' })
  assert.throws(() => new PrivateMessenger({ fallbackRelays: ['ftp://fallback.example'] }), { code: 'INVALID_RELAY_PROTOCOL' })
  const fallbackRelays = ['wss://FALLBACK.example/', 'wss://fallback.example']
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({
    fallbackRelays, _privateMessage: pm,
    _getRelaysByPubkey: async pubkeys => Object.fromEntries(pubkeys.map(pubkey => [pubkey, { read: [`wss://${pubkey}.example`], write: [] }]))
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel') }] })
  fallbackRelays.push('wss://changed.example')
  assert.deepEqual(messenger.fallbackRelays, ['wss://fallback.example'])
  assert.equal(messenger.channels.get('channel').usesNip65WatchRelays, true)
  assert.deepEqual(pm.watchCalls[0].relays, ['wss://user.example', 'wss://fallback.example'])
  await messenger.tell({ receiverPubkey: 'peer', relays: ['wss://explicit.example'], payload: 'explicit' })
  assert.deepEqual(pm.sent[0].options.relays, ['wss://explicit.example'])
  assert.equal(typeof pm.sent[0].options._publish, 'function', 'explicit per-call relays retain the fallback publisher')
})

test('fixed relay fallbacks preserve the receiver set and do not consult recipient NIP-65 lists', async () => {
  for (const source of ['global', 'channel', 'send', 'call']) {
    const pm = fakePrivateMessage()
    const primary = ['wss://fixed-one.example', 'wss://fixed-two.example', 'wss://fixed-three.example']
    const messenger = await new PrivateMessenger({
      fallbackRelays: ['wss://fallback.example'], _privateMessage: pm,
      _getRelaysByPubkey: async pubkeys => {
        assert.deepEqual(pubkeys, ['user'], 'only automatic receive routing discovers the owner relays')
        return { user: { read: ['wss://read.example'], write: [] } }
      }
    }).init({
      userSigner: signer('user'),
      ...(source === 'global' ? { relays: primary } : {}),
      channels: [{ signer: signer(`channel-${source}`), ...(source === 'channel' ? { relays: primary } : {}), ...(source === 'send' ? { sendRelays: primary } : {}) }]
    })
    await messenger.broadcastRumor({ receiverPubkeys: ['alice', 'bob'], ...(source === 'call' ? { relays: primary } : {}), rumor: { kind: 9, created_at: 1, tags: [], content: 'fixed destinations' } })
    assert.deepEqual(pm.sent[0].options.receiverPubkeys, ['alice', 'bob'])
    assert.deepEqual(pm.sent[0].options.relays, primary)
    assert.equal(pm.sent[0].options.relayToReceivers, undefined)
    assert.equal(typeof pm.sent[0].options._publish, 'function')
    assert.deepEqual(pm.watchCalls[0].relays, [...(['global', 'channel'].includes(source) ? primary : ['wss://read.example']), 'wss://fallback.example'])
  }
})

test('automatic group routing installs a fallback publisher without changing the initial recipient pairs', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({
    fallbackRelays: ['wss://fallback.example'], _privateMessage: pm,
    _getRelaysByPubkey: async () => ({
      user: { read: ['wss://owner.example'] },
      alice: { read: ['wss://shared.example', 'wss://alice.example', 'wss://alice-other.example'] },
      bob: { read: ['wss://shared.example', 'wss://bob.example', 'wss://bob-other.example'] }
    })
  }).init({ userSigner: signer('user'), channels: [{ pubkey: 'channel', signer: signer('channel') }] })
  await messenger.broadcastRumor({ receiverPubkeys: ['alice', 'bob'], rumor: { kind: 9, created_at: 1, tags: [], content: 'group' } })
  assert.equal(typeof pm.sent[0].options._publish, 'function')
  assert.deepEqual([...pm.sent[0].options.relayToReceivers], [
    ['wss://shared.example', ['alice', 'bob']],
    ['wss://alice.example', ['alice']],
    ['wss://bob.example', ['bob']]
  ])
})

test('nym recipient maps derive fallback coverage without a separate receiverPubkeys option', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, fallbackRelays: ['wss://fallback.example'] }).init({
    userSigner: signer('user'), nymSigner: signer('nym'),
    channels: [{ pubkey: 'channel', signer: signer('channel'), relays: ['wss://channel.example'] }]
  })
  const relayToReceivers = new Map([['wss://bob.example', ['bob']], ['wss://alice.example', ['alice']]])
  await messenger.broadcastNymRumor({ relayToReceivers, rumor: { kind: 9, created_at: 1, tags: [], content: 'nym group' } })
  assert.equal(pm.sent[0].options.relayToReceivers, relayToReceivers)
  assert.equal(typeof pm.sent[0].options._publish, 'function')
  assert.ok(messenger.sendRelayExclusions.has('channel:alice,bob'))
})

test('paged recovery persists partial deliveries but retains the whole interval across restart', async t => {
  const now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  let scheduled
  let rejectPage = true
  const rows = Array.from({ length: 35 }, (_, index) => ({ id: `page-${index}`, kind: 9, pubkey: 'peer', created_at: now - 40 + index, tags: [], content: String(index) }))
  const init = { userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] }
  const reads = []
  const channel = {
    fetchHistory: options => readHistory({
      filter: { since: options.since, until: options.until }, relays: options.relays, signal: options.signal,
      acquirePage: options._acquirePage,
      getEvents: async (filter, [relay]) => {
        reads.push(filter)
        const result = rows.filter(event => event.created_at >= filter.since && event.created_at <= filter.until).reverse().slice(0, filter.limit).map(event => ({ event, relay }))
        return { result, relays: [{ relay, status: result.length === filter.limit ? 'satisfied' : 'eose' }] }
      },
      processEvent: async event => {
        if (rejectPage && event.id === 'page-20') throw new Error('PERSISTENCE_FAILED')
        await options.onEvent(event, { created_at: event.created_at }, { channelPubkey: 'channel', senderPubkey: 'peer' })
      }
    })
  }
  const make = indexedDB => new PrivateMessenger({ _indexedDB: indexedDB, _privateMessage: fakePrivateMessage(), _privateChannel: channel, offlineRecoverySeconds: 60, _setTimeout: fn => { scheduled = fn }, onError: () => {} }).init(init)
  const first = await make(globalThis.indexedDB)
  await scheduled()
  const delivery = await first.nextMessage()
  assert.equal(delivery.message.event.id, 'page-0')
  await delivery.ack()
  const snapshot = await first.stateStore.load()
  assert.equal(snapshot.channel.recoveredThrough, 0)
  assert.deepEqual(snapshot.channel.offlineRanges, [{ start: now - 60, end: now }])
  assert.ok(reads.length > 1)
  const database = new IDBFactory()
  const state = await createChannelStateStore({ prefix: first.prefix, indexedDB: database })
  await state.update(snapshot)
  await state.close()
  rejectPage = false
  reads.length = 0
  const second = await make(database)
  await scheduled()
  assert.equal(reads[0].since, now - 60)
  const recovered = await second.stateStore.load()
  assert.equal(recovered.channel.recoveredThrough, now)
  assert.deepEqual(recovered.channel.offlineRanges, [])
})

test('event reply packer bounds UTF-8 bytes, sends oversized records alone and preserves order', async () => {
  const replies = []
  const events = Array.from({ length: 10 }, (_, index) => ({ id: String(index), content: 'á'.repeat(index === 4 ? 200 : 20) }))
  const packer = createEventReplyPacker({
    messenger: { reply: async ({ payload }) => replies.push(payload) },
    question: { id: 'question', pubkey: 'peer' }, code: 'test', bytesPerChunk: 160,
    recordsFromInput: event => [event]
  })
  for (const event of events) await packer.update(event)
  await packer.finalize()
  const actual = []
  for (const reply of replies) {
    const records = reply.jsonl.trim().split('\n').filter(Boolean).map(line => JSON.parse(line))
    assert.ok(Buffer.byteLength(reply.jsonl) <= 160 || records.length === 1)
    actual.push(...records)
  }
  assert.deepEqual(actual, events)
  assert.equal(replies.at(-1).isLast, true)
  assert.deepEqual(replies.map(reply => reply.index), replies.map((_, index) => index))
})

test('reply packer keeps failed final payload and index for retry', async () => {
  const failure = new Error('quota')
  let failing = true
  const replies = []
  const packer = createEventReplyPacker({
    messenger: { reply: async ({ payload }) => { replies.push(payload); if (failing) throw failure } },
    question: { id: 'question', pubkey: 'peer' }, code: 'test', recordsFromInput: event => [event]
  })
  await packer.update({ id: 'one' })
  await assert.rejects(packer.finalize(), error => error === failure)
  failing = false
  await packer.finalize()
  assert.deepEqual(replies[1], replies[0])
  await packer.finalize()
  assert.equal(replies.length, 2)
})

test('overflow persists the lost interval before readiness, then retries transient history without losing live progress', async t => {
  let now = 1800000000
  t.mock.method(Date, 'now', () => now * 1000)
  const pm = fakePrivateMessage()
  const timers = []
  const fetches = []
  let fail = false
  const messenger = await new PrivateMessenger({
    _privateMessage: pm, onError: () => {},
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer },
    _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: {
      fetchHistory: async options => {
        fetches.push(options)
        if (fail) throw Object.assign(new Error('temporary failure'), { category: 'transport' })
        return []
      }
    }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  await timers[0].fn()
  now += 60
  const callback = pm.watchCalls[0].onSubscriptionState
  await callback({ state: 'interrupted', relay: 'wss://relay.example', since: now - 30 })
  const before = await messenger.stateStore.load()
  assert.ok(before.channel.offlineRanges[0].start <= now - 30)
  assert.ok(before.channel.openOfflineStart <= now - 30)
  await pm.watchCalls[0].onMessage({ event: { id: 'live', kind: 9, pubkey: 'peer', created_at: now, tags: [], content: '' }, outer: { created_at: now }, senderPubkey: 'peer' })
  fail = true
  await callback({ state: 'ready', relay: 'wss://relay.example', until: now })
  await timers.at(-1).fn()
  assert.ok(messenger.readState().channels.channel.offlineRanges.length)
  assert.equal(messenger.readState().channels.channel.recoveredThrough, now - 60)
  assert.ok(timers.at(-1).delay >= 800)
  fail = false
  await timers.at(-1).fn()
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
  assert.equal(messenger.readState().channels.channel.recoveredThrough, now)
  assert.ok(fetches.at(-1).since <= now - 30)
})

test('overflow gap persistence rejection blocks readiness until storage recovers', async t => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, onError: () => {}, _setTimeout: () => null }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  const update = messenger.stateStore.update.bind(messenger.stateStore)
  t.mock.method(messenger.stateStore, 'update', async () => { throw new Error('storage unavailable') })
  const interrupted = { state: 'interrupted', relay: 'wss://relay.example', since: Math.floor(Date.now() / 1000) - 20 }
  await assert.rejects(pm.watchCalls[0].onSubscriptionState(interrupted), /storage unavailable/)
  messenger.stateStore.update = update
  await pm.watchCalls[0].onSubscriptionState(interrupted)
  assert.ok((await messenger.stateStore.load()).channel.offlineRanges.length)
})

test('dense-page limits remain pending without an automatic recovery loop; unwatch cancels retries', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  const messenger = await new PrivateMessenger({
    _privateMessage: pm, onError: () => {},
    _setTimeout: (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer }, _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: { fetchHistory: async () => { throw Object.assign(new Error('dense page'), { code: 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT' }) } }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  const callback = pm.watchCalls[0].onSubscriptionState
  const now = Math.floor(Date.now() / 1000)
  await callback({ state: 'interrupted', relay: 'wss://relay.example', since: now - 10 })
  await callback({ state: 'ready', relay: 'wss://relay.example', until: now })
  const timer = timers.at(-1)
  await timer.fn()
  assert.equal(timers.at(-1), timer)
  assert.ok(messenger.readState().channels.channel.offlineRanges.length)
  await callback({ state: 'interrupted', relay: 'wss://relay.example', since: now - 10 })
  await callback({ state: 'ready', relay: 'wss://relay.example', until: now })
  const cancelled = timers.at(-1)
  await messenger.unwatch()
  assert.equal(cancelled.cancelled, true)
  await cancelled.fn()
  assert.equal(messenger.liveRecoveryTimers.size, 0)
})

test('disabled historical recovery ignores subscription gaps and leaves live recovery to the session', async () => {
  const pm = fakePrivateMessage()
  const messenger = await new PrivateMessenger({ _privateMessage: pm, offlineRecoverySeconds: 0 }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  await pm.watchCalls[0].onSubscriptionState({ state: 'interrupted', relay: 'wss://relay.example', since: 0 })
  await pm.watchCalls[0].onSubscriptionState({ state: 'ready', relay: 'wss://relay.example', until: Math.floor(Date.now() / 1000) })
  assert.equal(messenger.liveInterruptions.size, 0)
  assert.equal(messenger.liveRecoveryTimers.size, 0)
})

test('overflow fences an in-flight history checkpoint and readiness starts a fresh scan after cancellation', async () => {
  const pm = fakePrivateMessage()
  const timers = []
  const entered = Promise.withResolvers()
  const release = Promise.withResolvers()
  let calls = 0
  let cancelledSignal
  const messenger = await new PrivateMessenger({
    _privateMessage: pm, onError: () => {},
    _setTimeout: fn => { const timer = { fn }; timers.push(timer); return timer }, _clearTimeout: timer => { timer.cancelled = true },
    _privateChannel: {
      fetchHistory: async options => {
        if (++calls === 1) { cancelledSignal = options.signal; entered.resolve(); await release.promise }
        return []
      }
    }
  }).init({ userSigner: signer('user'), channels: [{ signer: signer('channel'), relays: ['wss://relay.example'] }] })
  const initial = timers[0].fn()
  await entered.promise
  const now = Math.floor(Date.now() / 1000)
  await pm.watchCalls[0].onSubscriptionState({ state: 'interrupted', relay: 'wss://relay.example', since: now - 10 })
  assert.equal(cancelledSignal.aborted, true)
  await pm.watchCalls[0].onSubscriptionState({ state: 'ready', relay: 'wss://relay.example', until: now })
  const retry = timers.at(-1).fn()
  assert.equal(calls, 1)
  release.resolve()
  await initial
  await retry
  assert.equal(calls, 2)
  assert.deepEqual(messenger.readState().channels.channel.offlineRanges, [])
})
