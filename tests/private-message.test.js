import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { finalizeEvent, generateSecretKey, getEventHash } from 'nostr-tools'
import {
  ASK_KIND,
  REPLY_KIND,
  TELL_KIND,
  broadcastEvent,
  broadcastNymEvent,
  broadcastNymRumor,
  ask,
  broadcastRumor,
  parseRumorContent,
  reply,
  tell,
  unwatch,
  watch,
  yell
} from '../private-message/index.js'
import { keypairFromSeckey } from '../key/index.js'

const data = new Map()
globalThis.localStorage = {
  clear: () => data.clear(),
  getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
  removeItem: key => { data.delete(String(key)) },
  setItem: (key, value) => { data.set(String(key), String(value)) }
}

afterEach(() => {
  unwatch()
  globalThis.localStorage.clear()
})

function signer (pubkey) {
  return {
    getPublicKey: () => pubkey,
    withSharedKey: () => ({})
  }
}

function pubkeyFixture (index) {
  return index.toString(16).padStart(64, '0')
}

function fakeSubscribeFactory () {
  const calls = []
  const closed = []
  const fakeSubscribe = options => {
    calls.push(options)
    return {
      close: () => closed.push(options)
    }
  }
  return { calls, closed, fakeSubscribe }
}

test('watch merges overlapping relay subscriptions by channel author', async () => {
  const { calls, closed, fakeSubscribe } = fakeSubscribeFactory()
  const channel1 = signer('channel1')
  const reader1 = signer('reader1')
  await watch({
    channels: ['channel1'],
    relays: ['wss://a.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: channel1,
    privateChannelReaderSigner: reader1,
    privateChannelReaderPubkey: 'reader1',
    _subscribe: fakeSubscribe
  })
  await watch({
    channels: ['channel1'],
    relays: ['wss://a.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: channel1,
    privateChannelReaderSigner: reader1,
    privateChannelReaderPubkey: 'reader1',
    _subscribe: fakeSubscribe
  })
  await watch({
    channels: ['channel2'],
    relays: ['wss://a.example', 'wss://b.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: signer('channel2'),
    privateChannelReaderSigner: signer('reader2'),
    privateChannelReaderPubkey: 'reader2',
    mode: 'seeder',
    _subscribe: fakeSubscribe
  })

  assert.equal(calls.length, 3)
  assert.deepEqual(calls[0].privateChannelPubkeys, ['channel1'])
  assert.deepEqual(calls[0].relays, ['wss://a.example'])
  assert.equal(calls[0].limit, 0)
  assert.equal(calls[0].liveOnly, true)
  assert.deepEqual(calls[1].privateChannelPubkeys.sort(), ['channel1', 'channel2'])
  assert.deepEqual(calls[1].relays, ['wss://a.example'])
  assert.deepEqual(calls[1].modeByPubkey, { channel1: 'leecher', channel2: 'seeder' })
  assert.deepEqual(Object.fromEntries(Object.entries(calls[1].privateChannelReaderSignersByPubkey).map(([channel, nextSigner]) => [channel, nextSigner.getPublicKey()])), {
    channel1: 'reader1',
    channel2: 'reader2'
  })
  assert.deepEqual(calls[1].privateChannelReaderPubkeysByPubkey, {
    channel1: 'reader1',
    channel2: 'reader2'
  })
  assert.deepEqual(calls[2].privateChannelPubkeys, ['channel2'])
  assert.deepEqual(calls[2].relays, ['wss://b.example'])

  await new Promise(resolve => setTimeout(resolve, 550))
  assert.equal(closed.length, 1)
  assert.deepEqual(closed[0].privateChannelPubkeys, ['channel1'])
})

test('watch refresh keeps shared relay subscriptions and gracefully closes removed relays', async () => {
  const { calls, closed, fakeSubscribe } = fakeSubscribeFactory()
  await watch({
    channels: ['channel1'],
    relays: ['wss://a.example', 'wss://b.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: signer('channel1'),
    _subscribe: fakeSubscribe
  })
  await watch({
    channels: ['channel1'],
    relays: ['wss://b.example', 'wss://c.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: signer('channel1'),
    _subscribe: fakeSubscribe
  })

  assert.equal(calls.length, 3)
  assert.deepEqual(calls.map(call => call.relays[0]), ['wss://a.example', 'wss://b.example', 'wss://c.example'])
  assert.equal(closed.length, 0)

  await new Promise(resolve => setTimeout(resolve, 550))
  assert.equal(closed.length, 1)
  assert.deepEqual(closed[0].relays, ['wss://a.example'])
})

test('ask requires watching the sender private channel first', async () => {
  await assert.rejects(
    () => ask({
      senderSigner: signer('sender'),
      privateChannelSigner: signer('sender-channel'),
      receiverPubkey: 'receiver',
      relays: ['wss://relay.example'],
      message: { code: 'PING' },
      _publish: async () => []
    }),
    /PRIVATE_MESSAGE_NOT_WATCHING/
  )
})

test('ask from a reader-only channel requires a writer signer', async () => {
  const { fakeSubscribe } = fakeSubscribeFactory()
  await watch({
    channels: ['writer-channel'],
    relays: ['wss://relay.example'],
    receiverSigner: signer('sender'),
    privateChannelSigner: null,
    privateChannelReaderSigner: signer('reader'),
    _subscribe: fakeSubscribe
  })

  await assert.rejects(
    () => ask({
      senderSigner: signer('sender'),
      privateChannelSigner: null,
      receiverPubkey: 'receiver',
      relays: ['wss://relay.example'],
      message: { code: 'PING' },
      _publish: async () => []
    }),
    /PRIVATE_CHANNEL_WRITER_REQUIRED/
  )
})

test('watch dispatches content key usage callbacks', async () => {
  const { calls, fakeSubscribe } = fakeSubscribeFactory()
  const usages = []
  await watch({
    channels: ['channel1'],
    relays: ['wss://relay.example'],
    receiverSigner: signer('receiver'),
    privateChannelSigner: signer('channel1'),
    onContentKeyUsage: usage => usages.push(usage),
    _subscribe: fakeSubscribe
  })

  calls[0].onContentKeyUsage({
    channelPubkey: 'channel1',
    direction: 'sent',
    contentKeyPubkey: ''
  })

  assert.deepEqual(usages, [{ channelPubkey: 'channel1', direction: 'sent', contentKeyPubkey: '' }])
})

test('ask publishes an ask rumor and watch dispatches the reply with its question', async () => {
  const { calls, fakeSubscribe } = fakeSubscribeFactory()
  const replies = []
  let published = null
  const senderPubkey = pubkeyFixture(1)
  await watch({
    channels: ['sender-channel'],
    relays: ['wss://relay.example'],
    receiverSigner: signer(senderPubkey),
    privateChannelSigner: signer('sender-channel'),
    onReply: event => replies.push(event),
    _subscribe: fakeSubscribe
  })
  const result = await ask({
    senderSigner: signer(senderPubkey),
    privateChannelSigner: signer('sender-channel'),
    privateChannelReaderPubkey: 'reader-channel',
    receiverPubkey: 'receiver',
    relays: ['wss://relay.example'],
    temporaryStorageArea: globalThis.localStorage,
    message: { code: 'PING', payload: { ok: true } },
    _publish: async options => {
      published = options
      return [{ success: true }]
    }
  })

  assert.equal(published.event.kind, ASK_KIND)
  assert.equal(published.event.sig, undefined)
  assert.equal(published.event.id, undefined)
  assert.equal(published.event.pubkey, undefined)
  assert.equal(published.receiverTag, 'receiver')
  assert.deepEqual(published.receivers, ['receiver'])
  assert.equal(published.privateChannelReaderPubkey, 'reader-channel')
  assert.equal(published.temporaryStorageArea, globalThis.localStorage)
  assert.deepEqual(JSON.parse(published.event.content), { ok: true })
  assert.deepEqual(published.event.tags, [['r', 'receiver'], ['h', 'PING']])
  assert.equal(result.question.pubkey, senderPubkey)
  assert.equal(result.question.id, getEventHash({ ...published.event, pubkey: senderPubkey }))
  assert.deepEqual(result.delivery.reports, [{ success: true }])
  assert.equal(keypairFromSeckey(result.delivery.deletionSeckey).pubkey, published.deletionPubkey)
  assert.throws(() => getEventHash(published.event), /wrong or missing properties/)

  calls[0].onEvent({
    kind: REPLY_KIND,
    id: 'reply-id',
    pubkey: 'receiver',
    created_at: 1,
    tags: [['q', result.question.id]],
    content: 'pong'
  }, { created_at: 2 }, { channelPubkey: 'sender-channel' })

  assert.equal(replies.length, 1)
  assert.equal(replies[0].question, undefined)
  assert.equal(replies[0].questionId, result.question.id)
  assert.equal(replies[0].reply.pubkey, 'receiver')
  assert.deepEqual(replies[0].payload, { payload: 'pong' })
})

test('reply tell and yell publish recognizable private message rumors', async () => {
  const published = []
  const bobPubkey = pubkeyFixture(3)
  const _publish = async options => {
    published.push(options)
    return []
  }
  const question = {
    id: 'question-id',
    pubkey: 'alice',
    kind: ASK_KIND,
    created_at: 1,
    tags: [['r', 'bob']],
    content: '{}'
  }

  const replyResult = await reply({ senderSigner: signer(bobPubkey), question, relays: ['wss://relay.example'], payload: 'answer', _publish })
  const tellResult = await tell({ senderSigner: signer(bobPubkey), receiverPubkey: 'alice', relays: ['wss://relay.example'], payload: 'note', _publish })
  const yellResult = await yell({ senderSigner: signer(bobPubkey), receiverPubkeys: ['alice', 'carol'], relays: ['wss://relay.example'], payload: 'broadcast', _publish })
  const rawResult = await broadcastRumor({
    senderSigner: signer(bobPubkey),
    receiverPubkeys: ['alice', 'carol'],
    relays: ['wss://relay.example'],
    rumor: { kind: 9001, created_at: 22, tags: [['x', '1']], content: 'raw' },
    _publish
  })
  const signedEvent = finalizeEvent({ kind: 9002, created_at: 23, tags: [['x', '2']], content: 'signed' }, generateSecretKey())
  const signedResult = await broadcastEvent({
    senderSigner: signer(bobPubkey),
    receiverPubkeys: ['alice', 'carol'],
    relays: ['wss://relay.example'],
    event: signedEvent,
    _publish
  })

  assert.equal(published[0].event.kind, REPLY_KIND)
  assert.equal(published[0].event.id, undefined)
  assert.equal(published[0].event.pubkey, undefined)
  assert.equal(replyResult.reply.pubkey, bobPubkey)
  assert.equal(replyResult.reply.id, getEventHash({ ...published[0].event, pubkey: bobPubkey }))
  assert.deepEqual(published[0].event.tags, [['q', 'question-id'], ['r', 'alice']])
  assert.equal(published[0].receiverTag, 'alice')
  assert.equal(published[1].event.kind, TELL_KIND)
  assert.equal(tellResult.tell.id, getEventHash({ ...published[1].event, pubkey: bobPubkey }))
  assert.deepEqual(published[1].event.tags, [['r', 'alice']])
  assert.equal(published[1].receiverTag, 'alice')
  assert.equal(published[2].event.kind, TELL_KIND)
  assert.equal(yellResult.yell.id, getEventHash({ ...published[2].event, pubkey: bobPubkey }))
  assert.deepEqual(published[2].event.tags, [])
  assert.equal(published[2].receiverTag, '')
  assert.deepEqual(published[2].receivers, ['alice', 'carol'])
  assert.equal(published[3].event.kind, 9001)
  assert.equal(published[3].event.created_at, 22)
  assert.equal(published[3].event.content, 'raw')
  assert.equal(published[3].event.id, undefined)
  assert.equal(published[3].event.pubkey, undefined)
  assert.equal(published[3].event.sig, undefined)
  assert.deepEqual(published[3].event.tags, [['x', '1']])
  assert.equal(published[3].receiverTag, '')
  assert.deepEqual(published[3].receivers, ['alice', 'carol'])
  assert.equal(rawResult.rumor.pubkey, bobPubkey)
  assert.equal(rawResult.rumor.id, getEventHash({ ...published[3].event, pubkey: bobPubkey }))
  assert.deepEqual(published[4].event, signedEvent)
  assert.deepEqual(published[4].receivers, ['alice', 'carol'])
  assert.deepEqual(signedResult.event, signedEvent)
})

test('private message sends share one deletion capability and return delivery reports', async () => {
  const published = []
  const reports = [{ success: true, total: 1 }, { success: false, total: 1 }]
  const _publish = async options => {
    published.push(options)
    return reports
  }

  const generated = await tell({
    senderSigner: signer(pubkeyFixture(3)),
    receiverPubkey: 'alice',
    relays: ['wss://relay.example'],
    payload: 'managed',
    _publish
  })

  assert.deepEqual(generated.delivery.reports, reports)
  assert.equal(keypairFromSeckey(generated.delivery.deletionSeckey).pubkey, published[0].deletionPubkey)
  assert.equal('results' in generated, false)
  assert.equal('deletionPubkey' in generated, false)
  assert.equal('deletionSeckey' in generated, false)

  const supplied = await tell({
    senderSigner: signer(pubkeyFixture(3)),
    receiverPubkey: 'alice',
    relays: ['wss://relay.example'],
    payload: 'caller-managed',
    deletionPubkey: 'A'.repeat(64),
    _publish
  })
  assert.equal(published[1].deletionPubkey, 'a'.repeat(64))
  assert.deepEqual(supplied.delivery, { reports })

  const untagged = await tell({
    senderSigner: signer(pubkeyFixture(3)),
    receiverPubkey: 'alice',
    relays: ['wss://relay.example'],
    payload: 'unlinkable',
    autoDeletionCapability: false,
    _publish
  })
  assert.equal(published[2].deletionPubkey, undefined)
  assert.deepEqual(untagged.delivery, { reports })

  await assert.rejects(
    () => tell({
      senderSigner: signer(pubkeyFixture(3)),
      receiverPubkey: 'alice',
      relays: ['wss://relay.example'],
      payload: 'bad key',
      deletionPubkey: 'not-a-pubkey',
      _publish
    }),
    /INVALID_DELETION_PUBKEY/
  )
  await assert.rejects(
    () => tell({
      senderSigner: signer(pubkeyFixture(3)),
      receiverPubkey: 'alice',
      relays: ['wss://relay.example'],
      payload: 'bad secret',
      deletionSeckey: 'b'.repeat(64),
      _publish
    }),
    /DELETION_SECKEY_NOT_ACCEPTED/
  )
  await assert.rejects(
    () => tell({
      senderSigner: signer(pubkeyFixture(3)),
      receiverPubkey: 'alice',
      relays: ['wss://relay.example'],
      payload: 'bad policy',
      autoDeletionCapability: 'yes',
      _publish
    }),
    /AUTO_DELETION_CAPABILITY_BOOLEAN_REQUIRED/
  )
})

test('broadcastRumor validates normalized unsigned rumors before publishing', async () => {
  let published = false

  await assert.rejects(
    () => broadcastRumor({
      senderSigner: signer(pubkeyFixture(4)),
      receiverPubkeys: ['alice'],
      relays: ['wss://relay.example'],
      rumor: { kind: 9001, created_at: 22, tags: ['not-a-tag'], content: 'raw' },
      _publish: async () => { published = true }
    }),
    /INVALID_RUMOR/
  )

  assert.equal(published, false)
})

test('broadcastEvent refuses unsigned or invalid signed events', async () => {
  const valid = finalizeEvent({ kind: 9002, created_at: 23, tags: [], content: 'signed' }, generateSecretKey())
  let published = false

  await assert.rejects(
    () => broadcastEvent({
      senderSigner: signer(pubkeyFixture(5)),
      receiverPubkeys: ['alice'],
      relays: ['wss://relay.example'],
      event: { ...valid, content: 'tampered' },
      _publish: async () => { published = true }
    }),
    /INVALID_SIGNED_EVENT/
  )
  await assert.rejects(
    () => broadcastEvent({
      senderSigner: signer(pubkeyFixture(5)),
      receiverPubkeys: ['alice'],
      relays: ['wss://relay.example'],
      event: { kind: 9002, created_at: 23, tags: [], content: 'unsigned' },
      _publish: async () => { published = true }
    }),
    /INVALID_SIGNED_EVENT/
  )

  assert.equal(published, false)
})

test('nym broadcasts publish through the nym channel path without receivers', async () => {
  const published = []
  const nymPubkey = pubkeyFixture(6)
  const channelPubkey = pubkeyFixture(7)
  const authorEvent = finalizeEvent({ kind: 9002, created_at: 30, tags: [], content: 'signed by another key' }, generateSecretKey())
  const reports = [{ success: true, total: 1 }]
  const _publish = async options => {
    published.push(options)
    return reports
  }

  const rumorResult = await broadcastNymRumor({
    nymSigner: signer(nymPubkey),
    privateChannelSigner: signer(channelPubkey),
    relays: ['wss://relay.example'],
    rumor: { kind: 9001, created_at: 29, tags: [['x', 'nym']], content: 'rumor' },
    _publish
  })
  const eventResult = await broadcastNymEvent({
    nymSigner: signer(nymPubkey),
    privateChannelSigner: signer(channelPubkey),
    relays: ['wss://relay.example'],
    event: authorEvent,
    _publish
  })

  assert.equal(published.length, 2)
  assert.equal(published[0].nymSigner.getPublicKey(), nymPubkey)
  assert.equal(published[0].privateChannelSigner.getPublicKey(), channelPubkey)
  assert.equal(published[0].event.pubkey, undefined)
  assert.equal(published[0].event.id, undefined)
  assert.equal(rumorResult.rumor.pubkey, nymPubkey)
  assert.equal(rumorResult.rumor.id, getEventHash({ ...published[0].event, pubkey: nymPubkey }))
  assert.deepEqual(rumorResult.delivery.reports, reports)
  assert.equal(keypairFromSeckey(rumorResult.delivery.deletionSeckey).pubkey, published[0].deletionPubkey)
  assert.deepEqual(published[1].event, authorEvent)
  assert.notEqual(published[1].event.pubkey, nymPubkey)
  assert.deepEqual(eventResult.event, authorEvent)
  assert.deepEqual(eventResult.delivery.reports, reports)
  assert.equal(keypairFromSeckey(eventResult.delivery.deletionSeckey).pubkey, published[1].deletionPubkey)
})

test('parseRumorContent only reads h tags for private message kinds', () => {
  assert.deepEqual(
    parseRumorContent({ kind: TELL_KIND, tags: [['h', 'NOTE']], content: 'hello' }),
    { payload: 'hello', code: 'NOTE' }
  )
  assert.deepEqual(
    parseRumorContent({ kind: 9001, content: JSON.stringify(['hello', 'NOTE']) }),
    ['hello', 'NOTE']
  )
})
