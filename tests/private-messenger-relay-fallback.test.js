import { test } from 'node:test'
import assert from 'node:assert/strict'
import { RelayPool, pickRelaysForPubkeys } from '../relay/index.js'
import { createSendRelayRouting, isReplaceableRelayFailure } from '../private-messenger/helpers/send-routing.js'
import * as privateChannel from '../private-channel/index.js'
import TestSigner from './helpers/test-signer.js'
import { finalizeEvent } from '../event/index.js'

const urls = ['a', 'b', 'c', 'd', 'e'].map(name => `wss://${name}.example`)
const event = finalizeEvent({ kind: 26300, created_at: 1, tags: [['p', 'b'.repeat(64)]], content: 'same encrypted outer payload' }, new Uint8Array(32).fill(7))
const tick = () => new Promise(resolve => setImmediate(resolve))
function fixture (t, { reasons = {}, online = true, fallbackRelays = [], primaryRelays, peers, relaysByPubkey, primaryRelayToReceivers, realPublish = false, recoveryRelays } = {}) {
  const state = { reasons, online, probes: 0, current: true, held: new Set(), pending: [], now: 1000 }
  const sends = []; const batches = []; const exclusions = new Map()
  const pool = new RelayPool({
    WebSocket: class {
      constructor (url) {
        this.url = url; this.readyState = 0
        queueMicrotask(() => { this.readyState = 1; this.onopen?.() })
      }

      send (raw) {
        const [type, value] = JSON.parse(raw)
        if (type !== 'EVENT') return
        sends.push({ relay: this.url, event: value })
        const reason = state.reasons[this.url] || ''
        const settle = () => this.onmessage?.({ data: JSON.stringify(['OK', value.id, !reason, reason]) })
        if (state.held.has(this.url)) state.pending.push(settle)
        else queueMicrotask(settle)
      }

      close () { this.readyState = 3; queueMicrotask(() => this.onclose?.({ code: 1000, reason: '', wasClean: true })) }
    }
  })
  t.after(() => pool.disconnectAll())
  const publisher = options => options._publish(options.event, [...options.relayToReceivers.keys(), ...options.recoveryRelays].filter((url, i, list) => list.indexOf(url) === i))
  const controller = new AbortController()
  const routing = () => createSendRelayRouting({
    peer: 'peer', peers, fallbackRelays, primaryRelays, primaryRelayToReceivers, relaysByPubkey: relaysByPubkey || { peer: { read: urls, write: ['wss://write-only.example'] } },
    exclusions, pickRelays: pickRelaysForPubkeys, recoveryRelays: recoveryRelays || [...urls.slice(0, 2), ...fallbackRelays],
    publish: realPublish ? privateChannel.publish : publisher, publishNymEvent: realPublish ? privateChannel.publishNymEvent : publisher,
    sendEvent: (event, relays) => { batches.push([...relays]); return pool.sendEvent(event, relays, { timeout: 2000 }) },
    isOnline: async () => { state.probes++; return state.online },
    isCurrent: () => state.current, signal: controller.signal, now: () => state.now
  })
  return { state, sends, batches, exclusions, controller, routing, publish: route => route._publish({ event, ...route }) }
}

test('policy rejection rotates two read relays at a time and preserves the exact signed event', async t => {
  const f = fixture(t, { reasons: { [urls[0]]: 'blocked: only some kinds', [urls[1]]: 'restricted: members only' } })
  const result = await f.publish(f.routing())
  assert.equal(result.success, true)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4)])
  assert.ok(f.sends.every(send => JSON.stringify(send.event) === JSON.stringify(event)))
  assert.deepEqual((await result.promise).succeededRelays, urls.slice(2, 4))
  await tick()
  const next = f.routing()
  assert.deepEqual([...next.relayToReceivers.keys()], urls.slice(2, 4))
  assert.deepEqual(next.recoveryRelays, [], 'recovery mirrors cannot restore rejected relays')
  await f.publish(next)
  assert.deepEqual(f.batches.at(-1), urls.slice(2, 4))
})

test('exhaustion includes every read relay, emits one final report and permits a fresh retry', async t => {
  const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])) })
  const result = await f.publish(f.routing())
  assert.equal(result.success, false)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), [urls[4]]])
  const report = await result.promise
  assert.equal(report.total, 5)
  assert.equal(report.fulfilled, 0)
  assert.deepEqual(report.errors.map(error => error.relay), urls)
  assert.ok(report.errors.every(error => error.reason.category === 'relay'))
  f.state.reasons = {}
  assert.equal((await f.publish(f.routing())).success, true)
  assert.deepEqual(f.batches.at(-1), urls.slice(0, 2))
})

test('a slow redundant rejection does not delay an accepted send and preserves its healthy relay', async t => {
  const f = fixture(t, { reasons: { [urls[1]]: 'blocked: policy' } })
  f.state.held.add(urls[1])
  const result = await f.publish(f.routing())
  assert.equal(result.success, true)
  assert.equal(f.state.pending.length, 1)
  f.state.pending.pop()()
  await result.promise; await tick()
  assert.deepEqual([...f.routing().relayToReceivers.keys()], [urls[0], urls[2]])
  assert.equal(f.batches.length, 1, 'no redundant resend after acceptance')
})

test('offline, invalid messages, local authorization and unknown errors never rotate relays', async t => {
  for (const reason of ['invalid: bad signature', 'unknown: failure']) {
    const f = fixture(t, { fallbackRelays: ['wss://fallback.example'], reasons: Object.fromEntries(urls.map(url => [url, reason])) })
    assert.equal((await f.publish(f.routing())).success, false)
    assert.equal(f.batches.length, 1)
    assert.equal(f.state.probes, 0)
    assert.equal(f.exclusions.size, 0)
  }
  const f = fixture(t, { reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])), online: false, fallbackRelays: ['wss://fallback.example'] })
  const deferred = await f.publish(f.routing())
  assert.equal(deferred.success, false)
  assert.equal(deferred.retryWhenAvailable, true)
  assert.equal(deferred.retryWhenOnline, true)
  assert.equal(f.batches.length, 1)
  assert.equal(f.exclusions.size, 0)
  for (const reason of [new Error('PERMISSION_DENIED'), Object.assign(new Error('blocked: local permission'), { name: 'Nip42AuthenticationError' }), Object.assign(new Error('blocked: unrelated'), { category: 'validation' })]) assert.equal(isReplaceableRelayFailure(reason), false)
  assert.equal(isReplaceableRelayFailure(Object.assign(new Error('PUBLISH_TIMEOUT'), { category: 'timeout' })), true)
})

test('cancellation stops retries while a session pause keeps the send retryable', async t => {
  for (const abort of [true, false]) {
    const f = fixture(t, { fallbackRelays: ['wss://fallback.example'], reasons: Object.fromEntries(urls.map(url => [url, 'blocked: policy'])) })
    f.state.held = new Set(urls.slice(0, 2))
    const work = f.publish(f.routing())
    while (f.state.pending.length < 2) await tick()
    if (abort) f.controller.abort()
    else f.state.current = false
    f.state.pending.splice(0).forEach(settle => settle())
    const result = await work
    assert.equal(result.success, false)
    assert.equal(result.retryWhenAvailable, !abort)
    assert.equal(result.retryWhenOnline, !abort)
    assert.equal(f.batches.length, 1)
    assert.equal(f.exclusions.size, 0)
  }
})

test('failed relay preferences expire and other fragments reuse the replacement pair', async t => {
  const f = fixture(t, { reasons: { [urls[0]]: 'rate-limited: slow down', [urls[1]]: 'pow: more work' } })
  const routing = f.routing()
  await f.publish(routing)
  await f.publish(routing)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), urls.slice(2, 4)])
  f.state.now += 5 * 60 * 1000
  assert.deepEqual([...f.routing().relayToReceivers.keys()], urls.slice(0, 2))
})

test('configured fallback waits for every primary relay and retains the encrypted event unchanged', async t => {
  const fallback = 'wss://fallback.example'
  const f = fixture(t, { fallbackRelays: [fallback], reasons: Object.fromEntries(urls.map(url => [url, 'blocked: kind not supported'])) })
  const result = await f.publish(f.routing())
  assert.equal(result.success, true)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), [urls[4]], [fallback]])
  assert.ok(f.sends.every(send => JSON.stringify(send.event) === JSON.stringify(event)))
  assert.deepEqual((await result.promise).succeededRelays, [fallback])
  assert.deepEqual([...f.routing().relayToReceivers.keys()], [fallback], 'recent primary rejections remain excluded for subsequent sends')
  await f.publish(f.routing())
  assert.deepEqual(f.batches.at(-1), [fallback])
  f.state.now += 5 * 60 * 1000
  assert.deepEqual([...f.routing().relayToReceivers.keys()], urls.slice(0, 2), 'the NIP-65 relays are reconsidered after exclusions expire')
})

test('configured fallbacks also exhaust in pairs, deduplicate primary relays and retain all errors', async t => {
  const fallbacks = ['wss://fallback-one.example', 'wss://fallback-two.example', 'wss://fallback-three.example']
  const all = [...urls, ...fallbacks]
  const f = fixture(t, { fallbackRelays: [urls[0], ...fallbacks, fallbacks[0]], reasons: Object.fromEntries(all.map(url => [url, 'restricted: unavailable'])) })
  const result = await f.publish(f.routing())
  assert.equal(result.success, false)
  assert.deepEqual(f.batches, [urls.slice(0, 2), urls.slice(2, 4), [urls[4]], fallbacks.slice(0, 2), [fallbacks[2]]])
  const report = await result.promise
  assert.equal(report.total, all.length)
  assert.deepEqual(report.errors.map(error => error.relay), all)
  f.state.reasons = {}
  await f.publish(f.routing())
  assert.deepEqual(f.batches.at(-1), urls.slice(0, 2), 'an explicit retry rechecks an exhausted list')
})

test('a primary acknowledgement never publishes a redundant copy to configured fallback relays', async t => {
  const f = fixture(t, { fallbackRelays: ['wss://fallback.example'], reasons: { [urls[1]]: 'blocked: policy' } })
  assert.equal((await f.publish(f.routing())).success, true)
  await tick()
  assert.deepEqual(f.batches, [urls.slice(0, 2)])
  assert.deepEqual([...f.routing().relayToReceivers.keys()], [urls[0], urls[2]])
})

test('explicit primary relays keep their full requested fanout before configured fallbacks', async t => {
  const primary = urls.slice(0, 3)
  const fallback = 'wss://fallback.example'
  const f = fixture(t, { primaryRelays: primary, fallbackRelays: [fallback], reasons: Object.fromEntries(primary.map(url => [url, 'blocked: kind not supported'])) })
  assert.equal((await f.publish(f.routing())).success, true)
  assert.deepEqual(f.batches, [primary, [fallback]])
  assert.ok(f.sends.every(send => JSON.stringify(send.event) === JSON.stringify(event)))
})

const alice = TestSigner.getOrCreate('01'.repeat(32))
const bob = TestSigner.getOrCreate('02'.repeat(32))
const carol = TestSigner.getOrCreate('03'.repeat(32))
const dave = TestSigner.getOrCreate('04'.repeat(32))
const members = await Promise.all([bob, carol, dave].map(signer => signer.getPublicKey()))
const shared = ['wss://shared-one.example', 'wss://shared-two.example']
const fallback = ['wss://fallback-one.example', 'wss://fallback-two.example']
const bobRelays = ['wss://bob-one.example', 'wss://bob-two.example', 'wss://bob-three.example']
const carolRelays = ['wss://carol-one.example', 'wss://carol-two.example', 'wss://carol-three.example']
const groupLists = {
  [members[0]]: { read: [...shared, ...bobRelays] },
  [members[1]]: { read: [...shared, ...carolRelays] }
}
const groupMessage = { kind: 9, created_at: 1, tags: [], content: 'group message' }
const blocked = relays => Object.fromEntries(relays.map(relay => [relay, 'blocked: kind not supported']))

function groupFixture (t, options = {}) {
  return fixture(t, { peers: members.slice(0, 2), relaysByPubkey: groupLists, fallbackRelays: fallback, recoveryRelays: fallback, realPublish: true, ...options })
}

async function publishGroup (f, options = {}) {
  const route = f.routing()
  const data = new Map()
  const temporaryStorageArea = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value), removeItem: key => data.delete(key) }
  return route._publish({
    senderSigner: alice, privateChannelSigner: alice, receivers: members.slice(0, 2),
    event: groupMessage, temporaryStorageArea, _getIykcProofs: async () => ({}), ...route, ...options
  })
}

async function assertRecipients (outer, expected) {
  assert.equal(outer.kind, privateChannel.PRIVATE_BROADCAST_KIND)
  assert.equal(outer.tags.some(tag => tag[0] === 'p'), false)
  for (const [index, signer] of [bob, carol, dave].entries()) {
    const received = await privateChannel.unwrapEvent({ receiverSigner: signer, receiverPubkey: members[index], privateChannelSigner: alice, event: outer })
    assert.equal(received?.content || null, expected.includes(members[index]) ? groupMessage.content : null)
  }
}

test('automatic multi-recipient fallback exhausts each read list and preserves the shared ciphertext', async t => {
  const primaries = [...shared, ...bobRelays, ...carolRelays]
  const f = groupFixture(t, { reasons: blocked(primaries) })
  const reports = await publishGroup(f)
  assert.equal(reports.length, 1)
  assert.equal(reports[0].success, true)
  assert.deepEqual(f.batches, [shared, bobRelays.slice(0, 2), [bobRelays[2]], carolRelays.slice(0, 2), [carolRelays[2]], fallback])
  assert.ok(f.sends.every(send => JSON.stringify(send.event) === JSON.stringify(f.sends[0].event)))
  assert.deepEqual((await reports[0].promise).errors.map(error => error.relay), primaries)
  await assertRecipients(f.sends[0].event, members.slice(0, 2))
})

test('a recipient ACK does not hide another recipient failure in the same ciphertext', async t => {
  const f = groupFixture(t, { reasons: blocked([...shared, ...carolRelays, ...fallback]) })
  const reports = await publishGroup(f)
  assert.equal(reports[0].success, false)
  const report = await reports[0].promise
  assert.equal(report.success, false, 'some accepted copies do not cover every member')
  assert.deepEqual(report.succeededRelays, bobRelays.slice(0, 2))
  assert.deepEqual(f.batches, [shared, bobRelays.slice(0, 2), carolRelays.slice(0, 2), [carolRelays[2]], fallback])
})

test('each recipient can succeed on a different primary without a fallback or redundant ACK delay', async t => {
  const f = groupFixture(t, { reasons: blocked(shared) })
  f.state.held = new Set([bobRelays[1], carolRelays[1]])
  const reports = await publishGroup(f)
  assert.equal(reports[0].success, true)
  assert.deepEqual(f.batches, [shared, bobRelays.slice(0, 2), carolRelays.slice(0, 2)])
  assert.equal(f.state.pending.length, 2)
  f.state.pending.splice(0).forEach(settle => settle())
  assert.equal((await reports[0].promise).success, true)
})

test('explicit overlapping recipient maps keep encryption subsets, primary fanout and map precedence', async t => {
  for (const asObject of [false, true]) {
    const entries = [
      ...shared.map(relay => [relay, members.slice(0, 2)]),
      ...bobRelays.map(relay => [relay, [members[0]]]),
      ...carolRelays.map(relay => [relay, members.slice(1)])
    ]
    const primaryRelayToReceivers = asObject ? Object.fromEntries(entries) : new Map(entries)
    const f = groupFixture(t, {
      peers: members, primaryRelayToReceivers, reasons: blocked(entries.map(([relay]) => relay))
    })
    const reports = await publishGroup(f, { receivers: members })
    assert.equal(reports.length, 3)
    assert.ok(reports.every(report => report.success))
    const events = [...new Map(f.sends.map(send => [send.event.id, send.event])).values()]
    assert.equal(events.length, 3)
    await assertRecipients(events[0], members.slice(0, 2))
    await assertRecipients(events[1], [members[0]])
    await assertRecipients(events[2], members.slice(1))
    assert.deepEqual(f.batches.slice(0, 4), [shared, bobRelays, carolRelays, fallback])
    for (const outer of events) {
      const copies = f.sends.filter(send => send.event.id === outer.id)
      assert.ok(copies.every(send => JSON.stringify(send.event) === JSON.stringify(outer)))
      assert.ok(copies.some(send => fallback.includes(send.relay)))
    }
    assert.deepEqual([...(primaryRelayToReceivers instanceof Map ? primaryRelayToReceivers : Object.entries(primaryRelayToReceivers))], entries)
  }
})

test('multi-recipient routing retains final native errors and rechecks exhausted relays on retry', async t => {
  const primaries = [...shared, ...bobRelays, ...carolRelays]
  const f = groupFixture(t, { reasons: blocked([...primaries, ...fallback]) })
  const [failed] = await publishGroup(f)
  assert.equal(failed.success, false)
  const report = await failed.promise
  assert.equal(report.total, primaries.length + fallback.length)
  assert.equal(report.errors.length, report.total)
  assert.ok(report.errors.every(error => error.reason.category === 'relay'))
  f.state.reasons = {}
  const [retried] = await publishGroup(f)
  assert.equal(retried.success, true)
  assert.deepEqual(f.batches.at(-1), shared)
})

test('offline, cancellation and invalid events do not start group fallback publication', async t => {
  for (const mode of ['offline', 'cancelled', 'invalid']) {
    const f = groupFixture(t, { reasons: blocked([...shared, ...bobRelays, ...carolRelays]), online: mode !== 'offline' })
    if (mode === 'invalid') f.state.reasons = Object.fromEntries(shared.map(relay => [relay, 'invalid: bad event']))
    f.state.held = new Set(shared)
    const work = publishGroup(f)
    await Promise.race([work.then(() => assert.fail('publication completed before controlled ACKs')), (async () => { while (f.state.pending.length < 2) await tick() })()])
    if (mode === 'cancelled') f.controller.abort()
    f.state.pending.splice(0).forEach(settle => settle())
    const [report] = await work
    assert.equal(report.success, false)
    assert.deepEqual(f.batches, [shared])
    assert.equal(Boolean(report.retryWhenAvailable), mode === 'offline')
  }
})

test('nym carriers with explicit recipient maps use fallbacks without changing the signed carrier', async t => {
  const f = groupFixture(t, {
    primaryRelayToReceivers: new Map([[bobRelays[0], [members[0]]], [carolRelays[0], [members[1]]]]),
    reasons: blocked([bobRelays[0], carolRelays[0]])
  })
  const [report] = await publishGroup(f, { nymSigner: alice })
  assert.equal(report.success, true)
  assert.deepEqual(f.batches, [[bobRelays[0]], [carolRelays[0]], fallback])
  assert.ok(f.sends.every(send => send.event.kind === privateChannel.PRIVATE_BROADCAST_KIND && JSON.stringify(send.event) === JSON.stringify(f.sends[0].event)))
  assert.equal(f.sends[0].event.tags.some(tag => tag[0] === 'p'), false)
})

test('disjoint explicit routes use fallback only for the refused encrypted recipient subset', async t => {
  const f = groupFixture(t, {
    primaryRelayToReceivers: new Map([[bobRelays[0], [members[0]]], [carolRelays[0], [members[1]]]]),
    reasons: blocked([carolRelays[0]])
  })
  const reports = await publishGroup(f)
  assert.ok(reports.every(report => report.success))
  assert.deepEqual(f.batches, [[bobRelays[0]], [carolRelays[0]], fallback])
  const fallbackCopy = f.sends.find(send => fallback.includes(send.relay)).event
  await assertRecipients(fallbackCopy, [members[1]])
  assert.equal(f.sends.some(send => send.relay === bobRelays[0] && send.event.id === fallbackCopy.id), false)
})

test('group fragments reuse the replacement routes and preserve each signed outer event', async t => {
  const f = groupFixture(t, { reasons: blocked([...shared, ...bobRelays, ...carolRelays]) })
  const reports = await publishGroup(f, { event: { ...groupMessage, content: 'large group message'.repeat(8000) } })
  assert.ok(reports.length > 1)
  assert.ok(reports.every(report => report.success))
  const byId = new Map()
  for (const send of f.sends) {
    if (!byId.has(send.event.id)) byId.set(send.event.id, [])
    byId.get(send.event.id).push(send)
  }
  assert.equal(byId.size, reports.length)
  for (const copies of byId.values()) {
    assert.ok(copies.every(send => JSON.stringify(send.event) === JSON.stringify(copies[0].event)))
    assert.ok(copies.some(send => fallback.includes(send.relay)))
  }
  assert.ok([...byId.values()].slice(1).every(copies => copies.every(send => fallback.includes(send.relay))))
})

test('an exhausted member can retry even when another member succeeded and no fallback is configured', async t => {
  const f = groupFixture(t, { fallbackRelays: [], recoveryRelays: [], reasons: blocked([...shared, ...bobRelays]) })
  const [failed] = await publishGroup(f)
  assert.equal(failed.success, false)
  f.state.reasons = {}
  const [retried] = await publishGroup(f)
  assert.equal(retried.success, true)
  assert.deepEqual(f.batches.at(-1), shared)
})
