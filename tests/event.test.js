import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  classifyEvent,
  finalizeEvent,
  getEventHash,
  isAddressableEvent,
  isEphemeralEvent,
  isRegularEvent,
  isReplaceableEvent,
  validateEvent,
  verifyEvent
} from '../event/index.js'
import { serializeEvent } from '../event/helpers/serialize.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'

const secretKey = new Uint8Array(32).fill(1)

function template () {
  return { kind: 1, created_at: 1, tags: [['p', 'a'.repeat(64)]], content: 'hello', extension: true }
}

test('NIP-01 serialization, hashing and signing are deterministic', () => {
  const event = finalizeEvent(template(), secretKey)
  assert.equal(event.pubkey, getPublicKey(secretKey))
  assert.equal(event.id, getEventHash(event))
  assert.equal(verifyEvent(event), true)
  assert.equal(serializeEvent(event), JSON.stringify([0, event.pubkey, 1, 1, event.tags, 'hello']))
})

test('finalizeEvent is pure and copies tags', () => {
  const source = template()
  const snapshot = structuredClone(source)
  const event = finalizeEvent(source, secretKey)
  assert.deepEqual(source, snapshot)
  assert.notEqual(event.tags, source.tags)
  assert.notEqual(event.tags[0], source.tags[0])
  assert.equal(event.extension, true)
})

test('verifyEvent never caches or mutates and detects later mutations', () => {
  const event = finalizeEvent(template(), secretKey)
  const ownKeys = Reflect.ownKeys(event)
  assert.equal(verifyEvent(event), true)
  assert.deepEqual(Reflect.ownKeys(event), ownKeys)
  event.content = 'changed'
  assert.equal(verifyEvent(event), false)
  assert.deepEqual(Reflect.ownKeys(event), ownKeys)
})

test('validateEvent applies strict canonical NIP-01 bounds', () => {
  const valid = { ...template(), pubkey: 'a'.repeat(64) }
  assert.equal(validateEvent(valid), true)
  for (const changed of [
    { kind: -1 }, { kind: 65536 }, { kind: 1.1 }, { created_at: -1 },
    { created_at: Number.MAX_SAFE_INTEGER + 1 }, { pubkey: 'A'.repeat(64) },
    { tags: [[]] }, { tags: [['p', 1]] }, { content: null }
  ]) assert.equal(validateEvent({ ...valid, ...changed }), false)
})

test('event classification adds tag-defined behavior from any tag position', () => {
  const event = {
    kind: 1006,
    created_at: 123,
    tags: [['p', 'x'], ['d', 'address'], ['expiration', '123']]
  }
  assert.deepEqual(classifyEvent(event), ['regular', 'ephemeral', 'addressable'])
  assert.equal(isRegularEvent(event), true)
  assert.equal(isReplaceableEvent(event), false)
  assert.equal(isEphemeralEvent(event), true)
  assert.equal(isAddressableEvent(event), true)
  assert.deepEqual(classifyEvent(event, { includeLegacyKindRanges: false }), ['ephemeral', 'addressable'])
})

test('event classification uses only the first d tag and stays additive', () => {
  const malformedFirst = { kind: 10000, created_at: 1, tags: [['d'], ['d', 'later']] }
  assert.deepEqual(classifyEvent(malformedFirst), ['replaceable'])
  assert.equal(isAddressableEvent(malformedFirst), false)

  const overlapping = { kind: 30000, created_at: 1, tags: [['d', ''], ['expiration', '1']] }
  assert.deepEqual(classifyEvent(overlapping), ['replaceable', 'ephemeral', 'addressable'])
  assert.equal(isReplaceableEvent(overlapping), true)
  assert.equal(isAddressableEvent(overlapping), true)
})

test('events are regular when neither replaceable nor addressable', () => {
  const otherwiseUnclassified = { kind: 500, created_at: 1, tags: [] }
  assert.equal(isRegularEvent(otherwiseUnclassified), true)
  assert.deepEqual(classifyEvent(otherwiseUnclassified), ['regular'])
  assert.deepEqual(classifyEvent(otherwiseUnclassified, { includeLegacyKindRanges: false }), ['regular'])

  const ephemeral = { kind: 20000, created_at: 1, tags: [] }
  assert.deepEqual(classifyEvent(ephemeral), ['regular', 'ephemeral'])

  assert.equal(isRegularEvent({ ...otherwiseUnclassified, tags: [['d', '']] }), false)
  assert.equal(isRegularEvent({ ...otherwiseUnclassified, tags: [['d', 'address']] }), false)
  assert.equal(isRegularEvent(null), false)
})
