import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  assertSerializableEvent,
  assertValidEvent,
  classifyEvent,
  finalizeEvent,
  getEventHash,
  isAddressableEvent,
  isEphemeralEvent,
  isRegularEvent,
  isReplaceableEvent,
  isSerializableEvent,
  isValidEvent
} from '../event/index.js'
import { ValidationError } from '../error/index.js'
import { serializeEvent } from '../event/helpers/serialize.js'
import { getPublicKey } from '../key/index.js'

const secretKey = new Uint8Array(32).fill(1)

function template () {
  return { kind: 1, created_at: 1, tags: [['p', 'a'.repeat(64)]], content: 'hello', extension: true }
}

test('NIP-01 serialization, hashing and signing are deterministic', () => {
  const event = finalizeEvent(template(), secretKey)
  assert.equal(event.pubkey, getPublicKey(secretKey))
  assert.equal(event.id, getEventHash(event))
  assert.equal(isValidEvent(event), true)
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

test('finalizeEvent reports malformed secret keys as validation errors', () => {
  assert.throws(() => finalizeEvent(template(), new Uint8Array(32)), error => (
    error instanceof ValidationError && error.code === 'INVALID_SECRET_KEY'
  ))
})

test('isValidEvent never caches or mutates and detects later mutations', () => {
  const event = finalizeEvent(template(), secretKey)
  const ownKeys = Reflect.ownKeys(event)
  assert.equal(isValidEvent(event), true)
  assert.deepEqual(Reflect.ownKeys(event), ownKeys)
  event.content = 'changed'
  assert.equal(isValidEvent(event), false)
  assert.deepEqual(Reflect.ownKeys(event), ownKeys)
})

test('serializable event predicate and assert share strict canonical NIP-01 bounds', () => {
  const valid = { ...template(), pubkey: 'a'.repeat(64) }
  assert.equal(isSerializableEvent(valid), true)
  assert.equal(assertSerializableEvent(valid), valid)
  for (const changed of [
    { kind: -1 }, { kind: 65536 }, { kind: 1.1 }, { created_at: -1 },
    { created_at: Number.MAX_SAFE_INTEGER + 1 }, { pubkey: 'A'.repeat(64) },
    { tags: [[]] }, { tags: [['p', 1]] }, { content: null }
  ]) {
    const invalid = { ...valid, ...changed }
    assert.equal(isSerializableEvent(invalid), false)
    assert.throws(() => assertSerializableEvent(invalid), ValidationError)
  }
})

test('valid event predicate and assert distinguish ID from signature failures', () => {
  const event = finalizeEvent(template(), secretKey)
  assert.equal(assertValidEvent(event), event)

  const wrongId = { ...event, id: '0'.repeat(64) }
  assert.equal(isValidEvent(wrongId), false)
  assert.throws(() => assertValidEvent(wrongId), error => (
    error instanceof ValidationError && error.code === 'EVENT_ID_MISMATCH'
  ))

  const wrongSignature = { ...event, sig: '0'.repeat(128) }
  assert.equal(isValidEvent(wrongSignature), false)
  assert.throws(() => assertValidEvent(wrongSignature), error => (
    error instanceof ValidationError && error.code === 'INVALID_EVENT_SIGNATURE'
  ))
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
