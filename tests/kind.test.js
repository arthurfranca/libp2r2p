import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  classifyKind,
  CONTACT_LIST,
  CONTENT_KEY_ANNOUNCEMENT,
  eventKinds,
  isAddressableKind,
  isEphemeralKind,
  isRegularKind,
  isReplaceableKind,
  PERSONAL_COPY,
  SITE_CURATION_SET
} from '../kind/index.js'

test('kind helpers follow the exact NIP-01 ranges', () => {
  for (const kind of [1, 2, 4, 44, 1000, 9999]) assert.equal(isRegularKind(kind), true, String(kind))
  for (const kind of [0, 3, 45, 999, 10000]) assert.equal(isRegularKind(kind), false, String(kind))
  for (const kind of [0, 3, 10000, 19999]) assert.equal(isReplaceableKind(kind), true, String(kind))
  for (const kind of [20000, 29999]) assert.equal(isEphemeralKind(kind), true, String(kind))
  for (const kind of [30000, 39999]) assert.equal(isAddressableKind(kind), true, String(kind))
  for (const kind of [-1, 1.5, 40000, 65536, NaN]) assert.deepEqual(classifyKind(kind), [], String(kind))
  assert.deepEqual(classifyKind(1), ['regular'])
  assert.deepEqual(classifyKind(10000), ['replaceable'])
  assert.deepEqual(classifyKind(20000), ['ephemeral'])
  assert.deepEqual(classifyKind(30000), ['addressable'])
})

test('named kind exports and the aggregate retain project-specific kinds', () => {
  assert.equal(PERSONAL_COPY, 1006)
  assert.equal(eventKinds.PERSONAL_COPY, PERSONAL_COPY)
  assert.equal(CONTACT_LIST, 13300)
  assert.equal(eventKinds.CONTACT_LIST, CONTACT_LIST)
  assert.deepEqual(classifyKind(CONTACT_LIST), ['replaceable'])
  assert.equal(CONTENT_KEY_ANNOUNCEMENT, 18716)
  assert.equal(eventKinds.CONTENT_KEY_ANNOUNCEMENT, CONTENT_KEY_ANNOUNCEMENT)
  assert.deepEqual(classifyKind(CONTENT_KEY_ANNOUNCEMENT), ['replaceable'])
  assert.equal(eventKinds.BINARY_DATA_CHUNK, 34601)
  assert.equal(SITE_CURATION_SET, 30499)
  assert.equal(eventKinds.SITE_CURATION_SET, SITE_CURATION_SET)
  assert.equal(Object.isFrozen(eventKinds), true)
})
