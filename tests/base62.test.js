import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  BASE62_ALPHABET,
  base16ToBase62,
  base62ToBase16,
  base62ToBytes,
  bytesToBase62
} from '../base62/index.js'

test('base62 helpers preserve bytes and the established alphabet', () => {
  const vectors = [
    [[], ''],
    [[0], '0'],
    [[0, 0], '00'],
    [[255], '47'],
    [[0, 1, 2, 3, 254, 255], '04IX8mz']
  ]

  assert.equal(BASE62_ALPHABET, '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
  for (const [input, encoded] of vectors) {
    const bytes = Uint8Array.from(input)
    assert.equal(bytesToBase62(bytes), encoded)
    assert.deepEqual(base62ToBytes(encoded), bytes)
  }
})

test('base62 exposes hexadecimal conversions and validates input', () => {
  assert.equal(base16ToBase62('00010203feff'), '04IX8mz')
  assert.equal(base62ToBase16('04IX8mz'), '00010203feff')
  assert.equal(
    bytesToBase62(Uint8Array.of(255), { mode: 'integer', minLength: 4 }),
    '0047'
  )
  assert.throws(() => base62ToBytes('with-hyphen'), /Invalid Base62 character/)
  assert.throws(() => base62ToBytes(null), /should be a string/)
})

test('base62 integer mode separates textual width from leading zero bytes', () => {
  const options = { mode: 'integer', minLength: 4 }
  assert.equal(base16ToBase62('00ff', options), '0047')
  assert.equal(
    base62ToBase16('0047', { mode: 'integer', byteLength: 2 }),
    '00ff'
  )
  assert.equal(base62ToBase16('0047'), '0000ff')
  assert.equal(
    base62ToBase16('0'.repeat(43), { mode: 'integer', byteLength: 32 }),
    '0'.repeat(64)
  )
})

test('base62 validates integer mode options, empty input, and overflow', () => {
  assert.throws(() => bytesToBase62(Uint8Array.of(1), -1), /options should be an object/)
  assert.throws(
    () => bytesToBase62(Uint8Array.of(1), { minLength: 4 }),
    /requires integer mode/
  )
  assert.throws(
    () => base62ToBytes('1', { byteLength: 1 }),
    /requires integer mode/
  )
  assert.throws(
    () => bytesToBase62(Uint8Array.of(1), { mode: 'integer', minLength: -1 }),
    /non-negative safe integer/
  )
  assert.throws(
    () => base62ToBytes('1', { mode: 'integer', byteLength: 0 }),
    /positive safe integer/
  )
  assert.throws(() => bytesToBase62(new Uint8Array(), { mode: 'integer' }), /should not be empty/)
  assert.throws(() => base62ToBytes('', { mode: 'integer' }), /should not be empty/)
  assert.throws(
    () => base62ToBytes('48', { mode: 'integer', byteLength: 1 }),
    /exceeds 1 bytes/
  )
  assert.throws(
    () => base62ToBytes('1', { mode: 'integer', extra: true }),
    /Unknown Base62 option/
  )
})
