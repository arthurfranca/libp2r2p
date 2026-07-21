import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  BASE36_ALPHABET,
  base16ToBase36,
  base16ToBase36Nsite,
  base36NsiteToBase16,
  base36NsiteToBytes,
  base36ToBase16,
  base36ToBytes,
  bytesToBase36,
  bytesToBase36Nsite
} from '../base36/index.js'
import { bytesToBase16 } from '../base16/index.js'

test('binary-safe base36 preserves arbitrary bytes and leading zeros', () => {
  const vectors = [
    [[], ''],
    [[0], '0'],
    [[0, 0], '00'],
    [[255], '73'],
    [[0, 255], '073'],
    [[0, 1, 2, 3, 254, 255], '01zl8utb']
  ]

  assert.equal(BASE36_ALPHABET, '0123456789abcdefghijklmnopqrstuvwxyz')
  for (const [input, encoded] of vectors) {
    const bytes = Uint8Array.from(input)
    assert.equal(bytesToBase36(bytes), encoded)
    assert.deepEqual(base36ToBytes(encoded), bytes)
  }
  assert.equal(base16ToBase36('00ff'), '073')
  assert.equal(base36ToBase16('073'), '00ff')
  assert.throws(() => base36ToBytes('A'), /Invalid Base36 character/)
  assert.throws(() => base36ToBytes('-'), /Invalid Base36 character/)
  assert.throws(() => base36ToBytes(null), /should be a string/)
})

test('binary-safe base36 round-trips deterministic values of varying widths', () => {
  for (let length = 0; length <= 64; length++) {
    const bytes = Uint8Array.from(
      { length },
      (_, index) => index < length % 5 ? 0 : (length * 193 + index * 151) & 0xff
    )
    const encoded = bytesToBase36(bytes)
    assert.deepEqual(base36ToBytes(encoded), bytes)
    assert.equal(base36ToBase16(encoded), bytesToBase16(bytes))
    assert.equal(base16ToBase36(bytesToBase16(bytes)), encoded)
  }
})

test('nsite base36 matches the canonical NIP-5A representation', () => {
  const referenceHex = '0123456789abcdef'.repeat(4)
  const referenceBase36 = '010r2curot7aoi80l0gyf25bl7y111lpgrb8bzoi8f0c1uhmgf'

  assert.equal(base16ToBase36Nsite(referenceHex), referenceBase36)
  assert.equal(base36NsiteToBase16(referenceBase36), referenceHex)
  assert.equal(base16ToBase36Nsite('0'.repeat(64)), '0'.repeat(50))
  assert.equal(base36NsiteToBase16('0'.repeat(50)), '0'.repeat(64))
  assert.equal(
    base16ToBase36Nsite('0'.repeat(62) + 'ff'),
    '0'.repeat(48) + '73'
  )
  assert.equal(
    base16ToBase36Nsite('f'.repeat(64)),
    '6dp5qcb22im238nr3wvp0ic7q99w035jmy2iw7i6n43d37jtof'
  )
})

test('nsite base36 strictly validates width, alphabet, and 256-bit range', () => {
  assert.throws(() => bytesToBase36Nsite(new Uint8Array(31)), /32 bytes/)
  assert.throws(() => base16ToBase36Nsite('00'.repeat(31)), /32 bytes/)
  assert.throws(() => base36NsiteToBytes('0'.repeat(49)), /50 characters/)
  assert.throws(() => base36NsiteToBytes('0'.repeat(51)), /50 characters/)
  assert.throws(() => base36NsiteToBytes('0'.repeat(49) + 'A'), /character/)
  assert.throws(() => base36NsiteToBytes('0'.repeat(49) + '-'), /character/)
  assert.throws(
    () => base36NsiteToBytes('6dp5qcb22im238nr3wvp0ic7q99w035jmy2iw7i6n43d37jtog'),
    /exceeds 32 bytes/
  )
})

test('nsite base36 keeps the established 44billion 50-character output', () => {
  for (let seed = 0; seed < 128; seed++) {
    const bytes = Uint8Array.from({ length: 32 }, (_, index) => (seed * 131 + index * 73) & 0xff)
    const hex = bytesToBase16(bytes)
    const legacy = BigInt('0x' + hex).toString(36).padStart(50, '0')
    assert.equal(bytesToBase36Nsite(bytes), legacy)
    assert.deepEqual(base36NsiteToBytes(legacy), bytes)
  }
})
