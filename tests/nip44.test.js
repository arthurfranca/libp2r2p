import { test } from 'node:test'
import assert from 'node:assert/strict'
import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16, base16ToBytes } from '../base16/index.js'
import { generateSecretKey, getPublicKey } from '../key/index.js'
import { decrypt, encrypt, getConversationKey } from '../nip44/index.js'

test('NIP-44 v2 uses symmetric conversation keys and deterministic nonces', () => {
  const alice = generateSecretKey()
  const bob = generateSecretKey()
  const aliceKey = getConversationKey(alice, getPublicKey(bob))
  const bobKey = getConversationKey(bob, getPublicKey(alice))
  assert.deepEqual(aliceKey, bobKey)
  const nonce = new Uint8Array(32).fill(42)
  const payload = encrypt('hello', aliceKey, nonce)
  assert.equal(payload, encrypt('hello', aliceKey, nonce))
  assert.equal(decrypt(payload, bobKey), 'hello')
})

test('NIP-44 v2 matches the official deterministic vector', () => {
  const sec1 = base16ToBytes('0'.repeat(63) + '1')
  const sec2 = base16ToBytes('0'.repeat(63) + '2')
  const key = getConversationKey(sec1, getPublicKey(sec2))
  assert.equal(bytesToBase16(key), 'c41c775356fd92eadc63ff5a0dc1da211b268cbea22316767095b2871ea1412d')
  const payload = encrypt('a', key, base16ToBytes('0'.repeat(63) + '1'))
  assert.equal(payload, 'AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABee0G5VSK0/9YypIObAtDKfYEAjD35uVkHyB0F4DwrcNaCXlCWZKaArsGrY6M9wnuTMxWfp1RTN9Xga8no+kF5Vsb')
})

test('NIP-44 v2 supports the extended length prefix', () => {
  const key = base16ToBytes('c41c775356fd92eadc63ff5a0dc1da211b268cbea22316767095b2871ea1412d')
  const plaintext = 'x'.repeat(65536)
  const payload = encrypt(plaintext, key, base16ToBytes('0'.repeat(63) + '1'))
  assert.equal(decrypt(payload, key), plaintext)
  const officialPayload = encrypt('a'.repeat(65536), key, base16ToBytes('0'.repeat(63) + '1'))
  assert.equal(bytesToBase16(sha256(new TextEncoder().encode(officialPayload))), 'b7b4edb36ba92e267d322d56d9aebc22e7fa96ff52e3c12adc07f07a43cbc616')
})

test('NIP-44 v2 rejects malformed, non-canonical and modified payloads early', () => {
  const key = new Uint8Array(32).fill(7)
  const payload = encrypt('hello', key, new Uint8Array(32).fill(1))
  const changed = `${payload.slice(0, -2)}A=`
  assert.throws(() => decrypt(changed, key), /INVALID/)
  assert.throws(() => decrypt(`${payload}=`, key), /INVALID/)
  assert.throws(() => encrypt('', key), /INVALID_PLAINTEXT_SIZE/)
})

test('NIP-44 v2 accepts explicit salts without changing the interoperable default', () => {
  const alice = base16ToBytes('0'.repeat(63) + '1')
  const bob = base16ToBytes('0'.repeat(63) + '2')
  const alicePubkey = getPublicKey(alice)
  const bobPubkey = getPublicKey(bob)
  const defaultKey = getConversationKey(alice, bobPubkey)
  assert.deepEqual(getConversationKey(alice, bobPubkey, {}), defaultKey)
  const custom = getConversationKey(alice, bobPubkey, { salt: 'private-app' })
  assert.notDeepEqual(custom, defaultKey)
  assert.deepEqual(custom, getConversationKey(bob, alicePubkey, { salt: 'private-app' }))
  assert.throws(() => getConversationKey(alice, bobPubkey, { salt: '' }), /INVALID_SALT/)
  assert.throws(() => getConversationKey(alice, bobPubkey, { salt: 'x'.repeat(33) }), /INVALID_SALT/)
})
