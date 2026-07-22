import { test } from 'node:test'
import assert from 'node:assert/strict'

import { generateSecretKey, getPublicKey } from '../key/index.js'
import { decrypt, encrypt } from '../nip04/index.js'

test('NIP-04 roundtrips between peers', () => {
  const alice = generateSecretKey()
  const bob = generateSecretKey()
  const payload = encrypt(alice, getPublicKey(bob), 'Olá, Nostr')
  assert.equal(decrypt(bob, getPublicKey(alice), payload), 'Olá, Nostr')
})

test('NIP-04 rejects malformed and unauthenticatable envelopes', () => {
  const alice = generateSecretKey()
  const bob = generateSecretKey()
  const payload = encrypt(alice, getPublicKey(bob), 'secret')
  assert.throws(() => decrypt(bob, getPublicKey(alice), 'no-envelope'), /INVALID_NIP04_ENVELOPE/)
  assert.throws(() => decrypt(bob, getPublicKey(alice), `${payload}=hidden`), /INVALID/)
  assert.throws(() => decrypt(generateSecretKey(), getPublicKey(alice), payload), /INVALID_NIP04_CIPHERTEXT/)
})
