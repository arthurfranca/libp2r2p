import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { getPublicKey } from '../key/index.js'
import { sha256 } from '@noble/hashes/sha2.js'

import * as nip44v3 from '../nip44-v3/index.js'
import { bytesToHex, hexToBytes } from '../base16/index.js'

const vectors = JSON.parse(readFileSync(new URL('./fixtures/nip44v3-vectors.json', import.meta.url), 'utf8'))

function pubOf (sec) {
  return getPublicKey(hexToBytes(sec))
}

test('nip44-v3 passes the vendored upstream self-test vectors', () => {
  const fails = []
  const check = (section, name, cond, detail = '') => {
    if (!cond) fails.push(`[${section}] ${name}${detail ? `: ${detail}` : ''}`)
  }

  for (const [i, v] of (vectors.encrypt_decrypt || []).entries()) {
    const sec1 = hexToBytes(v.secret1)
    const sec2 = hexToBytes(v.secret2)
    const pub1 = pubOf(v.secret1)
    const pub2 = pubOf(v.secret2)
    const nonce = hexToBytes(v.nonce)
    const scope = hexToBytes(v.scope_hex)
    const plaintext = hexToBytes(v.plaintext_hex)
    const keys = nip44v3.deriveKeys(sec1, pub2, nonce)
    check('encrypt_decrypt', `ed[${i}] prk`, bytesToHex(keys.prk) === v.prk)
    check('encrypt_decrypt', `ed[${i}] encryption_key`, bytesToHex(keys.encryption_key) === v.encryption_key)
    check('encrypt_decrypt', `ed[${i}] mac_key`, bytesToHex(keys.mac_key) === v.mac_key)
    check('encrypt_decrypt', `ed[${i}] encrypt party1`, nip44v3.encryptBytes(sec1, pub2, v.kind, scope, plaintext, nonce) === v.ciphertext)
    check('encrypt_decrypt', `ed[${i}] encrypt party2`, nip44v3.encryptBytes(sec2, pub1, v.kind, scope, plaintext, nonce) === v.ciphertext)
    check('encrypt_decrypt', `ed[${i}] decrypt party1`, bytesToHex(nip44v3.decryptBytes(sec1, pub2, v.kind, scope, v.ciphertext)) === v.plaintext_hex)
    check('encrypt_decrypt', `ed[${i}] decrypt party2`, bytesToHex(nip44v3.decryptBytes(sec2, pub1, v.kind, scope, v.ciphertext)) === v.plaintext_hex)
  }

  for (const [i, v] of (vectors.decrypt_only || []).entries()) {
    check('decrypt_only', `do[${i}]`, bytesToHex(nip44v3.decryptBytes(hexToBytes(v.secret1), pubOf(v.secret2), v.kind, hexToBytes(v.scope_hex), v.ciphertext)) === v.plaintext_hex)
  }

  for (const [i, v] of (vectors.long_encrypt_decrypt || []).entries()) {
    const pattern = hexToBytes(v.pattern_hex)
    const plaintext = new Uint8Array(pattern.length * v.repeat)
    for (let r = 0; r < v.repeat; r++) plaintext.set(pattern, r * pattern.length)
    const scope = hexToBytes(v.scope_hex)
    const ciphertext = nip44v3.encryptBytes(hexToBytes(v.secret1), pubOf(v.secret2), v.kind, scope, plaintext, hexToBytes(v.nonce))
    check('long_encrypt_decrypt', `long[${i}] sha256`, bytesToHex(sha256(nip44v3.toBytes(ciphertext))) === v.ciphertext_sha256)
    check('long_encrypt_decrypt', `long[${i}] round-trip`, bytesToHex(nip44v3.decryptBytes(hexToBytes(v.secret1), pubOf(v.secret2), v.kind, scope, ciphertext)) === bytesToHex(plaintext))
  }

  for (const row of (vectors.padded_length || [])) {
    check('padded_length', `pad[${row[0]}]`, nip44v3.targetSize(row[0]) === row[1])
  }

  for (const [i, v] of (vectors.invalid_decryption || []).entries()) {
    assert.throws(() => nip44v3.decryptBytes(hexToBytes(v.secret), v.public, v.kind, hexToBytes(v.scope_hex), v.ciphertext), Error, `invalid vector ${i} should reject`)
  }

  assert.deepEqual(fails, [])
})

test('nip44-v3 byte payload helpers round-trip arbitrary bytes', () => {
  const alice = hexToBytes('1'.repeat(64))
  const bob = hexToBytes('2'.repeat(64))
  const plaintext = nip44v3.b64encode(new Uint8Array([0, 1, 2, 127, 128, 255]))
  const ciphertext = nip44v3.nip07Encrypt(alice, getPublicKey(bob), '30078', 'spec.nostr.land/nip44v3', plaintext)

  assert.equal(nip44v3.nip07Decrypt(bob, getPublicKey(alice), 30078, 'spec.nostr.land/nip44v3', ciphertext), plaintext)
  assert.throws(() => nip44v3.nip07Decrypt(bob, getPublicKey(alice), 1, 'spec.nostr.land/nip44v3', ciphertext), /kind mismatch/)
})
