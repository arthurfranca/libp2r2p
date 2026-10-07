import { test } from 'node:test'
import assert from 'node:assert/strict'

import { base64ToBytes, bytesToBase64 } from '../base64/index.js'
import { createMessengerSigner } from '../private-messenger/session/index.js'

const hex = char => char.repeat(64)

test('session signer identity is shared after success but a failed lookup can be reevaluated', async () => {
  let calls = 0
  const failure = new Error('locked')
  const adapter = createMessengerSigner({ getPublicKey: async () => { if (++calls === 1) throw failure; return hex('a') } })
  const first = adapter.getPublicKey()
  assert.equal(adapter.getPublicKey(), first)
  await assert.rejects(first, error => error === failure)
  assert.deepEqual(await Promise.all([adapter.getPublicKey(), adapter.getPublicKey()]), [hex('a'), hex('a')])
  assert.equal(await adapter.getPublicKey(), hex('a'))
  assert.equal(calls, 2)
})

function signerWith ({ encryptDoubleDH = false, decryptDoubleDH = false } = {}) {
  const calls = []
  const nip44v3 = {
    encrypt: async () => 'encrypted',
    decrypt: async () => new Uint8Array([1, 2, 3]).buffer
  }
  if (encryptDoubleDH) {
    nip44v3.encryptDoubleDH = async (...args) => {
      calls.push(['encryptDoubleDH', ...args])
      return ['ciphertext', hex('c')]
    }
  }
  if (decryptDoubleDH) {
    nip44v3.decryptDoubleDH = async (...args) => {
      calls.push(['decryptDoubleDH', ...args])
      return new Uint8Array([104, 105]).buffer
    }
  }
  return {
    calls,
    signer: {
      getPublicKey: async () => hex('a'),
      getRelays: async () => ({ read: [], write: [] }),
      signEvent: async event => event,
      nip44v3
    }
  }
}

test('session signer adapter omits double DH when the signer lacks it', () => {
  const adapter = createMessengerSigner(signerWith().signer)

  assert.equal(typeof adapter.nip44v3Encrypt, 'function')
  assert.equal(typeof adapter.nip44v3Decrypt, 'function')
  assert.equal(adapter.nip44EncryptDoubleDH, undefined)
  assert.equal(adapter.nip44DecryptDoubleDH, undefined)
})

test('session signer adapter requires both double DH methods', () => {
  const encryptOnly = createMessengerSigner(signerWith({ encryptDoubleDH: true }).signer)
  const decryptOnly = createMessengerSigner(signerWith({ decryptDoubleDH: true }).signer)

  assert.equal(encryptOnly.nip44EncryptDoubleDH, undefined)
  assert.equal(encryptOnly.nip44DecryptDoubleDH, undefined)
  assert.equal(decryptOnly.nip44EncryptDoubleDH, undefined)
  assert.equal(decryptOnly.nip44DecryptDoubleDH, undefined)
})

test('session signer adapter converts Base64 double DH plaintext to bytes', async () => {
  const { signer, calls } = signerWith({ encryptDoubleDH: true, decryptDoubleDH: true })
  const adapter = createMessengerSigner(signer)
  const plaintext = new TextEncoder().encode('hello double dh')
  const plaintextBase64 = bytesToBase64(plaintext)

  const [ciphertext, senderContentPubkey] = await adapter.nip44EncryptDoubleDH(
    hex('b'),
    9,
    'scope',
    plaintextBase64,
    hex('d')
  )

  assert.equal(ciphertext, 'ciphertext')
  assert.equal(senderContentPubkey, hex('c'))
  const [method, peer, kind, scope, raw, peerContentPubkey] = calls[0]
  assert.equal(method, 'encryptDoubleDH')
  assert.equal(peer, hex('b'))
  assert.equal(kind, 9)
  assert.equal(scope, 'scope')
  assert.ok(raw instanceof ArrayBuffer)
  assert.deepEqual([...new Uint8Array(raw)], [...plaintext])
  assert.equal(peerContentPubkey, hex('d'))
})

test('session signer adapter returns double DH plaintext as Base64', async () => {
  const { signer, calls } = signerWith({ encryptDoubleDH: true, decryptDoubleDH: true })
  const adapter = createMessengerSigner(signer)

  const plaintext = await adapter.nip44DecryptDoubleDH(
    hex('b'),
    9,
    'scope',
    'ciphertext',
    hex('d'),
    hex('e')
  )

  assert.equal(plaintext, bytesToBase64(new Uint8Array([104, 105])))
  assert.deepEqual(base64ToBytes(plaintext), new Uint8Array([104, 105]))
  assert.deepEqual(calls[0], ['decryptDoubleDH', hex('b'), 9, 'scope', 'ciphertext', hex('d'), hex('e')])
})
