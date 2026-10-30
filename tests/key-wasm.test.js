import { test } from 'node:test'
import assert from 'node:assert/strict'

import { initializeEventCrypto, isValidEvent } from '../event/index.js'
import {
  generateKeypair,
  getPublicKey,
  keypairFromSeckey,
  signProfileEvent,
  signRelayListEvent
} from '../key/index.js'
import {
  getPublicKey as getPublicKeyWithJavaScript,
  keypairFromSeckey as keypairFromSeckeyWithJavaScript
} from '../key/js/index.js'

test('key helpers preserve their public shapes with the WASM backend', async () => {
  assert.equal(await initializeEventCrypto(), 'wasm')

  const generated = generateKeypair()
  assert.ok(generated.secretKey instanceof Uint8Array)
  assert.equal(generated.pubkey, getPublicKey(generated.secretKey))
  assert.equal(generated.pubkey, getPublicKeyWithJavaScript(generated.secretKey))

  const fromHex = keypairFromSeckey(generated.seckey)
  const fromNsec = keypairFromSeckeyWithJavaScript(generated.nsec)
  assert.equal(fromHex.secretKey.length, 32)
  assert.equal(fromHex.pubkey, generated.pubkey)
  assert.deepEqual(fromNsec, generated)

  assert.equal(isValidEvent(signProfileEvent({
    secretKey: generated.secretKey,
    name: 'Alice',
    picture: 'https://example.com/a.png'
  })), true)
  assert.equal(isValidEvent(signRelayListEvent({
    secretKey: generated.secretKey,
    writeRelays: ['wss://write.example'],
    readRelays: ['wss://read.example', 'wss://write.example']
  })), true)
})
