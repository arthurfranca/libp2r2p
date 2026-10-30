import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  finalizeEvent,
  initializeEventCrypto
} from '../event/index.js'
import { getReadyEventCryptoBackend } from '../helpers/secp256k1-wasm/backend.js'
import { generateSecretKey } from '../key/index.js'

test('secret generation does not initialize WASM', () => {
  const secretKey = generateSecretKey()
  assert.ok(secretKey instanceof Uint8Array)
  assert.equal(secretKey.length, 32)
  assert.equal(getReadyEventCryptoBackend(), null)
})

test('the first signing operation returns before background WASM initialization', async () => {
  assert.equal(getReadyEventCryptoBackend(), null)
  const event = finalizeEvent({
    kind: 1,
    created_at: 1,
    tags: [],
    content: 'first call stays synchronous'
  }, new Uint8Array(32).fill(7))
  assert.equal(typeof event.id, 'string')
  assert.equal(getReadyEventCryptoBackend(), null)
  assert.equal(await initializeEventCrypto(), 'wasm')
  assert.ok(getReadyEventCryptoBackend())
})
