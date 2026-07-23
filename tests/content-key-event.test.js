import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { isValidEvent } from '../event/index.js'
import { ValidationError } from '../error/index.js'
import { generateSecretKey } from '../key/index.js'

import TestSigner from './helpers/test-signer.js'
import { bytesToHex } from '../base16/index.js'
import {
  CONTENT_KEY_KIND,
  assertValidContentKeyProof,
  assertValidIykcProof,
  isValidContentKeyProof,
  isValidIykcProof,
  makeContentKeyEvent,
  parseContentKeyEvent
} from '../content-key/event/index.js'

afterEach(() => {
  TestSigner.releaseAll()
})

function signer () {
  return TestSigner.getOrCreate(bytesToHex(generateSecretKey()))
}

test('makeContentKeyEvent publishes a signed content pubkey proof', async () => {
  const user = signer()
  const contentKey = signer()
  const userPubkey = await user.getPublicKey()
  const contentPubkey = await contentKey.getPublicKey()
  const event = await makeContentKeyEvent({ userSigner: user, contentKeySigner: contentKey, createdAt: 7 })
  const parsed = parseContentKeyEvent(event)

  assert.equal(event.kind, CONTENT_KEY_KIND)
  assert.equal(event.pubkey, userPubkey)
  assert.deepEqual(event.tags, [['cp', contentPubkey]])
  assert.equal(parsed.iykcPubkey, contentPubkey)
  assert.equal(parsed.iykcProof, `${event.created_at}:${event.sig}`)
  const iykc = { receiverPubkey: userPubkey, iykcPubkey: contentPubkey, iykcProof: parsed.iykcProof }
  const proofInput = { ownerPubkey: userPubkey, contentPubkey, proof: parsed.iykcProof }
  assert.equal(isValidIykcProof(iykc), true)
  assert.equal(assertValidIykcProof(iykc), iykc)
  assert.equal(isValidContentKeyProof(proofInput), true)
  assert.equal(assertValidContentKeyProof(proofInput), proofInput)
  assert.equal(isValidEvent(event), true)

  const invalid = { ...proofInput, proof: `${event.created_at}:${'0'.repeat(128)}` }
  assert.equal(isValidContentKeyProof(invalid), false)
  assert.throws(() => assertValidContentKeyProof(invalid), error => (
    error instanceof ValidationError && error.code === 'INVALID_CONTENT_KEY_PROOF_SIGNATURE'
  ))
  const invalidIykc = { ...iykc, receiverPubkey: 'not-a-pubkey' }
  assert.equal(isValidIykcProof(invalidIykc), false)
  assert.throws(() => assertValidIykcProof(invalidIykc), error => (
    error instanceof ValidationError && error.code === 'INVALID_IYKC_RECEIVER_PUBKEY'
  ))
})

test('parseContentKeyEvent rejects malformed content-key events', async () => {
  const event = await makeContentKeyEvent({ userSigner: signer(), contentKeySigner: signer(), createdAt: 7 })

  assert.equal(parseContentKeyEvent({ ...event, tags: event.tags.concat([['x', 'nope']]) }), null)
  assert.equal(parseContentKeyEvent({ ...event, tags: [['cp', event.tags[0][1], 'extra']] }), null)
  assert.equal(parseContentKeyEvent({ ...event, content: 'nope' }), null)
  assert.equal(parseContentKeyEvent({ ...event, tags: [['cp', 'f'.repeat(64)]] }), null)
})
