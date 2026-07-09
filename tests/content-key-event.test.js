import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { generateSecretKey, verifyEvent } from 'nostr-tools'

import TestSigner from './helpers/test-signer.js'
import { bytesToHex } from '../base16/index.js'
import {
  CONTENT_KEY_KIND,
  makeContentKeyEvent,
  parseContentKeyEvent,
  verifyContentKeyProof,
  verifyIykcProof
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
  assert.equal(verifyIykcProof({ receiverPubkey: userPubkey, iykcPubkey: contentPubkey, iykcProof: parsed.iykcProof }), true)
  assert.equal(verifyContentKeyProof({ ownerPubkey: userPubkey, contentPubkey, proof: parsed.iykcProof }), true)
  assert.equal(verifyEvent(event), true)
})

test('parseContentKeyEvent rejects malformed content-key events', async () => {
  const event = await makeContentKeyEvent({ userSigner: signer(), contentKeySigner: signer(), createdAt: 7 })

  assert.equal(parseContentKeyEvent({ ...event, tags: event.tags.concat([['x', 'nope']]) }), null)
  assert.equal(parseContentKeyEvent({ ...event, tags: [['cp', event.tags[0][1], 'extra']] }), null)
  assert.equal(parseContentKeyEvent({ ...event, content: 'nope' }), null)
  assert.equal(parseContentKeyEvent({ ...event, tags: [['cp', 'f'.repeat(64)]] }), null)
})
