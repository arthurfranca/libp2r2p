import assert from 'node:assert/strict'
import { test } from 'node:test'

import { base64ToBytes } from '../base64/index.js'
import { finalizeEvent, isValidEvent } from '../event/index.js'
import { getToken } from '../nip98/index.js'

const decoder = new TextDecoder()
const secretKey = new Uint8Array(32).fill(1)

test('NIP-98 signs exact payload bytes and optionally includes the scheme', async () => {
  const token = await getToken({
    loginUrl: 'https://example.com/upload',
    httpMethod: 'post',
    payload: new Uint8Array([0, 1, 2, 3]),
    includeAuthorizationScheme: true,
    signEvent: template => finalizeEvent(template, secretKey)
  })
  assert.match(token, /^Nostr /)
  const event = JSON.parse(decoder.decode(base64ToBytes(token.slice(6))))
  assert.equal(isValidEvent(event), true)
  assert.equal(event.kind, 27235)
  assert.deepEqual(event.tags.slice(0, 2), [
    ['u', 'https://example.com/upload'],
    ['method', 'POST']
  ])
  assert.deepEqual(event.tags[2], ['payload', '054edec1d0211f624fed0cbca9d4f9400b0e491c43742af2c5b0abebf0c990d8'])
})

test('NIP-98 accepts explicit hashes and rejects ambiguous or changed events', async () => {
  const hash = 'ab'.repeat(32)
  assert.equal(typeof await getToken({
    loginUrl: 'https://example.com',
    httpMethod: 'GET',
    payloadHash: hash,
    signEvent: template => finalizeEvent(template, secretKey)
  }), 'string')
  await assert.rejects(getToken({
    loginUrl: 'https://example.com', httpMethod: 'POST', payload: 'x', payloadHash: hash,
    signEvent: template => finalizeEvent(template, secretKey)
  }), /MUTUALLY_EXCLUSIVE/)
  await assert.rejects(getToken({
    loginUrl: 'https://example.com', httpMethod: 'GET',
    signEvent: template => finalizeEvent({ ...template, tags: [['u', 'https://changed.example']] }, secretKey)
  }), /CHANGED/)
})
