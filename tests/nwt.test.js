import assert from 'node:assert/strict'
import { test } from 'node:test'

import { bytesToBase64Url } from '../base64/index.js'
import { finalizeEvent } from '../event/index.js'
import { NWT } from '../kind/index.js'
import { createToken, decodeToken, encodeToken, validateToken } from '../nwt/index.js'

const secretKey = new Uint8Array(32).fill(1)
const signEvent = template => finalizeEvent(template, secretKey)
const NOW = 1_700_000_000

test('NWT accepts the signed event fixture from the reference Go implementation', () => {
  const event = {
    id: '366458cb01dd1f42d66cb71d31cc2e1217c69606181c83cbcdeb878942776d73',
    pubkey: '79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798',
    created_at: 1767957502,
    kind: NWT,
    tags: [],
    content: '',
    sig: '7c9a84e33fa7aaf6d85c3d90b3103b4197d7f964f5ff31dabe49aa4952b74579e4cfe6c4c4635e2501f5dbd742fdc4750a5ce26aae395a9b256a27b5533575b9'
  }
  assert.equal(validateToken(encodeToken(event), { now: event.created_at }).id, event.id)
})

test('NWT creates, encodes, decodes and validates signed claims', async () => {
  const event = await createToken({
    signEvent,
    createdAt: NOW,
    issuer: 'issuer',
    subject: 'subject',
    audience: ['api.example.com', 'cdn.example.com'],
    issuedAt: NOW,
    notBefore: NOW - 10,
    expiration: NOW + 300,
    claims: [
      ['action', 'upload'],
      ['roles', 'author', 'moderator']
    ],
    content: 'Upload example.webp'
  })

  assert.equal(event.kind, NWT)
  assert.deepEqual(event.tags, [
    ['iss', 'issuer'],
    ['sub', 'subject'],
    ['aud', 'api.example.com'],
    ['aud', 'cdn.example.com'],
    ['iat', String(NOW)],
    ['exp', String(NOW + 300)],
    ['nbf', String(NOW - 10)],
    ['action', 'upload'],
    ['roles', 'author', 'moderator']
  ])

  const encoded = encodeToken(event)
  assert.match(encoded, /^[A-Za-z0-9_-]+$/)
  assert.deepEqual(decodeToken(encoded), event)
  assert.deepEqual(decodeToken(`Nostr ${encoded}`), event)
  assert.equal(encodeToken(event, { includeAuthorizationScheme: true }), `Nostr ${encoded}`)
  assert.equal(decodeToken(encodeToken({ ...event, meta: { relay: 'wss://relay.example' } })).meta, undefined)

  const token = validateToken(`Nostr ${encoded}`, {
    audience: 'cdn.example.com',
    signer: event.pubkey,
    issuer: 'issuer',
    subject: 'subject',
    now: NOW
  })
  assert.equal(token.id, event.id)
  assert.equal(token.signer, event.pubkey)
  assert.equal(token.issuedAt, NOW)
  assert.equal(token.expiration, NOW + 300)
  assert.equal(token.notBefore, NOW - 10)
  assert.deepEqual(token.audience, ['api.example.com', 'cdn.example.com'])
  assert.deepEqual(token.claims, [
    ['action', 'upload'],
    ['roles', 'author', 'moderator']
  ])
})

test('NWT applies issuer, subject and time defaults', async () => {
  const event = await createToken({ signEvent, createdAt: NOW })
  const token = validateToken(event, { now: NOW })
  assert.equal(token.issuer, event.pubkey)
  assert.equal(token.subject, event.pubkey)
  assert.equal(token.issuedAt, NOW)
  assert.equal(token.expiration, null)
  assert.equal(token.notBefore, null)
  assert.deepEqual(token.audience, [])

  assert.throws(() => validateToken(event, { now: NOW, requireAudience: true }), /NWT_AUDIENCE_REQUIRED/)
  assert.throws(() => validateToken(event, { now: NOW, requireExpiration: true }), /NWT_EXPIRATION_REQUIRED/)
  assert.doesNotThrow(() => validateToken(event, { now: NOW, audience: 'anyone.example' }))
})

test('NWT enforces exact audience and optional identity constraints', async () => {
  const event = await createToken({
    signEvent,
    createdAt: NOW,
    audience: 'api.example.com',
    expiration: NOW + 60
  })

  assert.throws(() => validateToken(event, { now: NOW }), /NWT_AUDIENCE_REQUIRED/)
  assert.throws(() => validateToken(event, { now: NOW, audience: 'other.example' }), /NWT_AUDIENCE_MISMATCH/)
  assert.throws(() => validateToken(event, { now: NOW, audience: 'api.example.com', signer: 'other' }), /NWT_SIGNER_MISMATCH/)
  assert.throws(() => validateToken(event, { now: NOW, audience: 'api.example.com', issuer: 'other' }), /NWT_ISSUER_MISMATCH/)
  assert.throws(() => validateToken(event, { now: NOW, audience: 'api.example.com', subject: 'other' }), /NWT_SUBJECT_MISMATCH/)
  assert.doesNotThrow(() => validateToken(event, {
    now: NOW,
    audience: ['other.example', 'api.example.com'],
    requireAudience: true,
    requireExpiration: true
  }))
})

test('NWT enforces not-before, expiration and clock skew boundaries', async () => {
  const event = await createToken({
    signEvent,
    createdAt: NOW,
    notBefore: NOW + 100,
    expiration: NOW + 200
  })

  assert.throws(() => validateToken(event, { now: NOW + 89, clockSkewSeconds: 10 }), /NWT_NOT_YET_VALID/)
  assert.doesNotThrow(() => validateToken(event, { now: NOW + 90, clockSkewSeconds: 10 }))
  assert.doesNotThrow(() => validateToken(event, { now: NOW + 209, clockSkewSeconds: 10 }))
  assert.throws(() => validateToken(event, { now: NOW + 210, clockSkewSeconds: 10 }), /NWT_EXPIRED/)

  await assert.rejects(createToken({
    signEvent,
    createdAt: NOW,
    notBefore: NOW + 2,
    expiration: NOW + 1
  }), /INVALID_TIME_WINDOW/)
})

test('NWT rejects changed signer output and invalid signed events', async () => {
  await assert.rejects(createToken({
    createdAt: NOW,
    content: 'expected',
    signEvent: template => signEvent({ ...template, content: 'changed' })
  }), /SIGNED_NWT_EVENT_WAS_CHANGED/)

  const wrongKind = signEvent({ kind: 1, created_at: NOW, tags: [], content: '' })
  assert.throws(() => encodeToken(wrongKind), /INVALID_NWT_KIND/)
  const event = await createToken({ signEvent, createdAt: NOW })
  assert.throws(() => validateToken({ ...event, content: 'changed' }, { now: NOW }), /INVALID_NWT_EVENT/)
})

test('NWT rejects malformed and duplicate registered claims', () => {
  const event = tags => signEvent({ kind: NWT, created_at: NOW, tags, content: '' })

  assert.throws(() => validateToken(event([['iss', 'one'], ['iss', 'two']]), { now: NOW }), /DUPLICATE_SINGLE_CLAIM/)
  assert.throws(() => validateToken(event([['aud', 'a', 'extra']]), { now: NOW, audience: 'a' }), /INVALID_REGISTERED_CLAIM/)
  assert.throws(() => validateToken(event([['iat', '01']]), { now: NOW }), /INVALID_IAT_CLAIM/)
  assert.throws(() => validateToken(event([['exp', '-1']]), { now: NOW }), /INVALID_EXP_CLAIM/)
  assert.throws(() => validateToken(event([['nbf', '1.5']]), { now: NOW }), /INVALID_NBF_CLAIM/)
  assert.throws(() => validateToken(event([['exp', String(Number.MAX_SAFE_INTEGER + 1)]]), { now: NOW }), /INVALID_EXP_CLAIM/)
  assert.throws(() => validateToken(event([['custom']]), { now: NOW }), /INVALID_CLAIM/)
  assert.throws(() => validateToken(event(Array.from({ length: 513 }, (_, index) => ['claim', String(index)])), { now: NOW }), /TOO_MANY_CLAIMS/)
})

test('NWT creation validates claims before asking the signer', async () => {
  let calls = 0
  const signer = template => {
    calls++
    return signEvent(template)
  }

  await assert.rejects(createToken({ signEvent: signer, createdAt: NOW, claims: [['aud', 'hidden']] }), /INVALID_CUSTOM_CLAIM/)
  await assert.rejects(createToken({ signEvent: signer, createdAt: NOW, audience: [''] }), /INVALID_AUDIENCE/)
  await assert.rejects(createToken({ signEvent: signer, createdAt: NOW, expiration: -1 }), /INVALID_EXPIRATION/)
  assert.equal(calls, 0)
})

test('NWT transport decoding is strict and canonical', async () => {
  const event = await createToken({ signEvent, createdAt: NOW })
  const encoded = encodeToken(event)

  for (const value of [
    '',
    `${encoded}=`,
    `Bearer ${encoded}`,
    `Nostr  ${encoded}`,
    'A',
    bytesToBase64Url(new Uint8Array([0xff])),
    bytesToBase64Url(new TextEncoder().encode('null')),
    bytesToBase64Url(new TextEncoder().encode('{'))
  ]) assert.throws(() => decodeToken(value), /INVALID/)
})
