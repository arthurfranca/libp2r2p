import { test } from 'node:test'
import assert from 'node:assert/strict'

import { ValidationError } from '../error/index.js'

import {
  naddrEncode,
  neventEncode,
  noteEncode,
  npubEncode,
  nprofileEncode,
  nrelayEncode
} from '../nip19/index.js'
import {
  decodeMediaMetadata,
  decodeReference,
  decodeUserReference,
  encodeUserReference,
  extractMedia,
  resolveUserReference,
  tryDecodeMediaMetadata,
  tryDecodeReference,
  tryDecodeUserReference
} from '../nip27/index.js'

const hex = 'ab'.repeat(32)
const npub = npubEncode(hex)
const nprofile = nprofileEncode({
  pubkey: 'cd'.repeat(32),
  relays: ['wss://relay.example.com']
})
const note = noteEncode('ef'.repeat(32))
const nevent = neventEncode({
  id: '11'.repeat(32),
  author: '22'.repeat(32),
  kind: 1,
  relays: []
})
const naddr = naddrEncode({
  identifier: 'my-list',
  pubkey: '33'.repeat(32),
  kind: 30499,
  relays: []
})
const nrelay = nrelayEncode('wss://relay.example.com')

test('decodeReference parses NIP-05 standard, root and compact custom forms', () => {
  assert.deepEqual(decodeReference('fiatjaf.com'), {
    type: 'nip05',
    original: 'fiatjaf.com',
    value: 'fiatjaf.com',
    local: '_',
    domain: 'fiatjaf.com'
  })
  assert.deepEqual(decodeReference('@fiatjaf.com'), {
    type: 'nip05',
    original: '@fiatjaf.com',
    value: 'fiatjaf.com',
    local: '_',
    domain: 'fiatjaf.com'
  })
  assert.deepEqual(decodeReference('@bob@fiatjaf.com'), {
    type: 'nip05',
    original: '@bob@fiatjaf.com',
    value: 'bob.fiatjaf.com',
    local: 'bob',
    domain: 'fiatjaf.com'
  })
  assert.deepEqual(decodeReference('bob.fiatjaf.com'), {
    type: 'nip05',
    original: 'bob.fiatjaf.com',
    value: 'bob.fiatjaf.com',
    local: 'bob',
    domain: 'fiatjaf.com'
  })
  assert.equal(decodeReference('_@fiatjaf.com.br').value, '_@fiatjaf.com.br')
})

test('decodeReference accepts @ and nostr: prefixes for NIP-19 entities', () => {
  assert.equal(decodeReference(`nostr:${npub}`).type, 'pubkey')
  assert.equal(decodeReference(`nostr:${npub}`).pubkey, hex)
  assert.equal(decodeReference(`@${npub}`).pubkey, hex)

  const profileRef = decodeReference(`nostr:${nprofile}`)
  assert.equal(profileRef.type, 'pubkey')
  assert.equal(profileRef.pubkey, 'cd'.repeat(32))
  assert.deepEqual(profileRef.relays, ['wss://relay.example.com'])

  const noteRef = decodeReference(`nostr:${note}`)
  assert.deepEqual(noteRef, {
    type: 'note',
    original: `nostr:${note}`,
    value: note,
    id: 'ef'.repeat(32)
  })

  const neventRef = decodeReference(`@${nevent}`)
  assert.equal(neventRef.type, 'nevent')
  assert.equal(neventRef.id, '11'.repeat(32))
  assert.equal(neventRef.author, '22'.repeat(32))
  assert.equal(neventRef.kind, 1)

  const naddrRef = decodeReference(`nostr:${naddr}`)
  assert.equal(naddrRef.type, 'naddr')
  assert.equal(naddrRef.identifier, 'my-list')
  assert.equal(naddrRef.kind, 30499)

  const relayRef = decodeReference(`nostr:${nrelay}`)
  assert.equal(relayRef.type, 'nrelay')
  assert.equal(relayRef.relay, 'wss://relay.example.com')
})

test('decodeReference rejects invalid references', () => {
  for (const value of ['', 'not-a-reference', '@', 'nostr:']) {
    assert.throws(() => decodeReference(value), error => (
      error instanceof ValidationError && error.code === 'INVALID_REFERENCE'
    ), value)
    assert.equal(tryDecodeReference(value), null)
  }
})

test('decodeReference throws the underlying decoder error for malformed entities', () => {
  assert.throws(() => decodeReference('note1notavalidbech32'), error => (
    error instanceof ValidationError
  ))
  assert.equal(tryDecodeReference('note1notavalidbech32'), null)
})

test('extractMedia finds mentions, entities, URLs and hashtags', () => {
  const content = [
    'hi @fiatjaf.com and @bob@example.com',
    `nostr:${npub} @${nprofile}`,
    `nostr:${note} nostr:${nevent} nostr:${naddr} nostr:${nrelay}`,
    'https://example.com/a.png#m=image%2Fpng&dim=640x480 #nostr'
  ].join(' ')

  const items = extractMedia(content)
  const keys = items.map(item => item.key)
  assert.deepEqual(keys, [
    'text',
    'nip05',
    'text',
    'nip05',
    'text',
    'profile',
    'text',
    'profile',
    'text',
    'event',
    'text',
    'event',
    'text',
    'event',
    'text',
    'relay',
    'text',
    'url',
    'text',
    'hashtag'
  ])

  const nip05s = items.filter(item => item.key === 'nip05')
  assert.equal(nip05s[0].nip05.value, 'fiatjaf.com')
  assert.equal(nip05s[0].nip05.original, '@fiatjaf.com')
  assert.equal(nip05s[1].nip05.value, 'bob.example.com')
  assert.equal(nip05s[1].nip05.local, 'bob')

  const profiles = items.filter(item => item.key === 'profile')
  assert.equal(profiles[0].profile.pubkey, hex)
  assert.equal(profiles[0].profile.npub, npub)
  assert.equal(profiles[1].profile.pubkey, 'cd'.repeat(32))
  assert.deepEqual(profiles[1].profile.relays, ['wss://relay.example.com'])
  assert.equal(profiles[1].profile.nip19Type, 'nprofile')

  const events = items.filter(item => item.key === 'event')
  assert.equal(events[0].event.id, 'ef'.repeat(32))
  assert.equal(events[1].event.author, '22'.repeat(32))
  assert.equal(events[2].event.identifier, 'my-list')

  const relay = items.find(item => item.key === 'relay')
  assert.equal(relay.relay.relay, 'wss://relay.example.com')

  const url = items.find(item => item.key === 'url')
  assert.equal(url.url.m, 'image/png')
  assert.equal(url.url.width, '640')
  assert.equal(url.url.height, '480')

  const hashtag = items.find(item => item.key === 'hashtag')
  assert.equal(hashtag.hashtag.value, 'nostr')
})

test('extractMedia treats bare compact NIP-05 as URL unless opted in', () => {
  const content = 'visit bob.example.com today'
  const defaultItems = extractMedia(content)
  assert.equal(defaultItems[1].key, 'url')
  assert.equal(defaultItems[1].url.value, 'https://bob.example.com')

  const bareItems = extractMedia(content, { bareNip05: true })
  assert.equal(bareItems[1].key, 'nip05')
  assert.equal(bareItems[1].nip05.value, 'bob.example.com')
})

test('decodeMediaMetadata decodes fragment tags without depending on window', () => {
  const metadata = decodeMediaMetadata(
    'https://example.com/a.png#m=image%2Fpng&dim=640x480&alt=Hello%20world&x=a&x=b'
  )
  assert.equal(metadata.m, 'image/png')
  assert.equal(metadata.width, '640')
  assert.equal(metadata.height, '480')
  assert.equal(metadata.alt, 'Hello world')
  assert.deepEqual(metadata.x, ['a', 'b'])

  assert.deepEqual(decodeMediaMetadata('https://example.com/no-fragment'), {})
})

test('decodeMediaMetadata rejects invalid urls, tag configs and dim values', () => {
  assert.throws(() => decodeMediaMetadata(''), error => (
    error instanceof ValidationError && error.code === 'INVALID_MEDIA_METADATA_URL'
  ))
  assert.throws(() => decodeMediaMetadata('https://example.com/a#dim=abc'), error => (
    error instanceof ValidationError && error.code === 'INVALID_MEDIA_METADATA_DIM'
  ))
  assert.throws(() => decodeMediaMetadata('https://example.com/a', {
    extraTags: { m: 'not-an-array' }
  }), error => (
    error instanceof ValidationError && error.code === 'INVALID_MEDIA_METADATA_TAGS'
  ))
  assert.equal(tryDecodeMediaMetadata(''), null)
  assert.equal(tryDecodeMediaMetadata('https://example.com/a#dim=abc'), null)
  assert.deepEqual(tryDecodeMediaMetadata('https://example.com/no-fragment'), {})
})

test('extractMedia rejects non-string content and tolerates malformed tokens', () => {
  assert.throws(() => extractMedia(null), error => (
    error instanceof ValidationError && error.code === 'INVALID_MEDIA_CONTENT'
  ))

  const badNote = `note1${'a'.repeat(60)}`
  const items = extractMedia(`hello nostr:${badNote} world`)
  assert.deepEqual(items, [
    { key: 'text', text: { value: 'hello ' } },
    { key: 'text', text: { value: ' world' } }
  ])
})

test('decodeUserReference handles pubkey and NIP-05 references', () => {
  const hex = 'ef'.repeat(32)
  assert.deepEqual(decodeUserReference(hex), { type: 'pubkey', pubkey: hex, relays: [], raw: hex })
  assert.equal(decodeUserReference('fiatjaf.com').local, '_')
  assert.equal(decodeUserReference('bob.xyz.abc.example.com').local, 'bob')
  assert.equal(decodeUserReference('bob@example.com').raw, 'bob.example.com')
  assert.equal(decodeUserReference('@bob@example.com').raw, 'bob.example.com')
  assert.equal(decodeUserReference('@bob.example.com').raw, 'bob.example.com')
  assert.equal(decodeUserReference('@fiatjaf.com').local, '_')
  assert.equal(decodeUserReference('_@fiatjaf.com.br').raw, '_@fiatjaf.com.br')

  const npub = npubEncode(hex)
  assert.equal(decodeUserReference(`nostr:${npub}`).pubkey, hex)
  assert.equal(decodeUserReference(`@${npub}`).pubkey, hex)
  const nprofile = nprofileEncode({ pubkey: hex, relays: ['wss://relay.example'] })
  assert.deepEqual(decodeUserReference(`nostr:${nprofile}`).relays, ['wss://relay.example'])
})

test('decodeUserReference rejects invalid references', () => {
  for (const value of ['', 'not-a-reference', '@', 'nostr:']) {
    assert.throws(() => decodeUserReference(value), error => (
      error instanceof ValidationError && error.code === 'INVALID_USER_REFERENCE'
    ), value)
    assert.equal(tryDecodeUserReference(value), null)
  }
})

test('encodeUserReference returns the canonical compact spelling', () => {
  assert.equal(encodeUserReference('bob@example.com'), 'bob.example.com')
  assert.equal(encodeUserReference('_@fiatjaf.com.br'), '_@fiatjaf.com.br')
  assert.equal(
    encodeUserReference({ type: 'nip05', local: 'bob', domain: 'example.com' }),
    'bob.example.com'
  )
  const hex = 'ef'.repeat(32)
  assert.equal(encodeUserReference({ type: 'pubkey', pubkey: hex, relays: [] }), hex)
  assert.throws(() => encodeUserReference('nonsense'), error => (
    error instanceof ValidationError && error.code === 'INVALID_USER_REFERENCE'
  ))
})

test('resolveUserReference resolves root, compact and prefixed references', async () => {
  const hex = 'ef'.repeat(32)
  const result = await resolveUserReference('fiatjaf.com', {
    fetch: async () => new Response(JSON.stringify({
      names: { _: hex },
      relays: { [hex]: ['relay.example.com'] }
    }), { status: 200 })
  })
  assert.deepEqual(result, { pubkey: hex, relays: ['wss://relay.example.com'], label: 'fiatjaf.com' })

  const npub = npubEncode(hex)
  const npubResult = await resolveUserReference(npub)
  assert.deepEqual(npubResult, { pubkey: hex, relays: [], label: npub })

  const compactResult = await resolveUserReference('bob@example.com', {
    fetch: async () => new Response(JSON.stringify({
      names: { bob: hex },
      relays: {}
    }), { status: 200 })
  })
  assert.equal(compactResult.label, 'bob.example.com')
})

test('inline download intent requires exactly one explicit 0 or 1', () => {
  for (const download of ['0', '1']) {
    const url = `https://example.com/a.png#download=${download}`
    assert.deepEqual(decodeMediaMetadata(url), { download })
    assert.equal(extractMedia(url)[0].url.download, download)
  }
  for (const fragment of ['download', 'download=', 'download=2', 'download=true', 'download=01', 'download=1/foo', 'download=1:0', 'download=1,0', 'download=1=0', 'download=0&download=1']) {
    const url = `https://example.com/a#${fragment}`
    assert.throws(() => decodeMediaMetadata(url), { code: 'INVALID_MEDIA_METADATA_DOWNLOAD' })
    assert.equal(tryDecodeMediaMetadata(url), null)
  }
})
