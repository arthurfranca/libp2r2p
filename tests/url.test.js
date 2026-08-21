import { test } from 'node:test'
import assert from 'node:assert/strict'

import { ValidationError } from '../error/index.js'
import { appEncode, npubEncode, nprofileEncode } from '../nip19/index.js'
import {
  APP_URL_MIN_ENTITY_BODY_LENGTH,
  assertValidPublicBlossomServerUrl,
  assertValidPublicRelayUrl,
  decodeAppUrl,
  encodeAppUrl,
  isValidPublicBlossomServerUrl,
  isValidPublicRelayUrl,
  normalizeBlossomServerUrl,
  normalizeRelayUrl
} from '../url/index.js'

test('normalizeRelayUrl canonicalizes relay URLs', () => {
  assert.equal(normalizeRelayUrl('HTTPS://EXAMPLE.COM:443//relay/?z=2&a=1#x'), 'wss://example.com/relay?a=1&z=2')
  assert.equal(normalizeRelayUrl('example.com'), 'wss://example.com')
  assert.equal(normalizeRelayUrl('http://example.com:80/a/'), 'ws://example.com/a')
})

test('normalizeRelayUrl removes trailing separators', () => {
  assert.equal(normalizeRelayUrl('wss://example.com////?&&#'), 'wss://example.com')
  assert.equal(normalizeRelayUrl('wss://example.com/path///?a=1&&b=2&#fragment'), 'wss://example.com/path?a=1&b=2')
})

test('normalizeRelayUrl rejects unsupported or invalid URLs', () => {
  assert.throws(() => normalizeRelayUrl('ftp://example.com'), /INVALID_RELAY_PROTOCOL/)
  assert.throws(() => normalizeRelayUrl(''), /URL_SHOULD_BE_A_STRING/)
})

test('isValidPublicRelayUrl accepts secure public relay URLs', () => {
  const value = 'wss://relay.example.com'
  assert.equal(isValidPublicRelayUrl(value), true)
  assert.equal(assertValidPublicRelayUrl(value), value)
  assert.equal(isValidPublicRelayUrl('https://relay.example.com/'), true)
  assert.equal(isValidPublicRelayUrl('relay.example.com'), true)
  assert.equal(isValidPublicRelayUrl('wss://8.8.8.8'), true)
  assert.equal(isValidPublicRelayUrl('wss://[2606:4700:4700::1111]'), true)
})

test('isValidPublicRelayUrl rejects local, private and disguised relay URLs', () => {
  for (const value of [
    'ws://relay.example.com',
    'wss://localhost',
    'wss://localhost.',
    'wss://relay.local',
    'wss://relay.onion',
    'wss://127.0.0.1',
    'wss://10.0.0.1',
    'wss://100.64.0.1',
    'wss://172.16.0.1',
    'wss://192.168.0.1',
    'wss://169.254.0.1',
    'wss://[::1]',
    'wss://[fd00::1]',
    'wss://[fe80::1]',
    'wss://[2001:db8::1]',
    'wss://example',
    'wss://npub1example.com',
    'wss://relay.example.com/https://other.example'
  ]) {
    assert.equal(isValidPublicRelayUrl(value), false, value)
    assert.throws(() => assertValidPublicRelayUrl(value), ValidationError)
  }

  assert.throws(() => assertValidPublicRelayUrl('ws://relay.example.com'), error => (
    error instanceof ValidationError && error.code === 'INSECURE_RELAY_URL'
  ))
})

test('isValidPublicRelayUrl accepts onion, standardized local relays and nostr entity URLs under policy', () => {
  const onion = 'ws://oxtrdevav64z64yb7x6rjg4ntzqjhedm5b5zjqulugknhzr46ny2qbad.onion'
  const policy = { onion: true, localRelay: true, nostrEntityUrls: true }

  assert.equal(isValidPublicRelayUrl(onion, policy), true)
  assert.equal(isValidPublicRelayUrl('ws://localhost:4869', policy), true)
  assert.equal(isValidPublicRelayUrl('wss://relay.example/npub1abc', policy), true)
  assert.equal(isValidPublicRelayUrl('wss://nprofile1.example', policy), true)
  assert.equal(assertValidPublicRelayUrl(onion, policy), onion)

  // Each exception stays opt-in: without the matching policy flag it is rejected.
  assert.equal(isValidPublicRelayUrl(onion), false)
  assert.equal(isValidPublicRelayUrl('ws://localhost:4869'), false)
  assert.equal(isValidPublicRelayUrl('wss://npub1example.com', { nostrEntityUrls: true }), true)
  assert.equal(isValidPublicRelayUrl('wss://npub1example.com'), false)

  // Everything else keeps being rejected even with the policy enabled.
  for (const value of [
    'ws://relay.example.com',
    'ws://localhost:8080',
    'wss://localhost',
    'ws://relay.onion.evil.example',
    'ws://127.0.0.1:4869',
    'ws://localhost:4869/other'
  ]) {
    assert.equal(isValidPublicRelayUrl(value, policy), false, value)
  }
})

test('normalizeBlossomServerUrl canonicalizes root HTTP origins', () => {
  assert.equal(normalizeBlossomServerUrl(' HTTPS://BLOSSOM.EXAMPLE.COM:443/ '), 'https://blossom.example.com')
  assert.equal(normalizeBlossomServerUrl('http://localhost:3000////'), 'http://localhost:3000')
  assert.equal(normalizeBlossomServerUrl('https://BLOSSOM.EXAMPLE.COM../'), 'https://blossom.example.com')
})

test('normalizeBlossomServerUrl rejects ambiguous or non-Blossom base URLs', () => {
  const cases = [
    ['', 'INVALID_BLOSSOM_SERVER_URL'],
    ['blossom.example.com', 'INVALID_BLOSSOM_SERVER_URL'],
    ['wss://blossom.example.com', 'INVALID_BLOSSOM_SERVER_PROTOCOL'],
    ['https://user:secret@blossom.example.com', 'BLOSSOM_SERVER_URL_CREDENTIALS_NOT_ALLOWED'],
    ['https://blossom.example.com/api', 'BLOSSOM_SERVER_URL_PATH_NOT_ALLOWED'],
    ['https://blossom.example.com?token=x', 'BLOSSOM_SERVER_URL_QUERY_NOT_ALLOWED'],
    ['https://blossom.example.com#fragment', 'BLOSSOM_SERVER_URL_FRAGMENT_NOT_ALLOWED']
  ]
  for (const [value, code] of cases) {
    assert.throws(() => normalizeBlossomServerUrl(value), error => (
      error instanceof ValidationError && error.code === code
    ), value)
  }
})

test('decodeAppUrl recognizes legacy NIP-19 app entities', () => {
  const entity = appEncode({
    dTag: 'apps',
    pubkey: 'ab'.repeat(32),
    kind: 35128
  })
  assert.deepEqual(decodeAppUrl(entity), { type: 'entity', entity })
})

test('decodeAppUrl parses named URLs without user and enforces the reserved entity length', () => {
  assert.deepEqual(decodeAppUrl('+apps'), {
    type: 'named',
    prefix: '+',
    channel: 'main',
    appName: 'apps',
    user: null
  })
  assert.deepEqual(decodeAppUrl('++app store'), {
    type: 'named',
    prefix: '++',
    channel: 'next',
    appName: 'app store',
    user: null
  })
  assert.equal(decodeAppUrl(`+${'a'.repeat(APP_URL_MIN_ENTITY_BODY_LENGTH)}`), null)
  assert.equal(decodeAppUrl('+'), null)
  assert.equal(decodeAppUrl('+app/route'), null)
})

test('decodeAppUrl parses NIP-05 standard, root and custom extension forms', () => {
  assert.deepEqual(decodeAppUrl('+app@bob@example.com').user, {
    kind: 'nip05',
    local: 'bob',
    domain: 'example.com',
    raw: 'bob.example.com'
  })
  assert.deepEqual(decodeAppUrl('+app@fiatjaf.com').user, {
    kind: 'nip05',
    local: '_',
    domain: 'fiatjaf.com',
    raw: 'fiatjaf.com'
  })
  assert.deepEqual(decodeAppUrl('+app@bob.xyz.abc.example.com').user, {
    kind: 'nip05',
    local: 'bob',
    domain: 'xyz.abc.example.com',
    raw: 'bob.xyz.abc.example.com'
  })
  assert.deepEqual(decodeAppUrl('+app@_@fiatjaf.com.br').user, {
    kind: 'nip05',
    local: '_',
    domain: 'fiatjaf.com.br',
    raw: '_@fiatjaf.com.br'
  })
})

test('decodeAppUrl keeps @ inside app names and decodes percent-encoded UTF-8', () => {
  const decoded = decodeAppUrl('+my%40app@bob@example.com')
  assert.equal(decoded.type, 'named')
  assert.equal(decoded.appName, 'my@app')
  assert.equal(decoded.user.local, 'bob')

  const unicode = decodeAppUrl('+caf%C3%A9%20%E6%97%A5%E6%9C%AC@fiatjaf.com')
  assert.equal(unicode.appName, 'café 日本')
  assert.equal(unicode.user.local, '_')
})

test('decodeAppUrl accepts npub, nprofile and hex users', () => {
  const hex = 'ab'.repeat(32)
  assert.equal(decodeAppUrl(`+app@${hex}`).user.kind, 'pubkey')
  assert.equal(decodeAppUrl(`+app@${hex}`).user.pubkey, hex)

  const npub = npubEncode(hex)
  assert.equal(decodeAppUrl(`+app@${npub}`).user.kind, 'pubkey')
  const nprofile = nprofileEncode({ pubkey: hex, relays: ['wss://relay.example'] })
  const nprofileUser = decodeAppUrl(`+app@${nprofile}`).user
  assert.equal(nprofileUser.kind, 'pubkey')
  assert.deepEqual(nprofileUser.relays, ['wss://relay.example'])
})

test('encodeAppUrl round-trips through decodeAppUrl', () => {
  const cases = [
    { appName: 'apps', channel: 'main', user: 'fiatjaf.com' },
    { appName: 'my@app', channel: 'main', user: 'bob@example.com' },
    { appName: 'app store', channel: 'next', user: '_@fiatjaf.com.br' },
    { appName: 'café 日本', channel: 'draft', user: 'ab'.repeat(32) },
    { appName: 'my@app', channel: 'main', user: npubEncode('cd'.repeat(32)) }
  ]
  for (const input of cases) {
    const encoded = encodeAppUrl(input)
    const decoded = decodeAppUrl(encoded)
    assert.equal(decoded.type, 'named')
    assert.equal(decoded.channel, input.channel)
    assert.equal(decoded.appName, input.appName)
    assert.ok(decoded.user)
  }
})

test('encodeAppUrl picks the shortest unambiguous NIP-05 form', () => {
  assert.equal(
    encodeAppUrl({ appName: 'app', channel: 'main', user: 'bob@example.com' }),
    '+app@bob.example.com'
  )
  assert.equal(
    encodeAppUrl({ appName: 'my@app', channel: 'main', user: 'bob@example.com' }),
    '+my@app@bob@example.com'
  )
  assert.equal(
    encodeAppUrl({ appName: 'app', channel: 'main', user: 'fiatjaf.com' }),
    '+app@fiatjaf.com'
  )
  assert.equal(
    encodeAppUrl({ appName: 'my@app', channel: 'main', user: 'fiatjaf.com' }),
    '+my@app@_@fiatjaf.com'
  )
})

test('isValidPublicBlossomServerUrl accepts secure public origins', () => {
  const value = 'https://blossom.example.com/'
  assert.equal(isValidPublicBlossomServerUrl(value), true)
  assert.equal(assertValidPublicBlossomServerUrl(value), value)
  assert.equal(isValidPublicBlossomServerUrl('https://8.8.8.8'), true)
  assert.equal(isValidPublicBlossomServerUrl('https://[2606:4700:4700::1111]'), true)
})

test('isValidPublicBlossomServerUrl rejects insecure and non-public origins', () => {
  for (const value of [
    'http://blossom.example.com',
    'https://localhost',
    'https://localhost.',
    'https://blossom.local',
    'https://blossom.onion',
    'https://127.0.0.1',
    'https://10.0.0.1',
    'https://100.64.0.1',
    'https://172.16.0.1',
    'https://192.168.0.1',
    'https://169.254.0.1',
    'https://[::1]',
    'https://[fd00::1]',
    'https://[fe80::1]',
    'https://[2001:db8::1]',
    'https://blossom',
    'https://blossom.example.com/path',
    'https://user:secret@blossom.example.com'
  ]) {
    assert.equal(isValidPublicBlossomServerUrl(value), false, value)
    assert.throws(() => assertValidPublicBlossomServerUrl(value), ValidationError)
  }
  assert.throws(() => assertValidPublicBlossomServerUrl('http://blossom.example.com'), error => (
    error instanceof ValidationError && error.code === 'INSECURE_BLOSSOM_SERVER_URL'
  ))
})
