import { test } from 'node:test'
import assert from 'node:assert/strict'

import { isValidPublicRelayUrl, normalizeRelayUrl } from '../url/index.js'

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
  assert.equal(isValidPublicRelayUrl('wss://relay.example.com'), true)
  assert.equal(isValidPublicRelayUrl('https://relay.example.com/'), true)
  assert.equal(isValidPublicRelayUrl('relay.example.com'), true)
  assert.equal(isValidPublicRelayUrl('wss://8.8.8.8'), true)
  assert.equal(isValidPublicRelayUrl('wss://[2606:4700:4700::1111]'), true)
})

test('isValidPublicRelayUrl rejects local, private and disguised relay URLs', () => {
  for (const value of [
    'ws://relay.example.com',
    'wss://localhost',
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
  ]) assert.equal(isValidPublicRelayUrl(value), false, value)
})
