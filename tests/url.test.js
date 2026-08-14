import { test } from 'node:test'
import assert from 'node:assert/strict'

import { ValidationError } from '../error/index.js'
import {
  assertValidPublicBlossomServerUrl,
  assertValidPublicRelayUrl,
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
