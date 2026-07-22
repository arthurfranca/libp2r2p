import { test } from 'node:test'
import assert from 'node:assert/strict'

import { normalizeUrl } from '../url/index.js'

test('normalizeUrl canonicalizes relay URLs', () => {
  assert.equal(normalizeUrl('HTTPS://EXAMPLE.COM:443//relay/?z=2&a=1#x'), 'wss://example.com/relay?a=1&z=2')
  assert.equal(normalizeUrl('example.com'), 'wss://example.com/')
  assert.equal(normalizeUrl('http://example.com:80/a/'), 'ws://example.com/a')
})

test('normalizeUrl rejects unsupported or invalid URLs', () => {
  assert.throws(() => normalizeUrl('ftp://example.com'), /INVALID_RELAY_PROTOCOL/)
  assert.throws(() => normalizeUrl(''), /URL_SHOULD_BE_A_STRING/)
})
