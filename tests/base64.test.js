import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  base64ToBytes,
  base64UrlToBytes,
  bytesToBase64,
  bytesToBase64Url
} from '../base64/index.js'

test('base64 helpers encode bytes with plain and URL-safe alphabets', () => {
  const bytes = new Uint8Array([0, 1, 2, 251, 252, 253, 254, 255])

  assert.equal(bytesToBase64(bytes), 'AAEC+/z9/v8=')
  assert.deepEqual(base64ToBytes('AAEC+/z9/v8='), bytes)
  assert.equal(bytesToBase64Url(bytes), 'AAEC-_z9_v8')
  assert.deepEqual(base64UrlToBytes('AAEC-_z9_v8'), bytes)
})
