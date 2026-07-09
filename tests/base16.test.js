import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  base16ToBytes,
  bytesToBase16,
  bytesToHex,
  hexToBytes
} from '../base16/index.js'

test('base16 helpers encode bytes and expose hex aliases', () => {
  const bytes = new Uint8Array([0, 1, 15, 16, 254, 255])
  const encoded = '00010f10feff'

  assert.equal(bytesToBase16(bytes), encoded)
  assert.deepEqual(base16ToBytes(encoded), bytes)
  assert.equal(bytesToHex, bytesToBase16)
  assert.equal(hexToBytes, base16ToBytes)
})
