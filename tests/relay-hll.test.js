import assert from 'node:assert/strict'
import { test } from 'node:test'
import { decodeHll, encodeHll, estimateHllCount, mergeHll } from '../relay/helpers/hll.js'

function registers (entries = {}) {
  const value = new Uint8Array(256)
  for (const [index, register] of Object.entries(entries)) value[Number(index)] = register
  return value
}

test('HLL helpers validate and normalize NIP-45 register payloads', () => {
  const value = registers({ 0: 10, 255: 15 })
  const encoded = encodeHll(value)

  assert.equal(encoded.length, 512)
  assert.equal(encoded.slice(0, 2), '0a')
  assert.equal(encoded.slice(-2), '0f')
  assert.deepEqual(decodeHll(encoded.toUpperCase()), value)
  assert.equal(decodeHll(''), null)
  assert.equal(decodeHll('x'.repeat(512)), null)
  assert.equal(decodeHll('0'.repeat(511)), null)
})

test('HLL helpers merge registers and estimate cardinality', () => {
  const target = registers({ 0: 1, 1: 5 })
  const source = registers({ 0: 4, 2: 3 })

  assert.deepEqual(mergeHll(target, source), registers({ 0: 4, 1: 5, 2: 3 }))
  assert.equal(estimateHllCount(registers()), 0)
  assert.equal(estimateHllCount(registers({ 8: 1 })), 1)
  assert.equal(estimateHllCount(new Uint8Array(256).fill(1)), 367)
})
