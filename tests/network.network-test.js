import assert from 'node:assert/strict'
import { test } from 'node:test'
import { isOnline } from '../network/index.js'

test('connectivity probe reaches a real public endpoint', { timeout: 25000 }, async () => {
  assert.equal(await isOnline(), true)
})
