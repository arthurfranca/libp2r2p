import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createLookupWarningLogger } from '../private-channel/helpers/lookup-warning.js'

test('lookup warning logger keeps the first failure and debounces repeats', () => {
  let time = 1_000
  const calls = []
  const warn = createLookupWarningLogger({
    intervalMs: 30_000,
    now: () => time,
    warn: (...args) => calls.push(args)
  })

  assert.equal(warn(new Error('offline'), 'recipients: 2'), true)
  assert.equal(warn(new Error('offline'), 'recipients: 2'), false)
  assert.equal(warn(new Error('offline'), 'recipients: 2'), false)
  assert.deepEqual(calls, [
    ['private-messenger content-key lookup failed (recipients: 2)', 'offline']
  ])

  time += 30_000
  assert.equal(warn(new Error('offline'), 'recipients: 2'), true)
  assert.deepEqual(calls, [
    ['private-messenger content-key lookup failed (recipients: 2)', 'offline'],
    ['private-messenger content-key lookup failed (recipients: 2) [2 repeated failures suppressed]', 'offline']
  ])
})
