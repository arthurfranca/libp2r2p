import assert from 'node:assert/strict'
import { test } from 'node:test'
import { parseRelayRetryAdvice, isRetryableRelayFailure, isReplaceableRelayFailure } from '../relay/index.js'
import { relayRejectionError } from '../relay/helpers/error.js'

const message = 'rate-limited: busy'

test('absolute params retain their deadline across delayed and expired delivery', () => {
  for (const now of [10000, 14000, 25000]) {
    assert.deepEqual(parseRelayRetryAdvice(message, '@20000', { now }), { retryAt: 20000 })
  }
})

test('params use integer milliseconds, including one millisecond, and bound future waits', () => {
  assert.deepEqual(parseRelayRetryAdvice(message, '1', { now: 10000 }), { retryAfterMs: 1, retryAt: 10001 })
  assert.deepEqual(parseRelayRetryAdvice(message, '2500', { now: 10000 }), { retryAfterMs: 2500, retryAt: 12500 })
  assert.deepEqual(parseRelayRetryAdvice(message, '@10125', { now: 10000 }), { retryAt: 10125 })
  assert.deepEqual(parseRelayRetryAdvice(message, '999999', { now: 10000 }), { retryAfterMs: 300000, retryAt: 310000 })
  assert.deepEqual(parseRelayRetryAdvice(message, '@1791738154332', { now: 10000 }), { retryAt: 310000 })
})

test('malformed, unsafe and legacy params are ignored', () => {
  for (const params of [undefined, null, 2, [], {}, { retry_after: 2, retry_at: 20 },
    '', '0', '@0', '-1', '+1', '01', '@01', '1.5', '1e3', ' 1', '1 ', '@', '@-1',
    '1\n', '@1\n', 'Infinity', '9007199254740992', '@9007199254740992']) {
    assert.equal(parseRelayRetryAdvice(message, params, { now: 10000 }), null)
  }
  for (const now of [NaN, Infinity, -Infinity]) assert.equal(parseRelayRetryAdvice(message, '1', { now }), null)
})

test('only rate-limited refusals supply timing, without changing failure policy', () => {
  for (const reason of [undefined, null, 42, 'blocked: busy', 'error: busy', 'prefix rate-limited: busy', 'Rate-limited: busy']) {
    assert.equal(parseRelayRetryAdvice(reason, '2000', { now: 10000 }), null)
  }
  for (const reason of ['rate-limited: busy', 'blocked: denied', 'unknown']) {
    const plain = relayRejectionError(reason)
    const timed = relayRejectionError(reason, '2000')
    assert.equal(timed.message, plain.message)
    assert.equal(timed.category, 'relay')
    assert.equal(timed.origin, undefined)
    assert.equal(timed.code, undefined)
    assert.equal(isRetryableRelayFailure(timed), isRetryableRelayFailure(plain))
    assert.equal(isReplaceableRelayFailure(timed), isReplaceableRelayFailure(plain))
  }
})
