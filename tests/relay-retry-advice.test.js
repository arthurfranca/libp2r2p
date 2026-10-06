import assert from 'node:assert/strict'
import { test } from 'node:test'
import { parseRelayRetryAdvice, isRetryableRelayFailure, isReplaceableRelayFailure } from '../relay/index.js'
import { relayRejectionError } from '../relay/helpers/error.js'

const message = 'rate-limited: busy'

test('absolute advice preserves its deadline across delayed and expired delivery', () => {
  const extra = { retry_after: 10, retry_at: 20 }
  assert.deepEqual(parseRelayRetryAdvice(message, extra, { now: 10000 }), { retryAfterMs: 10000, retryAt: 20000 })
  assert.deepEqual(parseRelayRetryAdvice(message, extra, { now: 14000 }), { retryAfterMs: 10000, retryAt: 20000 })
  assert.deepEqual(parseRelayRetryAdvice(message, extra, { now: 25000 }), { retryAfterMs: 10000, retryAt: 20000 })
})

test('advice supports absolute-only and fractional seconds and bounds future waits', () => {
  assert.deepEqual(parseRelayRetryAdvice(message, { retry_at: 10.125 }, { now: 10000 }), { retryAt: 10125 })
  assert.deepEqual(parseRelayRetryAdvice(message, { retry_after: 0.25 }, { now: 10000 }), { retryAfterMs: 250, retryAt: 10250 })
  assert.deepEqual(parseRelayRetryAdvice(message, { retry_at: 9999999999, retry_after: 9999 }, { now: 10000 }), { retryAfterMs: 300000, retryAt: 310000 })
})

test('invalid absolute advice falls back to valid relative advice without combining waits', () => {
  for (const retryAt of [undefined, null, '20', 0, -1, NaN, Infinity, -Infinity, Number.MAX_VALUE]) {
    assert.deepEqual(parseRelayRetryAdvice(message, { retry_at: retryAt, retry_after: 2 }, { now: 10000 }), { retryAfterMs: 2000, retryAt: 12000 })
    assert.equal(parseRelayRetryAdvice(message, { retry_at: retryAt }, { now: 10000 }), null)
  }
  for (const retryAfter of [null, '2', 0, -1, NaN, Infinity, -Infinity]) {
    assert.deepEqual(parseRelayRetryAdvice(message, { retry_at: 20, retry_after: retryAfter }, { now: 10000 }), { retryAt: 20000 })
    assert.equal(parseRelayRetryAdvice(message, { retry_after: retryAfter }, { now: 10000 }), null)
  }
})

test('only rate-limited refusals accept timing advice and unrelated fields are inert', () => {
  for (const reason of [undefined, null, 42, 'blocked: busy', 'error: busy', 'prefix rate-limited: busy', 'Rate-limited: busy']) {
    assert.equal(parseRelayRetryAdvice(reason, { retry_after: 2, retry_at: 20 }, { now: 10000 }), null)
  }
  for (const extra of [undefined, null, '2', 2, [], {}]) assert.equal(parseRelayRetryAdvice(message, extra), null)
  for (const reason of ['rate-limited: busy', 'blocked: denied', 'unknown']) {
    const plain = relayRejectionError(reason, { retry_after: 2, retry_at: 20 })
    const declared = relayRejectionError(reason, { retry_after: 2, retry_at: 20, origin: 'local', local: true, category: 'local', retryable: true, code: 'LOCAL_RETRY' })
    assert.equal(declared.message, plain.message)
    assert.equal(declared.category, 'relay')
    assert.equal(declared.origin, undefined)
    assert.equal(declared.code, undefined)
    assert.equal(isRetryableRelayFailure(declared), isRetryableRelayFailure(plain))
    assert.equal(isReplaceableRelayFailure(declared), isReplaceableRelayFailure(plain))
  }
})
