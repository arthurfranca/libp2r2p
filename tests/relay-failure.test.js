import { test } from 'node:test'
import assert from 'node:assert/strict'
import { isRetryableRelayFailure, isReplaceableRelayFailure } from '../relay/index.js'

test('retry and replacement use native categories and leading protocol prefixes', () => {
  for (const category of ['connection', 'transport', 'timeout']) {
    const error = Object.assign(new Error('native detail'), { category })
    assert.equal(isRetryableRelayFailure(error), true)
    assert.equal(isReplaceableRelayFailure(error), true)
  }
  for (const prefix of ['rate-limited', 'error', 'blocked', 'restricted', 'auth-required', 'pow']) {
    const error = Object.assign(new Error(`${prefix}: detail`), { category: 'relay' })
    assert.equal(isRetryableRelayFailure(error), ['rate-limited', 'error'].includes(prefix))
    assert.equal(isReplaceableRelayFailure(error), true)
  }
  for (const error of [null, new Error('invalid: event'), new Error('unknown'), new Error('prose says rate-limited: busy'),
    Object.assign(new Error('blocked: locally'), { category: 'signer' }),
    Object.assign(new Error('error: signer denied'), { name: 'Nip42AuthenticationError' })]) {
    assert.equal(isRetryableRelayFailure(error), false)
    assert.equal(isReplaceableRelayFailure(error), false)
  }
})
