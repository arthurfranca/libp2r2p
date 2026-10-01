import test from 'node:test'
import assert from 'node:assert/strict'
import { recoveryRetryDelay } from '../private-messenger/helpers/recovery-retry.js'

test('history retry preserves rate-limit cooldowns and leaves permanent or unknown failures pending', t => {
  t.mock.method(Date, 'now', () => 1000)
  assert.equal(recoveryRetryDelay([new AggregateError([Object.assign(new Error('rate-limited: busy'), { retryAt: 5000 })])], 1000), 4000)
  assert.equal(recoveryRetryDelay([Object.assign(new Error('incomplete'), { relays: [{ status: 'timeout' }] })], 1000), 1000)
  const transport = Object.assign(new Error('network failed'), { category: 'transport' })
  assert.equal(recoveryRetryDelay([new AggregateError([transport])], 1000), 1000)
  for (const error of [new Error('blocked: denied'), new Error('unknown failure'), Object.assign(new Error('dense second'), { code: 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT' })]) {
    assert.equal(recoveryRetryDelay([error], 1000), null)
    if (error.code) assert.equal(recoveryRetryDelay([new AggregateError([transport, error])], 1000), null)
  }
})
