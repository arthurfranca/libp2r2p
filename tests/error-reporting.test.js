import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createPrivateMessengerErrorReporter } from '../private-messenger/helpers/error-reporting.js'

test('private messenger cancellation diagnostics are bounded and preserve pause context', () => {
  let now = 1_000
  const errors = []
  const report = createPrivateMessengerErrorReporter({
    now: () => now,
    intervalMs: 30_000,
    status: () => ({ paused: true, pauseReasons: ['capacity'] }),
    report: error => errors.push(error)
  })
  const abort = () => Object.assign(new DOMException('signal is aborted without reason', 'AbortError'), {
    operation: 'private-channel.subscribe',
    phase: 'teardown'
  })

  report(abort())
  report(abort())
  report(abort())
  assert.equal(errors.length, 1)
  assert.equal(errors[0].code, 'PRIVATE_MESSENGER_CANCELLED')
  assert.equal(errors[0].name, 'AbortError')
  assert.equal(errors[0].operation, 'private-channel.subscribe')
  assert.equal(errors[0].phase, 'teardown')
  assert.equal(errors[0].expectedCancellation, true)
  assert.deepEqual(errors[0].pauseReasons, ['capacity'])

  now += 30_000
  report(abort())
  assert.equal(errors.length, 2)
  assert.equal(errors[1].suppressedCount, 2)
})

test('private messenger reports non-cancellation errors unchanged', () => {
  const errors = []
  const report = createPrivateMessengerErrorReporter({ report: error => errors.push(error) })
  const error = new Error('storage failed')

  report(error)
  report(error)

  assert.deepEqual(errors, [error, error])
})
