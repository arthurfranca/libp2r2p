import assert from 'node:assert/strict'
import { test } from 'node:test'

import { base16ToBytes } from '../base16/index.js'
import { ValidationError } from '../error/index.js'
import { validateLocales } from '../i18n/index.js'
import { waitForNip46 } from '../nip46/services/transport.js'
import { validateToken } from '../nwt/index.js'

test('ValidationError exposes a stable code, message, and cause', () => {
  const cause = new Error('source')
  const error = new ValidationError('INVALID_EXAMPLE', 'Readable message', { cause })
  assert.equal(error.name, 'ValidationError')
  assert.equal(error.code, 'INVALID_EXAMPLE')
  assert.equal(error.message, 'Readable message')
  assert.equal(error.cause, cause)
  assert.deepEqual(Object.keys(error), ['code'])
})

test('strict public validators and codecs use ValidationError', () => {
  assert.throws(() => base16ToBytes('0'), error => (
    error instanceof ValidationError &&
    error.code === 'INVALID_BASE16_LENGTH' &&
    error.message === 'Invalid Base16 length'
  ))
  assert.throws(() => validateLocales(null), error => (
    error instanceof ValidationError && error.code === 'INVALID_LOCALES'
  ))
  assert.throws(() => validateToken('invalid'), error => (
    error instanceof ValidationError && error.code === 'INVALID_NWT_ENCODING'
  ))
})

test('operational timeout failures remain outside ValidationError', async () => {
  await assert.rejects(
    waitForNip46(new Promise(() => {}), { timeout: 0 }),
    error => !(error instanceof ValidationError) && error.message === 'NIP46_TIMEOUT'
  )
})
