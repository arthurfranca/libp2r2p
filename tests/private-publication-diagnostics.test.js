import test from 'node:test'
import assert from 'node:assert/strict'
import { getEventHash } from '../event/index.js'
import {
  assertMessagePublished,
  publicationFailureDiagnostic,
  sanitizePublicationError
} from '../private-messenger/helpers/publication.js'
import { recoveryRetryDelay } from '../private-messenger/helpers/recovery-retry.js'
import { relayCircuitError } from '../private-messenger/helpers/send-routing.js'

test('publication failures retain classified reports without logging arbitrary relay text', async t => {
  t.mock.method(Date, 'now', () => 1000)
  const event = {
    pubkey: 'a'.repeat(64),
    kind: 3560,
    created_at: 10,
    tags: [],
    content: 'ordinary rumor content'
  }
  const reason = Object.assign(new Error('rate-limited: secret diagnostic text'), {
    category: 'relay',
    code: 'RATE_LIMITED',
    retryAt: 7000
  })
  const result = {
    delivery: {
      reports: [{
        success: false,
        retryWhenAvailable: true,
        promise: Promise.resolve({
          total: 2,
          fulfilled: 0,
          errors: [{ relay: 'wss://user:password@relay.example/path?token=secret', reason }]
        })
      }]
    }
  }

  let publicationError
  try {
    await assertMessagePublished(result, event)
    assert.fail('expected a publication failure')
  } catch (error) {
    publicationError = error
  }
  const error = sanitizePublicationError(publicationError, {
    operation: 'seeder-recovery-ask',
    channelPubkey: 'channel',
    seederPubkey: 'b'.repeat(64),
    since: 1,
    until: 2
  })

  assert.equal(error.message, 'PRIVATE_MESSAGE_NOT_PUBLISHED')
  assert.equal(error.code, 'MESSAGE_NOT_PUBLISHED')
  assert.equal(error.eventId, getEventHash(event))
  assert.equal(error.eventKind, 3560)
  assert.equal(error.operation, 'seeder-recovery-ask')
  assert.equal(error.channelPubkey, 'channel')
  assert.equal(error.seederPubkey, 'b'.repeat(64))
  assert.equal(error.since, 1)
  assert.equal(error.until, 2)
  assert.deepEqual(error.reports, [{
    index: 0,
    total: 2,
    fulfilled: 0,
    retryWhenAvailable: true,
    errors: [{
      relay: 'wss://relay.example',
      code: 'RATE_LIMITED',
      category: 'relay',
      name: 'Error',
      retryAt: 7000
    }]
  }])
  assert.equal(recoveryRetryDelay([error], 1000), 6000)

  const diagnostic = publicationFailureDiagnostic(error)
  const serialized = JSON.stringify({ error, diagnostic })
  assert.doesNotMatch(serialized, /secret diagnostic text|password|token=secret/)
  assert.equal(diagnostic.code, 'MESSAGE_NOT_PUBLISHED')
  assert.equal(diagnostic.operation, 'seeder-recovery-ask')
})

test('publication diagnostics classify permanent and interrupted recovery reports', async t => {
  t.mock.method(Date, 'now', () => 1000)
  const blocked = sanitizePublicationError(Object.assign(new Error('MESSAGE_NOT_PUBLISHED'), {
    code: 'MESSAGE_NOT_PUBLISHED',
    reason: 'RELAY_PUBLICATION_FAILED',
    reports: [{
      total: 1,
      errors: [{ relay: 'wss://relay.example', reason: new Error('blocked: policy') }]
    }]
  }))
  const interrupted = sanitizePublicationError(Object.assign(new Error('MESSAGE_NOT_PUBLISHED'), {
    code: 'MESSAGE_NOT_PUBLISHED',
    reason: 'RELAY_PUBLICATION_FAILED',
    reports: [{ retryWhenAvailable: true, retryWhenOnline: true, errors: [] }]
  }))

  assert.equal(recoveryRetryDelay([blocked], 1000), null)
  assert.equal(recoveryRetryDelay([interrupted], 1000), 1000)
  assert.equal(recoveryRetryDelay([Object.assign(new Error('MESSAGE_NOT_PUBLISHED'), {
    code: 'MESSAGE_NOT_PUBLISHED',
    reason: 'NO_DELIVERY_REPORTS',
    reports: []
  })], 1000), null)

  const circuit = sanitizePublicationError(relayCircuitError(9000), {
    operation: 'seeder-recovery-ask',
    channelPubkey: 'channel',
    seederPubkey: 'seeder'
  })
  assert.equal(recoveryRetryDelay([circuit], 1000), 8000)
  assert.deepEqual(publicationFailureDiagnostic(circuit), {
    operation: 'seeder-recovery-ask',
    channelPubkey: 'channel',
    seederPubkey: 'seeder',
    code: 'RELAY_CIRCUIT_OPEN',
    category: 'timeout',
    retryAt: 9000,
    retryWhenAvailable: true,
    reports: []
  })
})
