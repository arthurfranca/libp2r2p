import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  createPublicationRelayHealth,
  PUBLICATION_RELAY_INITIAL_COOLDOWN_MS,
  PUBLICATION_RELAY_MAX_COOLDOWN_MS
} from '../private-messenger/helpers/publication-relay-health.js'

const timeout = () => Object.assign(new Error('GET_EVENTS_TIMEOUT'), { category: 'timeout' })

test('permanent publication refusals open immediately and double after failed probes', () => {
  let at = 1000
  const health = createPublicationRelayHealth({ now: () => at })
  const relay = 'wss://relay.example'

  assert.equal(health.recordFailure(relay, Object.assign(new Error('banned: policy'), { category: 'relay' })), at + PUBLICATION_RELAY_INITIAL_COOLDOWN_MS)
  assert.equal(health.isExcluded(relay), true)
  at += PUBLICATION_RELAY_INITIAL_COOLDOWN_MS
  assert.equal(health.isExcluded(relay), false)
  assert.equal(health.recordFailure(relay, Object.assign(new Error('banned: policy'), { category: 'relay' })), at + 2 * PUBLICATION_RELAY_INITIAL_COOLDOWN_MS)
})

test('three consecutive transport failures open a bounded half-open circuit', () => {
  let at = 1000
  const health = createPublicationRelayHealth({ now: () => at })
  const relay = 'wss://relay.example'

  assert.equal(health.recordFailure(relay, timeout()), 0)
  assert.equal(health.recordFailure(relay, timeout()), 0)
  assert.equal(health.recordFailure(relay, timeout()), at + PUBLICATION_RELAY_INITIAL_COOLDOWN_MS)
  at += PUBLICATION_RELAY_INITIAL_COOLDOWN_MS
  assert.equal(health.isExcluded(relay), false)
  assert.equal(health.recordFailure(relay, timeout()), at + 2 * PUBLICATION_RELAY_INITIAL_COOLDOWN_MS)
  for (let level = 3; level <= 10; level++) {
    at += PUBLICATION_RELAY_MAX_COOLDOWN_MS
    health.recordFailure(relay, timeout())
  }
  assert.equal(health.retryAt([relay]), at + PUBLICATION_RELAY_MAX_COOLDOWN_MS)
  health.recordSuccess(relay)
  assert.equal(health.isExcluded(relay), false)
})

test('success resets a timeout streak and rate-limit deadlines remain absolute', () => {
  const at = 1000
  const health = createPublicationRelayHealth({ now: () => at })
  const relay = 'wss://relay.example'

  health.recordFailure(relay, timeout())
  health.recordFailure(relay, timeout())
  health.recordSuccess(relay)
  assert.equal(health.recordFailure(relay, timeout()), 0)
  assert.equal(health.recordFailure(relay, timeout()), 0)
  assert.equal(health.isExcluded(relay), false)
  assert.equal(health.recordFailure(relay, Object.assign(new Error('rate-limited: busy'), { category: 'relay', retryAt: 7000 })), 7000)
  assert.equal(health.retryAt([relay]), 7000)
})

test('health storage remains bounded', () => {
  const at = 1000
  const health = createPublicationRelayHealth({ now: () => at, limit: 2 })
  const relays = ['wss://one.example', 'wss://two.example', 'wss://three.example']
  for (const relay of relays) health.recordFailure(relay, Object.assign(new Error('restricted: policy'), { category: 'relay' }))
  assert.equal(health.isExcluded(relays[0]), false)
  assert.equal(health.isExcluded(relays[1]), true)
  assert.equal(health.isExcluded(relays[2]), true)
})
