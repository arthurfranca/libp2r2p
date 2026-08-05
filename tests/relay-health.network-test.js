import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { describe, test } from 'node:test'

import { finalizeEvent } from 'libp2r2p/event'
import { generateSecretKey } from 'libp2r2p/key'
import { freeRelays, nappRelays, RelayPool, seedRelays } from 'libp2r2p/relay'

const OPERATION_TIMEOUT = 15000
const EXPIRATION_SECONDS = 300
const EMPTY_FILE_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
const AGGREGATE_HASH = createHash('sha256')
  .update(`${EMPTY_FILE_HASH} /index.html\n`)
  .digest('hex')

// Adds one bounded deadline to network operations that may otherwise retry.
function withTimeout (promise, label) {
  let timeoutId
  const timeout = new Promise((_resolve, reject) => {
    timeoutId = setTimeout(() => reject(new Error(`${label} timed out`)), OPERATION_TIMEOUT)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutId))
}

// Formats per-relay failures so test output remains useful during outages.
function formatRelayErrors (errors = []) {
  return errors.map(({ relay, reason }) => (
    `${relay}: ${reason?.message || String(reason)}`
  )).join('; ')
}

// Creates common timestamps and a fresh identity for every relay publication.
function signHealthEvent ({ kind, tags, content = '' }) {
  const createdAt = Math.floor(Date.now() / 1000)
  return finalizeEvent({
    kind,
    created_at: createdAt,
    tags: [...tags, ['expiration', String(createdAt + EXPIRATION_SECONDS)]],
    content
  }, generateSecretKey())
}

// Creates a valid NIP-65 relay-list event for testing seed-relay writes.
function createSeedRelayEvent (relay) {
  return signHealthEvent({
    kind: 10002,
    tags: [['r', relay]]
  })
}

// Creates an exotic ephemeral event that relays should broadcast without storing.
function createFreeRelayEvent () {
  return signHealthEvent({
    kind: 28333,
    tags: [],
    content: 'libp2r2p free relay health check'
  })
}

// Creates a valid draft-channel site manifest that stays out of the main listing.
function createNappRelayEvent () {
  const createdAt = Math.floor(Date.now() / 1000)
  return finalizeEvent({
    kind: 35130,
    created_at: createdAt,
    tags: [
      ['d', 'libp2r2p-relay-healthcheck'],
      ['path', 'index.html', EMPTY_FILE_HASH],
      ['service', 'blossom'],
      ['x', AGGREGATE_HASH, 'aggregate'],
      ['published_at', String(createdAt)],
      ['name', 'libp2r2p relay health check'],
      ['expiration', String(createdAt + EXPIRATION_SECONDS)]
    ],
    content: ''
  }, generateSecretKey())
}

// Verifies EOSE readiness, publish acknowledgement and subsequent live broadcast.
async function assertRelayWritesAndBroadcasts (relay, event) {
  const subscriberPool = new RelayPool()
  const publisherPool = new RelayPool()
  const abortController = new AbortController()
  const stream = subscriberPool.getLiveEventsGenerator({
    ids: [event.id],
    kinds: [event.kind],
    limit: 0
  }, [relay], {
    signal: abortController.signal,
    timeoutAfterFirstEose: null
  })
  const nextEvent = stream.next()
  nextEvent.catch(() => {})

  try {
    const ready = await withTimeout(stream.ready, `${relay} EOSE`)
    assert.equal(
      ready.relays.length,
      1,
      `subscription did not become live: ${formatRelayErrors(ready.errors)}`
    )

    const initialReport = await withTimeout(publisherPool.sendEvent(event, [relay], {
      timeout: OPERATION_TIMEOUT,
      timeoutUntilFirstFulfillment: OPERATION_TIMEOUT
    }), `${relay} publish acknowledgement`)
    const finalReport = await withTimeout(initialReport.promise, `${relay} publish report`)
    assert.equal(finalReport.success, true, formatRelayErrors(finalReport.errors))

    const received = await withTimeout(nextEvent, `${relay} live broadcast`)
    assert.equal(received.done, false)
    assert.equal(received.value.id, event.id)
  } finally {
    abortController.abort()
    await stream.return().catch(() => {})
    await nextEvent.catch(() => {})
    await Promise.all([
      subscriberPool.disconnectAll(),
      publisherPool.disconnectAll()
    ])
  }
}

describe('seed relay write health', () => {
  for (const relay of seedRelays) {
    test(relay, { timeout: OPERATION_TIMEOUT * 4 }, async () => {
      await assertRelayWritesAndBroadcasts(relay, createSeedRelayEvent(relay))
    })
  }
})

describe('free relay write health', () => {
  for (const relay of freeRelays) {
    test(relay, { timeout: OPERATION_TIMEOUT * 4 }, async () => {
      await assertRelayWritesAndBroadcasts(relay, createFreeRelayEvent())
    })
  }
})

describe('napp relay draft-manifest write health', () => {
  for (const relay of nappRelays) {
    test(relay, { timeout: OPERATION_TIMEOUT * 4 }, async () => {
      await assertRelayWritesAndBroadcasts(relay, createNappRelayEvent())
    })
  }
})
