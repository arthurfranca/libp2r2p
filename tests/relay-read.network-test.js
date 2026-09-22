import assert from 'node:assert/strict'
import { test } from 'node:test'
import { randomBytes } from 'node:crypto'
import { RelayPool, seedRelays } from '../relay/index.js'

// Read-only diagnostics: an unpredictable event ID avoids retrieving user data
// or publishing fixtures. External availability is not a deterministic gate.
for (const method of ['getEventsGenerator', 'getLiveEventsGenerator', 'getEventsFeedGenerator']) {
  test(`${method} initial completion on public relays`, { timeout: 15000 }, async t => {
    const pool = new RelayPool()
    const controller = new AbortController()
    const deadline = setTimeout(() => controller.abort(), 10000)
    const stream = pool[method]({ ids: [randomBytes(32).toString('hex')], kinds: [1] }, seedRelays.slice(0, 2), {
      timeout: 5000, timeoutAfterFirstEose: null, signal: controller.signal
    })
    let report
    try {
      for await (const item of stream) {
        assert.notEqual(item.type, 'event', 'unexpected match for random ID')
        if (item.type === 'eose') { report = item; break }
      }
      assert.ok(report, 'missing initial completion')
      for (const { relay, status, error } of report.relays) t.diagnostic(`${relay}: ${status}${error ? ` (${error.message})` : ''}`)
      assert.ok(report.relays.some(item => item.status === 'eose'), 'no relay reached EOSE')
    } finally {
      clearTimeout(deadline)
      controller.abort()
      await stream.return()
      await pool.disconnectAll()
    }
  })
}
