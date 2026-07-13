import { describe, it, beforeEach, mock, test } from 'node:test'
import assert from 'node:assert/strict'

// ─── Fake Relay infrastructure ────────────────────────────────────────────────

// Keyed by URL; populated by FakeRelay constructor, cleared in beforeEach.
const relayRegistry = new Map()

// Per-URL connect overrides: throw to simulate error or hang to simulate timeout.
const connectOverrides = new Map()

// Per-URL publish overrides: throw to simulate error or hang to simulate timeout.
const publishOverrides = new Map()

// Per-URL send overrides are used for control replies.
const sendOverrides = new Map()
let autoEoseForLiveSubscriptions = true

function overrideFor (overrides, url) {
  return overrides.get(url) ?? overrides.get(url.endsWith('/') ? url.slice(0, -1) : `${url}/`)
}

class FakeRelay {
  constructor (url) {
    this.url = url
    this.subscriptions = []
    this.ws = { readyState: 1 }
    this.publishTimeout = 100
    relayRegistry.set(url, this)
    // RelayPool canonicalizes URLs; retain the test's terse lookup spelling.
    if (url.endsWith('/')) relayRegistry.set(url.slice(0, -1), this)
  }

  async connect (options) {
    this.connectOptions = options
    const fn = overrideFor(connectOverrides, this.url)
    if (fn) await fn()
  }

  subscribe (filters, handlers) {
    const sub = {
      filters,
      handlers,
      isClosed: false,
      close (reason = 'closed by caller') {
        if (this.isClosed) return
        this.isClosed = true
        handlers.onclose?.(reason)
      }
    }
    this.subscriptions.push(sub)
    if (autoEoseForLiveSubscriptions && filters[0]?.limit === 0) {
      queueMicrotask(() => handlers.oneose?.())
    }
    return sub
  }

  async publish (event) {
    this.lastPublishedEvent = event
    const fn = overrideFor(publishOverrides, this.url)
    if (fn) await fn(event)
  }

  async send (message) {
    this.sentMessages ??= []
    this.sentMessages.push(message)
    const fn = overrideFor(sendOverrides, this.url)
    if (fn) await fn(message, this)
  }

  _onmessage (_message) {}

  async close () {
    this.ws.readyState = 3
    this.onclose?.()
  }
}

mock.module('nostr-tools/relay', {
  namedExports: {
    Relay: FakeRelay
  }
})

// Dynamic import AFTER mock.module so the module picks up FakeRelay
const { RelayPool, relayPool } = await import('../relay/index.js')

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Drain pending microtasks + one I/O turn — enough for async relay setup to settle
const tick = () => new Promise(resolve => setImmediate(resolve))

function deferred () {
  let resolve
  let reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

// Start consuming a generator concurrently; returns the collected array and a
// promise that resolves when the generator ends.
function startCollecting (gen) {
  const events = []
  const promise = (async () => {
    for await (const e of gen) events.push(e)
  })()
  return { events, promise }
}

let _nextId = 1
function makeEvent ({ id, kind = 0, created_at = 100 } = {}) {
  return { id: id ?? String(_nextId++), kind, created_at, tags: [], content: '' }
}

function receiveRelayMessage (relay, message) {
  relay._onmessage({ data: JSON.stringify(message) })
}

function countRequest (relay) {
  const message = relay.sentMessages?.map(JSON.parse).find(message => message[0] === 'COUNT')
  assert.ok(message, 'expected a COUNT request')
  return message
}

function receiveCount (relay, payload) {
  const [, id] = countRequest(relay)
  receiveRelayMessage(relay, ['COUNT', id, payload])
}

function closeCount (relay, reason) {
  const [, id] = countRequest(relay)
  receiveRelayMessage(relay, ['CLOSED', id, reason])
}

function hll (entries = {}) {
  const registers = new Uint8Array(256)
  for (const [index, value] of Object.entries(entries)) registers[Number(index)] = value
  return [...registers].map(value => value.toString(16).padStart(2, '0')).join('')
}

// ─── Tests ────────────────────────────────────────────────────────────────────

test('relay exports one shared RelayPool singleton', () => {
  assert.ok(relayPool instanceof RelayPool)
})

describe('RelayPool.getLiveEventsGenerator', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    autoEoseForLiveSubscriptions = true
    nostr = new RelayPool()
  })

  it('yields live events and runs until aborted', async () => {
    const ac = new AbortController()
    const gen = nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1'],
      { signal: ac.signal }
    )
    const { events, promise } = startCollecting(gen)

    await tick()
    const liveSub = relayRegistry.get('wss://r1').subscriptions[0]
    liveSub.handlers.onevent(makeEvent({ id: 'e1', created_at: 100 }))
    liveSub.handlers.onevent(makeEvent({ id: 'e2', created_at: 200 }))
    await tick()

    ac.abort()
    await promise
    assert.equal(events.length, 2)
    assert.equal(events[0].id, 'e1')
    assert.equal(events[1].id, 'e2')
  })

  it('discards initial events until every initial live subscription reaches EOSE', async () => {
    autoEoseForLiveSubscriptions = false
    const ac = new AbortController()
    const { events, promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1', 'wss://r2'], { signal: ac.signal })
    )

    await tick()
    const first = relayRegistry.get('wss://r1').subscriptions[0]
    const second = relayRegistry.get('wss://r2').subscriptions[0]
    first.handlers.onevent(makeEvent({ id: 'retained-first' }))
    first.handlers.oneose()
    first.handlers.onevent(makeEvent({ id: 'between-eoses' }))
    await tick()
    assert.deepEqual(events, [])

    second.handlers.oneose()
    first.handlers.onevent(makeEvent({ id: 'live-after-eose' }))
    await tick()

    ac.abort()
    await promise
    assert.deepEqual(events.map(event => event.id), ['live-after-eose'])
  })

  it('opens only a live sub (limit:0, since:now) — no initial fetch', async () => {
    const ac = new AbortController()
    startCollecting(nostr.getLiveEventsGenerator(
      { kinds: [0], since: 500 }, // since is set but should NOT trigger initial fetch
      ['wss://r1'],
      { signal: ac.signal }
    ))

    await tick()
    const relay = relayRegistry.get('wss://r1')

    assert.equal(relay.subscriptions.length, 1, 'only live sub — no initial gap fill')
    assert.equal(relay.subscriptions[0].filters[0].limit, 0)
    assert.ok(relay.subscriptions[0].filters[0].since > 0)

    ac.abort()
  })

  it('uses the private three-second deadline while opening a relay connection', async () => {
    const ac = new AbortController()
    startCollecting(nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal }))
    await tick()
    assert.equal(relayRegistry.get('wss://r1').connectOptions.timeout, 3000)
    ac.abort()
  })

  it('sets event.meta.relay to the relay URL', async () => {
    const ac = new AbortController()
    const { events, promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    )

    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    await tick()

    ac.abort()
    await promise
    assert.equal(events[0].meta.relay, 'wss://r1')
  })

  it('abort closes the live sub', async () => {
    const ac = new AbortController()
    const { promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    )

    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]

    ac.abort()
    await promise
    assert.ok(sub.isClosed)
  })

  it('reconnects after live sub disconnects', async () => {
    const ac = new AbortController()
    const { promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    )

    await tick()
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions[0].handlers.onclose()

    await new Promise(resolve => setTimeout(resolve, 1100))
    await tick()

    assert.equal(relay.subscriptions.length, 2, 'new live sub opened after reconnect')

    ac.abort()
    await promise
  })

  it('retries a failed initial connection with a fresh relay instance', async () => {
    let attempts = 0
    const originalConsoleError = console.error
    connectOverrides.set('wss://r1', () => {
      attempts++
      if (attempts === 1) throw new Error('connection failed')
    })
    console.error = () => {}
    try {
      const ac = new AbortController()
      const { promise } = startCollecting(
        nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
      )

      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()
      assert.equal(attempts, 2)
      assert.equal(relayRegistry.get('wss://r1').subscriptions.length, 1)

      ac.abort()
      await promise
    } finally {
      console.error = originalConsoleError
    }
  })

  it('evicts a failed connection and cancels its retry when the stream stops', async () => {
    let attempts = 0
    const originalConsoleError = console.error
    connectOverrides.set('wss://r1', () => {
      attempts++
      throw new Error('connection failed')
    })
    console.error = () => {}
    try {
      const ac = new AbortController()
      const { promise } = startCollecting(
        nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
      )

      await tick()
      assert.equal(attempts, 1)
      ac.abort()
      await promise
      await new Promise(resolve => setTimeout(resolve, 1100))
      assert.equal(attempts, 1)
    } finally {
      console.error = originalConsoleError
    }
  })

  it('reconnect opens a gap fill sub using lastSeenAt as since', async () => {
    const ac = new AbortController()
    let capturedArgs
    async function * mockGapEvents (f, r, o) {
      capturedArgs = { f, r, o }
    }

    const { promise } = startCollecting(nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1'],
      { signal: ac.signal, _gapEventsGenerator: mockGapEvents }
    ))

    await tick()
    const relay = relayRegistry.get('wss://r1')

    // Receive an event so lastSeenAt = 750
    relay.subscriptions[0].handlers.onevent(makeEvent({ id: 'e1', created_at: 750 }))
    await tick()

    // Disconnect → reconnect
    relay.subscriptions[0].handlers.onclose()
    await new Promise(resolve => setTimeout(resolve, 1100))
    await tick()

    assert.ok(capturedArgs, '_gapEventsGenerator should have been called on reconnect')
    assert.equal(capturedArgs.f.since, 750, 'reconnect gap fill uses lastSeenAt as since')
    assert.ok(capturedArgs.f.until > 0)
    assert.deepEqual(capturedArgs.r, ['wss://r1'])

    ac.abort()
    await promise
  })

  it('reconnect uses filter.since as gap baseline when no events have been seen', async () => {
    const ac = new AbortController()
    let capturedSince
    async function * mockGapEvents (f) { capturedSince = f.since }

    const { promise } = startCollecting(nostr.getLiveEventsGenerator(
      { kinds: [0], since: 500 },
      ['wss://r1'],
      { signal: ac.signal, _gapEventsGenerator: mockGapEvents }
    ))

    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()

    await new Promise(resolve => setTimeout(resolve, 1100))
    await tick()

    assert.equal(capturedSince, 500)

    ac.abort()
    await promise
  })

  describe('filter.until', () => {
    it('teardown fires when the wall clock reaches until', async () => {
      const until = Math.floor(Date.now() / 1000) + 1 // 1 second from now
      const { events, promise } = startCollecting(
        nostr.getLiveEventsGenerator({ kinds: [0], until }, ['wss://r1'])
      )

      await tick()
      const liveSub = relayRegistry.get('wss://r1').subscriptions[0]
      liveSub.handlers.onevent(makeEvent({ id: 'e1' }))

      await promise // resolves naturally when until timer fires
      assert.equal(events.length, 1)
      assert.equal(events[0].id, 'e1')
    })

    it('teardown fires immediately when until is already in the past', async () => {
      const until = Math.floor(Date.now() / 1000) - 10
      const { promise } = startCollecting(
        nostr.getLiveEventsGenerator({ kinds: [0], until }, ['wss://r1'])
      )
      await promise // should resolve on next tick
      // no assertion needed — just verifying it completes without hanging
    })

    it('forwards until to the live sub filter', async () => {
      const until = Math.floor(Date.now() / 1000) + 60
      const ac = new AbortController()
      startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], until },
        ['wss://r1'],
        { signal: ac.signal }
      ))

      await tick()
      const liveSub = relayRegistry.get('wss://r1').subscriptions[0]
      assert.equal(liveSub.filters[0].until, until)

      ac.abort()
    })

    it('does not include until in the live sub filter when not set', async () => {
      const ac = new AbortController()
      startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0] },
        ['wss://r1'],
        { signal: ac.signal }
      ))

      await tick()
      const liveSub = relayRegistry.get('wss://r1').subscriptions[0]
      assert.equal(liveSub.filters[0].until, undefined)

      ac.abort()
    })

    it('caps reconnect gap fill until at filter.until', async () => {
      const until = Math.floor(Date.now() / 1000) + 60
      const ac = new AbortController()
      let capturedUntil
      async function * mockGapEvents (f) { capturedUntil = f.until }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100, until },
        ['wss://r1'],
        { signal: ac.signal, _gapEventsGenerator: mockGapEvents }
      ))

      await tick()
      relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()

      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      assert.ok(capturedUntil <= until, 'reconnect gap fill until should be capped at filter.until')

      ac.abort()
      await promise
    })

    it('does not reconnect after filter.until has passed', async () => {
      const until = Math.floor(Date.now() / 1000) - 1 // already in the past
      const { promise } = startCollecting(
        nostr.getLiveEventsGenerator({ kinds: [0], until }, ['wss://r1'])
      )
      await promise

      const countAfter = relayRegistry.get('wss://r1')?.subscriptions.length ?? 0

      await new Promise(resolve => setTimeout(resolve, 200))
      await tick()

      assert.equal(
        relayRegistry.get('wss://r1')?.subscriptions.length ?? 0,
        countAfter,
        'no reconnect after until has passed'
      )
    })
  })

  it('does not reconnect after signal is aborted', async () => {
    const ac = new AbortController()
    const { promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    )

    await tick()
    ac.abort()
    await promise

    const countAfterAbort = relayRegistry.get('wss://r1').subscriptions.length
    await new Promise(resolve => setTimeout(resolve, 200))
    await tick()

    assert.equal(relayRegistry.get('wss://r1').subscriptions.length, countAfterAbort)
  })

  describe('reconnect gap fill routing — injectable generators', () => {
    it('forwards renamed reconnect timeouts to _gapEventsGenerator', async () => {
      const ac = new AbortController()
      let asapCalled = false
      let options
      async function * mockGapEvents (_filter, _relays, nextOptions) {
        asapCalled = true
        options = nextOptions
      }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100 },
        ['wss://r1'],
        {
          signal: ac.signal,
          timeoutForReconnectGap: 4321,
          timeoutAfterFirstReconnectGapEose: 321,
          _gapEventsGenerator: mockGapEvents
        }
      ))

      await tick()
      relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      assert.ok(asapCalled)
      assert.equal(options.timeout, 4321)
      assert.equal(options.timeoutAfterFirstEose, 321)
      ac.abort()
      await promise
    })

    it('forwards null EOSE grace to _gapEventsGenerator', async () => {
      const ac = new AbortController()
      let options
      async function * mockGapEvents (_filter, _relays, nextOptions) { options = nextOptions }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100 },
        ['wss://r1'],
        { signal: ac.signal, timeoutAfterFirstReconnectGapEose: null, _gapEventsGenerator: mockGapEvents }
      ))

      await tick()
      relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      assert.equal(options.timeoutAfterFirstEose, null)
      ac.abort()
      await promise
    })

    it('buffers live events during reconnect gap fill, yields gap events first', async () => {
      const ac = new AbortController()
      const liveEvent = makeEvent({ id: 'live1', created_at: 200 })
      const gapEvent = makeEvent({ id: 'gap1', created_at: 50 })
      let resolveGap

      async function * mockGapEvents () {
        yield { type: 'event', event: gapEvent, relay: 'wss://r1' }
        await new Promise(resolve => { resolveGap = resolve })
      }

      const { events, promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 1 },
        ['wss://r1'],
        { signal: ac.signal, _gapEventsGenerator: mockGapEvents }
      ))

      await tick()
      const relay = relayRegistry.get('wss://r1')

      // Disconnect → reconnect (gap fill opens)
      relay.subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      // Live event arrives during reconnect gap fill → buffered
      const newLiveSub = relay.subscriptions[1]
      newLiveSub.handlers.onevent(liveEvent)
      await tick()
      assert.ok(!events.find(e => e.id === 'live1'), 'live event should be buffered during gap fill')

      // Complete gap fill → buffer flushed
      resolveGap()
      await tick()
      await tick()

      assert.equal(events[events.length - 2]?.id, 'gap1', 'gap event comes first')
      assert.equal(events[events.length - 1]?.id, 'live1', 'live event comes after')

      ac.abort()
      await promise
    })

    it('deduplicates events between reconnect gap fill and live sub', async () => {
      const ac = new AbortController()
      const dupEvent = makeEvent({ id: 'dup', created_at: 150 })
      let resolveGap

      async function * mockGapEvents () {
        yield { type: 'event', event: dupEvent, relay: 'wss://r1' }
        await new Promise(resolve => { resolveGap = resolve })
      }

      const { events, promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 1 },
        ['wss://r1'],
        { signal: ac.signal, _gapEventsGenerator: mockGapEvents }
      ))

      await tick()
      const relay = relayRegistry.get('wss://r1')

      relay.subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      relay.subscriptions[1].handlers.onevent(dupEvent) // same event from live sub
      resolveGap()
      await tick()
      await tick()

      const dupCount = events.filter(e => e.id === 'dup').length
      assert.equal(dupCount, 1)

      ac.abort()
      await promise
    })
  })
})

describe('RelayPool.getEventsFeedGenerator', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  // ── live:true ────────────────────────────────────────────────────────────────

  describe('live:true — always does initial fetch + live', () => {
    it('starts _liveGenerator immediately and runs initial fetch concurrently', async () => {
      const callOrder = []
      let resolveFetch

      async function * mockLive () {
        callOrder.push('live')
        // stays open
        await new Promise(resolve => { resolveFetch = resolve })
      }
      async function * mockEvents () {
        callOrder.push('fetch')
      }

      const ac = new AbortController()
      const { promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _eventsGenerator: mockEvents
        })
      )

      await tick()
      assert.deepEqual(callOrder, ['live', 'fetch'], 'live generator should start before fetch')

      resolveFetch()
      ac.abort()
      await promise
    })

    it('yields stored events before buffered live events', async () => {
      const storedEvent = makeEvent({ id: 'stored', created_at: 50 })
      const liveEvent = makeEvent({ id: 'live', created_at: 200 })
      let resolveFetch

      async function * mockLive () {
        // Simulates a live event arriving during the fetch
        await new Promise(resolve => { resolveFetch = resolve })
        yield liveEvent
      }
      async function * mockEvents () {
        yield { type: 'event', event: storedEvent, relay: 'wss://r1' }
      }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 1 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _eventsGenerator: mockEvents
        })
      )

      await tick()
      // Unblock live generator after fetch has yielded the stored event
      resolveFetch()
      ac.abort()
      await promise

      assert.equal(events[0].id, 'stored', 'stored event should come first')
      assert.equal(events[1].id, 'live', 'live event should come after')
    })

    it('deduplicates live events that overlap with initial fetch events', async () => {
      const sharedEvent = makeEvent({ id: 'shared', created_at: 100 })
      let resolveLive

      // Live generator yields the shared event immediately (simulates it arriving while
      // the fetch is still running), then waits to keep the generator open
      async function * mockLive () {
        yield sharedEvent
        await new Promise(resolve => { resolveLive = resolve })
      }
      // Fetch also returns the same event (overlap around the time boundary)
      async function * mockEvents () {
        yield { type: 'event', event: sharedEvent, relay: 'wss://r1' }
      }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 1 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _eventsGenerator: mockEvents
        })
      )

      await tick()
      resolveLive()
      ac.abort()
      await promise

      assert.equal(events.filter(e => e.id === 'shared').length, 1, 'duplicate should appear once')
    })

    it('uses _eventsGenerator for initial fetch with either EOSE grace setting', async () => {
      const calls = []
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockEvents (_filter, _relays, options) { calls.push(options) }

      const asapAbort = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: asapAbort.signal,
        timeoutAfterFirstEose: 500,
        _liveGenerator: mockLive,
        _eventsGenerator: mockEvents
      }))
      await tick()
      asapAbort.abort()

      const fullAbort = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: fullAbort.signal,
        timeoutAfterFirstEose: null,
        _liveGenerator: mockLive,
        _eventsGenerator: mockEvents
      }))
      await tick()
      assert.deepEqual(calls.map(call => call.timeoutAfterFirstEose), [500, null])
      fullAbort.abort()
    })

    it('skips initial fetch and delegates directly to _liveGenerator when filter.limit === 0', async () => {
      let fetchCalled = false
      let liveCalled = false
      async function * mockLive () { liveCalled = true; yield makeEvent({ id: 'e1' }) }
      async function * mockEvents () { fetchCalled = true }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ limit: 0 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _eventsGenerator: mockEvents
        })
      )

      await promise
      assert.ok(liveCalled)
      assert.ok(!fetchCalled, 'no initial fetch when limit:0')
      assert.equal(events.length, 1)
    })

    it('triggers initial fetch even with no since and no limit', async () => {
      let fetchCalled = false
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockEvents () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({}, ['wss://r1'], {
        signal: ac.signal,
        _liveGenerator: mockLive,
        _eventsGenerator: mockEvents
      }))

      await tick()
      assert.ok(fetchCalled, 'initial fetch should always run for live:true')
      ac.abort()
    })

    it('triggers initial fetch for filter.limit > 0', async () => {
      let fetchCalled = false
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockEvents () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ limit: 3 }, ['wss://r1'], {
        signal: ac.signal,
        _liveGenerator: mockLive,
        _eventsGenerator: mockEvents
      }))

      await tick()
      assert.ok(fetchCalled)
      ac.abort()
    })

    it('passes timeout and timeoutAfterFirstEose to _eventsGenerator', async () => {
      let capturedOpts
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockEvents (_f, _r, o) { capturedOpts = o }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: ac.signal,
        timeout: 3000,
        timeoutAfterFirstEose: 200,
        _liveGenerator: mockLive,
        _eventsGenerator: mockEvents
      }))

      await tick()
      assert.equal(capturedOpts.timeout, 3000)
      assert.equal(capturedOpts.timeoutAfterFirstEose, 200)
      ac.abort()
    })
  })

  // ── live:false ──────────────────────────────────────────────────────────────

  describe('live:false', () => {
    it('delegates to _eventsGenerator with regular EOSE grace', async () => {
      const ac = new AbortController()
      let capturedArgs
      async function * mockEvents (f, r, o) {
        capturedArgs = { f, r, o }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }

      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ kinds: [0], since: 100 }, ['wss://r1'], {
          live: false, timeout: 3000, timeoutAfterFirstEose: 200,
          signal: ac.signal, _eventsGenerator: mockEvents
        })
      )

      await promise
      assert.equal(capturedArgs.o.timeout, 3000)
      assert.equal(capturedArgs.o.timeoutAfterFirstEose, 200)
      assert.equal(capturedArgs.o.signal, ac.signal)
      assert.equal(events.length, 1)
      assert.equal(events[0].id, 'e1')
    })

    it('skips non-event items', async () => {
      async function * mockEvents () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: 500, _eventsGenerator: mockEvents
        })
      )
      await promise
      assert.equal(events.length, 1)
      assert.equal(events[0].id, 'e1')
    })
    it('forwards null EOSE grace to _eventsGenerator', async () => {
      const ac = new AbortController()
      let capturedArgs
      async function * mockEvents (f, r, o) {
        capturedArgs = { f, r, o }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }

      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ kinds: [0] }, ['wss://r1'], {
          live: false, timeout: 4000, timeoutAfterFirstEose: null,
          signal: ac.signal, _eventsGenerator: mockEvents
        })
      )

      await promise
      assert.equal(capturedArgs.o.timeout, 4000)
      assert.equal(capturedArgs.o.timeoutAfterFirstEose, null)
      assert.equal(capturedArgs.o.signal, ac.signal)
      assert.equal(events.length, 1)
    })

    it('skips non-event items', async () => {
      async function * mockEvents () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: null, _eventsGenerator: mockEvents
        })
      )
      await promise
      assert.equal(events.length, 1)
    })
  })
})

describe('RelayPool.getEvents', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  it('collects events and resolves on EOSE', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1', created_at: 100 }))
    sub.handlers.onevent(makeEvent({ id: 'e2', created_at: 200 }))
    sub.handlers.oneose()
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 2)
    assert.equal(result[0].id, 'e1')
    assert.equal(result[1].id, 'e2')
    assert.equal(errors.length, 0)
    assert.ok(success)
  })

  it('sets event.meta.relay', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result[0].meta.relay, 'wss://r1')
  })

  it('adds timeout errors for relays still pending at the overall deadline', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 0)
    assert.equal(errors.length, 1)
    assert.equal(errors[0].reason.message, 'GET_EVENTS_TIMEOUT')
    assert.ok(!success)
  })

  it('retains completed relay success while reporting only pending timeout errors', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
      timeout: 30,
      timeoutAfterFirstEose: null
    })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()

    const { errors, success } = await resultPromise
    assert.ok(success)
    assert.deepEqual(errors.map(({ relay, reason }) => [relay, reason.message]), [
      ['wss://r2', 'GET_EVENTS_TIMEOUT']
    ])
  })

  it('does not create a deadline when timeout is null', async () => {
    let resolved = false
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], {
      timeout: null,
      timeoutAfterFirstEose: null
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(!resolved, 'null should not coerce to an immediate deadline')

    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    assert.ok((await resultPromise).success)
  })

  it('ignores events that arrive after the result has timed out', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { timeout: 10 })
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    const result = await resultPromise

    sub.handlers.onevent(makeEvent({ id: 'late' }))
    await tick()
    assert.equal(result.result.length, 0)
    assert.equal(result.errors[0].reason.message, 'GET_EVENTS_TIMEOUT')
  })

  it('adds relay error when relay closes with an error', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('connection dropped'))
    const { errors, success } = await resultPromise
    assert.equal(errors.length, 1)
    assert.ok(errors[0].reason.message.includes('connection dropped'))
    assert.ok(!success)
  })

  it('collects events from multiple relays', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    relayRegistry.get('wss://r2').subscriptions[0].handlers.onevent(makeEvent({ id: 'e2' }))
    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()
    const { result, errors } = await resultPromise
    assert.equal(result.length, 2)
    assert.equal(errors.length, 0)
  })

  it('deduplicates matching event ids across relays without changing relay completion', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
      timeoutAfterFirstEose: null
    })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'same-id' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    relayRegistry.get('wss://r2').subscriptions[0].handlers.onevent(makeEvent({ id: 'same-id' }))
    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()

    const { result, errors, success } = await resultPromise
    assert.deepEqual(result.map(event => event.id), ['same-id'])
    assert.deepEqual(errors, [])
    assert.ok(success)
  })

  it('success:true when at least one relay succeeds', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    relayRegistry.get('wss://r2').subscriptions[0].handlers.onclose(new Error('boom'))
    const { success } = await resultPromise
    assert.ok(success)
  })

  it('success:false when all relays error', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('err1'))
    relayRegistry.get('wss://r2').subscriptions[0].handlers.onclose(new Error('err2'))
    const { success, errors } = await resultPromise
    assert.ok(!success)
    assert.equal(errors.length, 2)
  })

  it('calls callback with event items', async () => {
    const items = []
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], {
      callback: item => items.push(item)
    })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await resultPromise
    assert.ok(items.some(i => i.type === 'event' && i.event.id === 'e1' && i.relay === 'wss://r1'))
  })

  it('rejects when signal is aborted', async () => {
    const ac = new AbortController()
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    await tick()
    ac.abort()
    await assert.rejects(resultPromise, /Aborted/)
  })

  it('returns immediately with an unsuccessful empty result when no relays are given', async () => {
    assert.deepEqual(
      await nostr.getEvents({ kinds: [0] }, []),
      { result: [], errors: [], success: false }
    )
  })

  describe('early close', () => {
    it('resolves after filter.limit events without waiting for EOSE', async () => {
      const resultPromise = nostr.getEvents({ kinds: [0], limit: 2 }, ['wss://r1'])
      await tick()
      const sub = relayRegistry.get('wss://r1').subscriptions[0]
      sub.handlers.onevent(makeEvent({ id: 'e1' }))
      sub.handlers.onevent(makeEvent({ id: 'e2' }))
      const { result } = await resultPromise
      assert.equal(result.length, 2)
      assert.ok(sub.isClosed)
    })

    it('counts oninvalidevent toward limit', async () => {
      const resultPromise = nostr.getEvents({ kinds: [0], limit: 2 }, ['wss://r1'])
      await tick()
      const sub = relayRegistry.get('wss://r1').subscriptions[0]
      sub.handlers.oninvalidevent(makeEvent({ id: 'bad' })) // count: 1
      sub.handlers.onevent(makeEvent({ id: 'e1' }))         // count: 2 → closes
      const { result } = await resultPromise
      assert.equal(result.length, 1)
      assert.ok(sub.isClosed)
    })

    it('closes when all filter.ids have been seen', async () => {
      const e1 = makeEvent({ id: 'aaa' })
      const e2 = makeEvent({ id: 'bbb' })
      const resultPromise = nostr.getEvents({ ids: ['aaa', 'bbb'] }, ['wss://r1'])
      await tick()
      const sub = relayRegistry.get('wss://r1').subscriptions[0]
      sub.handlers.onevent(e1)
      sub.handlers.onevent(e2)
      const { result } = await resultPromise
      assert.equal(result.length, 2)
      assert.ok(sub.isClosed)
    })
  })
})

describe('RelayPool.getEvents EOSE grace', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  it('collects events and resolves when all relay subs close', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    sub.handlers.oneose()
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 1)
    assert.equal(result[0].id, 'e1')
    assert.equal(errors.length, 0)
    assert.ok(success)
  })

  it('sets event.meta.relay', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result[0].meta.relay, 'wss://r1')
  })

  it('starts short timer after first relay with events EOSEs, finalizes before second relay', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
      timeoutAfterFirstEose: 50
    })
    await tick()
    const sub1 = relayRegistry.get('wss://r1').subscriptions[0]
    sub1.handlers.onevent(makeEvent({ id: 'e1' }))
    sub1.handlers.oneose() // has events → starts 50ms timer; r2 still pending
    const { result, success } = await resultPromise
    assert.equal(result.length, 1)
    assert.ok(success)
  })

  it('does not start short timer when first EOSE has no events', async () => {
    let resolved = false
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
      timeoutAfterFirstEose: 50,
      timeout: 500
    })
    resultPromise.then(() => { resolved = true })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose() // no events
    await new Promise(resolve => setTimeout(resolve, 80))
    assert.ok(!resolved, 'should not resolve early when first EOSE had no events')
    // clean up: trigger second relay to let promise resolve
    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()
    await resultPromise
  })

  it('waits for every relay when timeoutAfterFirstEose is null', async () => {
    let resolved = false
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
      timeout: 100,
      timeoutAfterFirstEose: null
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    const first = relayRegistry.get('wss://r1').subscriptions[0]
    first.handlers.onevent(makeEvent({ id: 'e1' }))
    first.handlers.oneose()
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(!resolved, 'null should disable the post-EOSE grace timer')

    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result.length, 1)
  })

  it('returns terminal timeout errors on overall timeout', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 0)
    assert.equal(errors[0].reason.message, 'GET_EVENTS_TIMEOUT')
    assert.ok(!success)
  })

  it('rejects when signal is aborted', async () => {
    const ac = new AbortController()
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    await tick()
    ac.abort()
    await assert.rejects(resultPromise, /Aborted/)
  })

  it('early close: resolves after filter.limit events without EOSE', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0], limit: 1 }, ['wss://r1'])
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    const { result } = await resultPromise
    assert.equal(result.length, 1)
    assert.ok(sub.isClosed)
  })

  it('early close via limit/ids triggers timeoutAfterFirstEose for remaining relays', async () => {
    // With 2 relays: r1 satisfies limit:1 → handleEose runs → 50ms timer → finalize
    const resultPromise = nostr.getEvents({ kinds: [0], limit: 1 }, ['wss://r1', 'wss://r2'], {
      timeoutAfterFirstEose: 50
    })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    // r2 still pending — should resolve after short timer, not after overall timeout
    const { result, success } = await resultPromise
    assert.equal(result.length, 1)
    assert.ok(success)
  })

  it('single relay: resolves immediately on EOSE without waiting for timeoutAfterFirstEose', async () => {
    let resolved = false
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], {
      timeoutAfterFirstEose: 500
    })
    resultPromise.then(() => { resolved = true })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await tick() // microtasks flush — .then() should have run
    assert.ok(resolved, 'single relay should not wait for timeoutAfterFirstEose')
    await resultPromise
  })

  it('single relay: early close also resolves immediately', async () => {
    let resolved = false
    const resultPromise = nostr.getEvents({ kinds: [0], limit: 1 }, ['wss://r1'], {
      timeoutAfterFirstEose: 500
    })
    resultPromise.then(() => { resolved = true })
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    await tick()
    assert.ok(resolved, 'single relay early close should not wait for timeoutAfterFirstEose')
    await resultPromise
  })

  it('adds errors when relay closes with an error', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('dropped'))
    const { errors, success } = await resultPromise
    assert.equal(errors.length, 1)
    assert.ok(errors[0].reason.message.includes('dropped'))
    assert.ok(!success) // all relays errored
  })
})

describe('RelayPool.countEvents', () => {
  let nostr

  beforeEach(() => {
    relayRegistry.clear()
    connectOverrides.clear()
    sendOverrides.clear()
    nostr = new RelayPool()
  })

  it('sends one NIP-45 filter and returns a single relay count immediately', async () => {
    const filter = { kinds: [1], '#p': ['pubkey'] }
    let resolved = false
    const resultPromise = nostr.countEvents(filter, ['wss://r1'], { timeoutAfterFirstCount: 500 })
    resultPromise.then(() => { resolved = true })

    await tick()
    const relay = relayRegistry.get('wss://r1')
    const request = countRequest(relay)
    assert.equal(request[0], 'COUNT')
    assert.match(request[1], /^p2r2p-count:\d+$/)
    assert.deepEqual(request.slice(2), [filter])

    receiveCount(relay, { count: 4, approximate: true })
    await tick()
    assert.ok(resolved, 'a sole relay should not wait for the grace timer')
    assert.deepEqual(await resultPromise, {
      count: 4,
      approximate: true,
      errors: [],
      success: true
    })
  })

  it('keeps zero as a valid count', async () => {
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1'])
    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 0 })

    assert.deepEqual(await resultPromise, {
      count: 0,
      approximate: false,
      errors: [],
      success: true
    })
  })

  it('waits after a plain first count for a higher count and later HLL', async () => {
    let resolved = false
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], {
      timeout: 500,
      timeoutAfterFirstCount: 100
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 4 })
    await tick()
    assert.ok(!resolved, 'the first plain count should open the grace window')

    receiveCount(relayRegistry.get('wss://r2'), { count: 7, hll: hll({ 3: 1 }) })
    const result = await resultPromise
    assert.equal(result.count, 7)
    assert.equal(result.approximate, false)
    assert.equal(result.hll, hll({ 3: 1 }))
    assert.equal(result.hllCount, 1)
    assert.equal(result.errors.length, 0)
    assert.ok(result.success)
  })

  it('waits after an invalid HLL but does not return it', async () => {
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], {
      timeout: 500,
      timeoutAfterFirstCount: 20
    })
    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 2, hll: '' })

    const result = await resultPromise
    assert.equal(result.count, 2)
    assert.ok(!('hll' in result))
    assert.ok(!('hllCount' in result))
    assert.equal(result.errors.length, 0)
  })

  it('uses only the overall timeout when timeoutAfterFirstCount is null', async () => {
    let resolved = false
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], {
      timeout: 30,
      timeoutAfterFirstCount: null
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 4 })
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(!resolved, 'null should not coerce to a zero-millisecond grace timer')

    const result = await resultPromise
    assert.equal(result.count, 4)
    assert.ok(result.success)
    assert.deepEqual(result.errors.map(({ relay, reason }) => [relay, reason.message]), [
      ['wss://r2', 'COUNT_TIMEOUT']
    ])
  })

  it('does not create an overall COUNT timer when timeout is null', async () => {
    let resolved = false
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], {
      timeout: null,
      timeoutAfterFirstCount: null
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(!resolved, 'null should not coerce to an immediate overall timeout')

    receiveCount(relayRegistry.get('wss://r1'), { count: 4 })
    receiveCount(relayRegistry.get('wss://r2'), { count: 7 })
    assert.equal((await resultPromise).count, 7)
  })

  it('merges HLL replies and returns as soon as all relays settle', async () => {
    let resolved = false
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], {
      timeoutAfterFirstCount: 500
    })
    resultPromise.then(() => { resolved = true })

    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 5, approximate: true, hll: hll({ 0: 1, 1: 2 }) })
    receiveCount(relayRegistry.get('wss://r2'), { count: 5, hll: hll({ 0: 4, 2: 3 }) })
    await tick()
    assert.ok(resolved, 'all relay replies should finish before the grace timer')

    const result = await resultPromise
    assert.equal(result.count, 5)
    assert.equal(result.approximate, false)
    assert.equal(result.hll, hll({ 0: 4, 1: 2, 2: 3 }))
    assert.equal(result.hllCount, 3)
  })

  it('reports malformed COUNT payloads as relay errors', async () => {
    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r1'])
    await tick()
    receiveCount(relayRegistry.get('wss://r1'), { count: 'four' })

    const result = await resultPromise
    assert.equal(result.count, null)
    assert.equal(result.success, false)
    assert.equal(result.errors.length, 1)
    assert.equal(result.errors[0].reason.message, 'INVALID_COUNT_RESPONSE')
  })

  it('reports connection failures and COUNT refusals without authenticating', async () => {
    connectOverrides.set('wss://r1', () => { throw new Error('connection failed') })
    const failedConnection = await nostr.countEvents({ kinds: [1] }, ['wss://r1'])
    assert.equal(failedConnection.errors[0].reason.message, 'connection failed')

    const refusalPromise = nostr.countEvents({ kinds: [1] }, ['wss://r2'])
    await tick()
    const relay = relayRegistry.get('wss://r2')
    closeCount(relay, 'auth-required: cannot count private events')
    const refusal = await refusalPromise
    assert.equal(refusal.errors[0].reason.message, 'auth-required: cannot count private events')
    assert.deepEqual(relay.sentMessages.map(JSON.parse).map(message => message[0]), ['COUNT'])
  })

  it('reports unresolved relays at the overall timeout', async () => {
    const result = await nostr.countEvents({ kinds: [1] }, ['wss://r1', 'wss://r2'], { timeout: 20 })

    assert.equal(result.count, null)
    assert.equal(result.success, false)
    assert.deepEqual(result.errors.map(({ relay, reason }) => [relay, reason.message]), [
      ['wss://r1', 'COUNT_TIMEOUT'],
      ['wss://r2', 'COUNT_TIMEOUT']
    ])
  })

  it('rejects caller aborts and ignores late COUNT replies', async () => {
    const ac = new AbortController()
    const aborted = nostr.countEvents({ kinds: [1] }, ['wss://r1'], { signal: ac.signal })
    await tick()
    ac.abort()
    await assert.rejects(aborted, /Aborted/)

    const resultPromise = nostr.countEvents({ kinds: [1] }, ['wss://r2', 'wss://r3'], {
      timeoutAfterFirstCount: 20
    })
    await tick()
    const r2 = relayRegistry.get('wss://r2')
    const r3 = relayRegistry.get('wss://r3')
    const [, r3RequestId] = countRequest(r3)
    receiveCount(r2, { count: 3 })
    const result = await resultPromise

    receiveRelayMessage(r3, ['COUNT', r3RequestId, { count: 99 }])
    await tick()
    assert.equal(result.count, 3)
  })

  it('requires exactly one filter', async () => {
    await assert.rejects(
      nostr.countEvents([{ kinds: [1] }], ['wss://r1']),
      /COUNT_FILTER_REQUIRED/
    )
  })
})

describe('RelayPool.getEventsGenerator', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  it('yields event items', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    sub.handlers.onevent(makeEvent({ id: 'e2' }))
    sub.handlers.oneose()
    await promise
    assert.equal(items.length, 2)
    assert.equal(items[0].type, 'event')
    assert.equal(items[0].event.id, 'e1')
    assert.equal(items[0].relay, 'wss://r1')
  })

  it('yields error items when relay closes with error', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('boom'))
    await promise
    assert.ok(items.some(i => i.type === 'error' && i.relay === 'wss://r1'))
  })

  it('completes once getEvents resolves', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await promise
    assert.equal(items.length, 1)
  })

  it('completes when getEvents reaches its overall timeout', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    )
    await promise
    assert.equal(items.length, 0)
  })
})

describe('RelayPool.sendEvent', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    publishOverrides.clear()
    sendOverrides.clear()
    nostr = new RelayPool()
  })

  it('returns after the first accepted relay and keeps a full settlement promise', async () => {
    const delayed = deferred()
    const relayResults = []
    publishOverrides.set('wss://r2', () => delayed.promise)
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1', 'wss://r2'], {
      timeoutUntilFirstFulfillment: 100,
      timeout: 1000,
      onRelayResult: result => relayResults.push(result)
    })

    assert.equal(early.result, null)
    assert.equal(early.total, 2)
    assert.equal(early.success, true)
    assert.deepEqual(relayResults, [{
      relay: 'wss://r1',
      success: true,
      outcome: 'published'
    }])

    delayed.reject(new Error('relay failed'))
    const full = await early.promise
    assert.equal(full.result, null)
    assert.equal(full.success, true)
    assert.equal(full.total, 2)
    assert.equal(full.fulfilled, 1)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
    assert.equal(full.errors.length, 1)
    assert.deepEqual(relayResults.map(({ reason, ...result }) => result), [{
      relay: 'wss://r1',
      success: true,
      outcome: 'published'
    }, {
      relay: 'wss://r2',
      success: false,
      outcome: 'failed'
    }])
    assert.equal(relayResults[1].reason.message, 'relay failed')
  })

  it('strips event.meta before publishing', async () => {
    let published
    publishOverrides.set('wss://r1', e => { published = e })
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '', meta: { relay: 'wss://old' } }
    const early = await nostr.sendEvent(event, ['wss://r1'])
    await early.promise
    assert.ok(!('meta' in published), 'meta should be stripped before publish')
  })

  it('treats duplicate: error as success', async () => {
    const relayResults = []
    publishOverrides.set('wss://r1', () => { throw new Error('duplicate: already have this event') })
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'], {
      onRelayResult: result => relayResults.push(result)
    })
    const full = await early.promise
    assert.ok(early.success)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
    assert.equal(full.errors.length, 0)
    assert.deepEqual(relayResults, [{
      relay: 'wss://r1',
      success: true,
      outcome: 'duplicate'
    }])
  })

  it('treats mute: error as success', async () => {
    const relayResults = []
    publishOverrides.set('wss://r1', () => { throw new Error('mute: author blocked') })
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'], {
      onRelayResult: result => relayResults.push(result)
    })
    const full = await early.promise
    assert.ok(early.success)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
    assert.equal(full.errors.length, 0)
    assert.deepEqual(relayResults, [{
      relay: 'wss://r1',
      success: true,
      outcome: 'muted'
    }])
  })

  it('retries auth-required publishes after sending the caller AUTH event', async () => {
    const authRequests = []
    let publishCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      if (publishCount === 1) {
        receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'challenge-one'])
        throw new Error('auth-required: sign in first')
      }
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      const [type, authEvent] = JSON.parse(message)
      assert.equal(type, 'AUTH')
      receiveRelayMessage(relay, ['OK', authEvent.id, true, ''])
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: request => {
        authRequests.push(request)
        return { id: 'auth-one', kind: 22242, pubkey: 'alice' }
      }
    })
    const full = await early.promise

    assert.equal(publishCount, 2)
    assert.deepEqual(authRequests, [{ relay: 'wss://r1/', challenge: 'challenge-one' }])
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
  })

  it('leaves auth-required publishes failed when no getAuthEvent is supplied', async () => {
    let publishCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'challenge-one'])
      throw new Error('auth-required: sign in first')
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'])
    const full = await early.promise

    assert.equal(publishCount, 1)
    assert.equal(relayRegistry.get('wss://r1').sentMessages?.length ?? 0, 0)
    assert.equal(full.errors[0].reason.message, 'auth-required: sign in first')
  })

  it('authenticates once after restricted so the current caller can retry', async () => {
    let publishCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      if (publishCount === 1) {
        receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'shared-challenge'])
        throw new Error('restricted: another identity is not allowed')
      }
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      const [, authEvent] = JSON.parse(message)
      receiveRelayMessage(relay, ['OK', authEvent.id, true, ''])
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: ({ relay, challenge }) => ({
        id: 'auth-current-caller', kind: 22242, pubkey: 'current', relay, challenge
      })
    })
    const full = await early.promise

    assert.equal(publishCount, 2)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
  })

  it('does not retry again when a post-auth publish remains restricted', async () => {
    let publishCount = 0
    let authCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      if (publishCount === 1) receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'challenge-one'])
      throw new Error('restricted: still not allowed')
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      authCount++
      const [, authEvent] = JSON.parse(message)
      receiveRelayMessage(relay, ['OK', authEvent.id, true, ''])
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: () => ({ id: 'auth-one', kind: 22242, pubkey: 'alice' })
    })
    const full = await early.promise

    assert.equal(publishCount, 2)
    assert.equal(authCount, 1)
    assert.equal(full.errors[0].reason.message, 'restricted: still not allowed')
  })

  it('keeps multiple caller auth events on the same relay connection', async () => {
    const authenticatedPubkeys = new Set()
    const authEventIds = []
    publishOverrides.set('wss://r1', event => {
      if (authenticatedPubkeys.has(event.pubkey)) return
      receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'shared-challenge'])
      throw new Error('auth-required: sign in first')
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      const [, authEvent] = JSON.parse(message)
      authEventIds.push(authEvent.id)
      authenticatedPubkeys.add(authEvent.pubkey)
      receiveRelayMessage(relay, ['OK', authEvent.id, true, ''])
    })

    const first = await nostr.sendEvent({ ...makeEvent({ id: 'publish-alice' }), pubkey: 'alice' }, ['wss://r1'], {
      getAuthEvent: () => ({ id: 'auth-alice', kind: 22242, pubkey: 'alice' })
    })
    await first.promise
    const relay = relayRegistry.get('wss://r1')

    const second = await nostr.sendEvent({ ...makeEvent({ id: 'publish-bob' }), pubkey: 'bob' }, ['wss://r1'], {
      getAuthEvent: () => ({ id: 'auth-bob', kind: 22242, pubkey: 'bob' })
    })
    await second.promise

    assert.equal(relayRegistry.get('wss://r1'), relay)
    assert.deepEqual(authEventIds, ['auth-alice', 'auth-bob'])
  })

  it('fails auth cleanly when no relay challenge was received', async () => {
    let authCalls = 0
    publishOverrides.set('wss://r1', () => { throw new Error('restricted: not allowed') })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: () => {
        authCalls++
        return { id: 'auth-one', kind: 22242 }
      }
    })
    const full = await early.promise

    assert.equal(authCalls, 0)
    assert.equal(full.errors[0].reason.message, 'AUTH_CHALLENGE_MISSING')
  })

  it('does not retry the event when the relay rejects its AUTH event', async () => {
    let publishCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'challenge-one'])
      throw new Error('auth-required: sign in first')
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      const [, authEvent] = JSON.parse(message)
      receiveRelayMessage(relay, ['OK', authEvent.id, false, 'restricted: AUTH is not allowed'])
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: () => ({ id: 'auth-one', kind: 22242, pubkey: 'alice' })
    })
    const full = await early.promise

    assert.equal(publishCount, 1)
    assert.equal(full.errors[0].reason.message, 'restricted: AUTH is not allowed')
  })

  it('does not mistake an AUTH rejection for a duplicate published event', async () => {
    let publishCount = 0
    publishOverrides.set('wss://r1', () => {
      publishCount++
      receiveRelayMessage(relayRegistry.get('wss://r1'), ['AUTH', 'challenge-one'])
      throw new Error('auth-required: sign in first')
    })
    sendOverrides.set('wss://r1', (message, relay) => {
      const [, authEvent] = JSON.parse(message)
      receiveRelayMessage(relay, ['OK', authEvent.id, false, 'duplicate: auth already exists'])
    })

    const early = await nostr.sendEvent(makeEvent({ id: 'publish-one' }), ['wss://r1'], {
      getAuthEvent: () => ({ id: 'auth-one', kind: 22242, pubkey: 'alice' })
    })
    const full = await early.promise

    assert.equal(publishCount, 1)
    assert.equal(full.success, false)
    assert.equal(full.errors[0].reason.message, 'duplicate: auth already exists')
  })

  it('does not authenticate while reading relay events', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    const relay = relayRegistry.get('wss://r1')
    receiveRelayMessage(relay, ['AUTH', 'read-challenge'])
    relay.subscriptions[0].handlers.oneose()
    await resultPromise

    assert.equal(relay.sentMessages?.length ?? 0, 0)
  })

  it('reports failed relays with their reasons', async () => {
    publishOverrides.set('wss://r1', () => { throw new Error('invalid: bad event') })
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'])
    const full = await early.promise
    assert.ok(!early.success)
    assert.equal(full.success, false)
    assert.deepEqual(full.succeededRelays, [])
    assert.equal(full.errors.length, 1)
    assert.equal(full.errors[0].relay, 'wss://r1')
    assert.ok(full.errors[0].reason.message.includes('invalid'))
  })

  it('includes both accepted and failed relays in the final report', async () => {
    publishOverrides.set('wss://r2', () => { throw new Error('invalid: bad event') })
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1', 'wss://r2'])
    const full = await early.promise
    assert.ok(early.success)
    assert.equal(full.success, true)
    assert.equal(full.fulfilled, 1)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
    assert.equal(full.errors.length, 1)
    assert.equal(full.errors[0].relay, 'wss://r2')
  })

  it('turns an unsuccessful first-fulfillment timeout into an operation timeout', async () => {
    const delayed = deferred()
    publishOverrides.set('wss://r1', () => delayed.promise)
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'], {
      timeoutUntilFirstFulfillment: 10,
      timeout: 1000
    })

    assert.equal(early.success, false)
    const full = await early.promise
    assert.equal(full.success, false)
    assert.deepEqual(full.succeededRelays, [])
    assert.equal(full.errors[0].reason.message, 'PUBLISH_TIMEOUT')

    delayed.resolve()
  })

  it('uses the overall timeout when timeoutUntilFirstFulfillment is null', async () => {
    const relayResults = []
    publishOverrides.set('wss://r1', () => new Promise(() => {}))
    const pending = nostr.sendEvent(makeEvent({ id: 'ev1' }), ['wss://r1'], {
      timeout: 20,
      timeoutUntilFirstFulfillment: null,
      onRelayResult: result => relayResults.push(result)
    })
    let returned = false
    pending.then(() => { returned = true })

    await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(!returned, 'null should disable only the early fulfillment timer')

    const early = await pending
    const full = await early.promise
    assert.equal(early.success, false)
    assert.equal(full.success, false)
    assert.equal(relayResults.length, 1)
    assert.equal(relayResults[0].outcome, 'timed-out')
  })

  it('allows an overall timeout to be disabled', async () => {
    const delayed = deferred()
    publishOverrides.set('wss://r1', () => delayed.promise)
    const pending = nostr.sendEvent(makeEvent({ id: 'ev1' }), ['wss://r1'], {
      timeout: null,
      timeoutUntilFirstFulfillment: null
    })
    let returned = false
    pending.then(() => { returned = true })

    await tick()
    assert.ok(!returned, 'disabled timers should not be coerced to zero')
    delayed.resolve()

    const early = await pending
    const full = await early.promise
    assert.equal(early.success, true)
    assert.equal(full.success, true)
  })

  it('records an operation timeout without cancelling the underlying publish', async () => {
    const relayResults = []
    const first = deferred()
    const second = deferred()
    publishOverrides.set('wss://r1', () => first.promise)
    publishOverrides.set('wss://r2', () => second.promise)
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1', 'wss://r2'], {
      timeoutUntilFirstFulfillment: null,
      timeout: 10,
      onRelayResult: result => relayResults.push(result)
    })
    const full = await early.promise

    assert.equal(early.success, false)
    assert.equal(full.success, false)
    assert.deepEqual(full.errors.map(({ relay, reason }) => [relay, reason.message]), [
      ['wss://r1', 'PUBLISH_TIMEOUT'],
      ['wss://r2', 'PUBLISH_TIMEOUT']
    ])
    assert.deepEqual(relayResults.map(({ relay, outcome }) => [relay, outcome]), [
      ['wss://r1', 'timed-out'],
      ['wss://r2', 'timed-out']
    ])

    first.resolve()
    second.reject(new Error('late failure'))
    await tick()
    assert.equal(relayResults.length, 2, 'late outcomes must not alter the finalized report')
  })

  it('returns an immediately settled failure report when given no relays', async () => {
    const early = await nostr.sendEvent({ id: 'ev1' }, [])
    const full = await early.promise
    assert.deepEqual(early, {
      result: null,
      total: 0,
      success: false,
      promise: early.promise
    })
    assert.deepEqual(full, {
      success: false,
      total: 0,
      fulfilled: 0,
      errors: [],
      result: null,
      succeededRelays: []
    })
  })
})
