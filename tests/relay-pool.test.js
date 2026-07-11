import { describe, it, beforeEach, mock, test } from 'node:test'
import assert from 'node:assert/strict'

// ─── Fake Relay infrastructure ────────────────────────────────────────────────

// Keyed by URL; populated by FakeRelay constructor, cleared in beforeEach.
const relayRegistry = new Map()

// Per-URL connect overrides: async () => void (throw to simulate error, hang to simulate timeout)
const connectOverrides = new Map()

// Per-URL publish overrides: async (event) => void (throw to simulate error, hang to simulate timeout)
const publishOverrides = new Map()

class FakeRelay {
  constructor (url) {
    this.url = url
    this.subscriptions = []
    this.ws = { readyState: 1 }
    relayRegistry.set(url, this)
  }

  async connect () {
    const fn = connectOverrides.get(this.url)
    if (fn) await fn()
  }

  subscribe (filters, handlers) {
    const sub = {
      filters,
      handlers,
      isClosed: false,
      close () {
        if (this.isClosed) return
        this.isClosed = true
        handlers.onclose?.()
      }
    }
    this.subscriptions.push(sub)
    return sub
  }

  async publish (event) {
    this.lastPublishedEvent = event
    const fn = publishOverrides.get(this.url)
    if (fn) await fn(event)
  }

  async close () {
    this.ws.readyState = 3
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

  it('reconnect opens a gap fill sub using lastSeenAt as since', async () => {
    const ac = new AbortController()
    let capturedArgs
    async function * mockGapAsap (f, r, o) {
      capturedArgs = { f, r, o }
    }

    const { promise } = startCollecting(nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1'],
      { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
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

    assert.ok(capturedArgs, '_gapAsapGenerator should have been called on reconnect')
    assert.equal(capturedArgs.f.since, 750, 'reconnect gap fill uses lastSeenAt as since')
    assert.ok(capturedArgs.f.until > 0)
    assert.deepEqual(capturedArgs.r, ['wss://r1'])

    ac.abort()
    await promise
  })

  it('reconnect uses filter.since as gap baseline when no events have been seen', async () => {
    const ac = new AbortController()
    let capturedSince
    async function * mockGapAsap (f) { capturedSince = f.since }

    const { promise } = startCollecting(nostr.getLiveEventsGenerator(
      { kinds: [0], since: 500 },
      ['wss://r1'],
      { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
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
      async function * mockGapAsap (f) { capturedUntil = f.until }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100, until },
        ['wss://r1'],
        { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
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
    it('uses _gapAsapGenerator by default (gapTimeoutAfterFirstEose set)', async () => {
      const ac = new AbortController()
      let asapCalled = false
      async function * mockGapAsap () { asapCalled = true }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100 },
        ['wss://r1'],
        { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
      ))

      await tick()
      relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      assert.ok(asapCalled)
      ac.abort()
      await promise
    })

    it('uses _gapFetchGenerator when gapTimeoutAfterFirstEose is null', async () => {
      const ac = new AbortController()
      let fetchCalled = false
      async function * mockGapFetch () { fetchCalled = true }

      const { promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 100 },
        ['wss://r1'],
        { signal: ac.signal, gapTimeoutAfterFirstEose: null, _gapFetchGenerator: mockGapFetch }
      ))

      await tick()
      relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose()
      await new Promise(resolve => setTimeout(resolve, 1100))
      await tick()

      assert.ok(fetchCalled)
      ac.abort()
      await promise
    })

    it('buffers live events during reconnect gap fill, yields gap events first', async () => {
      const ac = new AbortController()
      const liveEvent = makeEvent({ id: 'live1', created_at: 200 })
      const gapEvent = makeEvent({ id: 'gap1', created_at: 50 })
      let resolveGap

      async function * mockGapAsap () {
        yield { type: 'event', event: gapEvent, relay: 'wss://r1' }
        await new Promise(resolve => { resolveGap = resolve })
      }

      const { events, promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 1 },
        ['wss://r1'],
        { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
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

      async function * mockGapAsap () {
        yield { type: 'event', event: dupEvent, relay: 'wss://r1' }
        await new Promise(resolve => { resolveGap = resolve })
      }

      const { events, promise } = startCollecting(nostr.getLiveEventsGenerator(
        { kinds: [0], since: 1 },
        ['wss://r1'],
        { signal: ac.signal, _gapAsapGenerator: mockGapAsap }
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
      async function * mockAsap () {
        callOrder.push('fetch')
      }

      const ac = new AbortController()
      const { promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _asapGenerator: mockAsap
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
      async function * mockAsap () {
        yield { type: 'event', event: storedEvent, relay: 'wss://r1' }
      }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 1 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _asapGenerator: mockAsap
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
      async function * mockAsap () {
        yield { type: 'event', event: sharedEvent, relay: 'wss://r1' }
      }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ since: 1 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _asapGenerator: mockAsap
        })
      )

      await tick()
      resolveLive()
      ac.abort()
      await promise

      assert.equal(events.filter(e => e.id === 'shared').length, 1, 'duplicate should appear once')
    })

    it('uses _asapGenerator for initial fetch when timeoutAfterFirstEose is set', async () => {
      let asapCalled = false
      let fetchCalled = false

      async function * mockLive () { /* stays silent */ await new Promise(() => {}) }
      async function * mockAsap () { asapCalled = true }
      async function * mockFetch () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: ac.signal,
        timeoutAfterFirstEose: 500,
        _liveGenerator: mockLive,
        _asapGenerator: mockAsap,
        _fetchGenerator: mockFetch
      }))

      await tick()
      assert.ok(asapCalled)
      assert.ok(!fetchCalled)
      ac.abort()
    })

    it('uses _fetchGenerator for initial fetch when timeoutAfterFirstEose is null', async () => {
      let asapCalled = false
      let fetchCalled = false

      async function * mockLive () { await new Promise(() => {}) }
      async function * mockAsap () { asapCalled = true }
      async function * mockFetch () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: ac.signal,
        timeoutAfterFirstEose: null,
        _liveGenerator: mockLive,
        _asapGenerator: mockAsap,
        _fetchGenerator: mockFetch
      }))

      await tick()
      assert.ok(!asapCalled)
      assert.ok(fetchCalled)
      ac.abort()
    })

    it('skips initial fetch and delegates directly to _liveGenerator when filter.limit === 0', async () => {
      let fetchCalled = false
      let liveCalled = false
      async function * mockLive () { liveCalled = true; yield makeEvent({ id: 'e1' }) }
      async function * mockAsap () { fetchCalled = true }

      const ac = new AbortController()
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ limit: 0 }, ['wss://r1'], {
          signal: ac.signal,
          _liveGenerator: mockLive,
          _asapGenerator: mockAsap
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
      async function * mockAsap () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({}, ['wss://r1'], {
        signal: ac.signal,
        _liveGenerator: mockLive,
        _asapGenerator: mockAsap
      }))

      await tick()
      assert.ok(fetchCalled, 'initial fetch should always run for live:true')
      ac.abort()
    })

    it('triggers initial fetch for filter.limit > 0', async () => {
      let fetchCalled = false
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockAsap () { fetchCalled = true }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ limit: 3 }, ['wss://r1'], {
        signal: ac.signal,
        _liveGenerator: mockLive,
        _asapGenerator: mockAsap
      }))

      await tick()
      assert.ok(fetchCalled)
      ac.abort()
    })

    it('passes timeout and timeoutAfterFirstEose to _asapGenerator', async () => {
      let capturedOpts
      async function * mockLive () { await new Promise(() => {}) }
      async function * mockAsap (_f, _r, o) { capturedOpts = o }

      const ac = new AbortController()
      startCollecting(nostr.getEventsFeedGenerator({ since: 100 }, ['wss://r1'], {
        signal: ac.signal,
        timeout: 3000,
        timeoutAfterFirstEose: 200,
        _liveGenerator: mockLive,
        _asapGenerator: mockAsap
      }))

      await tick()
      assert.equal(capturedOpts.timeout, 3000)
      assert.equal(capturedOpts.timeoutAfterFirstEose, 200)
      ac.abort()
    })
  })

  // ── live:false ──────────────────────────────────────────────────────────────

  describe('live:false + timeoutAfterFirstEose set', () => {
    it('delegates to _asapGenerator with correct args', async () => {
      const ac = new AbortController()
      let capturedArgs
      async function * mockAsap (f, r, o) {
        capturedArgs = { f, r, o }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }

      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ kinds: [0], since: 100 }, ['wss://r1'], {
          live: false, timeout: 3000, timeoutAfterFirstEose: 200,
          signal: ac.signal, _asapGenerator: mockAsap
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
      async function * mockAsap () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: 500, _asapGenerator: mockAsap
        })
      )
      await promise
      assert.equal(events.length, 1)
      assert.equal(events[0].id, 'e1')
    })
  })

  describe('live:false + timeoutAfterFirstEose:null', () => {
    it('delegates to _fetchGenerator with correct args', async () => {
      const ac = new AbortController()
      let capturedArgs
      async function * mockFetch (f, r, o) {
        capturedArgs = { f, r, o }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }

      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({ kinds: [0] }, ['wss://r1'], {
          live: false, timeout: 4000, timeoutAfterFirstEose: null,
          signal: ac.signal, _fetchGenerator: mockFetch
        })
      )

      await promise
      assert.equal(capturedArgs.o.timeout, 4000)
      assert.equal(capturedArgs.o.signal, ac.signal)
      assert.equal(events.length, 1)
    })

    it('skips non-event items', async () => {
      async function * mockFetch () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: null, _fetchGenerator: mockFetch
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

  it('adds relay error on timeout', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 0)
    assert.equal(errors.length, 1)
    assert.ok(errors[0].reason.message.includes('timeout'))
    assert.ok(!success)
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

  it('returns errors (does not throw) when signal is aborted', async () => {
    const ac = new AbortController()
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    await tick()
    ac.abort()
    const { result, errors } = await resultPromise
    assert.equal(result.length, 0)
    assert.ok(errors.some(e => e.reason.message === 'Aborted'))
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

describe('RelayPool.getEventsAsap', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  it('collects events and resolves when all relay subs close', async () => {
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'])
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
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result[0].meta.relay, 'wss://r1')
  })

  it('starts short timer after first relay with events EOSEs, finalizes before second relay', async () => {
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
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
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1', 'wss://r2'], {
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

  it('resolves on overall timeout with empty result', async () => {
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    const { result, success } = await resultPromise
    assert.equal(result.length, 0)
    assert.ok(success) // no relay error, just empty
  })

  it('rejects when signal is aborted', async () => {
    const ac = new AbortController()
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    await tick()
    ac.abort()
    await assert.rejects(resultPromise, /Aborted/)
  })

  it('early close: resolves after filter.limit events without EOSE', async () => {
    const resultPromise = nostr.getEventsAsap({ kinds: [0], limit: 1 }, ['wss://r1'])
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    const { result } = await resultPromise
    assert.equal(result.length, 1)
    assert.ok(sub.isClosed)
  })

  it('early close via limit/ids triggers timeoutAfterFirstEose for remaining relays', async () => {
    // With 2 relays: r1 satisfies limit:1 → handleEose runs → 50ms timer → finalize
    const resultPromise = nostr.getEventsAsap({ kinds: [0], limit: 1 }, ['wss://r1', 'wss://r2'], {
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
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'], {
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
    const resultPromise = nostr.getEventsAsap({ kinds: [0], limit: 1 }, ['wss://r1'], {
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
    const resultPromise = nostr.getEventsAsap({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('dropped'))
    const { errors, success } = await resultPromise
    assert.equal(errors.length, 1)
    assert.ok(errors[0].reason.message.includes('dropped'))
    assert.ok(!success) // all relays errored
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
})

describe('RelayPool.getEventsAsapGenerator', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = new RelayPool()
  })

  it('yields event items', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsAsapGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    sub.handlers.oneose()
    await promise
    assert.equal(items.length, 1)
    assert.equal(items[0].type, 'event')
    assert.equal(items[0].event.id, 'e1')
    assert.equal(items[0].relay, 'wss://r1')
  })

  it('yields error items when relay closes with error', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsAsapGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('kaboom'))
    await promise
    assert.ok(items.some(i => i.type === 'error'))
  })

  it('completes once getEventsAsap resolves', async () => {
    const { events: items, promise } = startCollecting(
      nostr.getEventsAsapGenerator({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    )
    await promise // resolves on timeout
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
    nostr = new RelayPool()
  })

  it('returns after the first accepted relay and keeps a full settlement promise', async () => {
    const delayed = deferred()
    const relayResults = []
    publishOverrides.set('wss://r2', () => delayed.promise)
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1', 'wss://r2'], {
      firstFulfillmentTimeoutMs: 100,
      settlementTimeoutMs: 1000,
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

  it('can time out before acknowledgement and still settle successfully later', async () => {
    const delayed = deferred()
    publishOverrides.set('wss://r1', () => delayed.promise)
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'], {
      firstFulfillmentTimeoutMs: 10,
      settlementTimeoutMs: 1000
    })

    assert.equal(early.success, false)
    delayed.resolve()
    const full = await early.promise
    assert.equal(full.success, true)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
  })

  it('records a settlement timeout without cancelling the underlying publish', async () => {
    const relayResults = []
    publishOverrides.set('wss://r1', () => new Promise(() => {}))
    const event = { id: 'ev1', kind: 1, created_at: 100, tags: [], content: '' }
    const early = await nostr.sendEvent(event, ['wss://r1'], {
      firstFulfillmentTimeoutMs: 10,
      settlementTimeoutMs: 10,
      onRelayResult: result => relayResults.push(result)
    })
    const full = await early.promise

    assert.equal(early.success, false)
    assert.equal(full.success, false)
    assert.equal(full.errors.length, 1)
    assert.equal(full.errors[0].reason.message, 'PUBLISH_TIMEOUT')
    assert.equal(relayResults.length, 1)
    assert.equal(relayResults[0].outcome, 'timed-out')
    assert.equal(relayResults[0].reason.message, 'PUBLISH_TIMEOUT')
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
