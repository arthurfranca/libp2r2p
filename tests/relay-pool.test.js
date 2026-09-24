import { describe, it, beforeEach, test } from 'node:test'
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
    this.pendingCounts = new Map()
    this.pendingAuths = new Map()
    this.serial = 0
    this.challenge = null
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

  countWithHll (filters, { signal } = {}) {
    const id = `p2r2p-count:${++this.serial}`
    const pending = Promise.withResolvers()
    const onAbort = () => {
      this.pendingCounts.delete(id)
      pending.reject(new Error('COUNT_ABORTED'))
    }
    this.pendingCounts.set(id, { ...pending, signal, onAbort })
    signal?.addEventListener('abort', onAbort, { once: true })
    this.send(JSON.stringify(['COUNT', id, ...filters]))
    return pending.promise
  }

  async authenticate (getAuthEvent) {
    if (!this.challenge) throw new Error('AUTH_CHALLENGE_MISSING')
    const event = await getAuthEvent({ relay: this.url, challenge: this.challenge })
    const pending = Promise.withResolvers()
    this.pendingAuths.set(event.id, pending)
    await this.send(JSON.stringify(['AUTH', event]))
    return pending.promise
  }

  _onmessage (message) {
    const data = JSON.parse(message.data)
    if (data[0] === 'AUTH') this.challenge = data[1]
    if (data[0] === 'COUNT') {
      const pending = this.pendingCounts.get(data[1])
      if (!pending) return
      this.pendingCounts.delete(data[1])
      pending.signal?.removeEventListener('abort', pending.onAbort)
      pending.resolve(data[2])
    }
    if (data[0] === 'CLOSED') {
      const pending = this.pendingCounts.get(data[1])
      if (!pending) return
      this.pendingCounts.delete(data[1])
      pending.reject(new Error(data[2]))
    }
    if (data[0] === 'OK') {
      const pending = this.pendingAuths.get(data[1])
      if (!pending) return
      this.pendingAuths.delete(data[1])
      if (data[2]) pending.resolve(data[3])
      else pending.reject(new Error(data[3]))
    }
  }

  async close () {
    this.ws.readyState = 3
    this.onclose?.()
  }
}

const { RelayPool, relayPool } = await import('../relay/index.js')

function createRelayPool () {
  return new RelayPool({ _createRelay: url => new FakeRelay(url) })
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Drain pending microtasks + one I/O turn — enough for async relay setup to settle
const tick = () => new Promise(resolve => setImmediate(resolve))

function deferred () {
  let resolve
  let reject
  // eslint-disable-next-line promise/param-names
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
  const items = []
  const promise = (async () => {
    for await (const item of gen) {
      items.push(item)
      if (item.type === 'event') events.push(item.event)
    }
  })()
  return { events, items, promise }
}

async function nextEvent (stream) {
  for (;;) {
    const next = await stream.next()
    if (next.done) return next
    if (next.value.type === 'event') return { value: next.value.event, done: false }
  }
}

let _nextId = 1
// eslint-disable-next-line camelcase
function makeEvent ({ id, kind = 0, created_at = 100 } = {}) {
  // eslint-disable-next-line camelcase
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
    nostr = createRelayPool()
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

  it('discards pre-EOSE events per relay without holding another relay back', async () => {
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
    assert.deepEqual(events.map(event => event.id), ['between-eoses'])

    second.handlers.onevent(makeEvent({ id: 'retained-second' }))
    second.handlers.oneose()
    second.handlers.onevent(makeEvent({ id: 'live-after-eose' }))
    await tick()

    ac.abort()
    await promise
    assert.deepEqual(events.map(event => event.id), ['between-eoses', 'live-after-eose'])
  })

  it('reports relays that reach EOSE during the first-EOSE grace period', async () => {
    autoEoseForLiveSubscriptions = false
    const ac = new AbortController()
    const stream = nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1', 'wss://r2', 'wss://r3'],
      { signal: ac.signal, timeoutAfterFirstEose: 20 }
    )
    const { promise } = startCollecting(stream)

    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await new Promise(resolve => setTimeout(resolve, 5))
    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()

    const report = await stream.ready
    assert.deepEqual(report.relays, ['wss://r1', 'wss://r2'])
    assert.deepEqual(report.errors, [])
    assert.deepEqual(stream.readyRelays, ['wss://r1', 'wss://r2'])

    ac.abort()
    await promise
  })

  it('waits for every initial relay when the first-EOSE grace is null', async () => {
    autoEoseForLiveSubscriptions = false
    const ac = new AbortController()
    const stream = nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1', 'wss://r2'],
      { signal: ac.signal, timeoutAfterFirstEose: null }
    )
    const { promise } = startCollecting(stream)
    let resolved = false
    stream.ready.then(() => { resolved = true })

    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await tick()
    assert.equal(resolved, false)

    relayRegistry.get('wss://r2').subscriptions[0].handlers.oneose()
    const report = await stream.ready
    assert.deepEqual(report.relays, ['wss://r1', 'wss://r2'])

    ac.abort()
    await promise
  })

  it('opens only an overlapping live sub (limit:0) — no initial fetch', async () => {
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

  it('keeps live event provenance outside the event', async () => {
    const ac = new AbortController()
    const { events, promise } = startCollecting(
      nostr.getLiveEventsGenerator({ kinds: [0] }, ['wss://r1'], { signal: ac.signal })
    )

    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    await tick()

    ac.abort()
    await promise
    assert.equal(events[0].id, 'e1')
    assert.ok(!Object.hasOwn(events[0], 'meta'))
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

    assert.equal(relay.subscriptions.length, 3, 'new live and recovery subscriptions opened after reconnect')

    ac.abort()
    await promise
  })

  it('removes a disconnected relay from readyRelays until its replacement reaches EOSE', async () => {
    autoEoseForLiveSubscriptions = false
    const ac = new AbortController()
    const stream = nostr.getLiveEventsGenerator(
      { kinds: [0] },
      ['wss://r1'],
      { signal: ac.signal, timeoutAfterFirstEose: 0 }
    )
    const { promise } = startCollecting(stream)

    await tick()
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions[0].handlers.oneose()
    await stream.ready
    assert.deepEqual(stream.readyRelays, ['wss://r1'])

    relay.subscriptions[0].handlers.onclose()
    assert.deepEqual(stream.readyRelays, [])
    await new Promise(resolve => setTimeout(resolve, 1100))
    await tick()

    relay.subscriptions[1].handlers.oneose()
    assert.deepEqual(stream.readyRelays, ['wss://r1'])

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

  it('reconnect overlaps the last received timestamp by ten minutes', async () => {
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
    assert.equal(capturedArgs.f.since, 150, 'reconnect gap fill includes ten minutes before lastSeenAt')
    assert.ok(capturedArgs.f.until > 0)
    assert.deepEqual(capturedArgs.r, ['wss://r1'])

    ac.abort()
    await promise
  })

  it('reconnect uses opening time as gap baseline when no events have been seen', async () => {
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

    assert.equal(capturedSince, relayRegistry.get('wss://r1').subscriptions[0].filters[0].since)

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
    nostr = createRelayPool()
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
        yield { type: 'event', event: liveEvent, relay: 'wss://r1' }
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
      await promise
      ac.abort()

      assert.equal(events[0].id, 'stored', 'stored event should come first')
      assert.equal(events[1].id, 'live', 'live event should come after')
    })

    it('deduplicates live events that overlap with initial fetch events', async () => {
      const sharedEvent = makeEvent({ id: 'shared', created_at: 100 })
      let resolveLive

      // Live generator yields the shared event immediately (simulates it arriving while
      // the fetch is still running), then waits to keep the generator open
      async function * mockLive () {
        yield { type: 'event', event: sharedEvent, relay: 'wss://r1' }
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
      async function * mockLive () { liveCalled = true; yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' } }
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
      assert.equal(capturedArgs.o.signal.aborted, false)
      ac.abort()
      assert.equal(capturedArgs.o.signal.aborted, true)
      assert.equal(events.length, 1)
      assert.equal(events[0].id, 'e1')
    })

    it('forwards error items alongside events', async () => {
      async function * mockEvents () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, items, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: 500, _eventsGenerator: mockEvents
        })
      )
      await promise
      assert.deepEqual(items.map(item => item.type), ['error', 'event'])
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
      assert.equal(capturedArgs.o.signal.aborted, false)
      ac.abort()
      assert.equal(capturedArgs.o.signal.aborted, true)
      assert.equal(events.length, 1)
    })

    it('forwards error items alongside events', async () => {
      async function * mockEvents () {
        yield { type: 'error', error: new Error('oops'), relay: 'wss://r1' }
        yield { type: 'event', event: makeEvent({ id: 'e1' }), relay: 'wss://r1' }
      }
      const { events, items, promise } = startCollecting(
        nostr.getEventsFeedGenerator({}, ['wss://r1'], {
          live: false, timeoutAfterFirstEose: null, _eventsGenerator: mockEvents
        })
      )
      await promise
      assert.deepEqual(items.map(item => item.type), ['error', 'event'])
      assert.equal(events.length, 1)
    })
  })
})

describe('relay stream stopAndDrain', () => {
  beforeEach(() => {
    relayRegistry.clear()
    connectOverrides.clear()
    autoEoseForLiveSubscriptions = true
  })

  for (const options of [{}, { live: false }, { limit: 0 }]) {
    it(`preserves accepted events and rejects late callbacks: ${JSON.stringify(options)}`, { timeout: 3000 }, async () => {
      const pool = createRelayPool()
      const stream = pool.getEventsFeedGenerator(options.limit === 0 ? { limit: 0 } : {}, ['wss://r1'], options)
      const first = nextEvent(stream)
      await tick()
      const relay = relayRegistry.get('wss://r1')
      const live = relay.subscriptions.find(sub => sub.filters[0].limit === 0)
      const history = relay.subscriptions.find(sub => sub.filters[0].limit !== 0)
      history?.handlers.onevent(makeEvent({ id: 'history-1' }))
      history?.handlers.onevent(makeEvent({ id: 'history-2' }))
      live?.handlers.onevent(makeEvent({ id: 'live-1' }))
      live?.handlers.onevent(makeEvent({ id: 'live-2' }))
      stream.stopAndDrain()
      stream.stopAndDrain()
      assert.ok(relay.subscriptions.every(sub => sub.isClosed))
      for (const sub of relay.subscriptions) sub.handlers.onevent(makeEvent({ id: 'late' }))
      const events = [(await first).value]
      for await (const item of stream) if (item.type === 'event') events.push(item.event)
      assert.deepEqual(events.map(event => event.id), [
        ...(history ? ['history-1', 'history-2'] : []),
        ...(live ? ['live-1', 'live-2'] : [])
      ])
      assert.ok(events.every(event => !Object.hasOwn(event, 'meta')))
      await pool.disconnectAll()
    })
  }

  it('drains reconnect history and its live buffer while a recovery query is pending', { timeout: 4000 }, async () => {
    const pool = createRelayPool()
    const stream = pool.getLiveEventsGenerator({ since: 1 }, ['wss://r1'])
    const first = nextEvent(stream)
    await tick()
    const relay = relayRegistry.get('wss://r1')
    const old = relay.subscriptions[0]
    old.close()
    await new Promise(resolve => setTimeout(resolve, 1100))
    const live = relay.subscriptions.find(sub => !sub.isClosed && sub.filters[0].limit === 0)
    const history = relay.subscriptions.find(sub => !sub.isClosed && sub.filters[0].limit !== 0)
    assert.ok(live && history)
    history.handlers.onevent(makeEvent({ id: 'gap-1' }))
    history.handlers.onevent(makeEvent({ id: 'gap-2' }))
    live.handlers.onevent(makeEvent({ id: 'buffered-live' }))
    stream.stopAndDrain()
    for (const sub of relay.subscriptions) {
      assert.ok(sub.isClosed)
      sub.handlers.onevent(makeEvent({ id: 'late' }))
    }
    const events = [(await first).value]
    for await (const item of stream) if (item.type === 'event') events.push(item.event)
    assert.deepEqual(events.map(event => event.id), ['gap-1', 'gap-2', 'buffered-live'])
    await pool.disconnectAll()
  })

  it('allows caller abort to discard the remaining drain queue', async () => {
    const pool = createRelayPool()
    const ac = new AbortController()
    const stream = pool.getEventsFeedGenerator({ limit: 0 }, ['wss://r1'], { signal: ac.signal })
    const first = nextEvent(stream)
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'first' }))
    sub.handlers.onevent(makeEvent({ id: 'queued' }))
    assert.equal((await first).value.id, 'first')
    stream.stopAndDrain()
    ac.abort()
    assert.equal((await stream.next()).done, true)
    await pool.disconnectAll()
  })

  it('stops before the first next without opening connections', async () => {
    const pool = createRelayPool()
    for (const method of ['getEventsFeedGenerator', 'getLiveEventsGenerator']) {
      const stream = pool[method]({}, ['wss://r1'])
      stream.stopAndDrain()
      assert.equal((await stream.next()).done, true)
    }
    assert.equal(relayRegistry.size, 0)
  })

  it('return cancels a pending next and closes the network subscriptions', { timeout: 3000 }, async () => {
    const pool = createRelayPool()
    const stream = pool.getEventsFeedGenerator({}, ['wss://r1'])
    const next = stream.next()
    await tick()
    await stream.return()
    assert.equal((await next).done, true)
    assert.ok(relayRegistry.get('wss://r1').subscriptions.every(sub => sub.isClosed))
    await pool.disconnectAll()
  })

  it('stopping during connection setup prevents late subscriptions from opening', { timeout: 3000 }, async () => {
    const connecting = deferred()
    connectOverrides.set('wss://r1', () => connecting.promise)
    const pool = createRelayPool()
    const stream = pool.getEventsFeedGenerator({}, ['wss://r1'])
    const next = stream.next()
    await tick()
    stream.stopAndDrain()
    assert.equal((await next).done, true)
    connecting.resolve()
    await tick()
    assert.equal(relayRegistry.get('wss://r1').subscriptions.length, 0)
    await pool.disconnectAll()
  })
})

describe('RelayPool.getEvents', () => {
  let nostr

  beforeEach(() => {
    _nextId = 1
    relayRegistry.clear()
    connectOverrides.clear()
    nostr = createRelayPool()
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
    assert.equal(result[0].event.id, 'e1')
    assert.equal(result[1].event.id, 'e2')
    assert.equal(errors.length, 0)
    assert.ok(success)
  })

  it('keeps query event provenance outside the event', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result[0].relay, 'wss://r1')
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
    assert.deepEqual(result.map(({ event }) => event.id), ['same-id'])
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
      { result: [], errors: [], success: false, relays: [] }
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
    nostr = createRelayPool()
  })

  it('collects events and resolves when all relay subs close', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    sub.handlers.oneose()
    const { result, errors, success } = await resultPromise
    assert.equal(result.length, 1)
    assert.equal(result[0].event.id, 'e1')
    assert.equal(errors.length, 0)
    assert.ok(success)
  })

  it('keeps query event provenance outside the event', async () => {
    const resultPromise = nostr.getEvents({ kinds: [0] }, ['wss://r1'])
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    const { result } = await resultPromise
    assert.equal(result[0].relay, 'wss://r1')
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
    nostr = createRelayPool()
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
    nostr = createRelayPool()
  })

  it('yields event items', async () => {
    const { items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    const sub = relayRegistry.get('wss://r1').subscriptions[0]
    sub.handlers.onevent(makeEvent({ id: 'e1' }))
    sub.handlers.onevent(makeEvent({ id: 'e2' }))
    sub.handlers.oneose()
    await promise
    assert.equal(items.length, 3)
    assert.equal(items.at(-1).type, 'eose')
    assert.equal(items[0].type, 'event')
    assert.equal(items[0].event.id, 'e1')
    assert.equal(items[0].relay, 'wss://r1')
  })

  it('yields error items when relay closes with error', async () => {
    const { items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onclose(new Error('boom'))
    await promise
    assert.ok(items.some(i => i.type === 'error' && i.relay === 'wss://r1'))
  })

  it('completes once getEvents resolves', async () => {
    const { items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'])
    )
    await tick()
    relayRegistry.get('wss://r1').subscriptions[0].handlers.onevent(makeEvent({ id: 'e1' }))
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await promise
    assert.equal(items.length, 2)
    assert.equal(items.at(-1).type, 'eose')
  })

  it('completes when getEvents reaches its overall timeout', async () => {
    const { items, promise } = startCollecting(
      nostr.getEventsGenerator({ kinds: [0] }, ['wss://r1'], { timeout: 30 })
    )
    await promise
    assert.deepEqual(items.map(item => item.type), ['error', 'eose'])
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
    nostr = createRelayPool()
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

    assert.equal(early.total, 2)
    assert.equal(early.success, true)
    assert.deepEqual(relayResults, [{
      relay: 'wss://r1',
      success: true,
      outcome: 'published'
    }])

    delayed.reject(new Error('relay failed'))
    const full = await early.promise
    assert.equal(full.success, true)
    assert.equal(full.total, 2)
    assert.equal(full.fulfilled, 1)
    assert.deepEqual(full.succeededRelays, ['wss://r1'])
    assert.equal(full.errors.length, 1)
    // eslint-disable-next-line no-unused-vars
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
    assert.deepEqual(authRequests, [{ relay: 'wss://r1', challenge: 'challenge-one' }])
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
      total: 0,
      success: false,
      promise: early.promise
    })
    assert.deepEqual(full, {
      success: false,
      total: 0,
      fulfilled: 0,
      errors: [],
      succeededRelays: []
    })
  })
})

describe('RelayPool read admission and snapshots', () => {
  beforeEach(() => {
    relayRegistry.clear()
    connectOverrides.clear()
    autoEoseForLiveSubscriptions = true
  })

  const configuredPool = options => new RelayPool({ _createRelay: url => new FakeRelay(url), ...options })
  const active = relay => relay?.subscriptions.filter(sub => !sub.isClosed) ?? []

  it('coordinates 24 feeds on one normalized connection with at most two historical REQs', async () => {
    const pool = configuredPool({})
    const controller = new AbortController()
    const reports = []
    const tasks = Array.from({ length: 24 }, (_, index) => (async () => {
      const url = index % 2 ? 'wss://r1/' : 'wss://r1'
      for await (const item of pool.getEventsFeedGenerator({ authors: [String(index)], kinds: [1] }, [url], { signal: controller.signal })) {
        if (item.type === 'eose') reports.push(item)
      }
    })())
    try {
      for (let turn = 0; turn < 40 && reports.length < 24; turn++) {
        await tick()
        const subs = active(relayRegistry.get('wss://r1'))
        const history = subs.filter(sub => sub.filters[0].limit !== 0)
        assert.ok(subs.length <= 28)
        assert.ok(history.length <= 2)
        for (const sub of history) sub.handlers.oneose()
      }
      assert.equal(reports.length, 24)
      assert.equal(active(relayRegistry.get('wss://r1')).length, 24)
    } finally { controller.abort(); await Promise.all(tasks) }
    assert.equal(active(relayRegistry.get('wss://r1')).length, 0)
  })

  it('does not open queued feed live input and starts network timeout only after admission', async () => {
    const pool = configuredPool({ maxSubscriptionsPerRelay: 2, maxConcurrentHistoryPerRelay: 1 })
    const hold = pool.getEvents({}, ['wss://r1'], { timeout: null })
    await tick()
    const relay = relayRegistry.get('wss://r1')
    const stream = pool.getEventsFeedGenerator({ kinds: [1] }, ['wss://r1'], { timeout: 10, queueTimeout: 1000 })
    const first = stream.next()
    await new Promise(resolve => setTimeout(resolve, 25))
    assert.equal(relay.subscriptions.length, 1)
    relay.subscriptions[0].handlers.oneose()
    await hold
    await tick()
    assert.equal(active(relay).length, 2)
    active(relay).find(sub => sub.filters[0].limit !== 0).handlers.oneose()
    assert.equal((await first).value.relays[0].status, 'eose')
    await stream.return()
    assert.equal(active(relay).length, 0)
  })

  it('return cancels queued one-shot and feed reads immediately without later REQs', async () => {
    const pool = configuredPool({ maxConcurrentHistoryPerRelay: 1 })
    const hold = pool.getEvents({}, ['wss://r1'], { timeout: null })
    await tick()
    for (const method of ['getEventsGenerator', 'getEventsFeedGenerator']) {
      const stream = pool[method]({}, ['wss://r1'])
      const pending = stream.next()
      await tick()
      await stream.return()
      assert.equal((await pending).done, true)
    }
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions[0].handlers.oneose()
    await hold
    await tick()
    assert.equal(relay.subscriptions.length, 1)
  })

  it('reports queue expiry for one relay while preserving another relay result', async () => {
    const pool = configuredPool({ maxConcurrentHistoryPerRelay: 1 })
    const hold = pool.getEvents({}, ['wss://r1'], { timeout: null })
    await tick()
    const pending = pool.getEvents({}, ['wss://r1', 'wss://r2'], { queueTimeout: 10, timeoutAfterFirstEose: null })
    await tick()
    const second = relayRegistry.get('wss://r2').subscriptions[0]
    second.handlers.onevent(makeEvent({ id: 'available' }))
    second.handlers.oneose()
    await new Promise(resolve => setTimeout(resolve, 20))
    const report = await pending
    assert.equal(report.result[0].event.id, 'available')
    assert.equal(report.relays[0].error.code, 'RELAY_READ_QUEUE_TIMEOUT')
    assert.equal(report.relays[1].status, 'eose')
    relayRegistry.get('wss://r1').subscriptions[0].handlers.oneose()
    await hold
  })

  it('snapshot waits for live readiness, reports its bounds and keeps live past until', async () => {
    autoEoseForLiveSubscriptions = false
    const pool = configuredPool({})
    const now = Math.floor(Date.now() / 1000)
    const stream = pool.getEventsFeedGenerator({ since: now - 600, until: now }, ['wss://r1'], { snapshot: true, timeoutAfterFirstEose: null })
    const next = stream.next()
    await tick()
    const relay = relayRegistry.get('wss://r1')
    assert.equal(relay.subscriptions.length, 1)
    const live = relay.subscriptions[0]
    assert.equal(live.filters[0].until, undefined)
    live.handlers.oneose()
    await tick()
    const history = relay.subscriptions[1]
    assert.equal(history.filters[0].until, now)
    const overlap = makeEvent({ id: 'overlap', created_at: now })
    live.handlers.onevent(overlap)
    history.handlers.onevent(overlap)
    history.handlers.oneose()
    assert.equal((await next).value.event.id, 'overlap')
    const marker = (await stream.next()).value
    assert.deepEqual(marker.snapshot, { since: now - 600, until: now })
    live.handlers.onevent(makeEvent({ id: 'later', created_at: now + 1 }))
    assert.equal((await stream.next()).value.event.id, 'later')
    await stream.return()
    assert.equal(active(relay).length, 0)
  })

  it('snapshot reports bounds even with no relays or no history', async () => {
    const pool = configuredPool({})
    const empty = pool.getEventsFeedGenerator({ since: 12 }, [], { snapshot: true })
    const { value } = await empty.next()
    assert.equal(value.snapshot.since, 12)
    assert.ok(value.snapshot.until > 12)
    assert.deepEqual(value.relays, [])
    assert.equal((await empty.next()).done, true)
    const historyOnly = pool.getEventsFeedGenerator({ since: 1, until: 3 }, [], { snapshot: true, live: false })
    assert.deepEqual((await historyOnly.next()).value.snapshot, { since: 1, until: 3 })
    await historyOnly.return()
  })

  for (const capacity of [{ maxBufferedLiveEvents: 1 }, { maxBufferedLiveBytes: 1 }]) {
    it(`fails explicitly and frees subscriptions when live exceeds ${Object.keys(capacity)[0]}`, async () => {
      const pool = configuredPool({ maxConcurrentHistoryPerRelay: 1 })
      const stream = pool.getEventsFeedGenerator({}, ['wss://r1'], { ...capacity, timeout: null })
      const failure = assert.rejects(stream.next(), { code: 'RELAY_LIVE_BUFFER_FULL', phase: 'live-buffer' })
      await tick()
      const relay = relayRegistry.get('wss://r1')
      const live = relay.subscriptions.find(sub => sub.filters[0].limit === 0)
      live.handlers.onevent(makeEvent({ id: 'one' }))
      live.handlers.onevent(makeEvent({ id: 'two' }))
      await failure
      assert.equal(active(relay).length, 0)
      const retry = pool.getEvents({}, ['wss://r1'])
      await tick()
      active(relay)[0].handlers.oneose()
      assert.equal((await retry).relays[0].status, 'eose')
    })
  }

  it('consumer can reject a failed historical attempt before pending live is released', async () => {
    const pool = configuredPool({})
    const stream = pool.getEventsFeedGenerator({}, ['wss://r1'])
    const first = stream.next()
    await tick()
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions.find(sub => sub.filters[0].limit === 0).handlers.onevent(makeEvent({ id: 'pending-live' }))
    relay.subscriptions.find(sub => sub.filters[0].limit !== 0).handlers.onclose(new Error('rejected'))
    assert.equal((await first).value.type, 'error')
    assert.equal((await stream.next()).value.relays[0].status, 'error')
    await stream.return()
    assert.equal((await stream.next()).done, true)
    assert.equal(active(relay).length, 0)
  })
  it('reports overflow of even an empty live marker as a buffer error', async () => {
    const pool = configuredPool({})
    await assert.rejects(pool.getLiveEventsGenerator({}, [], { maxBufferedLiveBytes: 1 }).next(), { code: 'RELAY_LIVE_BUFFER_FULL' })
  })

  it('stopAndDrain removes queued feed reservations without opening live input', async () => {
    const pool = configuredPool({ maxConcurrentHistoryPerRelay: 1 })
    const hold = pool.getEvents({}, ['wss://r1'], { timeout: null })
    await tick()
    const stream = pool.getEventsFeedGenerator({}, ['wss://r1'], { snapshot: true })
    const pending = stream.next()
    await tick()
    stream.stopAndDrain()
    assert.equal((await pending).done, true)
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions[0].handlers.oneose()
    await hold
    await tick()
    assert.equal(relay.subscriptions.length, 1)
  })

  it('bounds reconnect buffers and cancels their historical REQs on overflow', async () => {
    const pool = configuredPool({ maxConcurrentHistoryPerRelay: 1 })
    const stream = pool.getLiveEventsGenerator({ since: 1 }, ['wss://r1'], { maxBufferedLiveEvents: 1 })
    assert.equal((await stream.next()).value.type, 'eose')
    const relay = relayRegistry.get('wss://r1')
    relay.subscriptions[0].close()
    assert.equal((await stream.next()).value.type, 'error')
    const failure = assert.rejects(stream.next(), { code: 'RELAY_LIVE_BUFFER_FULL' })
    await new Promise(resolve => setTimeout(resolve, 1100))
    const live = active(relay).find(sub => sub.filters[0].limit === 0)
    assert.ok(live)
    assert.equal(active(relay).length, 2)
    live.handlers.onevent(makeEvent({ id: 'buffered-1' }))
    live.handlers.onevent(makeEvent({ id: 'buffered-2' }))
    await failure
    assert.equal(active(relay).length, 0)
  })
})
