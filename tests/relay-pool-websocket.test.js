import { test } from 'node:test'
import assert from 'node:assert/strict'

import { RelayPool } from '../relay/services/relay-pool.js'

class FakeWebSocket {
  static instances = []

  constructor (url) {
    this.url = url
    this.readyState = 0
    this.sent = []
    FakeWebSocket.instances.push(this)
  }

  open () {
    this.readyState = 1
    this.onopen?.()
  }

  receive (message) {
    this.onmessage?.({ data: JSON.stringify(message) })
  }

  send (message) {
    this.sent.push(JSON.parse(message))
  }

  close () {
    this.readyState = 3
    queueMicrotask(() => this.onclose?.({ code: 1000, reason: '' }))
  }
}

const tick = () => new Promise(resolve => setImmediate(resolve))

async function readOnce (pool, url = 'wss://relay.example') {
  FakeWebSocket.instances = []
  const promise = pool.getEvents({ kinds: [1], limit: 0 }, [url])
  await tick()
  const socket = FakeWebSocket.instances.at(-1)
  socket.open()
  await tick()
  const request = socket.sent.find(([op]) => op === 'REQ')
  socket.receive(['EOSE', request[1]])
  return { result: await promise, socket }
}

test('RelayPool injects the WebSocket implementation through the constructor', async () => {
  const pool = new RelayPool({ WebSocket: FakeWebSocket })
  const { result, socket } = await readOnce(pool)
  assert.equal(socket.url, 'wss://relay.example')
  assert.equal(result.success, true)
})

test('RelayPool.setWebSocket applies to connections created afterwards', async () => {
  const pool = new RelayPool()
  pool.setWebSocket(FakeWebSocket)
  const { result, socket } = await readOnce(pool)
  assert.equal(socket.url, 'wss://relay.example')
  assert.equal(result.success, true)
  assert.throws(() => pool.setWebSocket(null), /INVALID_WEBSOCKET_IMPLEMENTATION/)
})

test('an explicit relay factory still overrides the WebSocket option', async () => {
  const created = []
  const pool = new RelayPool({
    WebSocket: FakeWebSocket,
    _createRelay: url => {
      created.push(url)
      return {
        connect: async () => {},
        close: async () => {},
        subscribe: () => ({ close () {} }),
        ws: { readyState: 1 }
      }
    }
  })
  FakeWebSocket.instances = []
  pool.getEvents({ kinds: [1], limit: 0 }, ['wss://relay.example'])
  await tick()
  assert.deepEqual(created, ['wss://relay.example'])
  assert.equal(FakeWebSocket.instances.length, 0)
  await pool.disconnectAll()
})
