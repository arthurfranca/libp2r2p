import { test } from 'node:test'
import assert from 'node:assert/strict'

import { finalizeEvent } from '../event/index.js'
import { generateSecretKey } from '../key/index.js'
import { RelayConnection } from '../relay/services/relay-connection.js'

class FakeWebSocket {
  static instances = []
  constructor (url) {
    this.url = url
    this.readyState = 0
    this.sent = []
    FakeWebSocket.instances.push(this)
  }

  open () { this.readyState = 1; this.onopen?.() }
  receive (message) { this.onmessage?.({ data: JSON.stringify(message) }) }
  send (message) { this.sent.push(JSON.parse(message)) }
  close () { this.readyState = 3; queueMicrotask(() => this.onclose?.({ reason: 'closed' })) }
}

const tick = () => new Promise(resolve => setImmediate(resolve))

function signedEvent (properties = {}) {
  return finalizeEvent({ kind: 1, created_at: 1, tags: [['t', 'x']], content: '', ...properties }, generateSecretKey())
}

async function connected () {
  const relay = new RelayConnection('wss://relay.example/', { WebSocket: FakeWebSocket })
  const promise = relay.connect()
  FakeWebSocket.instances.at(-1).open()
  await promise
  return { relay, socket: FakeWebSocket.instances.at(-1) }
}

test('RelayConnection validates and filters subscription events', async () => {
  const { relay, socket } = await connected()
  const valid = []
  const invalid = []
  const subscription = relay.subscribe([{ kinds: [1], '#t': ['x'] }], {
    onevent: event => valid.push(event),
    oninvalidevent: event => invalid.push(event)
  })
  socket.receive(['EVENT', subscription.id, signedEvent()])
  socket.receive(['EVENT', subscription.id, { ...signedEvent(), content: 'mutated' }])
  socket.receive(['EVENT', subscription.id, signedEvent({ kind: 2 })])
  await tick()
  assert.equal(valid.length, 1)
  assert.equal(invalid.length, 2)
  subscription.close()
  assert.equal(socket.sent.at(-1)[0], 'CLOSE')
})

test('RelayConnection keeps publish, AUTH and COUNT operations separate', async () => {
  const { relay, socket } = await connected()
  const published = signedEvent()
  const publish = relay.publish(published)
  socket.receive(['OK', published.id, true, 'saved'])
  assert.equal(await publish, 'saved')

  socket.receive(['AUTH', 'challenge'])
  await tick()
  const authEvent = signedEvent({ kind: 22242 })
  const auth = relay.authenticate(async ({ challenge }) => {
    assert.equal(challenge, 'challenge')
    return authEvent
  })
  await tick()
  socket.receive(['OK', authEvent.id, true, 'authenticated'])
  assert.equal(await auth, 'authenticated')

  const count = relay.countWithHll([{ kinds: [1] }])
  const id = socket.sent.find(message => message[0] === 'COUNT')[1]
  socket.receive(['COUNT', id, { count: 3, hll: '00' }])
  assert.deepEqual(await count, { count: 3, hll: '00' })
})

test('RelayConnection rejects pending work on close and can reopen', async () => {
  const { relay, socket } = await connected()
  const pending = relay.publish(signedEvent())
  socket.close()
  await assert.rejects(pending, /closed/i)
  const reconnect = relay.connect()
  const replacement = FakeWebSocket.instances.at(-1)
  assert.notEqual(replacement, socket)
  replacement.open()
  await reconnect
  await relay.close()
})
