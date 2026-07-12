import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import { pool, publish } from '../relay/services/index.js'

const originalPublish = pool.publish

afterEach(() => {
  pool.publish = originalPublish
})

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

test('publish returns after the first relay fulfills and keeps full settlement promise', async () => {
  const first = deferred()
  const second = deferred()
  pool.publish = () => [first.promise, second.promise]

  const pending = publish({ id: 'event' }, ['wss://a.example', 'wss://b.example'], {
    timeoutUntilFirstFulfillment: 1000,
    timeout: 1000
  })
  let returned = false
  pending.then(() => { returned = true })
  await Promise.resolve()
  assert.equal(returned, false)

  first.resolve()
  const early = await pending
  assert.equal(early.success, true)
  assert.equal(early.total, 2)
  assert.equal(early.fulfilled, undefined)
  assert.equal(early.errors, undefined)

  second.reject(new Error('relay failed'))
  const full = await early.promise
  assert.equal(full.success, true)
  assert.equal(full.total, 2)
  assert.equal(full.fulfilled, 1)
  assert.equal(full.errors.length, 1)
  assert.equal(full.errors[0].relay, 'wss://b.example')
  assert.equal(full.errors[0].reason.message, 'relay failed')
})

test('an unsuccessful first-fulfillment window closes pending relay reports', async () => {
  const first = deferred()
  pool.publish = () => [first.promise]

  const early = await publish({ id: 'event' }, ['wss://a.example'], {
    timeoutUntilFirstFulfillment: 10,
    timeout: 1000
  })

  assert.equal(early.success, false)
  const full = await early.promise
  assert.equal(full.success, false)
  assert.equal(full.total, 1)
  assert.equal(full.fulfilled, 0)
  assert.equal(full.errors.length, 1)
  assert.equal(full.errors[0].relay, 'wss://a.example')
  assert.equal(full.errors[0].reason.message, 'PUBLISH_TIMEOUT')

  first.resolve()
})

test('publish full settlement promise times out slow relay acknowledgements', async () => {
  const first = deferred()
  const second = deferred()
  pool.publish = () => [first.promise, second.promise]

  const pending = publish({ id: 'event' }, ['wss://a.example', 'wss://b.example'], {
    timeoutUntilFirstFulfillment: 1000,
    timeout: 10
  })
  first.resolve()
  const early = await pending
  const full = await early.promise

  assert.equal(full.success, true)
  assert.equal(full.total, 2)
  assert.equal(full.fulfilled, 1)
  assert.equal(full.errors.length, 1)
  assert.equal(full.errors[0].relay, 'wss://b.example')
  assert.equal(full.errors[0].reason.message, 'PUBLISH_TIMEOUT')

  second.resolve()
})
