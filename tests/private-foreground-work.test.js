import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createForegroundWork } from '../private-messenger/session/helpers/foreground-work.js'
import { backgroundSigner } from '../private-messenger/helpers/background-signer.js'

test('foreground work delays only the next background signer operation', async () => {
  const gate = createForegroundWork(); const calls = []
  const signer = { value: 1, async signEvent (event) { calls.push(event); return this.value } }
  const controller = new AbortController()
  const background = backgroundSigner(signer, gate.wait, controller.signal)
  const release = gate.enter(); const other = gate.enter()
  const pending = background.signEvent('presence')
  assert.equal(await signer.signEvent('text'), 1)
  release(); await Promise.resolve(); assert.deepEqual(calls, ['text'])
  other(); assert.equal(await pending, 1)
  assert.deepEqual(calls, ['text', 'presence'])
  gate.close()
})

test('cancelled or closed background waits never invoke the signer later', async () => {
  const gate = createForegroundWork(); let calls = 0
  const controller = new AbortController()
  const background = backgroundSigner({ signEvent: async () => calls++ }, gate.wait, controller.signal)
  const release = gate.enter()
  const pending = background.signEvent({}); controller.abort()
  await assert.rejects(pending, { name: 'AbortError' })
  const waiting = gate.wait(); gate.close()
  await assert.rejects(waiting, { name: 'AbortError' })
  release(); await Promise.resolve(); assert.equal(calls, 0)
})
