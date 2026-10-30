import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Worker } from 'node:worker_threads'

import { initializeEventCrypto } from '../event/index.js'

test('each worker initializes an independent WASM backend', async () => {
  const moduleUrl = new URL('../event/index.js', import.meta.url).href
  const worker = new Worker(`
    const { parentPort } = require('node:worker_threads')
    void (async () => {
      const event = await import(${JSON.stringify(moduleUrl)})
      parentPort.postMessage(await event.initializeEventCrypto())
    })()
  `, { eval: true })

  const workerBackend = await new Promise((resolve, reject) => {
    worker.once('message', resolve)
    worker.once('error', reject)
  })
  assert.equal(workerBackend, 'wasm')
  assert.equal(await initializeEventCrypto(), 'wasm')
  await worker.terminate()
})
