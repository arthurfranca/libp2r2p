import { test } from 'node:test'
import assert from 'node:assert/strict'

import * as esbuild from 'esbuild'

const root = new URL('..', import.meta.url).pathname

async function bundle (contents) {
  const result = await esbuild.build({
    absWorkingDir: root,
    bundle: true,
    format: 'esm',
    metafile: true,
    minify: true,
    platform: 'browser',
    stdin: {
      contents,
      loader: 'js',
      resolveDir: root,
      sourcefile: 'entry.js'
    },
    treeShaking: true,
    write: false
  })
  return {
    inputs: Object.keys(result.metafile.inputs),
    text: result.outputFiles[0].text
  }
}

const wasmLoaded = result => result.text.includes('AGFzbQE')

test('default event and key entrypoints include the accelerated backend', async () => {
  assert.equal(wasmLoaded(await bundle(
    "import { isValidEvent } from 'libp2r2p/event'; console.log(isValidEvent)"
  )), true)
  assert.equal(wasmLoaded(await bundle(
    "import { getPublicKey } from 'libp2r2p/key'; console.log(getPublicKey)"
  )), true)
})

test('JS-only event and key entrypoints exclude the WASM artifact', async () => {
  const event = await bundle(
    "import { isValidEvent } from 'libp2r2p/event/js'; console.log(isValidEvent)"
  )
  const key = await bundle(
    "import { getPublicKey } from 'libp2r2p/key/js'; console.log(getPublicKey)"
  )
  assert.equal(wasmLoaded(event), false)
  assert.equal(wasmLoaded(key), false)
  assert.equal(event.text.includes('AGFzbQE'), false)
  assert.equal(key.text.includes('AGFzbQE'), false)
})

test('unrelated subpaths remain free of event and key crypto', async () => {
  const result = await bundle(
    "import { normalizeRelayUrl } from 'libp2r2p/url'; console.log(normalizeRelayUrl)"
  )
  assert.equal(wasmLoaded(result), false)
  assert.equal(result.inputs.some(path => path.includes('event/index.js')), false)
  assert.equal(result.inputs.some(path => path.includes('key/index.js')), false)
})

test('JS-only key generation and event classification stay free of WASM bytes', async () => {
  const key = await bundle(
    "import { generateSecretKey } from 'libp2r2p/key'; console.log(generateSecretKey)"
  )
  const event = await bundle(
    "import { classifyEvent } from 'libp2r2p/event'; console.log(classifyEvent)"
  )
  assert.equal(wasmLoaded(key), false)
  assert.equal(wasmLoaded(event), false)
})
