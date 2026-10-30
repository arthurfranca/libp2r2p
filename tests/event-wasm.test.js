import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  assertValidEvent,
  configureEventCrypto,
  finalizeEvent,
  getEventHash,
  initializeEventCrypto,
  isValidEvent
} from '../event/index.js'
import {
  finalizeEvent as finalizeEventWithJavaScript,
  getEventHash as getEventHashWithJavaScript,
  isValidEvent as isValidEventWithJavaScript
} from '../event/js/index.js'
import {
  serializeEvent,
  serializeEventChunks
} from '../event/helpers/serialize.js'
import { ValidationError } from '../error/index.js'
import { getReadyEventCryptoBackend } from '../helpers/secp256k1-wasm/backend.js'
import { getPublicKey } from '../key/index.js'
import { getPublicKey as getPublicKeyWithJavaScript } from '../key/js/index.js'

const secretKey = new Uint8Array(32).fill(7)
const pubkey = getPublicKeyWithJavaScript(secretKey)

function template (content = 'hello') {
  return {
    kind: 1,
    created_at: 1_700_000_000,
    tags: [['t', 'benchmark']],
    content
  }
}

test('explicit initialization selects and reuses the WASM backend', async () => {
  const results = await Promise.all([
    initializeEventCrypto(),
    initializeEventCrypto(),
    initializeEventCrypto()
  ])
  assert.deepEqual(results, ['wasm', 'wasm', 'wasm'])
  assert.equal(getPublicKey(secretKey), pubkey)
})

test('incremental serialization is byte-identical to serializeEvent', () => {
  const controls = Array.from({ length: 32 }, (_, index) => String.fromCharCode(index)).join('')
  const contents = [
    '',
    'hello',
    controls,
    'form\f feed\n carriage\r tab\t backspace\b quote" slash\\',
    '日本語 😀 \u2028 \u2029 \ud800 \udc00',
    `${'a'.repeat(4095)}😀${'b'.repeat(4097)}`
  ]

  for (const content of contents) {
    const event = {
      kind: 1,
      created_at: 1_700_000_000,
      pubkey,
      tags: [
        ['t', `${'x'.repeat(4095)}😀`],
        ['description', controls]
      ],
      content
    }
    assert.equal([...serializeEventChunks(event)].join(''), serializeEvent(event))
  }

  for (const tags of [[], [['']], [['x', ''], ['y', 'value']]]) {
    const event = {
      kind: 1,
      created_at: 0,
      pubkey,
      tags,
      content: ''
    }
    assert.equal([...serializeEventChunks(event)].join(''), serializeEvent(event))
  }
})

test('incremental serialization matches across arbitrary UTF-16 boundaries', () => {
  let state = 0x12345678
  const random = () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state
  }
  for (let sample = 0; sample < 100; sample++) {
    let content = ''
    const length = random() % 200
    for (let index = 0; index < length; index++) {
      content += String.fromCharCode(random() & 0xffff)
    }
    const event = {
      kind: 1,
      created_at: sample,
      pubkey,
      tags: [['x', content.slice(0, length >> 1)], ['y', content.slice(length >> 1)]],
      content
    }
    for (const chunkSize of [1, 2, 3, 7, 31, 4096]) {
      assert.equal(
        [...serializeEventChunks(event, { chunkSize })].join(''),
        serializeEvent(event)
      )
    }
  }
})

test('WASM and JavaScript hashing and signing are interchangeable', () => {
  const javaScriptEvent = finalizeEventWithJavaScript(template('cross-backend'), secretKey)
  assert.equal(getEventHash(javaScriptEvent), getEventHashWithJavaScript(javaScriptEvent))
  assert.equal(isValidEvent(javaScriptEvent), true)
  assert.equal(isValidEventWithJavaScript(javaScriptEvent), true)

  const wasmEvent = finalizeEvent(template('cross-backend'), secretKey)
  assert.equal(getEventHash(wasmEvent), getEventHashWithJavaScript(wasmEvent))
  assert.equal(isValidEvent(wasmEvent), true)
  assert.equal(isValidEventWithJavaScript(wasmEvent), true)
})

test('WASM preserves purity, validation and public errors', () => {
  const source = { ...template(), extension: true }
  const snapshot = structuredClone(source)
  const event = finalizeEvent(source, secretKey)
  assert.deepEqual(source, snapshot)
  assert.notEqual(event.tags, source.tags)
  assert.notEqual(event.tags[0], source.tags[0])
  assert.equal(event.extension, true)
  assert.equal(assertValidEvent(event), event)

  const wrongId = { ...event, id: '0'.repeat(64) }
  assert.equal(isValidEvent(wrongId), false)
  assert.equal(isValidEventWithJavaScript(wrongId), false)
  assert.throws(() => assertValidEvent(wrongId), error => (
    error instanceof ValidationError && error.code === 'EVENT_ID_MISMATCH'
  ))

  const wrongSignature = { ...event, sig: '0'.repeat(128) }
  assert.equal(isValidEvent(wrongSignature), false)
  assert.throws(() => assertValidEvent(wrongSignature), error => (
    error instanceof ValidationError && error.code === 'INVALID_EVENT_SIGNATURE'
  ))

  const activeBackend = getReadyEventCryptoBackend()
  assert.throws(
    () => getEventHash({ ...event, tags: [[1]] }),
    error => error instanceof ValidationError && error.code === 'INVALID_EVENT_TAGS'
  )
  assert.equal(getReadyEventCryptoBackend(), activeBackend)
})

test('streaming hashes a 1 MiB event without the original 1 MiB heap limit', () => {
  const event = finalizeEventWithJavaScript(template('a'.repeat(1024 * 1024)), secretKey)
  assert.equal(getEventHash(event), getEventHashWithJavaScript(event))
  assert.equal(isValidEvent(event), true)
  assert.equal(
    isValidEventWithJavaScript(finalizeEvent(template('b'.repeat(1024 * 1024)), secretKey)),
    true
  )

  const largeTag = finalizeEventWithJavaScript({
    ...template(''),
    tags: [['x', '\0'.repeat(160 * 1024)]]
  }, secretKey)
  assert.equal(getEventHash(largeTag), getEventHashWithJavaScript(largeTag))
  assert.equal(isValidEvent(largeTag), true)
})

test('hashing falls back locally when TextEncoder.encodeInto is unavailable', () => {
  const original = TextEncoder.prototype.encodeInto
  TextEncoder.prototype.encodeInto = undefined
  try {
    const event = finalizeEventWithJavaScript(template('without encodeInto'), secretKey)
    assert.equal(getEventHash(event), getEventHashWithJavaScript(event))
  } finally {
    TextEncoder.prototype.encodeInto = original
  }
})

test('runtime configuration returns to JavaScript and can enable WASM again', async () => {
  configureEventCrypto({ wasm: false })
  assert.equal(await initializeEventCrypto(), 'javascript')
  const javaScriptEvent = finalizeEvent(template('javascript-first'), secretKey)
  assert.equal(isValidEventWithJavaScript(javaScriptEvent), true)

  configureEventCrypto({ wasm: true })
  assert.equal(await initializeEventCrypto(), 'wasm')
  assert.equal(isValidEvent(finalizeEvent(template('wasm-again'), secretKey)), true)

  assert.throws(
    () => configureEventCrypto({ wasm: 'yes' }),
    error => error instanceof ValidationError && error.code === 'INVALID_EVENT_CRYPTO_OPTIONS'
  )
})

test('missing WebAssembly and backend failures fall back and can be retried', async () => {
  configureEventCrypto({ wasm: false })
  configureEventCrypto({ wasm: true })

  const originalWebAssembly = globalThis.WebAssembly
  try {
    globalThis.WebAssembly = undefined
    assert.equal(await initializeEventCrypto(), 'javascript')
  } finally {
    globalThis.WebAssembly = originalWebAssembly
  }

  assert.equal(await initializeEventCrypto(), 'wasm')
  const backend = getReadyEventCryptoBackend()
  backend.hashEvent = () => { throw Error('simulated WASM heap failure') }

  const event = finalizeEventWithJavaScript(template('fallback-after-trap'), secretKey)
  assert.equal(getEventHash(event), getEventHashWithJavaScript(event))
  assert.equal(await initializeEventCrypto(), 'wasm')
})
