import assert from 'node:assert/strict'
import { test } from 'node:test'

import { ValidationError } from '../error/index.js'
import {
  assertValidDelayedProcessingResponse,
  assertValidFileUploadResponse,
  assertValidServerConfiguration,
  calculateFileHash,
  checkFileProcessingStatus,
  generateDownloadUrl,
  generateFSPEventTemplate,
  readServerConfig,
  isValidDelayedProcessingResponse,
  isValidFileUploadResponse,
  isValidServerConfiguration,
  uploadFile
} from '../nip96/index.js'

const success = {
  status: 'success',
  message: 'ok',
  nip94_event: {
    content: '',
    tags: [
      ['url', 'https://files.example/hash'],
      ['ox', 'ab'.repeat(32)]
    ]
  }
}

test('NIP-96 validates direct and delegated configuration with one hop', async () => {
  const direct = { api_url: 'https://files.example/api' }
  assert.equal(isValidServerConfiguration(direct), true)
  assert.equal(assertValidServerConfiguration(direct), direct)
  assert.equal(isValidServerConfiguration({ delegated_to_url: 'https://delegate.example/config.json' }), true)
  const conflict = { api_url: 'https://a', delegated_to_url: 'https://b' }
  assert.equal(isValidServerConfiguration(conflict), false)
  assert.throws(() => assertValidServerConfiguration(conflict), error => (
    error instanceof ValidationError && error.code === 'NIP96_SERVER_CONFIGURATION_SOURCE_CONFLICT'
  ))
  const calls = []
  const config = await readServerConfig({
    serverUrl: 'https://relay.example/path',
    fetch: async url => {
      calls.push(String(url))
      const data = calls.length === 1
        ? { delegated_to_url: 'https://delegate.example/config.json' }
        : { api_url: 'https://files.example/api' }
      return new Response(JSON.stringify(data), { status: 200 })
    }
  })
  assert.deepEqual(calls, [
    'https://relay.example/.well-known/nostr/nip96.json',
    'https://delegate.example/config.json'
  ])
  assert.deepEqual(config, { api_url: 'https://files.example/api' })
})

test('NIP-96 keeps network failures outside ValidationError', async () => {
  const networkError = new Error('network unavailable')
  await assert.rejects(readServerConfig({
    serverUrl: 'https://relay.example',
    fetch: async () => { throw networkError }
  }), error => error === networkError && !(error instanceof ValidationError))
})

test('NIP-96 fetch upload preserves falsy fields and reports estimated progress', async () => {
  const progress = []
  let body
  const file = new Blob(['hello'], { type: 'text/plain' })
  const response = await uploadFile({
    file,
    serverApiUrl: 'https://files.example/api',
    optionalFormDataFields: { expiration: '', size: 0, alt: false, omitted: undefined },
    onProgress: event => progress.push(event),
    fetch: async (_url, options) => {
      body = options.body
      return new Response(JSON.stringify(success), { status: 200 })
    }
  })
  assert.deepEqual(response, success)
  assert.equal(body.get('expiration'), '')
  assert.equal(body.get('size'), '0')
  assert.equal(body.get('alt'), 'false')
  assert.equal(body.has('omitted'), false)
  assert.deepEqual(progress.map(({ loaded, total }) => [loaded, total]), [[0, 5], [5, 5]])
})

test('NIP-96 uses XHR upload progress when available', async () => {
  const listeners = new Map()
  const uploadListeners = new Map()
  const xhr = {
    upload: { addEventListener: (name, listener) => uploadListeners.set(name, listener) },
    open () {},
    setRequestHeader () {},
    addEventListener: (name, listener) => listeners.set(name, listener),
    send () {
      uploadListeners.get('progress')({ lengthComputable: true, loaded: 3, total: 5 })
      this.status = 200
      this.responseText = JSON.stringify(success)
      listeners.get('load')()
    },
    abort () { listeners.get('abort')?.() }
  }
  const progress = []
  assert.deepEqual(await uploadFile({
    file: new Blob(['hello']),
    serverApiUrl: 'https://files.example/api',
    onProgress: event => progress.push(event.loaded),
    xhrFactory: () => xhr
  }), success)
  assert.deepEqual(progress, [3])
})

test('NIP-96 validates responses, status and file hashes', async () => {
  assert.equal(isValidFileUploadResponse(success), true)
  assert.equal(assertValidFileUploadResponse(success), success)
  const invalidUpload = { ...success, nip94_event: { tags: [] } }
  assert.equal(isValidFileUploadResponse(invalidUpload), false)
  assert.throws(() => assertValidFileUploadResponse(invalidUpload), error => (
    error instanceof ValidationError && error.code === 'NIP94_URL_TAG_REQUIRED'
  ))
  const processing = { status: 'processing', message: '', percentage: 0 }
  assert.equal(isValidDelayedProcessingResponse(processing), true)
  assert.equal(assertValidDelayedProcessingResponse(processing), processing)
  const invalidProcessing = { ...processing, percentage: 101 }
  assert.equal(isValidDelayedProcessingResponse(invalidProcessing), false)
  assert.throws(() => assertValidDelayedProcessingResponse(invalidProcessing), error => (
    error instanceof ValidationError && error.code === 'INVALID_NIP96_DELAYED_PERCENTAGE'
  ))
  assert.deepEqual(await checkFileProcessingStatus({
    processingUrl: 'https://files.example/status',
    fetch: async () => new Response(JSON.stringify({ status: 'processing', message: '', percentage: 0 }), { status: 200 })
  }), { status: 'processing', message: '', percentage: 0 })
  assert.equal(await calculateFileHash(new Blob(['hello world'])), 'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9')
  assert.equal(generateDownloadUrl({ fileHash: 'abc', serverDownloadUrl: 'https://files.example/' }), 'https://files.example/abc')
  assert.equal(generateFSPEventTemplate({ serverUrls: ['bad', 'https://files.example'], createdAt: 1 }).kind, 10096)
})
