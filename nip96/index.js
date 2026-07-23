import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16 } from '../base16/index.js'
import { ValidationError } from '../error/index.js'
import { FILE_SERVER_PREFERENCE } from '../kind/index.js'

const WELL_KNOWN_PATH = '/.well-known/nostr/nip96.json'

function isValidHttpUrl (value) {
  try {
    const url = new URL(value)
    return url.protocol === 'http:' || url.protocol === 'https:'
  } catch {
    return false
  }
}

function combineSignal (signal, timeoutMs) {
  if (signal?.aborted) throw signal.reason ?? new DOMException('This operation was aborted', 'AbortError')
  const controller = new AbortController()
  const onAbort = () => controller.abort(signal.reason)
  signal?.addEventListener('abort', onAbort, { once: true })
  const timeout = Number.isFinite(timeoutMs) && timeoutMs >= 0
    ? setTimeout(() => controller.abort(new DOMException('NIP-96 request timed out', 'TimeoutError')), timeoutMs)
    : null
  return {
    signal: controller.signal,
    cleanup () {
      if (timeout !== null) clearTimeout(timeout)
      signal?.removeEventListener('abort', onAbort)
    }
  }
}

async function fetchJson (url, options, { fetch: fetchImpl = globalThis.fetch, signal, timeoutMs = 10000 } = {}) {
  if (typeof fetchImpl !== 'function') throw new Error('FETCH_IS_NOT_AVAILABLE')
  const combined = combineSignal(signal, timeoutMs)
  try {
    const response = await fetchImpl(url, { ...options, signal: combined.signal })
    if (!response.ok) {
      const error = new Error(`NIP-96 request failed with status ${response.status}`)
      error.status = response.status
      throw error
    }
    try {
      return { response, data: await response.json() }
    } catch (cause) {
      throw new ValidationError('INVALID_NIP96_JSON_RESPONSE', { cause })
    }
  } finally {
    combined.cleanup()
  }
}

function serverConfigurationError (config) {
  if (!config || typeof config !== 'object' || Array.isArray(config)) return 'INVALID_NIP96_SERVER_CONFIGURATION'
  const hasApi = typeof config.api_url === 'string' && config.api_url.length > 0
  const hasDelegation = typeof config.delegated_to_url === 'string' && config.delegated_to_url.length > 0
  if (!hasApi && !hasDelegation) return 'NIP96_SERVER_CONFIGURATION_SOURCE_REQUIRED'
  if (hasApi && hasDelegation) return 'NIP96_SERVER_CONFIGURATION_SOURCE_CONFLICT'
  if (hasApi && !isValidHttpUrl(config.api_url)) return 'INVALID_NIP96_API_URL'
  if (hasDelegation && !isValidHttpUrl(config.delegated_to_url)) return 'INVALID_NIP96_DELEGATION_URL'
  if (config.download_url !== undefined && !isValidHttpUrl(config.download_url)) return 'INVALID_NIP96_DOWNLOAD_URL'
  return null
}

export function isValidServerConfiguration (config) {
  return serverConfigurationError(config) === null
}

export function assertValidServerConfiguration (config) {
  const code = serverConfigurationError(config)
  if (code) throw new ValidationError(code)
  return config
}

export async function readServerConfig ({ serverUrl, fetch, signal, timeoutMs = 10000 }) {
  if (!isValidHttpUrl(serverUrl)) throw new ValidationError('INVALID_SERVER_URL')
  const firstUrl = new URL(WELL_KNOWN_PATH, new URL(serverUrl).origin)
  const first = await fetchJson(firstUrl, { headers: { Accept: 'application/json' } }, { fetch, signal, timeoutMs })
  assertValidServerConfiguration(first.data)
  if (!first.data.delegated_to_url) return first.data

  const delegated = await fetchJson(first.data.delegated_to_url, { headers: { Accept: 'application/json' } }, { fetch, signal, timeoutMs })
  try {
    assertValidServerConfiguration(delegated.data)
  } catch (cause) {
    throw new ValidationError('INVALID_DELEGATED_SERVER_CONFIGURATION', { cause })
  }
  if (delegated.data.delegated_to_url) throw new ValidationError('INVALID_DELEGATED_SERVER_CONFIGURATION')
  return delegated.data
}

function fileUploadResponseError (response) {
  if (!response || typeof response !== 'object' || Array.isArray(response)) return 'INVALID_NIP96_FILE_UPLOAD_RESPONSE'
  if (!['success', 'error', 'processing'].includes(response.status)) return 'INVALID_NIP96_UPLOAD_STATUS'
  if (typeof response.message !== 'string') return 'INVALID_NIP96_UPLOAD_MESSAGE'
  if (response.status === 'processing' && !isValidHttpUrl(response.processing_url)) return 'INVALID_NIP96_PROCESSING_URL'
  if (response.processing_url !== undefined && !isValidHttpUrl(response.processing_url)) return 'INVALID_NIP96_PROCESSING_URL'
  if (response.status === 'success' && !response.nip94_event) return 'NIP96_FILE_METADATA_REQUIRED'
  if (response.nip94_event !== undefined) {
    const { tags } = response.nip94_event
    if (!Array.isArray(tags) || tags.some(tag => !Array.isArray(tag) || tag.length < 2 || tag.some(value => typeof value !== 'string'))) return 'INVALID_NIP94_TAGS'
    if (!tags.some(tag => tag[0] === 'url' && isValidHttpUrl(tag[1]))) return 'NIP94_URL_TAG_REQUIRED'
    if (!tags.some(tag => tag[0] === 'ox' && /^[0-9a-f]{64}$/.test(tag[1]))) return 'NIP94_ORIGINAL_HASH_TAG_REQUIRED'
  }
  return null
}

export function isValidFileUploadResponse (response) {
  return fileUploadResponseError(response) === null
}

export function assertValidFileUploadResponse (response) {
  const code = fileUploadResponseError(response)
  if (code) throw new ValidationError(code)
  return response
}

function uploadError (status) {
  const messages = {
    400: 'Bad request! Some fields are missing or invalid!',
    402: 'Payment required!',
    403: 'Forbidden! Payload tag does not match the requested file!',
    413: 'File too large!'
  }
  const error = new Error(messages[status] ?? 'Unknown error in uploading file!')
  error.status = status
  return error
}

function makeFormData (file, optionalFormDataFields) {
  const formData = new FormData()
  for (const [key, value] of Object.entries(optionalFormDataFields ?? {})) {
    if (value !== undefined) formData.append(key, value)
  }
  formData.append('file', file)
  return formData
}

function emitProgress (onProgress, event) {
  if (typeof onProgress !== 'function') return
  try { onProgress(event) } catch (error) { console.error('NIP-96 progress callback failed:', error) }
}

function uploadWithXhr ({ file, serverApiUrl, nip98AuthorizationHeader, optionalFormDataFields, onProgress, signal, timeoutMs, xhrFactory }) {
  return new Promise((resolve, reject) => {
    const xhr = xhrFactory ? xhrFactory() : new XMLHttpRequest()
    const onAbort = () => xhr.abort()
    const settle = callback => value => {
      signal?.removeEventListener('abort', onAbort)
      callback(value)
    }
    const resolveUpload = settle(resolve)
    const rejectUpload = settle(reject)
    xhr.open('POST', serverApiUrl, true)
    if (nip98AuthorizationHeader) xhr.setRequestHeader('Authorization', nip98AuthorizationHeader)
    if (Number.isFinite(timeoutMs) && timeoutMs >= 0) xhr.timeout = timeoutMs
    xhr.upload.addEventListener('progress', event => emitProgress(onProgress, event))
    xhr.addEventListener('abort', () => rejectUpload(signal?.reason ?? new DOMException('This operation was aborted', 'AbortError')))
    xhr.addEventListener('timeout', () => rejectUpload(new DOMException('NIP-96 request timed out', 'TimeoutError')))
    xhr.addEventListener('error', () => rejectUpload(new Error('NIP-96 upload failed')))
    xhr.addEventListener('load', () => {
      if (xhr.status < 200 || xhr.status >= 300) return rejectUpload(uploadError(xhr.status))
      let data
      try { data = JSON.parse(xhr.responseText) } catch (cause) {
        return rejectUpload(new ValidationError('INVALID_UPLOAD_RESPONSE_JSON', { cause }))
      }
      try {
        resolveUpload(assertValidFileUploadResponse(data))
      } catch (error) {
        rejectUpload(error)
      }
    })
    if (signal?.aborted) return rejectUpload(signal.reason ?? new DOMException('This operation was aborted', 'AbortError'))
    signal?.addEventListener('abort', onAbort, { once: true })
    xhr.send(makeFormData(file, optionalFormDataFields))
  })
}

export async function uploadFile ({
  file,
  serverApiUrl,
  nip98AuthorizationHeader,
  optionalFormDataFields = {},
  onProgress,
  signal,
  timeoutMs = 30000,
  fetch: fetchImpl,
  xhrFactory
}) {
  if (!file || !isValidHttpUrl(serverApiUrl)) throw new ValidationError('INVALID_UPLOAD_ARGUMENTS')
  if (!fetchImpl && (xhrFactory || typeof XMLHttpRequest === 'function') && typeof onProgress === 'function') {
    return uploadWithXhr({ file, serverApiUrl, nip98AuthorizationHeader, optionalFormDataFields, onProgress, signal, timeoutMs, xhrFactory })
  }

  const total = Number.isFinite(file.size) ? file.size : 0
  emitProgress(onProgress, { lengthComputable: Number.isFinite(file.size), loaded: 0, total })
  let result
  try {
    result = await fetchJson(serverApiUrl, {
      method: 'POST',
      headers: nip98AuthorizationHeader ? { Authorization: nip98AuthorizationHeader } : {},
      body: makeFormData(file, optionalFormDataFields)
    }, { fetch: fetchImpl, signal, timeoutMs })
  } catch (error) {
    if (error.status) throw uploadError(error.status)
    throw error
  }
  assertValidFileUploadResponse(result.data)
  emitProgress(onProgress, { lengthComputable: Number.isFinite(file.size), loaded: total, total })
  return result.data
}

export function generateDownloadUrl ({ fileHash, serverDownloadUrl, fileExtension = '' }) {
  if (typeof fileHash !== 'string' || !isValidHttpUrl(serverDownloadUrl) || typeof fileExtension !== 'string') {
    throw new ValidationError('INVALID_DOWNLOAD_ARGUMENTS')
  }
  return `${serverDownloadUrl.replace(/\/$/, '')}/${fileHash}${fileExtension}`
}

export async function deleteFile ({ fileHash, serverApiUrl, nip98AuthorizationHeader, fetch, signal, timeoutMs = 10000 }) {
  const url = generateDownloadUrl({ fileHash, serverDownloadUrl: serverApiUrl })
  const result = await fetchJson(url, {
    method: 'DELETE',
    headers: nip98AuthorizationHeader ? { Authorization: nip98AuthorizationHeader } : {}
  }, { fetch, signal, timeoutMs })
  return result.data
}

function delayedProcessingResponseError (response) {
  if (!response || typeof response !== 'object' || Array.isArray(response)) return 'INVALID_NIP96_DELAYED_RESPONSE'
  if (!['processing', 'error'].includes(response.status)) return 'INVALID_NIP96_DELAYED_STATUS'
  if (typeof response.message !== 'string') return 'INVALID_NIP96_DELAYED_MESSAGE'
  if (typeof response.percentage !== 'number' || !Number.isFinite(response.percentage) ||
      response.percentage < 0 || response.percentage > 100) return 'INVALID_NIP96_DELAYED_PERCENTAGE'
  return null
}

export function isValidDelayedProcessingResponse (response) {
  return delayedProcessingResponseError(response) === null
}

export function assertValidDelayedProcessingResponse (response) {
  const code = delayedProcessingResponseError(response)
  if (code) throw new ValidationError(code)
  return response
}

export async function checkFileProcessingStatus ({ processingUrl, fetch, signal, timeoutMs = 10000 }) {
  if (!isValidHttpUrl(processingUrl)) throw new ValidationError('INVALID_PROCESSING_URL')
  const { response, data } = await fetchJson(processingUrl, {}, { fetch, signal, timeoutMs })
  if (response.status === 201) return assertValidFileUploadResponse(data)
  if (response.status === 200) return assertValidDelayedProcessingResponse(data)
  throw new ValidationError('INVALID_PROCESSING_RESPONSE')
}

export function generateFSPEventTemplate ({ serverUrls, createdAt = Math.floor(Date.now() / 1000) }) {
  if (!Array.isArray(serverUrls)) throw new ValidationError('SERVER_URLS_SHOULD_BE_AN_ARRAY')
  return {
    kind: FILE_SERVER_PREFERENCE,
    content: '',
    tags: serverUrls.filter(isValidHttpUrl).map(serverUrl => ['server', serverUrl]),
    created_at: createdAt
  }
}

export async function calculateFileHash (file) {
  if (!file || typeof file.stream !== 'function') {
    if (!file || typeof file.arrayBuffer !== 'function') throw new ValidationError('FILE_SHOULD_BE_A_BLOB')
    return bytesToBase16(sha256(new Uint8Array(await file.arrayBuffer())))
  }
  const hash = sha256.create()
  const reader = file.stream().getReader()
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      hash.update(value)
    }
    return bytesToBase16(hash.digest())
  } finally {
    reader.releaseLock()
  }
}
