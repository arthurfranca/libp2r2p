import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16 } from '../base16/index.js'
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
    return { response, data: await response.json() }
  } finally {
    combined.cleanup()
  }
}

export function validateServerConfiguration (config) {
  if (!config || typeof config !== 'object' || Array.isArray(config)) return false
  const hasApi = typeof config.api_url === 'string' && config.api_url.length > 0
  const hasDelegation = typeof config.delegated_to_url === 'string' && config.delegated_to_url.length > 0
  if (hasApi === hasDelegation) return false
  if (hasApi && !isValidHttpUrl(config.api_url)) return false
  if (hasDelegation && !isValidHttpUrl(config.delegated_to_url)) return false
  if (config.download_url !== undefined && !isValidHttpUrl(config.download_url)) return false
  return true
}

export async function readServerConfig ({ serverUrl, fetch, signal, timeoutMs = 10000 }) {
  if (!isValidHttpUrl(serverUrl)) throw new Error('INVALID_SERVER_URL')
  const firstUrl = new URL(WELL_KNOWN_PATH, new URL(serverUrl).origin)
  const first = await fetchJson(firstUrl, { headers: { Accept: 'application/json' } }, { fetch, signal, timeoutMs })
  if (!validateServerConfiguration(first.data)) throw new Error('INVALID_SERVER_CONFIGURATION')
  if (!first.data.delegated_to_url) return first.data

  const delegated = await fetchJson(first.data.delegated_to_url, { headers: { Accept: 'application/json' } }, { fetch, signal, timeoutMs })
  if (!validateServerConfiguration(delegated.data) || delegated.data.delegated_to_url) {
    throw new Error('INVALID_DELEGATED_SERVER_CONFIGURATION')
  }
  return delegated.data
}

export function validateFileUploadResponse (response) {
  if (!response || typeof response !== 'object' || Array.isArray(response)) return false
  if (!['success', 'error', 'processing'].includes(response.status) || typeof response.message !== 'string') return false
  if (response.status === 'processing' && (!isValidHttpUrl(response.processing_url))) return false
  if (response.processing_url !== undefined && !isValidHttpUrl(response.processing_url)) return false
  if (response.status === 'success' && !response.nip94_event) return false
  if (response.nip94_event !== undefined) {
    const { tags } = response.nip94_event
    if (!Array.isArray(tags) || tags.some(tag => !Array.isArray(tag) || tag.length < 2 || tag.some(value => typeof value !== 'string'))) return false
    if (!tags.some(tag => tag[0] === 'url' && isValidHttpUrl(tag[1]))) return false
    if (!tags.some(tag => tag[0] === 'ox' && /^[0-9a-f]{64}$/.test(tag[1]))) return false
  }
  return true
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
      try { data = JSON.parse(xhr.responseText) } catch { return rejectUpload(new Error('INVALID_UPLOAD_RESPONSE_JSON')) }
      if (!validateFileUploadResponse(data)) return rejectUpload(new Error('INVALID_UPLOAD_RESPONSE'))
      resolveUpload(data)
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
  if (!file || !isValidHttpUrl(serverApiUrl)) throw new Error('INVALID_UPLOAD_ARGUMENTS')
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
  if (!validateFileUploadResponse(result.data)) throw new Error('INVALID_UPLOAD_RESPONSE')
  emitProgress(onProgress, { lengthComputable: Number.isFinite(file.size), loaded: total, total })
  return result.data
}

export function generateDownloadUrl ({ fileHash, serverDownloadUrl, fileExtension = '' }) {
  if (typeof fileHash !== 'string' || !isValidHttpUrl(serverDownloadUrl) || typeof fileExtension !== 'string') {
    throw new Error('INVALID_DOWNLOAD_ARGUMENTS')
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

export function validateDelayedProcessingResponse (response) {
  return !!response && typeof response === 'object' && !Array.isArray(response) &&
    ['processing', 'error'].includes(response.status) &&
    typeof response.message === 'string' &&
    typeof response.percentage === 'number' && Number.isFinite(response.percentage) &&
    response.percentage >= 0 && response.percentage <= 100
}

export async function checkFileProcessingStatus ({ processingUrl, fetch, signal, timeoutMs = 10000 }) {
  if (!isValidHttpUrl(processingUrl)) throw new Error('INVALID_PROCESSING_URL')
  const { response, data } = await fetchJson(processingUrl, {}, { fetch, signal, timeoutMs })
  if (response.status === 201 && validateFileUploadResponse(data)) return data
  if (response.status === 200 && validateDelayedProcessingResponse(data)) return data
  throw new Error('INVALID_PROCESSING_RESPONSE')
}

export function generateFSPEventTemplate ({ serverUrls, createdAt = Math.floor(Date.now() / 1000) }) {
  if (!Array.isArray(serverUrls)) throw new TypeError('SERVER_URLS_SHOULD_BE_AN_ARRAY')
  return {
    kind: FILE_SERVER_PREFERENCE,
    content: '',
    tags: serverUrls.filter(isValidHttpUrl).map(serverUrl => ['server', serverUrl]),
    created_at: createdAt
  }
}

export async function calculateFileHash (file) {
  if (!file || typeof file.stream !== 'function') {
    if (!file || typeof file.arrayBuffer !== 'function') throw new TypeError('FILE_SHOULD_BE_A_BLOB')
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
