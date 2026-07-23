import { sha256 } from '@noble/hashes/sha2.js'

import { bytesToBase16 } from '../base16/index.js'
import { bytesToBase64 } from '../base64/index.js'
import { ValidationError } from '../error/index.js'
import { isValidEvent } from '../event/index.js'
import { HTTP_AUTH } from '../kind/index.js'

const encoder = new TextEncoder()
const PAYLOAD_HASH = /^[0-9a-f]{64}$/

async function payloadBytes (payload) {
  if (typeof payload === 'string') return encoder.encode(payload)
  if (payload instanceof Uint8Array) return payload
  if (payload instanceof ArrayBuffer) return new Uint8Array(payload)
  if (ArrayBuffer.isView(payload)) return new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength)
  if (typeof Blob === 'function' && payload instanceof Blob) return new Uint8Array(await payload.arrayBuffer())
  throw new ValidationError('INVALID_PAYLOAD')
}

export async function getToken ({
  loginUrl,
  httpMethod,
  signEvent,
  includeAuthorizationScheme = false,
  payload,
  payloadHash
}) {
  if (typeof loginUrl !== 'string' || loginUrl.length === 0) throw new ValidationError('INVALID_LOGIN_URL')
  try { new URL(loginUrl) } catch (cause) { throw new ValidationError('INVALID_LOGIN_URL', { cause }) }
  if (typeof httpMethod !== 'string' || httpMethod.trim().length === 0) throw new ValidationError('INVALID_HTTP_METHOD')
  if (typeof signEvent !== 'function') throw new ValidationError('SIGN_EVENT_SHOULD_BE_A_FUNCTION')
  if (payload !== undefined && payloadHash !== undefined) throw new ValidationError('PAYLOAD_AND_HASH_ARE_MUTUALLY_EXCLUSIVE')

  let hash = payloadHash
  if (payload !== undefined) hash = bytesToBase16(sha256(await payloadBytes(payload)))
  if (hash !== undefined && !PAYLOAD_HASH.test(hash)) throw new ValidationError('INVALID_PAYLOAD_HASH')

  const method = httpMethod.trim().toUpperCase()
  const tags = [['u', loginUrl], ['method', method]]
  if (hash !== undefined) tags.push(['payload', hash])
  const event = await signEvent({
    kind: HTTP_AUTH,
    created_at: Math.floor(Date.now() / 1000),
    tags,
    content: ''
  })
  if (!isValidEvent(event) || event.kind !== HTTP_AUTH) throw new ValidationError('INVALID_SIGNED_HTTP_AUTH_EVENT')
  if (!event.tags.some(tag => tag[0] === 'u' && tag[1] === loginUrl) ||
      !event.tags.some(tag => tag[0] === 'method' && tag[1] === method) ||
      (hash !== undefined && !event.tags.some(tag => tag[0] === 'payload' && tag[1] === hash))) {
    throw new ValidationError('SIGNED_HTTP_AUTH_EVENT_WAS_CHANGED')
  }
  const token = bytesToBase64(encoder.encode(JSON.stringify(event)))
  return includeAuthorizationScheme ? `Nostr ${token}` : token
}
