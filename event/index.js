import { base16ToBytes, bytesToBase16 } from '../base16/index.js'
import { ValidationError } from '../error/index.js'
import {
  configureEventCrypto,
  getReadyEventCryptoBackend,
  initializeEventCrypto,
  markEventCryptoUnavailable,
  scheduleEventCryptoInitialization
} from '../helpers/secp256k1-wasm/backend.js'
import {
  copyTags,
  eventValidationError as eventValidationErrorWithJavaScript,
  finalizeEvent as finalizeEventWithJavaScript,
  getEventHash as getEventHashWithJavaScript,
  validateSecretKey
} from './helpers/core.js'
import {
  assertSerializableEvent,
  isSerializableEvent,
  serializableEventError
} from './helpers/serialize.js'

export {
  classifyEvent,
  isAddressableEvent,
  isEphemeralEvent,
  isRegularEvent,
  isReplaceableEvent
} from './helpers/core.js'
export { assertSerializableEvent, isSerializableEvent }
export { configureEventCrypto, initializeEventCrypto }

const HEX_32 = /^[0-9a-f]{64}$/
const HEX_64 = /^[0-9a-f]{128}$/

function useWasmOrFallback (operation, fallback) {
  scheduleEventCryptoInitialization()
  const backend = getReadyEventCryptoBackend()
  if (!backend) return fallback()
  try {
    return operation(backend)
  } catch {
    markEventCryptoUnavailable()
    return fallback()
  }
}

export function getEventHash (event) {
  assertSerializableEvent(event)
  return useWasmOrFallback(
    backend => bytesToBase16(backend.hashEvent(event)),
    () => getEventHashWithJavaScript(event)
  )
}

export function finalizeEvent (template, secretKey) {
  validateSecretKey(secretKey)
  const event = {
    ...template,
    tags: copyTags(template?.tags),
    pubkey: '0'.repeat(64)
  }
  assertSerializableEvent(event)
  return useWasmOrFallback(
    backend => {
      const entropy = crypto.getRandomValues(new Uint8Array(32))
      let id
      const { publicKey, signature } = backend.signEventHash(
        secretKey,
        publicKeyBytes => {
          event.pubkey = bytesToBase16(publicKeyBytes)
          const hash = backend.hashEvent(event)
          id = bytesToBase16(hash)
          return hash
        },
        entropy
      )
      return {
        ...event,
        pubkey: bytesToBase16(publicKey),
        id,
        sig: bytesToBase16(signature)
      }
    },
    () => finalizeEventWithJavaScript(template, secretKey)
  )
}

function eventValidationError (event) {
  const structureError = serializableEventError(event)
  if (structureError) return structureError
  if (typeof event.id !== 'string' || !HEX_32.test(event.id)) return 'INVALID_EVENT_ID'
  if (typeof event.sig !== 'string' || !HEX_64.test(event.sig)) return 'INVALID_EVENT_SIGNATURE'

  scheduleEventCryptoInitialization()
  const backend = getReadyEventCryptoBackend()
  if (!backend) return eventValidationErrorWithJavaScript(event)
  try {
    const id = bytesToBase16(backend.hashEvent(event))
    if (id !== event.id) return 'EVENT_ID_MISMATCH'
    if (!backend.verifyHash(
      base16ToBytes(event.sig),
      base16ToBytes(id),
      base16ToBytes(event.pubkey)
    )) {
      return 'INVALID_EVENT_SIGNATURE'
    }
    return null
  } catch {
    markEventCryptoUnavailable()
    return eventValidationErrorWithJavaScript(event)
  }
}

export function isValidEvent (event) {
  return eventValidationError(event) === null
}

export function assertValidEvent (event) {
  const code = eventValidationError(event)
  if (code) throw new ValidationError(code)
  return event
}
