import { bytesToBase16 } from '../../base16/index.js'
import { ValidationError } from '../../error/index.js'
import { createSecp256k1WasmBackend } from './binding.js'

const SELF_TEST_SECRET = new Uint8Array(32).fill(7)
const SELF_TEST_PUBLIC_KEY =
  '989c0b76cb563971fdc9bef31ec06c3560f3249d6ee9e5d83c57625596e05f6f'
const SELF_TEST_HASH = new Uint8Array(32).fill(9)
const SELF_TEST_ENTROPY = new Uint8Array(32).fill(11)

let enabled = true
let state = 'idle'
let backend = null
let pending = null
let generation = 0

async function initialize (forceRetry) {
  if (!enabled) return 'javascript'
  if (backend) return 'wasm'
  if (pending) return pending
  if (state === 'unavailable' && !forceRetry) return 'javascript'

  const currentGeneration = generation
  state = 'initializing'
  pending = (async () => {
    let candidate
    try {
      candidate = await createSecp256k1WasmBackend()
      const publicKey = bytesToBase16(candidate.getPublicKey(SELF_TEST_SECRET))
      const signature = candidate.signHash(
        SELF_TEST_HASH,
        SELF_TEST_SECRET,
        SELF_TEST_ENTROPY
      )
      if (
        publicKey !== SELF_TEST_PUBLIC_KEY ||
        !candidate.verifyHash(signature, SELF_TEST_HASH, candidate.getPublicKey(SELF_TEST_SECRET))
      ) {
        throw Error('secp256k1 WASM self-test failed')
      }
      if (!enabled || currentGeneration !== generation) {
        candidate.dispose()
        return 'javascript'
      }
      backend = candidate
      state = 'ready'
      return 'wasm'
    } catch {
      candidate?.dispose()
      if (enabled && currentGeneration === generation) state = 'unavailable'
      return 'javascript'
    } finally {
      pending = null
    }
  })()
  return pending
}

export function configureEventCrypto (options = {}) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new ValidationError('INVALID_EVENT_CRYPTO_OPTIONS')
  }
  if (options.wasm !== undefined && typeof options.wasm !== 'boolean') {
    throw new ValidationError('INVALID_EVENT_CRYPTO_OPTIONS')
  }
  const nextEnabled = options.wasm !== false
  if (nextEnabled === enabled) return

  enabled = nextEnabled
  generation++
  if (!enabled) {
    backend?.dispose()
    backend = null
    state = 'disabled'
  } else {
    state = 'idle'
  }
}

export function initializeEventCrypto () {
  return initialize(true)
}

export function scheduleEventCryptoInitialization () {
  if (!enabled || backend || pending || state === 'unavailable') return
  initialize(false).catch(() => {})
}

export function getReadyEventCryptoBackend () {
  return backend
}

export function markEventCryptoUnavailable () {
  if (!backend) return
  backend.dispose()
  backend = null
  state = 'unavailable'
}
