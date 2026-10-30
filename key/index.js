import { bytesToBase16 } from '../base16/index.js'
import { finalizeEvent } from '../event/index.js'
import {
  getReadyEventCryptoBackend,
  markEventCryptoUnavailable,
  scheduleEventCryptoInitialization
} from '../helpers/secp256k1-wasm/backend.js'
import {
  decodeSecretKey,
  generateKeypair as generateKeypairWithJavaScript,
  generateSecretKey,
  getPublicKey as getPublicKeyWithJavaScript,
  keypairFromSecretKey,
  keypairFromSeckey as keypairFromSeckeyWithJavaScript,
  npubFromPubkey,
  nsecFromHex,
  parseProfileEvent,
  profileEventTemplate,
  pubkeyFromNpub,
  relayListTags,
  validateSecretKey
} from './helpers/core.js'

export {
  generateSecretKey,
  npubFromPubkey,
  nsecFromHex,
  parseProfileEvent,
  profileEventTemplate,
  pubkeyFromNpub
}

export function getPublicKey (secretKey) {
  validateSecretKey(secretKey)
  scheduleEventCryptoInitialization()
  const backend = getReadyEventCryptoBackend()
  if (!backend) return getPublicKeyWithJavaScript(secretKey)
  try {
    return bytesToBase16(backend.getPublicKey(secretKey))
  } catch {
    markEventCryptoUnavailable()
    return getPublicKeyWithJavaScript(secretKey)
  }
}

export function generateKeypair () {
  scheduleEventCryptoInitialization()
  const backend = getReadyEventCryptoBackend()
  if (!backend) return generateKeypairWithJavaScript()

  try {
    const secretKey = generateSecretKey()
    return keypairFromSecretKey(secretKey, bytesToBase16(backend.getPublicKey(secretKey)))
  } catch {
    markEventCryptoUnavailable()
    return generateKeypairWithJavaScript()
  }
}

export function keypairFromSeckey (raw) {
  scheduleEventCryptoInitialization()
  const backend = getReadyEventCryptoBackend()
  if (!backend) return keypairFromSeckeyWithJavaScript(raw)

  try {
    const secretKey = decodeSecretKey(raw)
    return keypairFromSecretKey(secretKey, bytesToBase16(backend.getPublicKey(secretKey)))
  } catch (error) {
    if (error?.code) throw error
    markEventCryptoUnavailable()
    return keypairFromSeckeyWithJavaScript(raw)
  }
}

export function signProfileEvent ({ secretKey, name = '', picture, profileEvent = null }) {
  return finalizeEvent(profileEventTemplate({ name, picture, profileEvent }), secretKey)
}

export function signRelayListEvent ({ secretKey, writeRelays = [], readRelays = [] }) {
  return finalizeEvent({
    kind: 10002,
    created_at: Math.floor(Date.now() / 1000),
    tags: relayListTags({ writeRelays, readRelays }),
    content: ''
  }, secretKey)
}
