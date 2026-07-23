import { sharedXOnlySecret } from '../ecdh/index.js'
import { decodePayload, encodePayload, extractConversationKey } from './helpers.js'

export function getConversationKey (secretKey, pubkey, { salt = 'nip44-v2' } = {}) {
  return extractConversationKey(sharedXOnlySecret(secretKey, pubkey), salt)
}

export function encrypt (plaintext, conversationKey, nonce) {
  return encodePayload(conversationKey, plaintext, nonce)
}

export function decrypt (ciphertext, conversationKey) {
  return decodePayload(conversationKey, ciphertext)
}
