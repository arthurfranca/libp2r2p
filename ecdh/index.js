import { secp256k1 } from '@noble/curves/secp256k1.js'
import { hexToBytes } from '../base16/index.js'

export function sharedXOnlySecret (seckey, pubkey) {
  // Nostr pubkeys are x-only. secp256k1 ECDH expects a compressed point, so
  // use the even-y prefix and drop the returned parity byte.
  return secp256k1.getSharedSecret(seckey, hexToBytes(`02${pubkey}`)).subarray(1, 33)
}
