import { secp256k1 } from '@noble/curves/secp256k1.js'
import { hexToBytes } from '../base16/index.js'
import { ValidationError } from '../error/index.js'

export function sharedXOnlySecret (seckey, pubkey) {
  if (!(seckey instanceof Uint8Array) || seckey.length !== 32 || !secp256k1.utils.isValidSecretKey(seckey)) {
    throw new ValidationError('INVALID_SECRET_KEY')
  }
  if (typeof pubkey !== 'string' || !/^[0-9a-f]{64}$/.test(pubkey)) {
    throw new ValidationError('INVALID_PUBLIC_KEY')
  }
  // Nostr pubkeys are x-only. secp256k1 ECDH expects a compressed point, so
  // use the even-y prefix and drop the returned parity byte.
  try {
    return secp256k1.getSharedSecret(seckey, hexToBytes(`02${pubkey}`)).subarray(1, 33)
  } catch (cause) {
    throw new ValidationError('INVALID_PUBLIC_KEY', { cause })
  }
}
