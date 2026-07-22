import { verifyEvent } from '../../event/index.js'
import { validPubkey } from '../helpers/frame.js'
import { Nip46Client } from './client.js'

// A NIP-46 remote signer with the standard Nostr signing commands.
export class BunkerSigner extends Nip46Client {
  #cachedPubkey = null

  constructor (...args) {
    super(...args)
    Object.preventExtensions(this)
  }

  async getPublicKey (options) {
    if (!options?.extension && this.#cachedPubkey) return this.#cachedPubkey
    const pubkey = await this.sendRequest('get_public_key', [], options)
    if (!validPubkey(pubkey)) throw new Error('NIP46_INVALID_PUBLIC_KEY')
    if (!options?.extension) this.#cachedPubkey = pubkey
    return pubkey
  }

  async signEvent (event, options) {
    const response = await this.sendRequest('sign_event', [JSON.stringify(event)], options)
    let signed
    try {
      signed = JSON.parse(response)
    } catch {
      throw new Error('NIP46_INVALID_SIGNED_EVENT')
    }
    if (!verifyEvent(signed)) throw new Error('NIP46_INVALID_SIGNED_EVENT')
    return signed
  }

  nip04Encrypt (pubkey, plaintext, options) {
    return this.sendRequest('nip04_encrypt', [pubkey, plaintext], options)
  }

  nip04Decrypt (pubkey, ciphertext, options) {
    return this.sendRequest('nip04_decrypt', [pubkey, ciphertext], options)
  }

  nip44Encrypt (pubkey, plaintext, options) {
    return this.sendRequest('nip44_encrypt', [pubkey, plaintext], options)
  }

  nip44Decrypt (pubkey, ciphertext, options) {
    return this.sendRequest('nip44_decrypt', [pubkey, ciphertext], options)
  }
}
