import { base64ToBytes, bytesToBase64 } from '../../../base64/index.js'

// The library transport uses its Base64 signer contract. Conversion stays at
// this adapter; the injected launcher/vault channel always carries raw bytes.
export function messengerSigner (signer) {
  return {
    getPublicKey: () => signer.getPublicKey(),
    getRelays: () => signer.getRelays(),
    signEvent: event => signer.signEvent(event),
    nip44v3Encrypt: (peer, kind, scope, text) => signer.nip44v3.encrypt(peer, kind, scope, base64ToBytes(text).buffer),
    nip44v3Decrypt: async (peer, kind, scope, ciphertext) => bytesToBase64(new Uint8Array(await signer.nip44v3.decrypt(peer, kind, scope, ciphertext)))
  }
}
