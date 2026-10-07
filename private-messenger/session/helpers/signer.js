import { base64ToBytes, bytesToBase64 } from '../../../base64/index.js'

// The library transport uses its Base64 signer contract. Conversion stays at
// this adapter; the injected launcher/vault channel always carries raw bytes.
export function messengerSigner (signer) {
  // The adapter belongs to one account/channel generation. Share both the
  // lookup in flight and its immutable result; failed permissions are retryable
  // only when the owning session explicitly asks again.
  let publicKey
  const adapter = {
    getPublicKey: () => (publicKey ??= Promise.resolve().then(() => signer.getPublicKey()).catch(error => { publicKey = null; throw error })),
    getRelays: () => signer.getRelays(),
    signEvent: event => signer.signEvent(event),
    nip44v3Encrypt: (peer, kind, scope, text) => signer.nip44v3.encrypt(peer, kind, scope, base64ToBytes(text).buffer),
    nip44v3Decrypt: async (peer, kind, scope, ciphertext) => bytesToBase64(new Uint8Array(await signer.nip44v3.decrypt(peer, kind, scope, ciphertext)))
  }
  // Double DH stays opt-in by capability: a signer without both methods keeps
  // the identity-only fallback that the private channel selects by itself.
  if (typeof signer?.nip44v3?.encryptDoubleDH === 'function' &&
      typeof signer?.nip44v3?.decryptDoubleDH === 'function') {
    adapter.nip44EncryptDoubleDH = (peer, kind, scope, text, peerContentPubkey = '') =>
      signer.nip44v3.encryptDoubleDH(peer, kind, scope, base64ToBytes(text).buffer, peerContentPubkey)
    adapter.nip44DecryptDoubleDH = async (peer, kind, scope, ciphertext, peerContentPubkey = '', ownContentPubkey = '') =>
      bytesToBase64(new Uint8Array(await signer.nip44v3.decryptDoubleDH(peer, kind, scope, ciphertext, peerContentPubkey, ownContentPubkey)))
  }
  return adapter
}
