import { finalizeEvent } from '../../event/index.js'
import { getPublicKey } from '../../key/index.js'
import * as nip04 from '../../nip04/index.js'
import * as nip44 from '../../nip44/index.js'
import { extract as hkdfExtract, expand as hkdfExpand } from '@noble/hashes/hkdf.js'
import { sha256 } from '@noble/hashes/sha2.js'

import { deriveDoubleDhConversationKey } from '../../double-dh/index.js'
import { sharedXOnlySecret } from '../../ecdh/index.js'
import { bytesToHex, hexToBytes } from '../../base16/index.js'
import * as nip44v3 from '../../nip44-v3/index.js'

const textEncoder = new TextEncoder()
const SECP256K1_N = BigInt('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141')
const SHARED_KEY_SALT = textEncoder.encode('nostr-shared-key-v1')
const secretKeys = new WeakMap()
const contentSignersByOwner = new WeakMap()
const signersByPubkey = {}

function bytesToBigInt (bytes) {
  return BigInt(`0x${Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')}`)
}

function bigIntTo32Bytes (n) {
  return hexToBytes(n.toString(16).padStart(64, '0'))
}

function deriveSecretKeySync (masterKeyBytes, info = '') {
  const infoBytes = typeof info === 'string' ? textEncoder.encode(info) : info
  const prk = hkdfExtract(sha256, masterKeyBytes, SHARED_KEY_SALT)
  const wide = bytesToBigInt(hkdfExpand(sha256, prk, infoBytes, 48))
  return bigIntTo32Bytes((wide % (SECP256K1_N - 1n)) + 1n)
}

function deriveSharedKeySync (mySeckey, theirPubkey, info = '') {
  return deriveSecretKeySync(sharedXOnlySecret(mySeckey, theirPubkey), info)
}

export async function deriveSharedKey (mySeckey, theirPubkey, info = '') {
  return deriveSharedKeySync(mySeckey, theirPubkey, info)
}

class SharedKeySigner {
  constructor (signer, peerPubkey, info = '') {
    this.signer = signer
    this.peerPubkey = peerPubkey
    this.info = info
    this.shared = null
  }

  sharedSigner () {
    this.shared ??= TestSigner.getOrCreate(bytesToHex(deriveSharedKeySync(secretKeys.get(this.signer), this.peerPubkey, this.info)))
    return this.shared
  }

  getPublicKey () { return this.sharedSigner().getPublicKey() }
  signEvent (event) { return this.sharedSigner().signEvent(event) }
  nip04Encrypt (peerPubkey, plaintext) { return this.sharedSigner().nip04Encrypt(peerPubkey, plaintext) }
  nip04Decrypt (peerPubkey, ciphertext) { return this.sharedSigner().nip04Decrypt(peerPubkey, ciphertext) }
  nip44Encrypt (peerPubkey, plaintext) { return this.sharedSigner().nip44Encrypt(peerPubkey, plaintext) }
  nip44Decrypt (peerPubkey, ciphertext) { return this.sharedSigner().nip44Decrypt(peerPubkey, ciphertext) }
  nip44v3Encrypt (peerPubkey, kind, scope, plaintextB64) { return this.sharedSigner().nip44v3Encrypt(peerPubkey, kind, scope, plaintextB64) }
  nip44v3Decrypt (peerPubkey, kind, scope, ciphertext) { return this.sharedSigner().nip44v3Decrypt(peerPubkey, kind, scope, ciphertext) }
  nip44EncryptDoubleDH (...params) { return this.sharedSigner().nip44EncryptDoubleDH(...params) }
  nip44DecryptDoubleDH (...params) { return this.sharedSigner().nip44DecryptDoubleDH(...params) }
  withSharedKey (peerPubkey, info = this.info) { return new SharedKeySigner(this.signer, peerPubkey, info) }
}

export default class TestSigner {
  static getOrCreate (seckey) {
    const pubkey = getPublicKey(hexToBytes(seckey))
    return (signersByPubkey[pubkey] ??= new TestSigner(seckey, pubkey))
  }

  static releaseAll () {
    for (const pubkey of Object.keys(signersByPubkey)) delete signersByPubkey[pubkey]
  }

  static setContentSigners (ownerSigner, contentSigners = []) {
    const signers = new Map()
    for (const signer of contentSigners) signers.set(signer.getPublicKey(), signer)
    if (signers.size) contentSignersByOwner.set(ownerSigner, signers)
    else contentSignersByOwner.delete(ownerSigner)
  }

  constructor (seckey, pubkey) {
    secretKeys.set(this, hexToBytes(seckey))
    this.pubkey = pubkey
    this.conversationKeys = {}
  }

  getPublicKey () {
    return this.pubkey
  }

  signEvent (event) {
    return finalizeEvent(event, secretKeys.get(this))
  }

  nip04Encrypt (peerPubkey, plaintext) {
    return nip04.encrypt(secretKeys.get(this), peerPubkey, plaintext)
  }

  nip04Decrypt (peerPubkey, ciphertext) {
    return nip04.decrypt(secretKeys.get(this), peerPubkey, ciphertext)
  }

  nip44Encrypt (peerPubkey, plaintext) {
    const key = this.conversationKeys[peerPubkey] ??= nip44.getConversationKey(secretKeys.get(this), peerPubkey)
    return nip44.encrypt(plaintext, key)
  }

  nip44Decrypt (peerPubkey, ciphertext) {
    const key = this.conversationKeys[peerPubkey] ??= nip44.getConversationKey(secretKeys.get(this), peerPubkey)
    return nip44.decrypt(ciphertext, key)
  }

  nip44v3Encrypt (peerPubkey, kind, scope, plaintextB64) {
    return nip44v3.nip07Encrypt(secretKeys.get(this), peerPubkey, kind, scope, plaintextB64)
  }

  nip44v3Decrypt (peerPubkey, kind, scope, ciphertext) {
    return nip44v3.nip07Decrypt(secretKeys.get(this), peerPubkey, kind, scope, ciphertext)
  }

  contentKeyMaterial (requestedContentPubkey = '') {
    const contentSigners = contentSignersByOwner.get(this)
    const signer = requestedContentPubkey ? contentSigners?.get(requestedContentPubkey) : null
    if (!signer) return { contentPubkey: requestedContentPubkey || '', contentSecretKey: null }
    return {
      contentPubkey: signer.getPublicKey(),
      contentSecretKey: secretKeys.get(signer)
    }
  }

  latestContentKeyMaterial () {
    const contentSigners = contentSignersByOwner.get(this)
    const signer = contentSigners?.values?.().next().value
    if (!signer) return { contentPubkey: '', contentSecretKey: null }
    return {
      contentPubkey: signer.getPublicKey(),
      contentSecretKey: secretKeys.get(signer)
    }
  }

  async nip44EncryptDoubleDH (peerPubkey, kind, scope = '', plaintextB64, peerContentPubkey = '') {
    const normalizedKind = nip44v3.normalizeKind(kind)
    const { contentPubkey, contentSecretKey } = this.latestContentKeyMaterial()
    const { conversationKey } = deriveDoubleDhConversationKey({
      role: 'sender',
      identitySecretKey: secretKeys.get(this),
      identityPubkey: this.pubkey,
      contentSecretKey,
      contentPubkey,
      peerIdentityPubkey: peerPubkey,
      peerContentPubkey,
      kind: normalizedKind,
      scope
    })
    const ciphertext = conversationKey
      ? nip44v3.encryptWithConversationKeyBytes(conversationKey, normalizedKind, nip44v3.toBytes(scope || ''), nip44v3.b64decode(plaintextB64))
      : nip44v3.nip07Encrypt(secretKeys.get(this), peerPubkey, normalizedKind, scope, plaintextB64)
    return [ciphertext, contentPubkey]
  }

  async nip44DecryptDoubleDH (peerPubkey, kind, scope = '', ciphertext, peerContentPubkey = '', ownContentPubkey = '') {
    const normalizedKind = nip44v3.normalizeKind(kind)
    const { contentPubkey, contentSecretKey } = this.contentKeyMaterial(ownContentPubkey)
    const { conversationKey } = deriveDoubleDhConversationKey({
      role: 'receiver',
      identitySecretKey: secretKeys.get(this),
      identityPubkey: this.pubkey,
      contentSecretKey,
      contentPubkey,
      peerIdentityPubkey: peerPubkey,
      peerContentPubkey,
      kind: normalizedKind,
      scope
    })
    return conversationKey
      ? nip44v3.b64encode(nip44v3.decryptWithConversationKeyBytes(conversationKey, normalizedKind, nip44v3.toBytes(scope || ''), ciphertext))
      : nip44v3.nip07Decrypt(secretKeys.get(this), peerPubkey, normalizedKind, scope, ciphertext)
  }

  withSharedKey (peerPubkey, info) {
    return new SharedKeySigner(this, peerPubkey, info)
  }
}
