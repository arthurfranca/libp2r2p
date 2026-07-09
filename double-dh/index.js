import { extract, expand } from '@noble/hashes/hkdf.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { concatBytes } from '@noble/hashes/utils.js'
import { sharedXOnlySecret } from '../ecdh/index.js'

const textEncoder = new TextEncoder()

// HKDF extract salt naming this generic double-DH key schedule. It has no NUL
// suffix because this fixed label is not concatenated with variable bytes
// (unlike NIP-44 v3's "nip44-v3\x00" || nonce salt).
const DOUBLE_DH_SALT = textEncoder.encode('nostr-double-dh-v1')

/*
Considering the following nomenclature, where a/b are lexicographically
ordered identity pubkeys, not sender/receiver roles:
  II = DH(aIdentitySecret, bIdentityPubkey)
  IC = DH(aIdentitySecret, bContentPubkey)
  CI = DH(aContentSecret,  bIdentityPubkey)
  CC = DH(aContentSecret,  bContentPubkey)
  Double-DH uses a minimal, fixed-order transcript of raw 32-byte DH outputs:
  - identity mode uses no Double-DH transcript; callers fall back to identity NIP-44 v3.
  - one content key uses identity/identity || identity/content, i.e. II || CI
    when a has the content key, or II || IC when b has the content key.
  - two content keys use identity/identity || content/content, i.e. II || CC.
  Review of some DH combinations:
  1) Four DHs (II || IC || CI || CC):
  - useful when two participants have content keys
  - requires both secrets from the same participant (a identity + a content,
    or b identity + b content)
  2) Three DHs (II || CC || IC or II || CC || CI):
  - useful when two participants have content keys
  - requires both secrets from one participant, or the cross-participant pair
    matched by the mixed term (a identity + b content for IC, or
    a content + b identity for CI)
  3) Three DHs (CC || IC || CI):
  - useful when two participants have content keys
  - requires both content secrets, or both secrets from one participant
  - prevents identity-only compromise, but both content keys are enough
  - interesting that anyone can use two content keys and pretend that
  any two participants are talking to each other.
  4) [picked] Two DHs (II || CC):
  - useful when two participants have content keys
  - requires one identity secret and one content secret, from any participant pairing
  - prevents identity-only and content-only compromise; weaker than 1) because
    cross-participant identity+content leaks are enough
  5) [picked] Two DHs (II || IC or II || CI):
  - useful when exactly one participant has a content key
  - requires either the identity-only participant's identity secret, or both
    secrets from the content-key participant
*/

function u32be (n) {
  const b = new Uint8Array(4)
  new DataView(b.buffer).setUint32(0, n >>> 0, false)
  return b
}

function normalizeKind (kind) {
  const n = typeof kind === 'string' && kind.trim() !== '' ? Number(kind) : kind
  if (!Number.isInteger(n) || n < 0 || n > 0xffffffff) throw new Error('INVALID_KIND')
  return n
}

function sharedX (secretKey, pubkey) {
  return sharedXOnlySecret(secretKey, pubkey)
}

function modeFor ({ senderContentPubkey, receiverContentPubkey }) {
  if (senderContentPubkey && receiverContentPubkey) return 'both-content'
  if (senderContentPubkey) return 'sender-content'
  if (receiverContentPubkey) return 'receiver-content'
  return 'identity'
}

function orderedPair ({ identityPubkey, contentPubkey, peerIdentityPubkey, peerContentPubkey }) {
  const self = { identityPubkey, contentPubkey: contentPubkey || '', isSelf: true }
  const peer = { identityPubkey: peerIdentityPubkey, contentPubkey: peerContentPubkey || '', isSelf: false }
  return identityPubkey <= peerIdentityPubkey ? [self, peer] : [peer, self]
}

function hkdfInfo ({ kind, scope = '' }) {
  const scopeBytes = textEncoder.encode(scope || '')
  // By adding NIP-44 v3's public kind/scope context to the Double-DH conversation key
  // derivation, we ensure that a leaked conversation key only decrypts messages with
  // the same context.
  return concatBytes(u32be(normalizeKind(kind)), u32be(scopeBytes.length), scopeBytes)
}

function identityIdentity ({ a, b, identitySecretKey }) {
  return sharedX(identitySecretKey, b.isSelf ? a.identityPubkey : b.identityPubkey)
}

function aContentBIdentity ({ a, b, identitySecretKey, contentSecretKey }) {
  return a.isSelf
    ? sharedX(contentSecretKey, b.identityPubkey)
    : sharedX(identitySecretKey, a.contentPubkey)
}

function aIdentityBContent ({ a, b, identitySecretKey, contentSecretKey }) {
  return a.isSelf
    ? sharedX(identitySecretKey, b.contentPubkey)
    : sharedX(contentSecretKey, a.identityPubkey)
}

function contentContent ({ a, b, contentSecretKey }) {
  return a.isSelf
    ? sharedX(contentSecretKey, b.contentPubkey)
    : sharedX(contentSecretKey, a.contentPubkey)
}

export function deriveDoubleDhConversationKey ({
  role,
  identitySecretKey,
  identityPubkey,
  contentSecretKey,
  contentPubkey = '',
  peerIdentityPubkey,
  peerContentPubkey = '',
  kind,
  scope = ''
}) {
  if (role !== 'sender' && role !== 'receiver') throw new Error('INVALID_DOUBLE_DH_ROLE')
  if (!identitySecretKey || !identityPubkey || !peerIdentityPubkey) throw new Error('DOUBLE_DH_IDENTITY_REQUIRED')

  const isSender = role === 'sender'
  const senderContentPubkey = isSender ? contentPubkey : peerContentPubkey
  const receiverContentPubkey = isSender ? peerContentPubkey : contentPubkey
  const mode = modeFor({ senderContentPubkey, receiverContentPubkey })

  if (mode === 'identity') return { mode, conversationKey: null }
  if (isSender && senderContentPubkey && !contentSecretKey) throw new Error('SENDER_CONTENT_KEY_REQUIRED')
  if (!isSender && receiverContentPubkey && !contentSecretKey) throw new Error('RECEIVER_CONTENT_KEY_REQUIRED')

  const [a, b] = orderedPair({ identityPubkey, contentPubkey, peerIdentityPubkey, peerContentPubkey })
  const steps = []

  if (identityPubkey === peerIdentityPubkey) {
    const ownContentPubkey = contentPubkey || peerContentPubkey
    if (ownContentPubkey && !contentSecretKey) {
      throw new Error(isSender ? 'SENDER_CONTENT_KEY_REQUIRED' : 'RECEIVER_CONTENT_KEY_REQUIRED')
    }
    // In self-encryption, DH(identitySecret, contentPubkey) could be computed
    // with either single secret plus the other public key. The two self-DH
    // steps below are the minimal transcript that requires both secrets.
    steps.push(sharedX(identitySecretKey, identityPubkey))
    if (ownContentPubkey) {
      steps.push(sharedX(contentSecretKey, ownContentPubkey))
    }
  } else {
    steps.push(identityIdentity({ a, b, identitySecretKey }))
    if (a.contentPubkey && b.contentPubkey) {
      steps.push(contentContent({ a, b, contentSecretKey }))
    } else if (a.contentPubkey) {
      steps.push(aContentBIdentity({ a, b, identitySecretKey, contentSecretKey }))
    } else if (b.contentPubkey) {
      steps.push(aIdentityBContent({ a, b, identitySecretKey, contentSecretKey }))
    }
  }

  const ikm = concatBytes(...steps)
  const prk = extract(sha256, ikm, DOUBLE_DH_SALT)
  return {
    mode,
    conversationKey: expand(sha256, prk, hkdfInfo({ kind, scope }), 32)
  }
}
