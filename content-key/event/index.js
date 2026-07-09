import { getEventHash, verifyEvent } from 'nostr-tools'

export const CONTENT_KEY_KIND = 18716

const HEX_PUBKEY = /^[0-9a-f]{64}$/i
const HEX_SIG = /^[0-9a-f]{128}$/i

function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

export async function makeContentKeyEventForPubkey ({ userSigner, contentPubkey, createdAt = nowSeconds() }) {
  if (!userSigner?.getPublicKey || !userSigner?.signEvent) throw new Error('USER_SIGNER_REQUIRED')
  if (!HEX_PUBKEY.test(contentPubkey || '')) throw new Error('CONTENT_PUBKEY_REQUIRED')

  return userSigner.signEvent({
    kind: CONTENT_KEY_KIND,
    created_at: createdAt,
    tags: [['cp', contentPubkey]],
    content: ''
  })
}

export async function makeContentKeyEvent ({ userSigner, contentKeySigner, createdAt = nowSeconds() }) {
  if (!contentKeySigner?.getPublicKey) throw new Error('CONTENT_KEY_SIGNER_REQUIRED')
  return makeContentKeyEventForPubkey({
    userSigner,
    contentPubkey: await contentKeySigner.getPublicKey(),
    createdAt
  })
}

export function parseContentKeyEvent (event) {
  if (!event || event.kind !== CONTENT_KEY_KIND || event.content !== '') return null
  if (!HEX_PUBKEY.test(event.pubkey) || !Number.isSafeInteger(event.created_at)) return null
  if (!Array.isArray(event.tags) || event.tags.length !== 1) return null
  if (event.id !== getEventHash(event) || !verifyEvent(event)) return null

  const [name, contentPubkey, ...rest] = event.tags[0] || []
  if (name !== 'cp' || rest.length || !HEX_PUBKEY.test(contentPubkey || '')) return null
  return { iykcPubkey: contentPubkey, iykcProof: makeContentKeyProof(event) }
}

export function makeContentKeyProof (contentKeyEvent) {
  if (!Number.isSafeInteger(contentKeyEvent?.created_at) || !HEX_SIG.test(contentKeyEvent?.sig || '')) return ''
  return `${contentKeyEvent.created_at}:${contentKeyEvent.sig}`
}

export const makeIykcProof = makeContentKeyProof

function parseContentKeyProof (proof) {
  if (typeof proof !== 'string') return null
  const [createdAtString, sig, extra] = proof.split(':')
  if (extra != null || !/^\d+$/.test(createdAtString || '') || !HEX_SIG.test(sig || '')) return null
  // eslint-disable-next-line camelcase
  const created_at = Number(createdAtString)
  if (!Number.isSafeInteger(created_at)) return null
  // eslint-disable-next-line camelcase
  return { created_at, sig }
}

export function verifyContentKeyProof ({ ownerPubkey, contentPubkey, proof }) {
  if (!HEX_PUBKEY.test(ownerPubkey || '') || !HEX_PUBKEY.test(contentPubkey || '')) return false
  const parsed = parseContentKeyProof(proof)
  if (!parsed) return false

  const event = {
    kind: CONTENT_KEY_KIND,
    pubkey: ownerPubkey,
    created_at: parsed.created_at,
    tags: [['cp', contentPubkey]],
    content: '',
    sig: parsed.sig
  }
  event.id = getEventHash(event)
  return verifyEvent(event)
}

export function verifyIykcProof ({ receiverPubkey, iykcPubkey, iykcProof }) {
  return verifyContentKeyProof({
    ownerPubkey: receiverPubkey,
    contentPubkey: iykcPubkey,
    proof: iykcProof
  })
}
