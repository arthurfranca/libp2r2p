import { NYM_CARRIER_KIND, ROUTER_KIND } from '../constants/index.js'
import { ValidationError } from '../../error/index.js'

const encoder = new TextEncoder()

export function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

export function eventByteLength (event) {
  return encoder.encode(JSON.stringify(event)).length
}

export function readReceiverTag (event) {
  return event.tags?.find(t => t[0] === 'r')?.[1] || ''
}

export function readSenderTag (event) {
  const senderPubkey = event.tags?.find(t => t[0] === 'f')?.[1]
  if (!senderPubkey) throw new ValidationError('MISSING_SENDER_TAG')
  return senderPubkey
}

export function readImkcTag (event) {
  return event.tags?.find(t => t[0] === 'imkc')?.[1] || ''
}

export function hasImkcTag (event) {
  return event.tags?.some(t => t[0] === 'imkc') || false
}

export function readImkcProof (event) {
  return event.tags?.find(t => t[0] === 'imkc')?.[2] || ''
}

export function readIdTag (event) {
  return event.tags?.find(t => t[0] === 'id')?.[1] || ''
}

export function readChunkTag (event) {
  const tag = event.tags?.find(t => t[0] === 'c')
  const index = Number(tag?.[1])
  const total = Number(tag?.[2])
  if (!Number.isInteger(index) || !Number.isInteger(total) || index < 0 || total < 1) {
    throw new ValidationError('INVALID_CHUNK_TAG')
  }
  return { index, total }
}

export function makeRouterEvent ({ pubkey, senderPubkey, imkcPubkey, imkcProof, receiverPubkey, chunkIndex, chunkTotal, content }) {
  const tags = [['f', senderPubkey]]
  if (imkcPubkey) {
    if (!imkcProof) throw new ValidationError('INVALID_IMKC_PROOF')
    tags.push(['imkc', imkcPubkey, imkcProof])
  }
  tags.push(['c', String(chunkIndex), String(chunkTotal)])
  if (receiverPubkey) tags.push(['r', receiverPubkey])
  return { kind: ROUTER_KIND, pubkey, created_at: nowSeconds(), tags, content }
}

export function makeNymCarrierEvent ({ innerId, chunkIndex, chunkTotal, content, createdAt = nowSeconds() }) {
  if (!innerId) throw new ValidationError('INNER_EVENT_ID_REQUIRED')
  return {
    kind: NYM_CARRIER_KIND,
    created_at: createdAt,
    tags: [['id', innerId], ['c', String(chunkIndex), String(chunkTotal)]],
    content
  }
}
