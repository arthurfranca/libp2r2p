import { generateSecretKey, getPublicKey, finalizeEvent, getEventHash, validateEvent, verifyEvent } from 'nostr-tools'
import { bytesToBase64, base64ToBytes } from '../base64/index.js'
import { hexToBytes } from '../base16/index.js'
import { makeContentKeyEventForPubkey, parseContentKeyEvent, verifyContentKeyProof, verifyIykcProof } from '../content-key/event/index.js'
import { getIykcProofs } from '../content-key/index.js'
import * as nip44v3 from '../nip44-v3/index.js'
import { relayPool } from '../relay/index.js'
import { JSONL_CHUNK_BYTES, NYM_CARRIER_CHUNK_CHARS } from './helpers/chunk-size.js'
import {
  cleanupChunks,
  cleanupEnvelopeRows,
  decodeChunkLines,
  prepareEnvelopeRows,
  preparedRowIndexesForReceivers,
  readChunkContent,
  receiverPubkeys,
  receiverPubkeysWithoutContentKeys,
  writeChunksFromPreparedRows
} from './helpers/chunks.js'
import { EXPIRATION_SECONDS, MAX_EVENT_BYTES, NYM_CARRIER_KIND, PRIVATE_BROADCAST_KIND, ROUTER_KIND } from './constants/index.js'
import { eventByteLength, hasImkcTag, makeNymCarrierEvent, makeRouterEvent, nowSeconds, readChunkTag, readIdTag, readImkcProof, readImkcTag, readReceiverTag, readSenderTag } from './helpers/event.js'
import { createReceivedChunkStore, DEFAULT_RECEIVED_CHUNK_MAX_BYTES, DEFAULT_RECEIVED_CHUNK_TTL_MS } from './services/received-chunks.js'

export { EXPIRATION_SECONDS, MAX_EVENT_BYTES, NYM_CARRIER_KIND, PRIVATE_BROADCAST_KIND, ROUTER_KIND } from './constants/index.js'

const DEFAULT_IGNORED_GROUP_TTL_MS = 30 * 60 * 1000
const DEFAULT_IGNORED_GROUP_MAX_ENTRIES = 5000
const HEX_SECKEY = /^[0-9a-f]{64}$/i
const HEX_PUBKEY = /^[0-9a-f]{64}$/i
const encoder = new TextEncoder()
const decoder = new TextDecoder()
const NIP44_V3_SCOPE = ''

const sendToRelays = (...args) => relayPool.sendEvent(...args)
const getEvents = (...args) => relayPool.getEvents(...args)
const getLiveEventsGenerator = (...args) => relayPool.getLiveEventsGenerator(...args)
const getEventsFeedGenerator = (...args) => relayPool.getEventsFeedGenerator(...args)

function uniq (values) {
  return [...new Set((values || []).filter(Boolean))]
}

function normalizeDeletionPubkey (deletionPubkey) {
  if (deletionPubkey === undefined) return undefined
  if (typeof deletionPubkey !== 'string' || !HEX_PUBKEY.test(deletionPubkey)) {
    throw new Error('INVALID_DELETION_PUBKEY')
  }
  return deletionPubkey.toLowerCase()
}

function privateBroadcastTags ({ deletionPubkey, createdAt, expirationSeconds }) {
  return [
    ...(deletionPubkey ? [['s', deletionPubkey]] : []),
    ['expiration', String(createdAt + expirationSeconds)]
  ]
}

export function getJsonlChunkByteSize () {
  return JSONL_CHUNK_BYTES
}

export function getNymCarrierChunkSize () {
  return NYM_CARRIER_CHUNK_CHARS
}

function textToBase64 (text) {
  return bytesToBase64(encoder.encode(text))
}

function base64ToText (b64) {
  return decoder.decode(base64ToBytes(b64))
}

async function nip44v3EncryptText (signer, peerPubkey, kind, plaintext) {
  return signer.nip44v3Encrypt(peerPubkey, kind, NIP44_V3_SCOPE, textToBase64(plaintext))
}

async function nip44v3DecryptText (signer, peerPubkey, kind, ciphertext) {
  return base64ToText(await signer.nip44v3Decrypt(peerPubkey, kind, NIP44_V3_SCOPE, ciphertext))
}

function storesRecoverySeeds (mode) {
  return mode === 'seeder' || mode === 'watchtower'
}

async function makeImkcProof ({ senderSigner, senderPubkey, imkcPubkey }) {
  const event = await makeContentKeyEventForPubkey({ userSigner: senderSigner, contentPubkey: imkcPubkey })
  const parsed = parseContentKeyEvent(event)
  if (
    event.pubkey !== senderPubkey ||
    parsed?.iykcPubkey !== imkcPubkey ||
    !verifyContentKeyProof({ ownerPubkey: senderPubkey, contentPubkey: imkcPubkey, proof: parsed?.iykcProof })
  ) throw new Error('INVALID_IMKC_PROOF')
  return parsed.iykcProof
}

async function prepareRoutedMessage ({ senderSigner, imkcSigner, privateChannelSigner = senderSigner, privateChannelReaderPubkey, receivers, event, temporaryStorageArea, _getIykcProofs = getIykcProofs }) {
  if (!senderSigner?.getPublicKey) throw new Error('SENDER_SIGNER_REQUIRED')
  if (!senderSigner?.nip44EncryptDoubleDH && !senderSigner?.nip44v3Encrypt) throw new Error('SIGNER_NIP44V3_ENCRYPT_UNSUPPORTED')
  if (!privateChannelSigner?.getPublicKey || !privateChannelSigner?.nip44v3Encrypt || !privateChannelSigner?.signEvent) throw new Error('PRIVATE_CHANNEL_SIGNER_REQUIRED')
  if (!Array.isArray(receivers) || !receivers.length) throw new Error('NO_RECEIVERS')

  const senderPubkey = await senderSigner.getPublicKey()
  const useDoubleDh = typeof senderSigner.nip44EncryptDoubleDH === 'function'
  const channelPubkey = await privateChannelSigner.getPublicKey()
  const channelReaderPubkey = privateChannelReaderPubkey || channelPubkey
  const receiverContentKeys = useDoubleDh ? await _getIykcProofs(receiverPubkeysWithoutContentKeys(receivers)) : {}
  const preparedRows = await prepareEnvelopeRows({
    senderSigner,
    imkcSigner: useDoubleDh ? imkcSigner : null,
    receivers,
    receiverContentKeys,
    event,
    rowScope: channelPubkey,
    temporaryStorageArea
  })
  const imkcPubkey = preparedRows.ownContentPubkey || ''
  const imkcProof = imkcPubkey ? await makeImkcProof({ senderSigner, senderPubkey, imkcPubkey }) : ''
  return {
    senderPubkey,
    channelPubkey,
    channelReaderPubkey,
    preparedRows,
    imkcPubkey,
    imkcProof
  }
}

async function * wrapPreparedEvents ({ privateChannelSigner, receivers, receiverTag, deletionPubkey, expirationSeconds = EXPIRATION_SECONDS, context }) {
  const routerSeckey = generateSecretKey()
  const routerPubkey = getPublicKey(routerSeckey)
  const receiverPubkeyList = receiverPubkeys(receivers)
  const routerReceiverTag = receiverTag ?? (receiverPubkeyList.length === 1 ? receiverPubkeyList[0] : '')
  const rowIndexes = preparedRowIndexesForReceivers(context.preparedRows, receivers)
  const {
    id,
    total
  } = writeChunksFromPreparedRows(context.preparedRows, rowIndexes)
  const temporaryStorage = context.preparedRows.temporaryStorage

  try {
    for (let index = 0; index < total; index++) {
      const content = readChunkContent(id, index, temporaryStorage)
      const router = finalizeEvent(makeRouterEvent({
        pubkey: routerPubkey,
        senderPubkey: context.senderPubkey,
        imkcPubkey: context.imkcPubkey,
        imkcProof: context.imkcProof,
        receiverPubkey: routerReceiverTag,
        chunkIndex: index,
        chunkTotal: total,
        content
      }), routerSeckey)
      const createdAt = nowSeconds()
      const outer = await privateChannelSigner.signEvent({
        kind: PRIVATE_BROADCAST_KIND,
        created_at: createdAt,
        tags: privateBroadcastTags({ deletionPubkey, createdAt, expirationSeconds }),
        content: await nip44v3EncryptText(privateChannelSigner, context.channelReaderPubkey, PRIVATE_BROADCAST_KIND, JSON.stringify(router))
      })
      if (eventByteLength(outer) > MAX_EVENT_BYTES) throw new Error('EVENT_TOO_LARGE')
      yield outer
    }
  } finally {
    cleanupChunks(id, total, temporaryStorage)
  }
}

// Streaming version of wrapEvent
export async function * wrapEvents ({ senderSigner, imkcSigner, privateChannelSigner = senderSigner, privateChannelReaderPubkey, receivers, receiverTag, deletionPubkey, event, expirationSeconds = EXPIRATION_SECONDS, temporaryStorageArea, _getIykcProofs = getIykcProofs }) {
  const normalizedDeletionPubkey = normalizeDeletionPubkey(deletionPubkey)
  const context = await prepareRoutedMessage({
    senderSigner,
    imkcSigner,
    privateChannelSigner,
    privateChannelReaderPubkey,
    receivers,
    event,
    temporaryStorageArea,
    _getIykcProofs
  })
  try {
    yield * wrapPreparedEvents({ privateChannelSigner, receivers, receiverTag, deletionPubkey: normalizedDeletionPubkey, expirationSeconds, context })
  } finally {
    cleanupEnvelopeRows(context.preparedRows)
  }
}

export async function wrapEvent (options) {
  const events = []
  for await (const event of wrapEvents(options)) events.push(event)
  return events
}

export async function * wrapNymEvents ({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey, event, expirationSeconds = EXPIRATION_SECONDS }) {
  if (!nymSigner?.getPublicKey || !nymSigner?.signEvent) throw new Error('NYM_SIGNER_REQUIRED')
  if (!privateChannelSigner?.getPublicKey || !privateChannelSigner?.nip44v3Encrypt || !privateChannelSigner?.signEvent) throw new Error('PRIVATE_CHANNEL_SIGNER_REQUIRED')
  const normalizedDeletionPubkey = normalizeDeletionPubkey(deletionPubkey)

  const nymPubkey = await nymSigner.getPublicKey()
  const channelPubkey = await privateChannelSigner.getPublicKey()
  const channelReaderPubkey = privateChannelReaderPubkey || channelPubkey
  const wireEvent = isSignedEvent(event)
    ? assertValidSignedInnerEvent(event)
    : wireNymRumor({ ...event, created_at: event?.created_at !== undefined ? event.created_at : nowSeconds() })
  const innerEvent = isSignedEvent(wireEvent) ? wireEvent : normalizeNymRumor(wireEvent, nymPubkey)
  const encoded = bytesToBase64(encoder.encode(JSON.stringify(wireEvent)))
  const total = Math.max(1, Math.ceil(encoded.length / NYM_CARRIER_CHUNK_CHARS))
  const carrierCreatedAt = nowSeconds()

  for (let index = 0; index < total; index++) {
    const carrier = assertValidNymCarrierEvent(await nymSigner.signEvent(makeNymCarrierEvent({
      innerId: innerEvent.id,
      chunkIndex: index,
      chunkTotal: total,
      content: encoded.slice(index * NYM_CARRIER_CHUNK_CHARS, (index + 1) * NYM_CARRIER_CHUNK_CHARS),
      createdAt: carrierCreatedAt
    })))
    const createdAt = nowSeconds()
    const outer = await privateChannelSigner.signEvent({
      kind: PRIVATE_BROADCAST_KIND,
      created_at: createdAt,
      tags: privateBroadcastTags({ deletionPubkey: normalizedDeletionPubkey, createdAt, expirationSeconds }),
      content: await nip44v3EncryptText(privateChannelSigner, channelReaderPubkey, PRIVATE_BROADCAST_KIND, JSON.stringify(carrier))
    })
    if (eventByteLength(outer) > MAX_EVENT_BYTES) throw new Error('EVENT_TOO_LARGE')
    yield outer
  }
}

export async function wrapNymEvent (options) {
  const events = []
  for await (const event of wrapNymEvents(options)) events.push(event)
  return events
}

function joinedRouter (router, content = '') {
  return {
    ...router,
    content,
    tags: router.tags.filter(t => t[0] !== 'c').concat([['c', '0', '1']])
  }
}

function parsePayloadEnvelope (line, index = 0) {
  const record = JSON.parse(line)
  if (!Array.isArray(record) || record.length !== 1 || typeof record[0] !== 'string') throw new Error('INVALID_PAYLOAD_ENVELOPE')
  return { index, type: 'payload', ciphertext: record[0] }
}

function parseRecipientEnvelope (line, index = 0) {
  const record = JSON.parse(line)
  if (!Array.isArray(record) || (record.length !== 2 && record.length !== 4)) throw new Error('INVALID_RECIPIENT_ENVELOPE')
  const [receiverPubkey, ciphertext, iykcPubkey = '', iykcProof = ''] = record
  return { index, receiverPubkey, ciphertext, iykcPubkey, iykcProof }
}

function isSignedEvent (event) {
  return Object.prototype.hasOwnProperty.call(event || {}, 'sig')
}

function assertValidSignedInnerEvent (event) {
  if (!validateEvent(event) || event.id !== getEventHash(event) || !verifyEvent(event)) {
    throw new Error('INVALID_SIGNED_INNER_EVENT')
  }
  return event
}

function normalizeNymRumor (event, pubkey) {
  const normalized = { ...event, pubkey }
  if (!validateEvent(normalized)) throw new Error('INVALID_NYM_RUMOR')
  return { ...normalized, id: getEventHash(normalized) }
}

function wireNymRumor (event = {}) {
  return {
    kind: event.kind,
    tags: event.tags,
    content: event.content,
    created_at: event.created_at
  }
}

function assertValidNymCarrierEvent (carrier) {
  if (!validateEvent(carrier) || carrier.id !== getEventHash(carrier) || !verifyEvent(carrier)) {
    throw new Error('INVALID_NYM_CARRIER')
  }
  if (carrier.kind !== NYM_CARRIER_KIND) throw new Error('INVALID_NYM_CARRIER_KIND')
  if (!readIdTag(carrier)) throw new Error('MISSING_NYM_CARRIER_ID')
  readChunkTag(carrier)
  return carrier
}

function nymCarrierGroupId (carrier) {
  return `nym:${carrier.pubkey}:${readIdTag(carrier)}:${readChunkTag(carrier).total}`
}

function validateNymCarriers (carriers) {
  if (!Array.isArray(carriers) || !carriers.length) throw new Error('NYM_CARRIERS_REQUIRED')
  const chunks = []
  let nymPubkey = ''
  let innerId = ''
  let total = 0

  for (const carrier of carriers) {
    assertValidNymCarrierEvent(carrier)
    const nextInnerId = readIdTag(carrier)
    const { index, total: nextTotal } = readChunkTag(carrier)
    if (!nymPubkey) {
      nymPubkey = carrier.pubkey
      innerId = nextInnerId
      total = nextTotal
    }
    if (carrier.pubkey !== nymPubkey || nextInnerId !== innerId || nextTotal !== total) {
      throw new Error('MISMATCHED_NYM_CARRIER_CHUNKS')
    }
    if (chunks[index] !== undefined) throw new Error('DUPLICATE_NYM_CARRIER_CHUNK')
    chunks[index] = carrier.content
  }

  if (chunks.length !== total) throw new Error('MISSING_NYM_CARRIER_CHUNK')
  for (let index = 0; index < total; index++) {
    if (chunks[index] == null) throw new Error('MISSING_NYM_CARRIER_CHUNK')
  }
  return { nymPubkey, innerId, content: chunks.join('') }
}

export function eventFromNymCarriers (carriers) {
  const { nymPubkey, innerId, content } = validateNymCarriers(carriers)
  let parsed
  try {
    parsed = JSON.parse(decoder.decode(base64ToBytes(content)))
  } catch {
    throw new Error('INVALID_NYM_CARRIER_PAYLOAD')
  }

  if (isSignedEvent(parsed)) {
    const event = assertValidSignedInnerEvent(parsed)
    if (event.id !== innerId) throw new Error('INVALID_NYM_CARRIER_INNER_ID')
    return event
  }

  const event = normalizeNymRumor(parsed, nymPubkey)
  if (event.id !== innerId) throw new Error('INVALID_NYM_CARRIER_INNER_ID')
  return event
}

function assertValidEnvelopeIykcProof (envelope) {
  if (!envelope.iykcPubkey) return
  if (!verifyIykcProof({
    receiverPubkey: envelope.receiverPubkey,
    iykcPubkey: envelope.iykcPubkey,
    iykcProof: envelope.iykcProof
  })) throw new Error('INVALID_IYKC_PROOF')
}

function assertValidRouterImkcProof ({ router, senderPubkey, imkcPubkey, imkcProof }) {
  if (!hasImkcTag(router)) return
  if (!verifyContentKeyProof({
    ownerPubkey: senderPubkey,
    contentPubkey: imkcPubkey,
    proof: imkcProof
  })) throw new Error('INVALID_IMKC_PROOF')
}

function eventFromPayload ({ payloadCiphertext, messageSeckey, senderPubkey }) {
  if (!HEX_SECKEY.test(messageSeckey || '')) throw new Error('INVALID_MESSAGE_SECKEY')
  const messageSecretKey = hexToBytes(messageSeckey)
  const messagePubkey = getPublicKey(messageSecretKey)
  const decrypted = JSON.parse(nip44v3.decrypt(messageSecretKey, messagePubkey, ROUTER_KIND, NIP44_V3_SCOPE, payloadCiphertext))
  if (isSignedEvent(decrypted)) return assertValidSignedInnerEvent(decrypted)
  const normalized = { ...decrypted, pubkey: senderPubkey }
  return { ...normalized, id: getEventHash(normalized) }
}

async function unwrapRecipientEnvelope ({ payloadCiphertext, envelope, receiverSigner, receiverPubkey, senderPubkey, imkcPubkey, rowScope = '' }) {
  if (receiverPubkey && envelope.receiverPubkey !== receiverPubkey) return null
  let messageSeckey
  if (envelope.iykcPubkey || imkcPubkey) {
    if (!receiverSigner?.nip44DecryptDoubleDH) throw new Error('RECEIVER_DOUBLE_DH_UNSUPPORTED')
    if (envelope.iykcPubkey) {
      assertValidEnvelopeIykcProof(envelope)
    }
    messageSeckey = base64ToText(await receiverSigner.nip44DecryptDoubleDH(
      senderPubkey,
      ROUTER_KIND,
      rowScope,
      envelope.ciphertext,
      imkcPubkey,
      envelope.iykcPubkey || ''
    ))
  } else {
    if (!receiverSigner?.nip44v3Decrypt) throw new Error('RECEIVER_SIGNER_NIP44V3_DECRYPT_UNSUPPORTED')
    messageSeckey = base64ToText(await receiverSigner.nip44v3Decrypt(senderPubkey, ROUTER_KIND, rowScope, envelope.ciphertext))
  }
  return eventFromPayload({ payloadCiphertext, messageSeckey, senderPubkey })
}

export async function unwrapEvent ({ receiverSigner, privateChannelSigner = receiverSigner, privateChannelReaderSigner = privateChannelSigner, privateChannelReaderPubkey, event, receiverPubkey }) {
  if (!event || event.kind !== PRIVATE_BROADCAST_KIND) return null
  if (!receiverSigner?.nip44DecryptDoubleDH && !receiverSigner?.nip44v3Decrypt) throw new Error('RECEIVER_SIGNER_NIP44V3_DECRYPT_UNSUPPORTED')
  const channelReaderSigner = privateChannelReaderSigner || privateChannelSigner
  if (!channelReaderSigner?.nip44v3Decrypt) throw new Error('PRIVATE_CHANNEL_READER_REQUIRED')

  const channelPubkey = event.pubkey || await privateChannelSigner?.getPublicKey?.()
  if (!channelPubkey) throw new Error('PRIVATE_CHANNEL_PUBKEY_REQUIRED')
  const router = await decryptRouter({
    content: event.content,
    channelPubkey,
    channelSigner: privateChannelSigner,
    channelReaderSigner,
    channelReaderPubkey: privateChannelReaderPubkey
  })
  if (router.kind !== ROUTER_KIND) throw new Error('INVALID_ROUTER_KIND')
  if (receiverPubkey && readReceiverTag(router) && readReceiverTag(router) !== receiverPubkey) return null

  const senderPubkey = readSenderTag(router)
  const imkcPubkey = readImkcTag(router)
  assertValidRouterImkcProof({ router, senderPubkey, imkcPubkey, imkcProof: readImkcProof(router) })
  const lines = decodeChunkLines(router.content)
  if (!lines.length) throw new Error('MISSING_PAYLOAD_ENVELOPE')
  const payload = parsePayloadEnvelope(lines[0], 0)
  for (let index = 1; index < lines.length; index++) {
    const event = await unwrapRecipientEnvelope({
      payloadCiphertext: payload.ciphertext,
      envelope: parseRecipientEnvelope(lines[index], index),
      receiverSigner,
      receiverPubkey,
      senderPubkey,
      imkcPubkey,
      rowScope: channelPubkey
    })
    if (event) return event
  }
  return null
}

function receiverPubkeyFor (receiver) {
  return receiverPubkeys([receiver])[0] || ''
}

function relayReceiverEntries (relayToReceivers) {
  if (!relayToReceivers) return []
  if (relayToReceivers instanceof Map) return [...relayToReceivers.entries()]
  if (typeof relayToReceivers === 'object') return Object.entries(relayToReceivers)
  throw new Error('INVALID_RELAY_RECEIVERS')
}

function groupedRelayReceivers ({ relayToReceivers, receivers }) {
  const entries = relayReceiverEntries(relayToReceivers)
  if (!entries.length) return null

  const receiverByPubkey = new Map()
  const orderedPubkeys = []
  for (const receiver of receivers || []) {
    const pubkey = receiverPubkeyFor(receiver)
    if (!pubkey || receiverByPubkey.has(pubkey)) continue
    receiverByPubkey.set(pubkey, receiver)
    orderedPubkeys.push(pubkey)
  }

  const wanted = new Set(orderedPubkeys)
  const covered = new Set()
  const groupsByKey = new Map()
  for (const [relay, value] of entries) {
    if (!relay) continue
    const pubkeys = uniq(Array.isArray(value) ? value : [value])
    if (!pubkeys.length) continue
    for (const pubkey of pubkeys) {
      if (!wanted.has(pubkey)) throw new Error('RELAY_RECEIVER_NOT_REQUESTED')
      covered.add(pubkey)
    }
    const key = [...pubkeys].sort().join(',')
    if (!groupsByKey.has(key)) {
      const set = new Set(pubkeys)
      groupsByKey.set(key, {
        pubkeys: set,
        relays: []
      })
    }
    groupsByKey.get(key).relays.push(relay)
  }

  if (!groupsByKey.size) throw new Error('NO_RELAYS')
  for (const pubkey of orderedPubkeys) {
    if (!covered.has(pubkey)) throw new Error('RELAY_RECEIVER_MISSING')
  }

  return [...groupsByKey.values()].map(group => ({
    relays: uniq(group.relays),
    receivers: orderedPubkeys
      .filter(pubkey => group.pubkeys.has(pubkey))
      .map(pubkey => receiverByPubkey.get(pubkey))
  }))
}

function relaysFromRelayReceivers (relayToReceivers) {
  return uniq(relayReceiverEntries(relayToReceivers).map(([relay]) => relay))
}

function withRecoveryRelays (relays, recoveryRelays) {
  return uniq([...(relays || []), ...(recoveryRelays || [])])
}

export async function publish ({ senderSigner, imkcSigner, privateChannelSigner = senderSigner, privateChannelReaderPubkey, receivers, receiverTag, deletionPubkey, event, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs = getIykcProofs, _publish = sendToRelays }) {
  const normalizedDeletionPubkey = normalizeDeletionPubkey(deletionPubkey)
  const results = []
  // Relays are grouped only when they have the exact same recipient pubkey set
  const groups = groupedRelayReceivers({ relayToReceivers, receivers })
  if (groups) {
    const context = await prepareRoutedMessage({
      senderSigner,
      imkcSigner,
      privateChannelSigner,
      privateChannelReaderPubkey,
      receivers,
      event,
      temporaryStorageArea,
      _getIykcProofs
    })
    try {
      for (const group of groups) {
        for await (const wrappedEvent of wrapPreparedEvents({ privateChannelSigner, receivers: group.receivers, receiverTag, deletionPubkey: normalizedDeletionPubkey, expirationSeconds, context })) {
          results.push(await _publish(wrappedEvent, withRecoveryRelays(group.relays, recoveryRelays)))
        }
      }
    } finally {
      cleanupEnvelopeRows(context.preparedRows)
    }
    return results
  }

  for await (const wrappedEvent of wrapEvents({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers, receiverTag, deletionPubkey: normalizedDeletionPubkey, event, expirationSeconds, temporaryStorageArea, _getIykcProofs })) {
    results.push(await _publish(wrappedEvent, withRecoveryRelays(relays, recoveryRelays)))
  }
  return results
}

export async function publishNymEvent ({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey, event, relays, relayToReceivers, recoveryRelays, expirationSeconds, _publish = sendToRelays }) {
  const normalizedDeletionPubkey = normalizeDeletionPubkey(deletionPubkey)
  const results = []
  const publishRelays = withRecoveryRelays(relayToReceivers ? relaysFromRelayReceivers(relayToReceivers) : relays, recoveryRelays)
  for await (const wrappedEvent of wrapNymEvents({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey: normalizedDeletionPubkey, event, expirationSeconds })) {
    results.push(await _publish(wrappedEvent, publishRelays))
  }
  return results
}

function readSignerFromMap (signersByPubkey, pubkey) {
  if (!signersByPubkey || !pubkey) return null
  if (signersByPubkey instanceof Map) return signersByPubkey.get(pubkey) || null
  return signersByPubkey[pubkey] || null
}

async function decryptRouter ({ content, channelPubkey, channelSigner, channelReaderSigner, channelReaderPubkey }) {
  const signer = channelReaderSigner || channelSigner
  if (!signer?.nip44v3Decrypt) throw new Error('PRIVATE_CHANNEL_READER_REQUIRED')

  const readerPubkey = channelReaderPubkey || channelPubkey
  const signerPubkey = await signer.getPublicKey?.()
  // Writer-side reads use writer secret + reader pubkey; reader-side reads use reader secret + channel pubkey.
  const isWriterSide = readerPubkey !== channelPubkey && (signer === channelSigner || signerPubkey === channelPubkey)
  const peerPubkey = isWriterSide ? readerPubkey : channelPubkey
  return JSON.parse(await nip44v3DecryptText(signer, peerPubkey, PRIVATE_BROADCAST_KIND, content))
}

function readValueFromMap (map, key) {
  if (!map || !key) return null
  if (map instanceof Map) return map.get(key) || null
  return map[key] || null
}

function privateChannelPubkeyList ({ privateChannelPubkey, privateChannelPubkeys }) {
  return [...new Set([
    ...(privateChannelPubkeys || []),
    ...(privateChannelPubkey ? [privateChannelPubkey] : [])
  ].filter(Boolean))]
}

function createTtlSet ({ ttlMs, maxEntries }) {
  const entries = new Map()

  function prune (now = Date.now()) {
    if (Number.isFinite(ttlMs)) {
      for (const [key, expiresAt] of entries) {
        if (expiresAt > now) break
        entries.delete(key)
      }
    }
    if (Number.isFinite(maxEntries)) {
      while (entries.size > maxEntries) entries.delete(entries.keys().next().value)
    }
  }

  return {
    add (key) {
      if (!key || maxEntries <= 0 || ttlMs <= 0) return
      const now = Date.now()
      const expiresAt = Number.isFinite(ttlMs) ? now + ttlMs : Infinity
      if (entries.has(key)) entries.delete(key)
      entries.set(key, expiresAt)
      prune(now)
    },
    has (key) {
      prune()
      return entries.has(key)
    }
  }
}

function contentKeyUsageBase ({ outer, router, channelPubkey, receiverPubkeys = [] }) {
  const senderPubkey = readSenderTag(router)
  const routerReceiverPubkey = readReceiverTag(router)
  return {
    outer,
    router,
    channelPubkey,
    senderPubkey,
    routerReceiverPubkey,
    receiverPubkeys,
    isBroadcast: !routerReceiverPubkey
  }
}

function emitSentContentKeyUsage ({ outer, router, channelPubkey, receiverPubkey, receiverPubkeys, onContentKeyUsage }) {
  if (!receiverPubkey || !onContentKeyUsage) return

  const base = contentKeyUsageBase({ outer, router, channelPubkey, receiverPubkeys })
  if (base.senderPubkey !== receiverPubkey) return
  onContentKeyUsage({
    ...base,
    direction: 'sent',
    keyRole: 'sender',
    receiverPubkey: base.routerReceiverPubkey || '',
    contentKeyPubkey: readImkcTag(router),
    contentKeyProof: readImkcProof(router)
  })
}

function emitReceivedContentKeyUsage ({ outer, router, channelPubkey, receiverPubkey, receiverPubkeys, envelope, onContentKeyUsage }) {
  if (!receiverPubkey || !onContentKeyUsage || envelope.receiverPubkey !== receiverPubkey) return

  onContentKeyUsage({
    ...contentKeyUsageBase({ outer, router, channelPubkey, receiverPubkeys }),
    direction: 'received',
    keyRole: 'receiver',
    receiverPubkey: envelope.receiverPubkey,
    contentKeyPubkey: envelope.iykcPubkey,
    contentKeyProof: envelope.iykcProof,
    rowIndex: envelope.index
  })
}

function createProcessor ({
  receiverSigner,
  privateChannelSigner,
  privateChannelSignersByPubkey,
  privateChannelReaderSigner = privateChannelSigner,
  privateChannelReaderSignersByPubkey,
  privateChannelReaderPubkey,
  privateChannelReaderPubkeysByPubkey,
  receiverPubkey,
  mode = 'leecher',
  modeByPubkey,
  onChunk,
  onEvent,
  onNymEvent,
  onSeedEvent,
  onContentKeyUsage,
  onError,
  receivedChunkTtlMs = DEFAULT_RECEIVED_CHUNK_TTL_MS,
  receivedChunkMaxBytes = DEFAULT_RECEIVED_CHUNK_MAX_BYTES,
  receivedChunkIndexedDB = globalThis.indexedDB,
  ignoredGroupTtlMs = DEFAULT_IGNORED_GROUP_TTL_MS,
  ignoredGroupMaxEntries = DEFAULT_IGNORED_GROUP_MAX_ENTRIES
}) {
  const receivedChunks = createReceivedChunkStore({
    ttlMs: receivedChunkTtlMs,
    maxBytes: receivedChunkMaxBytes,
    indexedDB: receivedChunkIndexedDB
  })
  const ignoredGroups = createTtlSet({
    ttlMs: ignoredGroupTtlMs,
    maxEntries: ignoredGroupMaxEntries
  })

  return async function processOuterEvent (outer) {
    let groupKey = ''
    try {
      const channelPubkey = outer.pubkey || await privateChannelSigner?.getPublicKey?.()
      const channelSigner = readSignerFromMap(privateChannelSignersByPubkey, channelPubkey) || privateChannelSigner
      const channelReaderSigner = readSignerFromMap(privateChannelReaderSignersByPubkey, channelPubkey) || privateChannelReaderSigner || channelSigner
      const channelReaderPubkey = readValueFromMap(privateChannelReaderPubkeysByPubkey, channelPubkey) || privateChannelReaderPubkey || channelPubkey
      const channelMode = readValueFromMap(modeByPubkey, channelPubkey) || mode
      if (!channelPubkey) throw new Error('PRIVATE_CHANNEL_PUBKEY_REQUIRED')

      const decrypted = await decryptRouter({
        content: outer.content,
        channelPubkey,
        channelSigner,
        channelReaderSigner,
        channelReaderPubkey
      })
      if (decrypted.kind === NYM_CARRIER_KIND) {
        const carrier = assertValidNymCarrierEvent(decrypted)
        const { index, total } = readChunkTag(carrier)
        groupKey = receivedChunks.groupKeyFor(channelPubkey, nymCarrierGroupId(carrier))
        if (ignoredGroups.has(groupKey)) return

        const meta = await receivedChunks.put({
          channelPubkey,
          routerPubkey: nymCarrierGroupId(carrier),
          index,
          total,
          contentBytes: encoder.encode(JSON.stringify(carrier))
        })
        const status = await receivedChunks.status(meta)
        onChunk?.({
          outer,
          nymCarrier: carrier,
          channelPubkey,
          index,
          total,
          received: status.received,
          missing: status.missing
        })
        if (status.received < total) return

        const carriers = (await receivedChunks.readChunkContents(groupKey)).map(raw => JSON.parse(raw))
        const event = eventFromNymCarriers(carriers)
        const shouldSeed = storesRecoverySeeds(channelMode)
        if (shouldSeed) {
          await onSeedEvent?.({
            recordType: 'nymCarrier_v1',
            outer,
            carriers,
            carrier: carriers[0],
            channelPubkey,
            event
          })
        }
        await onNymEvent?.(event, outer, {
          carrier: carriers[0],
          carriers,
          channelPubkey
        })
        await receivedChunks.removeGroup(groupKey)
        return
      }
      const router = decrypted
      if (router.kind !== ROUTER_KIND) return
      const senderPubkey = readSenderTag(router)
      if (receiverPubkey && readReceiverTag(router) && readReceiverTag(router) !== receiverPubkey && senderPubkey !== receiverPubkey) return
      const { index, total } = readChunkTag(router)
      groupKey = receivedChunks.groupKeyFor(channelPubkey, router.pubkey)
      if (ignoredGroups.has(groupKey)) return

      const imkcPubkey = readImkcTag(router)
      assertValidRouterImkcProof({ router, senderPubkey, imkcPubkey, imkcProof: readImkcProof(router) })
      const meta = await receivedChunks.put({
        channelPubkey,
        routerPubkey: router.pubkey,
        index,
        total,
        contentBytes: base64ToBytes(router.content)
      })
      const status = await receivedChunks.status(meta)
      onChunk?.({
        outer,
        router,
        channelPubkey,
        index,
        total,
        received: status.received,
        missing: status.missing
      })

      const shouldSeed = storesRecoverySeeds(channelMode)
      const sentByReceiver = receiverPubkey && senderPubkey === receiverPubkey
      // Recovery seeders and own-sent messages need the full recipient list;
      // regular leechers can stop as soon as their envelope is decrypted.
      const mustScanWholeBundle = shouldSeed || sentByReceiver
      let event = null
      const innerEventIdsByRowIndex = {}

      const drained = await receivedChunks.drainAvailable(groupKey, {
        onLine: async (line, rowIndex, groupMeta, helpers) => {
          if (rowIndex === 0) {
            helpers.rememberPayloadCiphertext(groupMeta, parsePayloadEnvelope(line, rowIndex).ciphertext)
            return
          }
          if (!groupMeta.payloadCiphertext) throw new Error('MISSING_PAYLOAD_ENVELOPE')
          const envelope = parseRecipientEnvelope(line, rowIndex)
          helpers.rememberReceiverPubkey(groupMeta, envelope.receiverPubkey)

          if (receiverPubkey && envelope.receiverPubkey === receiverPubkey) {
            assertValidEnvelopeIykcProof(envelope)
            emitReceivedContentKeyUsage({
              outer,
              router,
              channelPubkey,
              receiverPubkey,
              receiverPubkeys: groupMeta.receiverPubkeys,
              envelope,
              onContentKeyUsage
            })
            if (receiverSigner && !event) {
              event = await unwrapRecipientEnvelope({
                payloadCiphertext: groupMeta.payloadCiphertext,
                envelope,
                receiverSigner,
                receiverPubkey,
                senderPubkey,
                imkcPubkey,
                rowScope: channelPubkey
              })
              if (event) {
                innerEventIdsByRowIndex[rowIndex] = event.id
                if (!mustScanWholeBundle) return { stop: true }
              }
            }
          }
        }
      })

      if (event && !mustScanWholeBundle) {
        await onEvent?.(event, outer, { router: joinedRouter(router), channelPubkey })
        ignoredGroups.add(groupKey)
        await receivedChunks.removeGroup(groupKey)
        return
      }

      if (!drained.complete) return

      const receiverPubkeys = drained.meta?.receiverPubkeys || []
      const content = shouldSeed ? await receivedChunks.readEnvelopeBundleContent(groupKey) : ''
      const jsonl = shouldSeed ? await receivedChunks.readEnvelopeBundleText(groupKey) : ''
      const completeRouter = joinedRouter(router, content)

      emitSentContentKeyUsage({
        outer,
        router: completeRouter,
        channelPubkey,
        receiverPubkey,
        receiverPubkeys,
        onContentKeyUsage
      })
      if (shouldSeed) await onSeedEvent?.({ recordType: 'routerRow_v1', outer, router: completeRouter, channelPubkey, jsonl, innerEventIdsByRowIndex })
      if (event) await onEvent?.(event, outer, { router: completeRouter, channelPubkey, jsonl })

      await receivedChunks.removeGroup(groupKey)
    } catch (err) {
      if (shouldIgnoreGroupError(err) && groupKey) {
        ignoredGroups.add(groupKey)
        await receivedChunks.removeGroup(groupKey).catch(() => {})
      }
      onError?.(err)
    }
  }
}

function shouldIgnoreGroupError (err) {
  return [
    'DUPLICATE_NYM_CARRIER_CHUNK',
    'INVALID_IYKC_PROOF',
    'INVALID_IMKC_PROOF',
    'INVALID_MESSAGE_SECKEY',
    'INVALID_NYM_CARRIER',
    'INVALID_NYM_CARRIER_ID',
    'INVALID_NYM_CARRIER_INNER_ID',
    'INVALID_NYM_CARRIER_KIND',
    'INVALID_NYM_CARRIER_PAYLOAD',
    'INVALID_NYM_RUMOR',
    'INVALID_PAYLOAD_ENVELOPE',
    'INVALID_RECIPIENT_ENVELOPE',
    'INVALID_SIGNED_INNER_EVENT',
    'MISSING_PAYLOAD_ENVELOPE',
    'RECEIVED_CHUNK_GROUP_TOO_LARGE',
    'MISMATCHED_NYM_CARRIER_CHUNKS',
    'MISSING_NYM_CARRIER_CHUNK',
    'MISSING_NYM_CARRIER_ID'
  ].includes(err?.message)
}

export async function fetch ({ receiverSigner, iykcSigner, privateChannelSigner = receiverSigner, privateChannelSignersByPubkey, privateChannelReaderSigner = privateChannelSigner, privateChannelReaderSignersByPubkey, privateChannelReaderPubkey, privateChannelReaderPubkeysByPubkey, privateChannelPubkey, privateChannelPubkeys, receiverPubkey, relays, onChunk, onEvent, onNymEvent, onSeedEvent, onContentKeyUsage, onError, since, until, limit, mode = 'leecher', modeByPubkey, receivedChunkTtlMs = DEFAULT_RECEIVED_CHUNK_TTL_MS, receivedChunkMaxBytes = DEFAULT_RECEIVED_CHUNK_MAX_BYTES, receivedChunkIndexedDB = globalThis.indexedDB, ignoredGroupTtlMs = DEFAULT_IGNORED_GROUP_TTL_MS, ignoredGroupMaxEntries = DEFAULT_IGNORED_GROUP_MAX_ENTRIES, _getEvents = getEvents }) {
  if (!relays?.length) throw new Error('NO_RELAYS')
  const authors = privateChannelPubkeyList({ privateChannelPubkey, privateChannelPubkeys })
  const filter = { kinds: [PRIVATE_BROADCAST_KIND] }
  if (authors.length) filter.authors = authors
  if (since != null) filter.since = since
  if (until != null) filter.until = until
  if (limit != null) filter.limit = limit

  const { result: events } = await _getEvents(filter, relays, {
    timeout: 5000,
    timeoutAfterFirstEose: null
  })
  events.sort((a, b) => a.created_at - b.created_at)
  const processOuterEvent = createProcessor({ receiverSigner, iykcSigner, privateChannelSigner, privateChannelSignersByPubkey, privateChannelReaderSigner, privateChannelReaderSignersByPubkey, privateChannelReaderPubkey, privateChannelReaderPubkeysByPubkey, receiverPubkey, mode, modeByPubkey, onChunk, onEvent, onNymEvent, onSeedEvent, onContentKeyUsage, onError, receivedChunkTtlMs, receivedChunkMaxBytes, receivedChunkIndexedDB, ignoredGroupTtlMs, ignoredGroupMaxEntries })
  for (const event of events) await processOuterEvent(event)
  return events
}

export function subscribe ({ receiverSigner, iykcSigner, privateChannelSigner = receiverSigner, privateChannelSignersByPubkey, privateChannelReaderSigner = privateChannelSigner, privateChannelReaderSignersByPubkey, privateChannelReaderPubkey, privateChannelReaderPubkeysByPubkey, privateChannelPubkey, privateChannelPubkeys, receiverPubkey, relays, onChunk, onEvent, onNymEvent, onSeedEvent, onContentKeyUsage, onError, since = nowSeconds() - 5, limit, liveOnly = false, mode = 'leecher', modeByPubkey, receivedChunkTtlMs = DEFAULT_RECEIVED_CHUNK_TTL_MS, receivedChunkMaxBytes = DEFAULT_RECEIVED_CHUNK_MAX_BYTES, receivedChunkIndexedDB = globalThis.indexedDB, ignoredGroupTtlMs = DEFAULT_IGNORED_GROUP_TTL_MS, ignoredGroupMaxEntries = DEFAULT_IGNORED_GROUP_MAX_ENTRIES, _liveEventsGenerator = getLiveEventsGenerator, _eventsFeedGenerator = getEventsFeedGenerator }) {
  if (!relays?.length) throw new Error('NO_RELAYS')
  if (receiverSigner && !receiverSigner?.nip44DecryptDoubleDH && !receiverSigner?.nip44v3Decrypt) throw new Error('RECEIVER_SIGNER_NIP44V3_DECRYPT_UNSUPPORTED')
  if (!privateChannelReaderSigner && !privateChannelReaderSignersByPubkey && !privateChannelSigner && !privateChannelSignersByPubkey) throw new Error('PRIVATE_CHANNEL_READER_REQUIRED')

  const authors = privateChannelPubkeyList({ privateChannelPubkey, privateChannelPubkeys })
  const filter = { kinds: [PRIVATE_BROADCAST_KIND], since }
  if (authors.length) filter.authors = authors
  if (limit != null) filter.limit = limit
  const processOuterEvent = createProcessor({ receiverSigner, iykcSigner, privateChannelSigner, privateChannelSignersByPubkey, privateChannelReaderSigner, privateChannelReaderSignersByPubkey, privateChannelReaderPubkey, privateChannelReaderPubkeysByPubkey, receiverPubkey, mode, modeByPubkey, onChunk, onEvent, onNymEvent, onSeedEvent, onContentKeyUsage, onError, receivedChunkTtlMs, receivedChunkMaxBytes, receivedChunkIndexedDB, ignoredGroupTtlMs, ignoredGroupMaxEntries })
  const controller = new AbortController()
  const events = liveOnly
    ? _liveEventsGenerator(filter, relays, {
        signal: controller.signal
      })
    : _eventsFeedGenerator(filter, relays, {
        signal: controller.signal,
        timeout: 5000,
        timeoutAfterFirstEose: null
      })

  async function consumeEvents () {
    try {
      for await (const outer of events) {
        if (controller.signal.aborted) continue
        await processOuterEvent(outer)
      }
    } catch (error) {
      if (!controller.signal.aborted && error?.message !== 'Aborted') onError?.(error)
    }
  }

  consumeEvents()

  return {
    close () {
      controller.abort()
    }
  }
}
