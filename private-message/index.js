import { getEventHash, validateEvent, verifyEvent } from 'nostr-tools'
import { generateKeypair } from '../key/index.js'
import * as privateChannel from '../private-channel/index.js'

export const ASK_KIND = 7329
export const REPLY_KIND = 7330
export const TELL_KIND = 7331

const RESUBSCRIBE_GRACE_MS = 500
const PRIVATE_MESSAGE_KINDS = [ASK_KIND, REPLY_KIND, TELL_KIND]
const HEX_PUBKEY = /^[0-9a-f]{64}$/i

const watchesByChannel = new Map()
const subsByRelay = new Map()

function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

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

function resolveDeletionCapability ({ deletionPubkey, deletionSeckey, autoDeletionCapability = true }) {
  if (deletionSeckey !== undefined) throw new Error('DELETION_SECKEY_NOT_ACCEPTED')
  if (typeof autoDeletionCapability !== 'boolean') throw new Error('AUTO_DELETION_CAPABILITY_BOOLEAN_REQUIRED')

  const normalizedDeletionPubkey = normalizeDeletionPubkey(deletionPubkey)
  if (normalizedDeletionPubkey) return { deletionPubkey: normalizedDeletionPubkey }
  if (!autoDeletionCapability) return {}

  const { pubkey, seckey } = generateKeypair()
  return { deletionPubkey: pubkey, deletionSeckey: seckey }
}

function withDelivery (result, reports, deletionSeckey) {
  const delivery = { reports }
  if (deletionSeckey !== undefined) delivery.deletionSeckey = deletionSeckey
  return {
    ...result,
    delivery
  }
}

function setEquals (a, b) {
  if (a.size !== b.size) return false
  for (const value of a) if (!b.has(value)) return false
  return true
}

function normalizePayloadContent (payload) {
  if (payload == null || payload === '') return ''
  if (typeof payload === 'string') return payload
  return JSON.stringify(payload)
}

function normalizeMessage (message = {}) {
  if (typeof message === 'string') return { content: normalizePayloadContent(message), code: '', error: '' }
  const hasPayload = Object.prototype.hasOwnProperty.call(message, 'payload')
  const payload = hasPayload ? message.payload : message.content
  return {
    content: normalizePayloadContent(payload),
    code: message.code == null ? '' : String(message.code),
    error: message.error == null ? '' : String(message.error)
  }
}

function addHeaderTag (tags, { code, error }) {
  const out = cloneTags(tags)
  if (!code && !error) return out
  const header = ['h', code || '']
  if (error) header.push(error)
  return out.concat([header])
}

function makeMessageRumor ({ kind, tags, message }) {
  const normalized = normalizeMessage(message)
  return {
    kind,
    tags: addHeaderTag(tags, normalized),
    content: normalized.content
  }
}

function parsePayloadContent (content) {
  if (content === '') return null
  try { return JSON.parse(content) } catch { return content }
}

function parseMessageContent (event) {
  const payload = parsePayloadContent(event.content)
  const header = event.tags?.find(tag => tag[0] === 'h') || []
  const message = {}
  if (payload !== null) message.payload = payload
  if (header[1]) message.code = header[1]
  if (header[2]) message.error = header[2]
  return message
}

export function parseRumorContent (event) {
  if (PRIVATE_MESSAGE_KINDS.includes(event.kind)) return parseMessageContent(event)
  return parsePayloadContent(event.content)
}

function cloneTags (tags) {
  if (!Array.isArray(tags)) return tags
  return tags.map(tag => Array.isArray(tag) ? [...tag] : tag)
}

async function makeOutgoingRumor ({ senderSigner, rumor }) {
  if (!senderSigner?.getPublicKey) throw new Error('SENDER_SIGNER_REQUIRED')
  const senderPubkey = await senderSigner.getPublicKey()
  // This is what gets sent. Id and pubkey are added later by recipient.
  const wireEvent = {
    kind: rumor.kind,
    tags: cloneTags(rumor.tags),
    content: rumor.content,
    created_at: rumor.created_at !== undefined
      ? rumor.created_at
      : nowSeconds()
  }
  const event = normalizeRumor(wireEvent, senderPubkey)
  return { event, wireEvent }
}

function normalizeRumor (event, pubkey) {
  const normalized = { ...event, pubkey }
  if (!validateEvent(normalized)) throw new Error('INVALID_RUMOR')
  return { ...normalized, id: getEventHash(normalized) }
}

function assertValidSignedEvent (event) {
  if (!validateEvent(event) || event.id !== getEventHash(event) || !verifyEvent(event)) {
    throw new Error('INVALID_SIGNED_EVENT')
  }
  return event
}

function readTag (event, name) {
  return event.tags?.find(tag => tag[0] === name)?.[1] || ''
}

async function ownPrivateChannelPubkey (signer) {
  if (!signer?.getPublicKey) throw new Error('PRIVATE_CHANNEL_SIGNER_REQUIRED')
  return signer.getPublicKey()
}

function assertWatching (channelPubkey) {
  if (!watchesByChannel.has(channelPubkey)) throw new Error('PRIVATE_MESSAGE_NOT_WATCHING')
}

function watchCallbacks (channelPubkey) {
  return watchesByChannel.get(channelPubkey)?.callbacks || {}
}

function dispatchWatchedEvent (event, outer, meta) {
  const callbacks = watchCallbacks(meta.channelPubkey)
  const payload = parseRumorContent(event)
  const message = { event, outer, meta, payload }

  if (event.kind === ASK_KIND) {
    callbacks.onAsk?.({ ...message, question: event })
  } else if (event.kind === REPLY_KIND) {
    const questionId = readTag(event, 'q')
    callbacks.onReply?.({ ...message, questionId, reply: event })
  } else if (event.kind === TELL_KIND) {
    const receiverTag = readTag(event, 'r')
    if (receiverTag) callbacks.onTell?.({ ...message, tell: event })
    else callbacks.onYell?.({ ...message, yell: event })
  }

  callbacks.onMessage?.(message)
}

function dispatchWatchedNymEvent (event, outer, meta) {
  const callbacks = watchCallbacks(meta.channelPubkey)
  callbacks.onNym?.({
    event,
    outer,
    meta,
    payload: parseRumorContent(event),
    nym: event
  })
}

function dispatchSeedEvent (seed) {
  watchCallbacks(seed.channelPubkey).onSeed?.(seed)
}

function dispatchContentKeyUsage (usage) {
  watchCallbacks(usage.channelPubkey).onContentKeyUsage?.(usage)
}

function handleChunk (chunk) {
  watchCallbacks(chunk.channelPubkey).onChunk?.(chunk)
}

function desiredRelayState () {
  const relayToChannels = new Map()
  for (const [channelPubkey, watch] of watchesByChannel) {
    for (const relay of watch.relays) {
      if (!relayToChannels.has(relay)) relayToChannels.set(relay, new Set())
      relayToChannels.get(relay).add(channelPubkey)
    }
  }
  return relayToChannels
}

function signersForChannels (channels) {
  const out = {}
  for (const channel of channels) {
    const signer = watchesByChannel.get(channel)?.privateChannelSigner
    if (signer) out[channel] = signer
  }
  return out
}

function readerSignersForChannels (channels) {
  const out = {}
  for (const channel of channels) {
    const signer = watchesByChannel.get(channel)?.privateChannelReaderSigner
    if (signer) out[channel] = signer
  }
  return out
}

function readerPubkeysForChannels (channels) {
  const out = {}
  for (const channel of channels) {
    const pubkey = watchesByChannel.get(channel)?.privateChannelReaderPubkey
    if (pubkey) out[channel] = pubkey
  }
  return out
}

function modesForChannels (channels) {
  const out = {}
  for (const channel of channels) out[channel] = watchesByChannel.get(channel)?.mode || 'leecher'
  return out
}

function maxWatchNumber (channels, field) {
  const values = channels
    .map(channel => watchesByChannel.get(channel)?.[field])
    .filter(value => Number.isFinite(value))
  return values.length ? Math.max(...values) : undefined
}

function firstWatchValue (channels, field) {
  for (const channel of channels) {
    const value = watchesByChannel.get(channel)?.[field]
    if (value !== undefined) return value
  }
  return undefined
}

function closeSubscription (sub, gracefulClose) {
  if (gracefulClose) {
    setTimeout(() => Promise.resolve().then(() => sub.close()).catch(() => {}), RESUBSCRIBE_GRACE_MS)
    return null
  }
  try {
    return Promise.resolve(sub.close())
  } catch (err) {
    return Promise.reject(err)
  }
}

function rebuildSubscriptions ({ _subscribe = privateChannel.subscribe, gracefulClose = true } = {}) {
  const desired = desiredRelayState()
  const closing = []

  for (const [relay, current] of subsByRelay) {
    const nextChannels = desired.get(relay)
    if (nextChannels && setEquals(current.channels, nextChannels)) continue
    if (!nextChannels) {
      const close = closeSubscription(current.sub, gracefulClose)
      if (close) closing.push(close)
      subsByRelay.delete(relay)
    }
  }

  for (const [relay, channels] of desired) {
    const current = subsByRelay.get(relay)
    if (current && setEquals(current.channels, channels)) continue

    const channelList = [...channels]
    const firstWatch = watchesByChannel.get(channelList[0])
    const sub = _subscribe({
      receiverSigner: firstWatch.receiverSigner,
      iykcSigner: firstWatch.iykcSigner,
      privateChannelSigner: firstWatch.privateChannelSigner,
      privateChannelSignersByPubkey: signersForChannels(channelList),
      privateChannelReaderSigner: firstWatch.privateChannelReaderSigner,
      privateChannelReaderSignersByPubkey: readerSignersForChannels(channelList),
      privateChannelReaderPubkey: firstWatch.privateChannelReaderPubkey,
      privateChannelReaderPubkeysByPubkey: readerPubkeysForChannels(channelList),
      privateChannelPubkeys: channelList,
      receiverPubkey: firstWatch.receiverPubkey,
      relays: [relay],
      mode: firstWatch.mode,
      modeByPubkey: modesForChannels(channelList),
      receivedChunkTtlMs: maxWatchNumber(channelList, 'receivedChunkTtlMs'),
      receivedChunkMaxBytes: maxWatchNumber(channelList, 'receivedChunkMaxBytes'),
      receivedChunkIndexedDB: firstWatchValue(channelList, 'receivedChunkIndexedDB'),
      ignoredGroupTtlMs: maxWatchNumber(channelList, 'ignoredGroupTtlMs'),
      ignoredGroupMaxEntries: maxWatchNumber(channelList, 'ignoredGroupMaxEntries'),
      limit: 0,
      since: nowSeconds(),
      liveOnly: true,
      onChunk: handleChunk,
      onEvent: (event, outer, meta) => {
        dispatchWatchedEvent(event, outer, meta)
      },
      onNymEvent: (event, outer, meta) => {
        dispatchWatchedNymEvent(event, outer, meta)
      },
      onSeedEvent: (seed) => {
        dispatchSeedEvent(seed)
      },
      onContentKeyUsage: dispatchContentKeyUsage,
      onError: err => firstWatch.callbacks.onError?.(err)
    })

    subsByRelay.set(relay, { channels: new Set(channels), sub })
    if (current) {
      const close = closeSubscription(current.sub, gracefulClose)
      if (close) closing.push(close)
    }
  }
  return Promise.allSettled(closing)
}

export async function watch ({
  channels,
  relays,
  receiverSigner,
  iykcSigner,
  privateChannelSigner = receiverSigner,
  privateChannelReaderSigner = privateChannelSigner,
  privateChannelReaderPubkey,
  receiverPubkey,
  mode = 'leecher',
  onAsk,
  onReply,
  onTell,
  onYell,
  onNym,
  onMessage,
  onSeed,
  onChunk,
  onContentKeyUsage,
  onError,
  receivedChunkTtlMs,
  receivedChunkMaxBytes,
  receivedChunkIndexedDB,
  ignoredGroupTtlMs,
  ignoredGroupMaxEntries,
  since = nowSeconds(),
  _subscribe = privateChannel.subscribe
}) {
  if (!relays?.length) throw new Error('NO_RELAYS')
  const channelList = uniq(channels?.length ? channels : [await ownPrivateChannelPubkey(privateChannelSigner)])
  const ownPubkey = receiverPubkey || await receiverSigner?.getPublicKey?.()
  const callbacks = { onAsk, onReply, onTell, onYell, onNym, onMessage, onSeed, onChunk, onContentKeyUsage, onError }

  let changed = false
  for (const channel of channelList) {
    const next = {
      relays: uniq(relays),
      receiverSigner,
      iykcSigner,
      privateChannelSigner,
      privateChannelReaderSigner: privateChannelReaderSigner || privateChannelSigner,
      privateChannelReaderPubkey,
      receiverPubkey: ownPubkey,
      mode,
      receivedChunkTtlMs,
      receivedChunkMaxBytes,
      receivedChunkIndexedDB,
      ignoredGroupTtlMs,
      ignoredGroupMaxEntries,
      callbacks,
      since
    }
    const current = watchesByChannel.get(channel)
    if (
      current &&
      setEquals(new Set(current.relays), new Set(next.relays)) &&
      current.privateChannelSigner === next.privateChannelSigner &&
      current.privateChannelReaderSigner === next.privateChannelReaderSigner &&
      current.privateChannelReaderPubkey === next.privateChannelReaderPubkey &&
      current.mode === next.mode &&
      current.receivedChunkTtlMs === next.receivedChunkTtlMs &&
      current.receivedChunkMaxBytes === next.receivedChunkMaxBytes &&
      current.receivedChunkIndexedDB === next.receivedChunkIndexedDB &&
      current.ignoredGroupTtlMs === next.ignoredGroupTtlMs &&
      current.ignoredGroupMaxEntries === next.ignoredGroupMaxEntries
    ) {
      current.callbacks = callbacks
      continue
    }
    watchesByChannel.set(channel, next)
    changed = true
  }

  if (changed) await rebuildSubscriptions({ _subscribe })
  return () => unwatch(channelList)
}

export function unwatch (channels) {
  const channelList = channels ? uniq(Array.isArray(channels) ? channels : [channels]) : [...watchesByChannel.keys()]
  for (const channel of channelList) watchesByChannel.delete(channel)
  return rebuildSubscriptions({ gracefulClose: false })
}

export function clearChannelState (channelPubkey) {
  if (watchesByChannel.has(channelPubkey)) return unwatch(channelPubkey)
  return Promise.resolve([])
}

async function sendPrivateMessage ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receivers,
  receiverTag,
  event,
  relays,
  relayToReceivers,
  recoveryRelays,
  expirationSeconds,
  temporaryStorageArea,
  deletionPubkey,
  _getIykcProofs,
  _publish = privateChannel.publish
}) {
  if (!privateChannelSigner?.getPublicKey) throw new Error('PRIVATE_CHANNEL_WRITER_REQUIRED')
  return _publish({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers, receiverTag, deletionPubkey, event, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs })
}

async function sendNymMessage ({
  nymSigner,
  privateChannelSigner,
  privateChannelReaderPubkey,
  event,
  relays,
  relayToReceivers,
  recoveryRelays,
  expirationSeconds,
  deletionPubkey,
  _publish = privateChannel.publishNymEvent
}) {
  if (!nymSigner?.getPublicKey) throw new Error('NYM_SIGNER_REQUIRED')
  if (!privateChannelSigner?.getPublicKey) throw new Error('PRIVATE_CHANNEL_WRITER_REQUIRED')
  return _publish({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey, event, relays, relayToReceivers, recoveryRelays, expirationSeconds })
}

export async function ask ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receiverPubkey,
  relays,
  relayToReceivers,
  recoveryRelays,
  message,
  code,
  payload,
  error,
  content,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  if (!receiverPubkey) throw new Error('RECEIVER_PUBKEY_REQUIRED')
  if (!privateChannelSigner?.getPublicKey) throw new Error('PRIVATE_CHANNEL_WRITER_REQUIRED')
  const privateChannelPubkey = await ownPrivateChannelPubkey(privateChannelSigner)
  assertWatching(privateChannelPubkey)

  const { event: question, wireEvent } = await makeOutgoingRumor({
    senderSigner,
    rumor: makeMessageRumor({
      kind: ASK_KIND,
      tags: [['r', receiverPubkey]],
      message: message || { code, payload, error, content }
    })
  })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers: [receiverPubkey], receiverTag: receiverPubkey, deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })

  return withDelivery({ question }, reports, deletion.deletionSeckey)
}

export async function reply ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  question,
  receiverPubkey = question?.pubkey,
  relays,
  relayToReceivers,
  recoveryRelays,
  message,
  code,
  payload,
  error,
  content,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  if (!question?.id) throw new Error('QUESTION_REQUIRED')
  if (!receiverPubkey) throw new Error('RECEIVER_PUBKEY_REQUIRED')
  const { event, wireEvent } = await makeOutgoingRumor({
    senderSigner,
    rumor: makeMessageRumor({
      kind: REPLY_KIND,
      tags: [['q', question.id], ['r', receiverPubkey]],
      message: message || { code, payload, error, content }
    })
  })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers: [receiverPubkey], receiverTag: receiverPubkey, deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })
  return withDelivery({ reply: event }, reports, deletion.deletionSeckey)
}

export async function tell ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receiverPubkey,
  relays,
  relayToReceivers,
  recoveryRelays,
  message,
  code,
  payload,
  error,
  content,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  if (!receiverPubkey) throw new Error('RECEIVER_PUBKEY_REQUIRED')
  const { event, wireEvent } = await makeOutgoingRumor({
    senderSigner,
    rumor: makeMessageRumor({
      kind: TELL_KIND,
      tags: [['r', receiverPubkey]],
      message: message || { code, payload, error, content }
    })
  })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers: [receiverPubkey], receiverTag: receiverPubkey, deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })
  return withDelivery({ tell: event }, reports, deletion.deletionSeckey)
}

export async function yell ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receiverPubkeys,
  relays,
  relayToReceivers,
  recoveryRelays,
  message,
  code,
  payload,
  error,
  content,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  const receivers = uniq(receiverPubkeys)
  if (!receivers.length) throw new Error('NO_RECEIVERS')
  const { event, wireEvent } = await makeOutgoingRumor({
    senderSigner,
    rumor: makeMessageRumor({
      kind: TELL_KIND,
      tags: [],
      message: message || { code, payload, error, content }
    })
  })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers, receiverTag: '', deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })
  return withDelivery({ yell: event }, reports, deletion.deletionSeckey)
}

export async function broadcastRumor ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receiverPubkeys,
  relays,
  relayToReceivers,
  recoveryRelays,
  rumor,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  const receivers = uniq(receiverPubkeys)
  if (!receivers.length) throw new Error('NO_RECEIVERS')
  const { event, wireEvent } = await makeOutgoingRumor({ senderSigner, rumor })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers, receiverTag: '', deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })
  return withDelivery({ rumor: event }, reports, deletion.deletionSeckey)
}

export async function broadcastEvent ({
  senderSigner,
  imkcSigner,
  privateChannelSigner = senderSigner,
  privateChannelReaderPubkey,
  receiverPubkeys,
  relays,
  relayToReceivers,
  recoveryRelays,
  event,
  expirationSeconds,
  temporaryStorageArea,
  _getIykcProofs,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publish
}) {
  const receivers = uniq(receiverPubkeys)
  if (!receivers.length) throw new Error('NO_RECEIVERS')
  const wireEvent = assertValidSignedEvent({ ...event, tags: cloneTags(event?.tags) })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendPrivateMessage({ senderSigner, imkcSigner, privateChannelSigner, privateChannelReaderPubkey, receivers, receiverTag: '', deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, temporaryStorageArea, _getIykcProofs, _publish })
  return withDelivery({ event: wireEvent }, reports, deletion.deletionSeckey)
}

export async function broadcastNymRumor ({
  nymSigner,
  privateChannelSigner,
  privateChannelReaderPubkey,
  relays,
  relayToReceivers,
  recoveryRelays,
  rumor,
  expirationSeconds,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publishNymEvent
}) {
  if (!nymSigner?.getPublicKey) throw new Error('NYM_SIGNER_REQUIRED')
  const { event, wireEvent } = await makeOutgoingRumor({ senderSigner: nymSigner, rumor })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendNymMessage({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, _publish })
  return withDelivery({ rumor: event }, reports, deletion.deletionSeckey)
}

export async function broadcastNymEvent ({
  nymSigner,
  privateChannelSigner,
  privateChannelReaderPubkey,
  relays,
  relayToReceivers,
  recoveryRelays,
  event,
  expirationSeconds,
  deletionPubkey,
  deletionSeckey,
  autoDeletionCapability = true,
  _publish = privateChannel.publishNymEvent
}) {
  if (!nymSigner?.getPublicKey) throw new Error('NYM_SIGNER_REQUIRED')
  const wireEvent = assertValidSignedEvent({ ...event, tags: cloneTags(event?.tags) })
  const deletion = resolveDeletionCapability({ deletionPubkey, deletionSeckey, autoDeletionCapability })
  const reports = await sendNymMessage({ nymSigner, privateChannelSigner, privateChannelReaderPubkey, deletionPubkey: deletion.deletionPubkey, event: wireEvent, relays, relayToReceivers, recoveryRelays, expirationSeconds, _publish })
  return withDelivery({ event: wireEvent }, reports, deletion.deletionSeckey)
}
