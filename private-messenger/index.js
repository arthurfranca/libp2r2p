// Expected use:
// const messenger = await createPrivateMessenger({
//   userSigner,
//   contentKeySigner, // optional when userSigner handles content keys internally
//   nymSigner: optionalDefaultNymSigner,
//   channels: [{ signer: privateChannelSigner, relays, mode: 'leecher', seeders: optionalSeederPubkeys }],
//   onContentKeyChange: event => reviewContentKeyUse(event),
//   onError: err => reportPrivateMessengerError(err)
// })
//
// Channel roles:
// - Default channel: { signer } signs, publishes, and decrypts the outer router with the same channel key.
// - Split reader channel: { signer, readerPubkey } signs as the channel key but encrypts/decrypts the outer router with the reader pubkey.
// - Reader-secret channel: { signer, readerSigner } is also valid; the reader signer decrypts the router.
// - Reader-only channel: { pubkey, readerSigner } can watch/fetch/drain messages but cannot send or seed recovery replies.
// for await (const msg of messenger.messages()) handlePrivateMessage(msg)
// await messenger.ask({ receiverPubkey, payload: { ping: true } })
// await messenger.reply({ question: msg.question, payload: { ok: true } })
// await messenger.tell({ receiverPubkey, payload: { note: 'hello' } })
// await messenger.yell({ receiverPubkeys, payload: { notice: 'hello all' } })
// await messenger.broadcastRumor({ receiverPubkeys, rumor: { kind, tags: [], content } })
// await messenger.broadcastEvent({ receiverPubkeys, event: signedNostrEvent })
// await messenger.broadcastNymRumor({ rumor: { kind, tags: [], content } })
// await messenger.broadcastNymEvent({ event: signedNostrEvent })
// await messenger.update({ channels: [{ signer: privateChannelSigner, relays, seeders: nextOptionalSeederPubkeys }] })
// messenger.clearChannel(channelPubkey)
//
// Missed-message recovery:
// - Each watched channel stores lastSeenAt/lastWatchedAt in localStorage.
// - Re-watching after reload fetches the gap from lastSeenAt to now.
// - Browser offline/online events add explicit offline ranges with a small skew.
// - Ranges older than 7 days are ignored; channel state not watched for 45 days is pruned.
// - Seeders announce presence every 10min and are used for the relay-uncovered left edge of a missed range.
// - Configured seeders are all asked; auto-discovered seeders are capped to the 8 most recently active.
// - Seeder/watchtower channels store reconstructed router events in a separate web-storage queue and auto-reply to recovery asks.
// - Seeder replies stream compact routers with createMissingMessageReplyPacker({ messenger, question }).update(seed), then finalize(optionalLastSeed).
// - For other event-list replies, use createEventReplyPacker({ messenger, question, code }).update(event).

import * as privateMessage from '../private-message/index.js'
import { bytesToBase64 } from '../base64/index.js'
import { getRelaysByPubkey, pickRelaysForPubkeys, subscribeRelayListUpdates } from '../relay/index.js'
import * as privateChannel from '../private-channel/index.js'
import { cleanupTemporaryStorage as cleanupTemporaryChannelStorage } from '../temporary-storage/index.js'
import { createQueue } from '../web-storage-queue/index.js'
import { DEFAULT_STALE_CHANNEL_SECONDS } from './constants/index.js'
import {
  compactSeedNymCarriers,
  compactSeedRouterRows,
  createEventReplyPacker,
  createMissingMessageReplyPacker,
  MISSING_MESSAGES_ASK_CODE,
  MISSING_MESSAGES_REPLY_CODE,
  NYM_CARRIER_SEED_RECORD_TYPE,
  ROUTER_SEED_RECORD_TYPE,
  routerSeedRowKey,
  SEEDER_PRESENCE_CODE
} from './recovery/index.js'

export { createQueue } from '../web-storage-queue/index.js'
export { DEFAULT_STALE_CHANNEL_SECONDS } from './constants/index.js'
export {
  compactSeedNymCarriers,
  compactSeedRouterRows,
  createEventReplyPacker,
  createMissingMessageReplyPacker,
  MISSING_MESSAGES_ASK_CODE,
  MISSING_MESSAGES_REPLY_CODE,
  NYM_CARRIER_SEED_RECORD_TYPE,
  ROUTER_SEED_RECORD_TYPE,
  routerSeedRowKey,
  SEEDER_PRESENCE_CODE
} from './recovery/index.js'

const DEFAULT_OFFLINE_RECOVERY_SECONDS = 7 * 24 * 60 * 60
const DEFAULT_OFFLINE_SKEW_SECONDS = 30
const DEFAULT_RELOAD_GAP_DELAY_MS = 500
const DEFAULT_SEEDER_PRESENCE_INTERVAL_MS = 10 * 60 * 1000
const DEFAULT_SEEDER_ONLINE_SECONDS = 20 * 60
const DEFAULT_MAX_DYNAMIC_RECOVERY_SEEDERS = 8
const DEFAULT_MESSAGE_QUEUE_MAX_BYTES = 1024 * 1024 // 1 MiB
const DEFAULT_SEED_QUEUE_MAX_BYTES = 3 * 1024 * 1024 // 3 MiB
const encoder = new TextEncoder()
const noContentKeys = async () => ({})

function textToBase64 (text) {
  return bytesToBase64(encoder.encode(text))
}

function defaultOnError (err) {
  console.warn('private-messenger failed', err?.message ?? err)
}

function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

function uniq (values) {
  return [...new Set((values || []).filter(Boolean))]
}

function isPlainObject (value) {
  return value && typeof value === 'object' && !Array.isArray(value)
}

function storesRecoverySeeds (mode) {
  return mode === 'seeder' || mode === 'watchtower'
}

function parseJson (raw, fallback) {
  try { return JSON.parse(raw || '') } catch { return fallback }
}

export class PrivateMessenger {
  static cleanupTemporaryStorage ({ storageArea = globalThis.sessionStorage } = {}) {
    cleanupTemporaryChannelStorage({ storageArea })
  }

  constructor ({
    offlineRecoverySeconds = DEFAULT_OFFLINE_RECOVERY_SECONDS,
    staleChannelSeconds = DEFAULT_STALE_CHANNEL_SECONDS,
    offlineSkewSeconds = DEFAULT_OFFLINE_SKEW_SECONDS,
    reloadGapDelayMs = DEFAULT_RELOAD_GAP_DELAY_MS,
    seederPresenceIntervalMs = DEFAULT_SEEDER_PRESENCE_INTERVAL_MS,
    seederOnlineSeconds = DEFAULT_SEEDER_ONLINE_SECONDS,
    maxDynamicRecoverySeeders = DEFAULT_MAX_DYNAMIC_RECOVERY_SEEDERS,
    messageQueueMaxBytes = DEFAULT_MESSAGE_QUEUE_MAX_BYTES,
    seedQueueMaxBytes = DEFAULT_SEED_QUEUE_MAX_BYTES,
    temporaryStorageArea = globalThis.sessionStorage,
    useContentKeys = true,
    onContentKeyChange,
    onMessageQueued,
    onDebug,
    onError = defaultOnError,
    _privateMessage = privateMessage,
    _privateChannel = privateChannel,
    _getRelaysByPubkey = getRelaysByPubkey,
    _pickRelaysForPubkeys = pickRelaysForPubkeys,
    _subscribeRelayListUpdates = subscribeRelayListUpdates,
    _setTimeout = globalThis.setTimeout.bind(globalThis),
    _setInterval = globalThis.setInterval.bind(globalThis),
    _clearInterval = globalThis.clearInterval.bind(globalThis)
  } = {}) {
    this.offlineRecoverySeconds = offlineRecoverySeconds
    this.staleChannelSeconds = staleChannelSeconds
    this.offlineSkewSeconds = offlineSkewSeconds
    this.reloadGapDelayMs = reloadGapDelayMs
    this.seederPresenceIntervalMs = seederPresenceIntervalMs
    this.seederOnlineSeconds = seederOnlineSeconds
    this.maxDynamicRecoverySeeders = maxDynamicRecoverySeeders
    this.messageQueueMaxBytes = messageQueueMaxBytes
    this.seedQueueMaxBytes = seedQueueMaxBytes
    this.temporaryStorageArea = temporaryStorageArea
    this.useContentKeys = useContentKeys
    this.onContentKeyChange = onContentKeyChange
    this.onMessageQueued = onMessageQueued
    this.onDebug = onDebug
    this.onError = onError
    this._privateMessage = _privateMessage
    this._privateChannel = _privateChannel
    this._getRelaysByPubkey = _getRelaysByPubkey
    this._pickRelaysForPubkeys = _pickRelaysForPubkeys
    this._subscribeRelayListUpdates = _subscribeRelayListUpdates
    this._setTimeout = _setTimeout
    this._setInterval = _setInterval
    this._clearInterval = _clearInterval

    this.userSigner = null
    this.contentKeySigner = null
    this.nymSigner = null
    this.userPubkey = ''
    this.contentKeyPubkey = ''
    this.prefix = ''
    this.queue = null
    this.seedQueue = null
    this.channels = new Map()
    this.stopByChannel = new Map()
    this.presenceTimers = new Map()
    this.stopRelayListWatcher = null
    this.relayListWatcherPubkey = ''
    this.relayListRefreshPromise = null
    this.stopOnline = null
    this.stopOffline = null
  }

  async init ({ userSigner, contentKeySigner, nymSigner, channels = [], relays = [], mode = 'leecher' }) {
    if (!userSigner?.getPublicKey) throw new Error('USER_SIGNER_REQUIRED')
    PrivateMessenger.cleanupTemporaryStorage({ storageArea: this.temporaryStorageArea })
    this.userSigner = userSigner
    this.contentKeySigner = contentKeySigner || null
    this.nymSigner = nymSigner || null
    this.userPubkey = await userSigner.getPublicKey()
    this.contentKeyPubkey = await this.contentKeySigner?.getPublicKey?.() || ''
    this.prefix = `libp2r2p:private-messenger:${this.userPubkey}`
    this.queue = createQueue({ prefix: this.prefix, maxBytes: this.messageQueueMaxBytes, evictionPolicy: 'fifo' })
    this.seedQueue = createQueue({ prefix: `${this.prefix}:seeds`, maxBytes: this.seedQueueMaxBytes, evictionPolicy: 'fifo' })
    this.cleanupStaleChannels()
    await this.update({ userSigner, contentKeySigner, nymSigner: this.nymSigner, channels, relays, mode })
    return this
  }

  debug (action, detail = {}) {
    try {
      this.onDebug?.({ source: 'private-messenger', action, ...detail })
    } catch (err) {
      this.onError?.(err)
    }
  }

  debugSend (method, channelPubkey, detail = {}) {
    const receiverPubkeys = uniq(detail.receiverPubkeys || (detail.receiverPubkey ? [detail.receiverPubkey] : []))
    this.debug('send', {
      method,
      type: method,
      code: detail.code || '',
      channelPubkey,
      senderPubkey: this.userPubkey,
      receiverPubkey: detail.receiverPubkey || '',
      receiverPubkeys,
      receiverCount: receiverPubkeys.length
    })
  }

  async update ({ userSigner = this.userSigner, contentKeySigner = this.contentKeySigner, nymSigner = this.nymSigner, channels = [...this.channels.values()], relays = [], mode = 'leecher' } = {}) {
    if (userSigner) {
      const userPubkey = await userSigner.getPublicKey?.()
      if (!userPubkey) throw new Error('USER_SIGNER_REQUIRED')
      if (this.userPubkey && userPubkey !== this.userPubkey) throw new Error('USER_SIGNER_MISMATCH')
      this.userSigner = userSigner
    }
    this.contentKeySigner = contentKeySigner || null
    this.nymSigner = nymSigner || null
    this.contentKeyPubkey = await this.contentKeySigner?.getPublicKey?.() || ''
    const nextChannels = await this.normalizeChannels(channels, { relays, mode })
    const nextPubkeys = new Set(nextChannels.map(channel => channel.pubkey))

    for (const pubkey of [...this.channels.keys()]) {
      if (!nextPubkeys.has(pubkey)) {
        this.unwatch(pubkey)
        this.channels.delete(pubkey)
      }
    }
    for (const channel of nextChannels) this.channels.set(channel.pubkey, channel)

    this.cleanupStaleChannels()
    await this.watch()
    await this.reconcilePresencePublishers()
    return this
  }

  async normalizeChannels (channels, defaults) {
    const out = []
    for (const entry of channels || []) {
      const channel = typeof entry === 'string' ? { pubkey: entry } : entry
      const signer = channel.signer || channel.privateChannelSigner || null
      const readerSigner = channel.readerSigner || channel.privateChannelReaderSigner || signer || null
      const nymSigner = channel.nymSigner || null
      const hasChannelRelays = Boolean(channel.relays?.length)
      const hasChannelSendRelays = Boolean(channel.sendRelays?.length)
      const hasDefaultRelays = Boolean(defaults.relays?.length)
      const pubkey = channel.pubkey || await signer?.getPublicKey?.()
      if (!pubkey) throw new Error('CHANNEL_PUBKEY_REQUIRED')
      if (!signer && !readerSigner) throw new Error('CHANNEL_SIGNER_REQUIRED')
      const mode = channel.mode || defaults.mode || 'leecher'
      if (!signer && storesRecoverySeeds(mode)) throw new Error('PRIVATE_CHANNEL_WRITER_REQUIRED')
      const readerPubkey = channel.readerPubkey || channel.privateChannelReaderPubkey || await readerSigner?.getPublicKey?.() || pubkey
      out.push({
        pubkey,
        signer,
        readerSigner,
        readerPubkey,
        nymSigner,
        relays: uniq(hasChannelRelays ? channel.relays : defaults.relays),
        sendRelays: uniq(hasChannelSendRelays ? channel.sendRelays : []),
        usesNip65WatchRelays: !hasChannelRelays && !hasDefaultRelays,
        mode,
        seeders: uniq(channel.seeders)
      })
    }
    return out
  }

  async readRelayToReceivers (receiverPubkeys) {
    const pubkeys = uniq(receiverPubkeys)
    if (!pubkeys.length) return new Map()
    const relaysByPubkey = await this._getRelaysByPubkey(pubkeys)
    return this._pickRelaysForPubkeys(pubkeys, relaysByPubkey, { relayType: 'read' })
  }

  async readRelaysForPubkey (pubkey) {
    const relaysByPubkey = await this._getRelaysByPubkey([pubkey])
    const readRelays = uniq(relaysByPubkey?.[pubkey]?.read)
    if (readRelays.length) return readRelays
    return relayMapRelays(this._pickRelaysForPubkeys([pubkey], relaysByPubkey, { relayType: 'read' }))
  }

  async recoveryMirrorRelays (channelPubkey) {
    const seeders = this.recoverySeeders(channelPubkey)
    if (!seeders.length) return []
    try {
      return relayMapRelays(await this.readRelayToReceivers(seeders))
    } catch (err) {
      this.onError?.(err)
      return []
    }
  }

  async resolveWatchRelays (channel) {
    if (!channel.usesNip65WatchRelays && channel.relays.length) return channel.relays
    return this.readRelaysForPubkey(this.userPubkey)
  }

  async resolveSendRouting ({ channel, receiverPubkeys, relays, relayToReceivers }) {
    const recoveryRelays = await this.recoveryMirrorRelays(channel.pubkey)
    if (relayToReceivers) return { relayToReceivers, recoveryRelays }
    if (relays?.length) return { relays: uniq(relays), recoveryRelays }
    if (channel.sendRelays.length) return { relays: channel.sendRelays, recoveryRelays }
    if (channel.relays.length) return { relays: channel.relays, recoveryRelays }
    const derived = await this.readRelayToReceivers(receiverPubkeys)
    if (!relayMapRelays(derived).length) throw new Error('NO_RELAYS')
    return { relayToReceivers: derived, recoveryRelays }
  }

  stateKey () {
    return `${this.prefix}:state`
  }

  readState () {
    const state = parseJson(localStorage.getItem(this.stateKey()), { channels: {} })
    if (!isPlainObject(state.channels)) state.channels = {}
    return state
  }

  writeState (state) {
    localStorage.setItem(this.stateKey(), JSON.stringify(state))
  }

  updateChannelState (pubkey, patch) {
    const state = this.readState()
    const current = state.channels[pubkey] || {}
    state.channels[pubkey] = { ...current, ...patch }
    this.writeState(state)
    return state.channels[pubkey]
  }

  removeChannelState (pubkey) {
    const state = this.readState()
    delete state.channels[pubkey]
    this.writeState(state)
  }

  markSeen (pubkey, createdAt = nowSeconds()) {
    const state = this.readState()
    const current = state.channels[pubkey] || {}
    current.lastSeenAt = Math.max(current.lastSeenAt || 0, createdAt || 0)
    state.channels[pubkey] = current
    this.writeState(state)
  }

  knownSeeders (pubkey) {
    const channel = this.channels.get(pubkey)
    if (channel?.seeders?.length) return channel.seeders
    const activity = this.readState().channels[pubkey]?.seederActivity || {}
    return Object.keys(activity)
  }

  recoverySeeders (pubkey) {
    const channel = this.channels.get(pubkey)
    const configuredSeeders = channel?.seeders || []
    if (configuredSeeders.length) return configuredSeeders.filter(seeder => seeder !== this.userPubkey)

    const activity = this.readState().channels[pubkey]?.seederActivity || {}
    const cutoff = nowSeconds() - this.seederOnlineSeconds
    return Object.entries(activity)
      .filter(([seeder, entry]) => seeder !== this.userPubkey && (entry.lastActiveAt || 0) >= cutoff)
      .sort((a, b) => (b[1].lastActiveAt || 0) - (a[1].lastActiveAt || 0))
      .slice(0, this.maxDynamicRecoverySeeders)
      .map(([seeder]) => seeder)
  }

  markSeederActive (channelPubkey, seederPubkey, { announced = false, at = nowSeconds() } = {}) {
    const state = this.readState()
    const current = state.channels[channelPubkey] || {}
    const activity = current.seederActivity || {}
    const entry = activity[seederPubkey] || {}
    activity[seederPubkey] = {
      ...entry,
      firstSeenAt: entry.firstSeenAt || at,
      lastActiveAt: Math.max(entry.lastActiveAt || 0, at)
    }
    if (announced) activity[seederPubkey].announcedAt = at
    current.seederActivity = activity
    state.channels[channelPubkey] = current
    this.writeState(state)
  }

  trackSeederActivity (channelPubkey, message) {
    const senderPubkey = message.event?.pubkey
    if (!senderPubkey) return false

    const channel = this.channels.get(channelPubkey)
    if (!channel) return false

    const activity = this.readState().channels[channelPubkey]?.seederActivity || {}
    const isPresence = messageCode(message) === SEEDER_PRESENCE_CODE
    const configuredSeeders = channel.seeders || []
    const isConfiguredSeeder = configuredSeeders.includes(senderPubkey)
    const isKnownDynamicSeeder = Boolean(activity[senderPubkey])
    const at = messageTime(message)

    if (isPresence) {
      if (configuredSeeders.length && !isConfiguredSeeder) return false
      this.markSeederActive(channelPubkey, senderPubkey, { announced: true, at })
      return true
    }

    if (!isConfiguredSeeder && !isKnownDynamicSeeder) return false
    this.markSeederActive(channelPubkey, senderPubkey, { at })
    return true
  }

  contentKeyStatus (contentKeyPubkey) {
    if (!contentKeyPubkey) return 'none'
    return contentKeyPubkey === this.contentKeyPubkey ? 'known' : 'unknown'
  }

  handleContentKeyUsage (channelPubkey, usage) {
    const direction = usage.direction === 'sent' ? 'sent' : 'received'
    const contentKeyPubkey = usage.contentKeyPubkey || ''
    const state = this.readState()
    const current = state.channels[channelPubkey] || {}
    const contentKeyUsage = current.contentKeyUsage || {}
    const previous = contentKeyUsage[direction] || null

    const contentKeyStatus = this.contentKeyStatus(contentKeyPubkey)
    if (
      previous &&
      (previous.contentKeyPubkey || '') === contentKeyPubkey &&
      previous.contentKeyStatus === contentKeyStatus
    ) {
      return false
    }
    const event = {
      type: 'content-key-change',
      channelPubkey,
      direction,
      keyRole: usage.keyRole || (direction === 'sent' ? 'sender' : 'receiver'),
      contentKeyPubkey,
      hasContentKey: Boolean(contentKeyPubkey),
      contentKeyStatus,
      previousContentKeyPubkey: previous?.contentKeyPubkey ?? null,
      previousContentKeyStatus: previous?.contentKeyStatus ?? null,
      senderPubkey: usage.senderPubkey || '',
      receiverPubkey: usage.receiverPubkey || '',
      receiverPubkeys: usage.receiverPubkeys || [],
      counterpartyPubkey: direction === 'sent' ? (usage.receiverPubkey || '') : (usage.senderPubkey || ''),
      isBroadcast: Boolean(usage.isBroadcast),
      outerId: usage.outer?.id || '',
      outerCreatedAt: usage.outer?.created_at || 0,
      routerPubkey: usage.router?.pubkey || '',
      routerCreatedAt: usage.router?.created_at || 0
    }

    contentKeyUsage[direction] = {
      contentKeyPubkey,
      contentKeyStatus,
      changedAt: nowSeconds(),
      senderPubkey: event.senderPubkey,
      receiverPubkey: event.receiverPubkey,
      isBroadcast: event.isBroadcast
    }
    current.contentKeyUsage = contentKeyUsage
    state.channels[channelPubkey] = current
    this.writeState(state)
    this.onContentKeyChange?.(event)
    return true
  }

  addOfflineRange (pubkey, start, end) {
    const now = nowSeconds()
    const minStart = now - this.offlineRecoverySeconds
    const normalized = {
      start: Math.max(0, Math.floor(start)),
      end: Math.floor(end)
    }
    if (normalized.end <= normalized.start || normalized.end < minStart) return
    normalized.start = Math.max(normalized.start, minStart)

    const state = this.readState()
    const current = state.channels[pubkey] || {}
    const ranges = (current.offlineRanges || [])
      .filter(range => range.end >= minStart)
      .concat([normalized])
      .sort((a, b) => a.start - b.start)
    current.offlineRanges = mergeRanges(ranges)
    state.channels[pubkey] = current
    this.writeState(state)
  }

  closeOpenOfflineRanges () {
    const state = this.readState()
    const end = nowSeconds()
    const minStart = end - this.offlineRecoverySeconds
    for (const pubkey of Object.keys(state.channels)) {
      const current = state.channels[pubkey]
      if (!current.openOfflineStart) continue
      const start = Math.max(minStart, Math.max(0, current.openOfflineStart))
      if (end > start) {
        current.offlineRanges = mergeRanges((current.offlineRanges || []).concat([{ start, end }]))
      }
      delete current.openOfflineStart
      state.channels[pubkey] = current
    }
    this.writeState(state)
  }

  async watch (channels = [...this.channels.keys()], { scheduleReloadGap = true } = {}) {
    const channelPubkeys = uniq(channels)
    for (const pubkey of channelPubkeys) {
      const channel = this.channels.get(pubkey)
      if (!channel) throw new Error('UNKNOWN_CHANNEL')
      const watchRelays = await this.resolveWatchRelays(channel)
      const stop = await this._privateMessage.watch({
        channels: [pubkey],
        relays: watchRelays,
        receiverSigner: this.userSigner,
        iykcSigner: this.contentKeySigner,
        privateChannelSigner: channel.signer,
        privateChannelReaderSigner: channel.readerSigner,
        privateChannelReaderPubkey: channel.readerPubkey,
        mode: channel.mode,
        onAsk: message => this.handleAsk(pubkey, message),
        onReply: message => this.handleReply(pubkey, message),
        onTell: message => this.handleTell(pubkey, message),
        onYell: message => this.handleYell(pubkey, message),
        onNym: message => this.handleNym(pubkey, message),
        onMessage: message => this.handleMessage(pubkey, message),
        onSeed: seed => this.enqueueSeed(pubkey, seed),
        onContentKeyUsage: usage => this.handleContentKeyUsage(pubkey, usage),
        receivedChunkTtlMs: this.offlineRecoverySeconds * 1000,
        onError: err => this.onError?.(err)
      })
      this.stopByChannel.set(pubkey, stop)
      this.updateChannelState(pubkey, {
        lastWatchedAt: nowSeconds(),
        mode: channel.mode,
        relays: watchRelays,
        seeders: channel.seeders
      })
      this.debug('watch', {
        channelPubkey: pubkey,
        relays: watchRelays,
        mode: channel.mode,
        seeders: channel.seeders,
        seederCount: channel.seeders.length
      })
      if (scheduleReloadGap) this.scheduleReloadGap(pubkey)
    }
    this.ensureNetworkWatchers()
    this.ensureRelayListWatcher()
    return this
  }

  unwatch (channels) {
    const channelPubkeys = channels ? uniq(Array.isArray(channels) ? channels : [channels]) : [...this.stopByChannel.keys()]
    for (const pubkey of channelPubkeys) {
      this.stopByChannel.get(pubkey)?.()
      this.stopByChannel.delete(pubkey)
      this.stopPresencePublisher(pubkey)
    }
    this.ensureRelayListWatcher()
  }

  nip65WatchChannelPubkeys () {
    return [...this.channels.values()]
      .filter(channel => channel.usesNip65WatchRelays && this.stopByChannel.has(channel.pubkey))
      .map(channel => channel.pubkey)
  }

  ensureRelayListWatcher () {
    const channelPubkeys = this.nip65WatchChannelPubkeys()
    if (!channelPubkeys.length) {
      this.stopRelayListWatcher?.()
      this.stopRelayListWatcher = null
      this.relayListWatcherPubkey = ''
      return
    }
    if (this.stopRelayListWatcher && this.relayListWatcherPubkey === this.userPubkey) return
    this.stopRelayListWatcher?.()
    if (typeof window === 'undefined' && this._subscribeRelayListUpdates === subscribeRelayListUpdates) return
    this.relayListWatcherPubkey = this.userPubkey
    this.stopRelayListWatcher = this._subscribeRelayListUpdates([this.userPubkey], {
      relayType: 'read',
      onChange: () => this.refreshNip65WatchRelays()
    })
  }

  refreshNip65WatchRelays () {
    if (!this.relayListRefreshPromise) {
      this.relayListRefreshPromise = Promise.resolve()
        .then(() => this.refreshNip65WatchRelaysNow())
        .catch(err => this.onError?.(err))
        .finally(() => { this.relayListRefreshPromise = null })
    }
    return this.relayListRefreshPromise
  }

  async refreshNip65WatchRelaysNow () {
    const channelPubkeys = this.nip65WatchChannelPubkeys()
    if (!channelPubkeys.length) {
      this.ensureRelayListWatcher()
      return
    }
    const until = nowSeconds()
    for (const pubkey of channelPubkeys) {
      const lastSeenAt = this.readState().channels[pubkey]?.lastSeenAt
      if (lastSeenAt) this.addOfflineRange(pubkey, Math.max(0, lastSeenAt - this.offlineSkewSeconds), until)
    }
    await this.watch(channelPubkeys, { scheduleReloadGap: false })
    await this.recoverOfflineRanges(channelPubkeys)
  }

  async handleAsk (channelPubkey, message) {
    try {
      this.trackSeederActivity(channelPubkey, message)
      if (storesRecoverySeeds(this.channels.get(channelPubkey)?.mode) && messageCode(message) === MISSING_MESSAGES_ASK_CODE) {
        await this.replyWithStoredSeeds(channelPubkey, message)
        return
      }
      this.enqueueRumor('ask', channelPubkey, message)
    } catch (err) {
      console.warn('private-messenger ask handling failed', err?.message ?? err)
    }
  }

  async handleReply (channelPubkey, message) {
    try {
      this.trackSeederActivity(channelPubkey, message)
      if (messageCode(message) === MISSING_MESSAGES_REPLY_CODE) {
        await this.consumeMissingMessagesReply(channelPubkey, message)
        return
      }
      this.enqueueRumor('reply', channelPubkey, message)
    } catch (err) {
      console.warn('private-messenger reply handling failed', err?.message ?? err)
    }
  }

  handleTell (channelPubkey, message) {
    this.trackSeederActivity(channelPubkey, message)
    this.enqueueRumor('tell', channelPubkey, message)
  }

  handleYell (channelPubkey, message) {
    this.trackSeederActivity(channelPubkey, message)
    if (messageCode(message) === SEEDER_PRESENCE_CODE) return
    this.enqueueRumor('yell', channelPubkey, message)
  }

  handleNym (channelPubkey, message) {
    this.enqueueRumor('nym', channelPubkey, message)
  }

  handleMessage (channelPubkey, message) {
    if (eventType(message.event) !== 'message') return
    this.trackSeederActivity(channelPubkey, message)
    this.enqueueRumor('message', channelPubkey, message)
  }

  enqueueRumor (type, channelPubkey, message) {
    const channel = this.channels.get(channelPubkey)
    if (channel?.mode === 'watchtower' && type !== 'ask') return
    this.markSeen(channelPubkey, message.outer?.created_at || message.event?.created_at || nowSeconds())
    const eventId = message.event?.id || ''
    if (eventId && this.queue.some(item => item.channelPubkey === channelPubkey && item.type === type && item.event?.id === eventId)) {
      this.debug('dedupe', debugMessageInfo(type, channelPubkey, message))
      return
    }
    this.queue.enqueue({
      type,
      channelPubkey,
      receivedAt: nowSeconds(),
      event: message.event,
      payload: message.payload,
      question: message.question || null,
      questionId: message.questionId || null,
      outer: message.outer || null,
      meta: message.meta || null
    })
    this.debug('enqueue', debugMessageInfo(type, channelPubkey, message))
    this.onMessageQueued?.()
  }

  enqueueSeed (channelPubkey, seed) {
    const receivedAt = nowSeconds()
    if (seed.recordType === NYM_CARRIER_SEED_RECORD_TYPE || seed.carriers?.length) {
      const carriers = compactSeedNymCarriers(seed.carriers)
      this.markSeen(channelPubkey, nymCarrierRecordTime({ carriers }) || seed.outer?.created_at || receivedAt)
      const seedKey = nymCarrierSeedKey({ channelPubkey, carriers })
      if (seedKey && this.seedQueue.some(item => item.recordType === NYM_CARRIER_SEED_RECORD_TYPE && nymCarrierSeedKey(item) === seedKey)) return
      this.seedQueue.enqueue({
        type: 'seed',
        recordType: NYM_CARRIER_SEED_RECORD_TYPE,
        channelPubkey,
        receivedAt,
        carriers,
        meta: { channelPubkey: seed.channelPubkey }
      })
      this.pruneStoredSeeds(channelPubkey)
      return
    }

    const rows = compactSeedRouterRows(seed)
    let newest = seed.outer?.created_at || receivedAt
    for (const row of rows) {
      const rowTime = row.lastSeenAt || row.router?.created_at || receivedAt
      newest = Math.max(newest, rowTime)
      const rowKey = routerSeedRowKey({ ...row, channelPubkey })
      let previous = null
      this.seedQueue.removeWhere(item => {
        if (item.recordType !== ROUTER_SEED_RECORD_TYPE) return false
        if (routerSeedRowKey(item) !== rowKey) return false
        previous = item
        return true
      })
      const firstSeenAt = Math.min(previous?.firstSeenAt ?? rowTime, row.firstSeenAt ?? rowTime)
      const lastSeenAt = Math.max(previous?.lastSeenAt ?? rowTime, row.lastSeenAt ?? rowTime)
      this.seedQueue.enqueue({
        ...row,
        type: 'seed',
        recordType: ROUTER_SEED_RECORD_TYPE,
        channelPubkey,
        receivedAt,
        firstSeenAt,
        lastSeenAt,
        meta: { channelPubkey: seed.channelPubkey }
      })
    }
    this.markSeen(channelPubkey, newest)
    this.pruneStoredSeeds(channelPubkey)
  }

  messages () {
    // seedQueue is retained replay material for recovery replies, not an app-message stream.
    return this.queue.items()
  }

  nextMessage () {
    // seedQueue is retained replay material for recovery replies, not an app-message stream.
    return this.queue.shift()
  }

  async ask ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkey, relays, relayToReceivers, message, code, payload, error, content }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys: [receiverPubkey], relays, relayToReceivers })
    this.debugSend('ask', channelPubkey, { code, receiverPubkey })
    return this._privateMessage.ask({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkey,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      message,
      code,
      payload,
      error,
      content,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async reply ({ channelPubkey = this.defaultChannelPubkey(), question, receiverPubkey, relays, relayToReceivers, message, code, payload, error, content }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const resolvedReceiverPubkey = receiverPubkey || question?.pubkey || ''
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys: [resolvedReceiverPubkey], relays, relayToReceivers })
    this.debugSend('reply', channelPubkey, { code, receiverPubkey: receiverPubkey || question?.pubkey || '' })
    return this._privateMessage.reply({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      question,
      receiverPubkey,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      message,
      code,
      payload,
      error,
      content,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async tell ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkey, relays, relayToReceivers, message, code, payload, error, content }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys: [receiverPubkey], relays, relayToReceivers })
    this.debugSend('tell', channelPubkey, { code, receiverPubkey })
    return this._privateMessage.tell({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkey,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      message,
      code,
      payload,
      error,
      content,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async yell ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, message, code, payload, error, content }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('yell', channelPubkey, { code, receiverPubkeys })
    return this._privateMessage.yell({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      message,
      code,
      payload,
      error,
      content,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastRumor ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, rumor }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastRumor', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastRumor({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      rumor,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastEvent ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, event }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastEvent', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastEvent({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      event,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastNymRumor ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, rumor, nymSigner }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const resolvedNymSigner = this.requireNymSigner(channel, nymSigner)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastNymRumor', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastNymRumor({
      nymSigner: resolvedNymSigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      rumor
    })
  }

  async broadcastNymEvent ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, event, nymSigner }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const resolvedNymSigner = this.requireNymSigner(channel, nymSigner)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastNymEvent', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastNymEvent({
      nymSigner: resolvedNymSigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      event
    })
  }

  async publishSeederPresence (channelPubkey = this.defaultChannelPubkey()) {
    const channel = this.requireWritableChannel(channelPubkey)
    const receiverPubkeys = uniq([...this.knownSeeders(channelPubkey), this.userPubkey])
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys })
    this.debugSend('yell', channelPubkey, { code: SEEDER_PRESENCE_CODE, receiverPubkeys })
    return this._privateMessage.yell({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.offlineRecoverySeconds,
      temporaryStorageArea: this.temporaryStorageArea,
      code: SEEDER_PRESENCE_CODE,
      payload: {},
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async startPresencePublisher (channelPubkey) {
    if (this.presenceTimers.has(channelPubkey)) return
    try {
      await this.publishSeederPresence(channelPubkey)
    } catch (err) {
      console.warn('private-messenger seeder presence failed', err?.message ?? err)
    }
    const timer = this._setInterval(() => {
      return this.publishSeederPresence(channelPubkey).catch(err => {
        console.warn('private-messenger seeder presence failed', err?.message ?? err)
      })
    }, this.seederPresenceIntervalMs)
    timer?.unref?.()
    this.presenceTimers.set(channelPubkey, timer)
  }

  stopPresencePublisher (channelPubkey) {
    const timer = this.presenceTimers.get(channelPubkey)
    if (timer) this._clearInterval(timer)
    this.presenceTimers.delete(channelPubkey)
  }

  async reconcilePresencePublishers () {
    const starts = []
    for (const pubkey of [...this.presenceTimers.keys()]) {
      if (!storesRecoverySeeds(this.channels.get(pubkey)?.mode)) this.stopPresencePublisher(pubkey)
    }
    for (const [pubkey, channel] of this.channels) {
      if (storesRecoverySeeds(channel.mode)) starts.push(this.startPresencePublisher(pubkey))
      else this.stopPresencePublisher(pubkey)
    }
    await Promise.all(starts)
  }

  createMissingMessageReplyPacker (options) {
    return createMissingMessageReplyPacker({ messenger: this, ...options })
  }

  createEventReplyPacker (options) {
    return createEventReplyPacker({ messenger: this, ...options })
  }

  defaultChannelPubkey () {
    return this.channels.keys().next().value
  }

  requireChannel (pubkey) {
    const channel = this.channels.get(pubkey)
    if (!channel) throw new Error('UNKNOWN_CHANNEL')
    return channel
  }

  requireWritableChannel (pubkey) {
    const channel = this.requireChannel(pubkey)
    if (!channel.signer) throw new Error('PRIVATE_CHANNEL_WRITER_REQUIRED')
    return channel
  }

  requireNymSigner (channel, override) {
    const signer = override || channel?.nymSigner || this.nymSigner
    if (!signer?.getPublicKey) throw new Error('NYM_SIGNER_REQUIRED')
    return signer
  }

  contentKeyLookup () {
    return this.useContentKeys ? undefined : noContentKeys
  }

  scheduleReloadGap (pubkey) {
    const current = this.readState().channels[pubkey]
    const start = current?.openOfflineStart || current?.lastSeenAt
    if (!start) return
    this._setTimeout(async () => {
      this.addOfflineRange(pubkey, Math.max(0, start - this.offlineSkewSeconds), nowSeconds())
      await this.recoverOfflineRanges([pubkey])
    }, this.reloadGapDelayMs)
  }

  ensureNetworkWatchers () {
    if (typeof window === 'undefined') return
    if (!this.stopOffline) {
      const offline = () => {
        const state = this.readState()
        const start = Math.max(0, nowSeconds() - this.offlineSkewSeconds)
        for (const pubkey of this.stopByChannel.keys()) {
          const current = state.channels[pubkey] || {}
          current.openOfflineStart ||= start
          state.channels[pubkey] = current
        }
        this.writeState(state)
      }
      window.addEventListener('offline', offline)
      this.stopOffline = () => window.removeEventListener('offline', offline)
    }
    if (!this.stopOnline) {
      const online = async () => {
        this.closeOpenOfflineRanges()
        await this.watch([...this.stopByChannel.keys()])
        await this.recoverOfflineRanges()
      }
      window.addEventListener('online', online)
      this.stopOnline = () => window.removeEventListener('online', online)
    }
  }

  async askSeedersForMissingRange (channelPubkey, since, until) {
    if (!this.channels.get(channelPubkey)?.signer) return []
    const seeders = this.recoverySeeders(channelPubkey)
    if (!seeders.length || until < since) return []

    const asks = []
    for (const seeder of seeders) {
      try {
        asks.push(await this.ask({
          channelPubkey,
          receiverPubkey: seeder,
          code: MISSING_MESSAGES_ASK_CODE,
          payload: { since, until }
        }))
      } catch (err) {
        console.warn('private-messenger seeder recovery ask failed', seeder, err?.message ?? err)
      }
    }
    return asks
  }

  async askSeedersForRelayLeftEdge (channelPubkey, range, fetchedEvents) {
    const oldest = oldestCreatedAt(fetchedEvents)
    const until = oldest == null ? range.end : Math.min(range.end, oldest)
    if (until < range.start) return []
    return this.askSeedersForMissingRange(channelPubkey, range.start, until)
  }

  async replyWithStoredSeeds (channelPubkey, message) {
    const payload = isPlainObject(message.payload?.payload) ? message.payload.payload : {}
    const since = Number.isFinite(payload.since) ? payload.since : undefined
    const until = Number.isFinite(payload.until) ? payload.until : undefined
    const packer = this.createMissingMessageReplyPacker({
      channelPubkey,
      question: message.event,
      receiverPubkey: message.event?.pubkey,
      since,
      until
    })

    for await (const seed of this.seedQueue.storedItems()) {
      if (seed.channelPubkey !== channelPubkey) continue
      await packer.update(seed)
    }
    await packer.finalize()
  }

  async consumeMissingMessagesReply (channelPubkey, message) {
    const payload = message.payload?.payload
    const jsonl = typeof payload?.jsonl === 'string' ? payload.jsonl : ''
    if (!jsonl) return

    for (const line of splitJsonl(jsonl)) {
      const record = parseJson(line, null)
      if (!record) continue
      const recovered = await this.messageFromBackfillRecord(channelPubkey, record)
      if (!recovered) continue
      this.enqueueRumor(recovered.type, channelPubkey, {
        event: recovered.event,
        outer: recovered.outer,
        meta: { ...(recovered.meta || {}), channelPubkey, recoveredFromSeeder: message.event?.pubkey || '' },
        payload: recovered.payload
      })
    }
  }

  async messageFromBackfillRecord (channelPubkey, record) {
    if (record?.recordType === NYM_CARRIER_SEED_RECORD_TYPE) {
      const event = this._privateChannel.eventFromNymCarriers(record.carriers)
      return {
        type: 'nym',
        event,
        outer: { id: '', created_at: nymCarrierRecordTime(record) },
        meta: { channelPubkey, carriers: record.carriers },
        payload: parseEventContent(event)
      }
    }

    const routerRecord = record?.recordType === ROUTER_SEED_RECORD_TYPE ? record.router : null
    if (!isPrivateChannelRouter(routerRecord)) return null
    if (!this._privateChannel.unwrapEvent) throw new Error('PRIVATE_CHANNEL_UNWRAP_UNSUPPORTED')

    const channel = this.requireChannel(channelPubkey)
    const router = {
      kind: privateChannel.ROUTER_KIND,
      pubkey: routerRecord.pubkey,
      created_at: routerRecord.created_at || nowSeconds(),
      tags: (routerRecord.tags || []).filter(tag => tag[0] !== 'c').concat([['c', '0', '1']]),
      content: routerRecord.content
    }
    const encryptSigner = channel.readerSigner && channel.readerSigner !== channel.signer ? channel.readerSigner : channel.signer
    const encryptPeerPubkey = encryptSigner === channel.signer ? channel.readerPubkey : channelPubkey
    const outer = {
      kind: privateChannel.PRIVATE_BROADCAST_KIND,
      pubkey: channelPubkey,
      created_at: router.created_at,
      tags: [],
      content: await encryptSigner.nip44v3Encrypt(
        encryptPeerPubkey,
        privateChannel.PRIVATE_BROADCAST_KIND,
        '',
        textToBase64(JSON.stringify(router))
      )
    }
    const event = await this._privateChannel.unwrapEvent({
      receiverSigner: this.userSigner,
      iykcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderSigner: channel.readerSigner,
      privateChannelReaderPubkey: channel.readerPubkey,
      event: outer,
      receiverPubkey: this.userPubkey
    })
    if (!event) return null
    return {
      type: eventType(event),
      event,
      outer,
      meta: { channelPubkey },
      payload: parseEventContent(event)
    }
  }

  async recoverOfflineRanges (channels = [...this.stopByChannel.keys()]) {
    const state = this.readState()
    const now = nowSeconds()
    const minStart = now - this.offlineRecoverySeconds

    for (const pubkey of uniq(channels)) {
      const channel = this.channels.get(pubkey)
      const current = state.channels[pubkey]
      if (!channel || !current?.offlineRanges?.length) continue

      const remaining = []
      for (const range of current.offlineRanges) {
        if (range.end < minStart) continue
        try {
          const fetchRelays = await this.resolveWatchRelays(channel)
          const fetchedEvents = await this._privateChannel.fetch({
            receiverSigner: this.userSigner,
            iykcSigner: this.contentKeySigner,
            privateChannelSigner: channel.signer,
            privateChannelReaderSigner: channel.readerSigner,
            privateChannelReaderPubkey: channel.readerPubkey,
            privateChannelPubkeys: [pubkey],
            receiverPubkey: this.userPubkey,
            relays: fetchRelays,
            since: Math.max(0, range.start),
            until: range.end,
            mode: channel.mode,
            modeByPubkey: { [pubkey]: channel.mode },
            receivedChunkTtlMs: this.offlineRecoverySeconds * 1000,
            onEvent: (event, outer, meta) => this.enqueueRumor(eventType(event), pubkey, { event, outer, meta, payload: parseEventContent(event) }),
            onNymEvent: (event, outer, meta) => this.enqueueRumor('nym', pubkey, { event, outer, meta, payload: parseEventContent(event) }),
            onSeedEvent: seed => this.enqueueSeed(pubkey, seed),
            onContentKeyUsage: usage => this.handleContentKeyUsage(pubkey, usage),
            onError: err => { throw err }
          }) || []
          await this.askSeedersForRelayLeftEdge(pubkey, range, fetchedEvents)
        } catch (err) {
          this.onError?.(err)
          remaining.push(range)
        }
      }
      const fresh = this.readState()
      fresh.channels[pubkey] = {
        ...(fresh.channels[pubkey] || {}),
        offlineRanges: remaining
      }
      this.writeState(fresh)
    }
  }

  clearChannel (pubkey) {
    this.unwatch(pubkey)
    this._privateMessage.clearChannelState?.(pubkey)
    this.channels.delete(pubkey)
    this.removeChannelState(pubkey)
    this.queue.removeWhere(item => item.channelPubkey === pubkey)
    this.seedQueue.removeWhere(item => item.channelPubkey === pubkey)
    this.ensureRelayListWatcher()
  }

  clearQueue () {
    this.queue.clear()
  }

  cleanupStaleChannels () {
    if (!this.prefix) return
    const state = this.readState()
    const cutoff = nowSeconds() - this.staleChannelSeconds
    for (const [pubkey, channel] of Object.entries(state.channels)) {
      if ((channel.lastWatchedAt || 0) >= cutoff) continue
      delete state.channels[pubkey]
      this.queue?.removeWhere(item => item.channelPubkey === pubkey)
      this.seedQueue?.removeWhere(item => item.channelPubkey === pubkey)
    }
    this.writeState(state)
  }

  pruneStoredSeeds (channelPubkey) {
    const cutoff = nowSeconds() - this.offlineRecoverySeconds
    this.seedQueue?.removeWhere(item => {
      if (channelPubkey && item.channelPubkey !== channelPubkey) return false
      return (seedRecordTime(item) || item.receivedAt || 0) < cutoff
    })
  }

  close () {
    this.unwatch()
    for (const pubkey of [...this.presenceTimers.keys()]) this.stopPresencePublisher(pubkey)
    this.stopRelayListWatcher?.()
    this.stopRelayListWatcher = null
    this.relayListWatcherPubkey = ''
    this.stopOffline?.()
    this.stopOnline?.()
    this.stopOffline = null
    this.stopOnline = null
  }
}

function mergeRanges (ranges) {
  const out = []
  for (const range of ranges) {
    const last = out[out.length - 1]
    if (!last || range.start > last.end + 1) out.push({ ...range })
    else last.end = Math.max(last.end, range.end)
  }
  return out
}

function eventType (event) {
  if (event.kind === privateMessage.ASK_KIND) return 'ask'
  if (event.kind === privateMessage.REPLY_KIND) return 'reply'
  if (event.kind === privateMessage.TELL_KIND) return event.tags?.some(t => t[0] === 'r') ? 'tell' : 'yell'
  return 'message'
}

function parseEventContent (event) {
  return privateMessage.parseRumorContent(event)
}

function messageCode (message) {
  return isPlainObject(message.payload) && Object.prototype.hasOwnProperty.call(message.payload, 'code')
    ? message.payload.code
    : null
}

function debugMessageInfo (type, channelPubkey, message) {
  return {
    type,
    code: messageCode(message) || '',
    channelPubkey,
    senderPubkey: message.event?.pubkey || '',
    eventId: message.event?.id || '',
    outerId: message.outer?.id || '',
    outerCreatedAt: message.outer?.created_at || message.event?.created_at || 0
  }
}

function messageTime (message) {
  return message.outer?.created_at || message.event?.created_at || nowSeconds()
}

function oldestCreatedAt (events) {
  let oldest = null
  for (const event of events || []) {
    if (!Number.isFinite(event?.created_at)) continue
    oldest = oldest == null ? event.created_at : Math.min(oldest, event.created_at)
  }
  return oldest
}

function relayMapRelays (relayToReceivers) {
  if (!relayToReceivers) return []
  const entries = relayToReceivers instanceof Map ? relayToReceivers.entries() : Object.entries(relayToReceivers)
  return uniq([...entries].map(([relay]) => relay))
}

function isPrivateChannelRouter (event) {
  return event?.kind === privateChannel.ROUTER_KIND &&
    typeof event.content === 'string' &&
    event.tags?.some(tag => tag[0] === 'c')
}

function nymCarrierRecordTime (record) {
  return record?.carriers?.reduce((max, carrier) => Math.max(max, carrier.created_at || 0), 0) || 0
}

function nymCarrierSeedKey (record) {
  const carriers = record?.carriers || []
  if (!carriers.length) return ''
  const ids = carriers.map(carrier => carrier.id || '').join(',')
  return `${record.channelPubkey || ''}:${carriers[0]?.pubkey || ''}:${ids}`
}

function seedRecordTime (record) {
  if (record?.recordType === NYM_CARRIER_SEED_RECORD_TYPE || record?.carriers?.length) return nymCarrierRecordTime(record)
  if (record?.recordType === ROUTER_SEED_RECORD_TYPE) return record.lastSeenAt || record.router?.created_at || 0
  return record?.router?.created_at || 0
}

function splitJsonl (jsonl) {
  return String(jsonl || '').split('\n').filter(Boolean)
}

export async function createPrivateMessenger (options) {
  return new PrivateMessenger(options).init(options)
}
