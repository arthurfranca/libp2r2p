// Expected use:
// const messenger = await createPrivateMessenger({
//   userSigner,
//   contentKeySigner, // optional when userSigner handles content keys internally
//   nymSigner: optionalDefaultNymSigner,
//   staleChannelSeconds: optionalStaleChannelRetention,
//   identityStorageRetentionSeconds: optionalIdentityStorageRetention,
//   channels: [{ signer: privateChannelSigner, relays, mode: 'leecher', seeders: optionalSeederPubkeys, offlineRecoverySeconds: optionalChannelOverride }],
//   onContentKeyChange: event => reviewContentKeyUse(event),
//   onError: err => reportPrivateMessengerError(err)
// })
//
// Channel roles:
// - Default channel: { signer } signs, publishes, and decrypts the outer router with the same channel key.
// - Split reader channel: { signer, readerPubkey } signs as the channel key but encrypts/decrypts the outer router with the reader pubkey.
// - Reader-secret channel: { signer, readerSigner } is also valid; the reader signer decrypts the router.
// - Reader-only channel: { pubkey, readerSigner } can watch/fetch/drain messages but cannot send or seed recovery replies.
// for await (const { message, ack } of messenger.messages()) {
//   await persistPrivateMessage(message)
//   await ack()
// }
// await messenger.ask({ receiverPubkey, payload: { ping: true } })
// await messenger.reply({ question: msg.question, payload: { ok: true } })
// await messenger.tell({ receiverPubkey, payload: { note: 'hello' } })
// await messenger.yell({ receiverPubkeys, payload: { notice: 'hello all' } })
// await messenger.broadcastRumor({ receiverPubkeys, rumor: { kind, tags: [], content } })
// await messenger.broadcastEvent({ receiverPubkeys, event: signedNostrEvent })
// await messenger.broadcastNymRumor({ rumor: { kind, tags: [], content } })
// await messenger.broadcastNymEvent({ event: signedNostrEvent })
// await messenger.update({ channels: [{ signer: privateChannelSigner, relays, seeders: nextOptionalSeederPubkeys }] })
// await messenger.clearChannel(channelPubkey)
//
// Missed-message recovery:
// - Each watched channel stores lastSeenAt/lastWatchedAt in IndexedDB.
// - First watch scans the recovery window; reload resumes persisted gaps/checkpoints.
// - Browser offline/online events add explicit offline ranges with a small skew.
// - Recovery defaults to 7 days and can be overridden per channel; zero disables durable recovery.
// - Channel state not actively leased or watched within the configured stale window is pruned.
// - Seeders announce presence every 10min and are used for the relay-uncovered left edge of a missed range.
// - Configured seeders are all asked; auto-discovered seeders are capped to the 8 most recently active.
// - Seeder/watchtower channels store reconstructed router events in a separate IndexedDB queue and auto-reply to recovery asks.
// - Seeder replies stream compact routers with createMissingMessageReplyPacker({ messenger, question }).update(seed), then finalize(optionalLastSeed).
// - For other event-list replies, use createEventReplyPacker({ messenger, question, code }).update(event).

import { createAbortableSemaphore } from '../helpers/abortable-semaphore.js'
import { mergeRanges, subtractRanges, intersectRanges } from '../helpers/ranges.js'
import * as privateMessage from '../private-message/index.js'
import { isOnline, onOnline } from '../network/index.js'
import { isRetryableRelayFailure, isReplaceableRelayFailure } from '../relay/index.js'
import { createPauseRecovery, isRecoverableStorageFailure } from './helpers/pause-recovery.js'
import { backgroundSigner } from './helpers/background-signer.js'
import { createSendRelayRouting, normalizeFallbackRelays, normalizeFallbackDelay } from './helpers/send-routing.js'
import { incompleteFetchError } from '../private-channel/helpers/fetch-error.js'
import { recoveryRetryDelay, isPermanentRecoveryError } from './helpers/recovery-retry.js'
import { normalizeRelayUrl } from '../url/index.js'
import { deliveryInfo } from '../private-channel/helpers/rumor.js'
import { bytesToBase64 } from '../base64/index.js'
import { ValidationError } from '../error/index.js'
import { getRelaysByPubkey, pickRelaysForPubkeys, subscribeRelayListUpdates, relayPool } from '../relay/index.js'
import * as privateChannel from '../private-channel/index.js'
import { DEFAULT_RECEIVED_CHUNK_TTL_MS } from '../private-channel/services/received-chunks.js'
import { createQueue } from '../idb-queue/index.js'
import { createChannelStateStore } from './services/channel-state.js'
import { DEFAULT_STALE_CHANNEL_SECONDS } from './constants/index.js'
import {
  activatePrivateMessengerStorage,
  DEFAULT_IDENTITY_STORAGE_RETENTION_SECONDS,
  maintainPrivateMessengerStorage,
  PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS,
  PRIVATE_MESSENGER_STORAGE_MAINTENANCE_MS,
  readPrivateMessengerStorage,
  releasePrivateMessengerStorage
} from './services/storage-maintenance.js'
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

export { DEFAULT_STALE_CHANNEL_SECONDS } from './constants/index.js'
export { DEFAULT_IDENTITY_STORAGE_RETENTION_SECONDS } from './services/storage-maintenance.js'
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
const MAX_OFFLINE_RECOVERY_SECONDS = Math.floor(Number.MAX_SAFE_INTEGER / 1000)
const STORAGE_POLICY_BROADCAST_CHANNEL = 'libp2r2p:private-messenger:storage-policy'
const DEFAULT_OFFLINE_SKEW_SECONDS = 30
const DEFAULT_RELOAD_GAP_DELAY_MS = 500
const DEFAULT_SEEDER_PRESENCE_INTERVAL_MS = 10 * 60 * 1000
const DEFAULT_SEEDER_ONLINE_SECONDS = 20 * 60
const DEFAULT_MAX_DYNAMIC_RECOVERY_SEEDERS = 8
const DEFAULT_MESSAGE_QUEUE_MAX_BYTES = 16 * 1024 * 1024 // 16 MiB
const DEFAULT_SEED_QUEUE_MAX_BYTES = 64 * 1024 * 1024 // 64 MiB
const SEED_KEY = '__p2r2pSeedKey'
const SEED_TIME = '__p2r2pSeedTime'
const MESSAGE_QUEUE_INDEXES = {
  byChannel: 'channelPubkey',
  byChannelTypeEventId: {
    keyPath: ['channelPubkey', 'type', 'event.id', 'provenance'],
    unique: true
  }
}
const SEED_QUEUE_INDEXES = {
  byChannel: 'channelPubkey',
  bySeedKey: { keyPath: SEED_KEY, unique: true },
  byChannelTime: ['channelPubkey', SEED_TIME],
  byTime: SEED_TIME
}
const encoder = new TextEncoder()
const noContentKeys = async () => ({})

export const RECOVERY_RELAY_RETRY_LIMITS = Object.freeze({
  partial: 3,
  emptyEose: 6,
  allFailed: 10
})
const RECOVERY_ONLINE_CACHE_MS = 5000
const DEFAULT_PRIORITY_TAIL_SECONDS = 6 * 3600
const DEFAULT_PRIORITY_TTL_MS = 10 * 60 * 1000
const DEFAULT_PRIORITY_HEDGE_DELAY_MS = 500
const DEFAULT_PRIORITY_HEDGE_RELAYS = 2

function textToBase64 (text) {
  return bytesToBase64(encoder.encode(text))
}

function defaultOnError (err) {
  console.warn('private-messenger failed', err?.message ?? err)
}

function nowSeconds () {
  return Math.floor(Date.now() / 1000)
}

function normalizeOfflineRecoverySeconds (value) {
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_OFFLINE_RECOVERY_SECONDS) {
    throw new ValidationError('INVALID_OFFLINE_RECOVERY_SECONDS')
  }
  return value
}

function normalizeStaleChannelSeconds (value) {
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_OFFLINE_RECOVERY_SECONDS) {
    throw new ValidationError('INVALID_STALE_CHANNEL_SECONDS')
  }
  return value
}

function normalizeIdentityStorageRetentionSeconds (value) {
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_OFFLINE_RECOVERY_SECONDS) {
    throw new ValidationError('INVALID_IDENTITY_STORAGE_RETENTION_SECONDS')
  }
  return value
}

function uniq (values) {
  return [...new Set((values || []).filter(Boolean))]
}

function normalizeAutoDeletionCapability (value) {
  if (typeof value !== 'boolean') throw new ValidationError('AUTO_DELETION_CAPABILITY_BOOLEAN_REQUIRED')
  return value
}

function isPlainObject (value) {
  return value && typeof value === 'object' && !Array.isArray(value)
}

function parseJson (raw, fallback) {
  try { return JSON.parse(raw || '') } catch { return fallback }
}

function areStateValuesEqual (left, right) {
  return JSON.stringify(left) === JSON.stringify(right)
}

function doesModeStoreRecoverySeeds (mode) {
  return mode === 'seeder' || mode === 'watchtower'
}

function randomStorageLeaseId () {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID()
  const bytes = globalThis.crypto?.getRandomValues?.(new Uint8Array(16))
  if (bytes) return [...bytes].map(value => value.toString(16).padStart(2, '0')).join('')
  return `${Date.now().toString(36)}:${Math.random().toString(36).slice(2)}`
}

export class PrivateMessenger {
  static maintainStorage ({
    indexedDB = globalThis.indexedDB,
    temporaryStorageArea = globalThis.sessionStorage
  } = {}) {
    return maintainPrivateMessengerStorage({ indexedDB, temporaryStorageArea })
  }

  constructor ({
    fallbackRelays = [],
    fallbackDelayMs = null,
    offlineRecoverySeconds = DEFAULT_OFFLINE_RECOVERY_SECONDS,
    staleChannelSeconds = DEFAULT_STALE_CHANNEL_SECONDS,
    identityStorageRetentionSeconds = DEFAULT_IDENTITY_STORAGE_RETENTION_SECONDS,
    offlineSkewSeconds = DEFAULT_OFFLINE_SKEW_SECONDS,
    reloadGapDelayMs = DEFAULT_RELOAD_GAP_DELAY_MS,
    seederPresenceIntervalMs = DEFAULT_SEEDER_PRESENCE_INTERVAL_MS,
    seederOnlineSeconds = DEFAULT_SEEDER_ONLINE_SECONDS,
    priorityTailSeconds = DEFAULT_PRIORITY_TAIL_SECONDS,
    priorityTtlMs = DEFAULT_PRIORITY_TTL_MS,
    priorityHedgeDelayMs = DEFAULT_PRIORITY_HEDGE_DELAY_MS,
    priorityHedgeRelays = DEFAULT_PRIORITY_HEDGE_RELAYS,
    maxDynamicRecoverySeeders = DEFAULT_MAX_DYNAMIC_RECOVERY_SEEDERS,
    messageQueueMaxBytes = DEFAULT_MESSAGE_QUEUE_MAX_BYTES,
    seedQueueMaxBytes = DEFAULT_SEED_QUEUE_MAX_BYTES,
    seedStorage,
    temporaryStorageArea = globalThis.sessionStorage,
    autoDeletionCapability = true,
    _indexedDB = globalThis.indexedDB,
    useContentKeys = true,
    onContentKeyChange,
    onMessageQueued,
    onStateChanged,
    onDebug,
    onError = defaultOnError,
    _privateMessage = privateMessage,
    _privateChannel = privateChannel,
    _getRelaysByPubkey = getRelaysByPubkey,
    _pickRelaysForPubkeys = pickRelaysForPubkeys,
    _subscribeRelayListUpdates = subscribeRelayListUpdates,
    _isOnline = isOnline,
    _waitForForeground,
    _onOnline = onOnline,
    _random = Math.random,
    _setTimeout = globalThis.setTimeout.bind(globalThis),
    _clearTimeout = globalThis.clearTimeout.bind(globalThis),
    _setInterval = globalThis.setInterval.bind(globalThis),
    _clearInterval = globalThis.clearInterval.bind(globalThis),
    _storageSetInterval = globalThis.setInterval.bind(globalThis),
    _storageClearInterval = globalThis.clearInterval.bind(globalThis),
    _BroadcastChannel = _indexedDB === globalThis.indexedDB ? globalThis.BroadcastChannel : undefined
  } = {}) {
    if (onStateChanged != null && typeof onStateChanged !== 'function') throw new ValidationError('INVALID_ON_STATE_CHANGED')
    this.fallbackRelays = normalizeFallbackRelays(fallbackRelays)
    this.fallbackDelayMs = normalizeFallbackDelay(fallbackDelayMs)
    this.offlineRecoverySeconds = normalizeOfflineRecoverySeconds(offlineRecoverySeconds)
    this.staleChannelSeconds = normalizeStaleChannelSeconds(staleChannelSeconds)
    this.identityStorageRetentionSeconds = normalizeIdentityStorageRetentionSeconds(identityStorageRetentionSeconds)
    this.offlineSkewSeconds = offlineSkewSeconds
    this.reloadGapDelayMs = reloadGapDelayMs
    this.seederPresenceIntervalMs = seederPresenceIntervalMs
    this.seederOnlineSeconds = seederOnlineSeconds
    this.priorityTailSeconds = Number.isFinite(priorityTailSeconds) && priorityTailSeconds >= 0
      ? Math.floor(priorityTailSeconds)
      : DEFAULT_PRIORITY_TAIL_SECONDS
    this.priorityTtlMs = Number.isFinite(priorityTtlMs) && priorityTtlMs > 0
      ? Math.floor(priorityTtlMs)
      : DEFAULT_PRIORITY_TTL_MS
    this.priorityHedgeDelayMs = Number.isFinite(priorityHedgeDelayMs) && priorityHedgeDelayMs >= 0
      ? Math.floor(priorityHedgeDelayMs)
      : DEFAULT_PRIORITY_HEDGE_DELAY_MS
    const hedgeRelays = Math.floor(Number(priorityHedgeRelays))
    this.priorityHedgeRelays = Number.isSafeInteger(hedgeRelays) && hedgeRelays >= 1
      ? Math.min(hedgeRelays, 3)
      : DEFAULT_PRIORITY_HEDGE_RELAYS
    this.maxDynamicRecoverySeeders = maxDynamicRecoverySeeders
    this.messageQueueMaxBytes = messageQueueMaxBytes
    this.seedQueueMaxBytes = seedQueueMaxBytes
    this.seedStorage = seedStorage
    this.temporaryStorageArea = temporaryStorageArea
    this.autoDeletionCapability = normalizeAutoDeletionCapability(autoDeletionCapability)
    this._indexedDB = _indexedDB
    this.useContentKeys = useContentKeys
    this.onContentKeyChange = onContentKeyChange
    this.onStateChanged = onStateChanged
    this.statusFingerprint = null
    this.automaticPauses = new Set()
    this.pendingStorageWrites = new Map()
    this.pauseRecoveries = new Map()
    this.watchRecoveries = new Map()
    this.pauseOnlineWork = null
    this.pauseOnlineController = null
    this._onOnline = _onOnline
    this._random = _random
    this.onMessageQueued = onMessageQueued
    this.onDebug = onDebug
    this.onError = onError
    this._privateMessage = _privateMessage.createPrivateMessageSession?.() ?? _privateMessage
    this._privateChannel = _privateChannel
    this._getRelaysByPubkey = _getRelaysByPubkey
    this._pickRelaysForPubkeys = _pickRelaysForPubkeys
    this._subscribeRelayListUpdates = _subscribeRelayListUpdates
    this._isOnline = _isOnline
    this._waitForForeground = _waitForForeground
    this.sendRelayExclusions = new Map()
    this.sendRoutingLifetime = new AbortController()
    this.sendRoutingPause = new AbortController()
    this._setTimeout = _setTimeout
    this._clearTimeout = _clearTimeout
    this._setInterval = _setInterval
    this._clearInterval = _clearInterval
    this._storageSetInterval = _storageSetInterval
    this._storageClearInterval = _storageClearInterval
    this._BroadcastChannel = _BroadcastChannel

    this.userSigner = null
    this.contentKeySigner = null
    this.nymSigner = null
    this.userPubkey = ''
    this.contentKeyPubkey = ''
    this.prefix = ''
    this.queue = null
    this.seedQueue = null
    this.stateStore = null
    this.state = { channels: {} }
    this.stateWriteTail = Promise.resolve()
    this.stateWriteError = null
    this.extensions = new Set()
    this.channels = new Map()
    this.stopByChannel = new Map()
    this.desiredChannels = new Set()
    this.pauseReasons = new Set()
    this.deliveries = new Set()
    this.deliveryReads = new Set()
    this.deliveryWaiters = new Set()
    this.recoveries = new Map()
    this.recoveryAdmission = createAbortableSemaphore(2)
    this.recoveryControllers = new Set()
    this.recoveryRanges = new Map()
    this.recoveryPriority = new Map()
    this.recoveryOnline = { at: 0, value: true }
    this.channelBackground = new Map()
    // At most one queued job exists per configured channel. Keep setup below
    // foreground demand; history has its own independent admission limits.
    this.channelSetupAdmission = createAbortableSemaphore(3, () => {}, { maxQueued: Infinity })
    this.presenceAdmission = createAbortableSemaphore(1, () => {}, { maxQueued: Infinity })
    this.updateTail = Promise.resolve()
    this.pauseRevisions = new Map()
    this.capacityTimer = null
    this.capacityCheck = null
    this.capacityRequiredBytes = 0
    this.reloadGapTimers = new Map()
    this.liveInterruptions = new Map()
    this.liveRecoveryTimers = new Map()
    this.watchRevisionByChannel = new Map()
    this.presenceTimers = new Map()
    this.presenceJobs = new Map()
    this.stopRelayListWatcher = null
    this.relayListWatcherPubkey = ''
    this.relayListRefreshPromise = null
    this.stopOnline = null
    this.stopOffline = null
    this.queueOperationTail = Promise.resolve()
    this.storageActive = false
    this.storageLeaseId = randomStorageLeaseId()
    this.lastStorageTouch = 0
    this.storageTouchPromise = null
    this.storageHeartbeatTimer = null
    this.storageMaintenanceTimer = null
    this.storageMaintenancePromise = null
    this.storagePolicyRevision = 0
    this.storagePolicyNeedsApply = false
    this.storagePolicyBroadcast = null
    this.storagePolicyRefreshTail = Promise.resolve()
    this.closePromise = null
    this.initSettledPromise = null
    this.initialized = false
    this.notifyStatus()
  }

  async init ({ userSigner, contentKeySigner, nymSigner, channels = [], relays = [], mode = 'leecher' }) {
    if (!userSigner?.getPublicKey) throw new ValidationError('USER_SIGNER_REQUIRED')
    this.assertOpen()
    if (this.initSettledPromise) throw new Error('PRIVATE_MESSENGER_INIT_IN_PROGRESS')
    if (this.initialized) throw new Error('PRIVATE_MESSENGER_ALREADY_INITIALIZED')
    let settleInit
    const initSettledPromise = new Promise(resolve => { settleInit = resolve })
    this.initSettledPromise = initSettledPromise
    try {
      this.userSigner = userSigner
      this.contentKeySigner = contentKeySigner || null
      this.nymSigner = nymSigner || null
      this.userPubkey = await userSigner.getPublicKey()
      this.prefix = `libp2r2p:private-messenger:${this.userPubkey}`
      const storageSnapshot = await activatePrivateMessengerStorage({
        userPubkey: this.userPubkey,
        leaseId: this.storageLeaseId,
        activeChannelPubkeys: [],
        storagePolicy: {
          staleChannelSeconds: this.staleChannelSeconds,
          identityStorageRetentionSeconds: this.identityStorageRetentionSeconds
        },
        indexedDB: this._indexedDB
      })
      this.applyStoragePolicySnapshot(storageSnapshot)
      this.storageActive = true
      this.lastStorageTouch = Date.now()
      this.startStoragePolicyBroadcast()
      this.assertOpen()
      await PrivateMessenger.maintainStorage({
        indexedDB: this._indexedDB,
        temporaryStorageArea: this.temporaryStorageArea
      })
      this.assertOpen()
      this.contentKeyPubkey = await this.contentKeySigner?.getPublicKey?.() || ''
      this.assertOpen()
      this.queue = await createQueue({
        prefix: this.prefix,
        indexes: MESSAGE_QUEUE_INDEXES,
        maxBytes: this.messageQueueMaxBytes,
        evictionPolicy: 'reject',
        indexedDB: this._indexedDB
      })
      this.assertOpen()
      this.seedQueue = this.seedStorage
        ? null
        : await createQueue({
          prefix: `${this.prefix}:seeds`,
          indexes: SEED_QUEUE_INDEXES,
          maxBytes: this.seedQueueMaxBytes,
          evictionPolicy: 'fifo',
          indexedDB: this._indexedDB
        })
      this.assertOpen()
      this.stateStore = await createChannelStateStore({
        prefix: this.prefix,
        indexedDB: this._indexedDB
      })
      this.assertOpen()
      this.startStorageMaintenance()
      this.state = { channels: await this.stateStore.load() }
      await this.update({ userSigner, contentKeySigner, nymSigner: this.nymSigner, channels, relays, mode })
      this.initialized = true
      await this.refreshAndApplyStoragePolicy()
      this.broadcastStoragePolicyChange()
      return this
    } catch (err) {
      this.stopStorageMaintenance()
      this.stopStoragePolicyBroadcast()
      try { await this.queueOperationTail } catch {}
      try { await this.stateWriteTail } catch {}
      await Promise.allSettled([
        this.queue?.close?.(),
        this.seedQueue?.close?.(),
        this.stateStore?.close?.()
      ])
      if (this.storageActive) {
        try {
          await releasePrivateMessengerStorage({
            userPubkey: this.userPubkey,
            leaseId: this.storageLeaseId,
            indexedDB: this._indexedDB
          })
        } catch {}
      }
      this.storageActive = false
      this.initialized = false
      throw err
    } finally {
      settleInit()
      if (this.initSettledPromise === initSettledPromise) this.initSettledPromise = null
    }
  }

  assertOpen () {
    if (this.closePromise) throw new Error('PRIVATE_MESSENGER_CLOSED')
  }

  applyStoragePolicySnapshot (snapshot) {
    if (!snapshot) return false
    const staleChannelSeconds = normalizeStaleChannelSeconds(snapshot.staleChannelSeconds)
    const identityStorageRetentionSeconds = normalizeIdentityStorageRetentionSeconds(
      snapshot.identityStorageRetentionSeconds
    )
    const policyRevision = Math.max(0, Number(snapshot.policyRevision) || 0)
    const changed = this.staleChannelSeconds !== staleChannelSeconds ||
      this.identityStorageRetentionSeconds !== identityStorageRetentionSeconds
    this.staleChannelSeconds = staleChannelSeconds
    this.identityStorageRetentionSeconds = identityStorageRetentionSeconds
    this.storagePolicyRevision = policyRevision
    if (changed && this.initialized) this.storagePolicyNeedsApply = true
    return changed
  }

  startStoragePolicyBroadcast () {
    if (this.storagePolicyBroadcast || typeof this._BroadcastChannel !== 'function') return
    try {
      const channel = new this._BroadcastChannel(STORAGE_POLICY_BROADCAST_CHANNEL)
      channel.unref?.()
      channel.onmessage = event => {
        const message = event?.data
        if (message?.userPubkey !== this.userPubkey) return
        if (!Number.isSafeInteger(message.policyRevision) || message.policyRevision <= (this.storagePolicyRevision || 0)) return
        this.refreshAndApplyStoragePolicy().catch(err => {
          try { this.onError?.(err) } catch {}
        })
      }
      this.storagePolicyBroadcast = channel
    } catch {}
  }

  stopStoragePolicyBroadcast () {
    const channel = this.storagePolicyBroadcast
    this.storagePolicyBroadcast = null
    if (!channel) return
    channel.onmessage = null
    channel.close?.()
  }

  broadcastStoragePolicyChange () {
    try {
      this.storagePolicyBroadcast?.postMessage({
        userPubkey: this.userPubkey,
        policyRevision: this.storagePolicyRevision || 0
      })
    } catch (err) {
      try { this.onError?.(err) } catch {}
    }
  }

  async readStoragePolicySnapshot () {
    if (!this.userPubkey) return null
    const snapshot = await readPrivateMessengerStorage({
      userPubkey: this.userPubkey,
      indexedDB: this._indexedDB
    })
    this.applyStoragePolicySnapshot(snapshot)
    return snapshot
  }

  async applyPendingStoragePolicy () {
    if (!this.storagePolicyNeedsApply || !this.initialized || this.closePromise) return false
    this.storagePolicyNeedsApply = false
    try {
      const channels = [...this.channels.values()]
      await this.applyRecoveryPolicies(channels)
      await this.cleanupStaleChannels()
      const pubkeys = [...this.desiredChannels]
      if (pubkeys.length) {
        await this.stopWatches(pubkeys)
        await this.watch(pubkeys)
      }
      await this.reconcilePresencePublishers()
      return true
    } catch (err) {
      this.storagePolicyNeedsApply = true
      throw err
    }
  }

  refreshAndApplyStoragePolicy () {
    const previous = this.storagePolicyRefreshTail || Promise.resolve()
    const refresh = previous.catch(() => {}).then(async () => {
      await this.readStoragePolicySnapshot()
      return this.applyPendingStoragePolicy()
    })
    this.storagePolicyRefreshTail = refresh
    return refresh
  }

  startStorageMaintenance () {
    if (this.storageHeartbeatTimer || this.storageMaintenanceTimer) return
    this.storageHeartbeatTimer = this._storageSetInterval(() => (
      this.runStorageHeartbeat().catch(err => {
        try { this.onError?.(err) } catch {}
      })
    ), PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS)
    this.storageMaintenanceTimer = this._storageSetInterval(
      () => this.runStorageMaintenance(),
      PRIVATE_MESSENGER_STORAGE_MAINTENANCE_MS
    )
    this.storageHeartbeatTimer?.unref?.()
    this.storageMaintenanceTimer?.unref?.()
  }

  stopStorageMaintenance () {
    if (this.storageHeartbeatTimer) this._storageClearInterval(this.storageHeartbeatTimer)
    if (this.storageMaintenanceTimer) this._storageClearInterval(this.storageMaintenanceTimer)
    this.storageHeartbeatTimer = null
    this.storageMaintenanceTimer = null
  }

  runStorageMaintenance () {
    if (this.storageMaintenancePromise) return this.storageMaintenancePromise
    const maintenance = (async () => {
      await PrivateMessenger.maintainStorage({
        indexedDB: this._indexedDB,
        temporaryStorageArea: this.temporaryStorageArea
      })
      await this.refreshAndApplyStoragePolicy()
      await this.cleanupStaleChannels()
      await this.pruneStoredSeeds()
    })().catch(err => {
      try { this.onError?.(err) } catch {}
    }).finally(() => {
      if (this.storageMaintenancePromise === maintenance) this.storageMaintenancePromise = null
    })
    this.storageMaintenancePromise = maintenance
    return maintenance
  }

  async runStorageHeartbeat () {
    await this.stampActiveChannelActivity()
    await this.touchStorageActivity({ force: true })
    await this.applyPendingStoragePolicy()
  }

  async touchStorageActivity ({ force = false } = {}) {
    if (!this.storageActive || this.closePromise) return false
    if (this.storageTouchPromise) return this.storageTouchPromise
    const now = Date.now()
    if (!force && now - this.lastStorageTouch < PRIVATE_MESSENGER_STORAGE_HEARTBEAT_MS) return false
    const previousTouch = this.lastStorageTouch
    this.lastStorageTouch = now
    const touch = activatePrivateMessengerStorage({
      userPubkey: this.userPubkey,
      leaseId: this.storageLeaseId,
      activeChannelPubkeys: [...this.channels.keys()],
      indexedDB: this._indexedDB,
      now
    }).then(snapshot => {
      this.applyStoragePolicySnapshot(snapshot)
      return true
    }, err => {
      this.lastStorageTouch = previousTouch
      throw err
    }).finally(() => {
      if (this.storageTouchPromise === touch) this.storageTouchPromise = null
    })
    this.storageTouchPromise = touch
    return touch
  }

  runQueueOperation (operation) {
    if (this.closePromise) return Promise.reject(new Error('PRIVATE_MESSENGER_CLOSED'))
    this.touchStorageActivity().catch(err => this.onError?.(err))
    const run = this.queueOperationTail.then(operation)
    this.queueOperationTail = run.catch(err => {
      try { this.onError?.(err) } catch {}
    })
    return run
  }

  queueIncoming (operation) {
    return this.runQueueOperation(operation)
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

  async update (options = {}, { waitForBackground = false } = {}) {
    if (typeof waitForBackground !== 'boolean') throw new ValidationError('INVALID_WAIT_FOR_BACKGROUND')
    const work = this.updateTail.catch(() => {}).then(() => this.applyUpdate(options))
    this.updateTail = work
    const background = await work
    if (waitForBackground) await Promise.all(background)
    return this
  }

  async applyUpdate (options) {
    this.assertOpen()
    const {
      userSigner = this.userSigner,
      contentKeySigner = this.contentKeySigner,
      nymSigner = this.nymSigner,
      channels = [...this.channels.values()],
      relays = [],
      mode = 'leecher'
    } = options
    const readerIdentityChanged = userSigner !== this.userSigner || contentKeySigner !== this.contentKeySigner || nymSigner !== this.nymSigner
    const updatesStalePolicy = Object.hasOwn(options, 'staleChannelSeconds')
    const updatesIdentityPolicy = Object.hasOwn(options, 'identityStorageRetentionSeconds')
    let nextStaleChannelSeconds = updatesStalePolicy
      ? normalizeStaleChannelSeconds(options.staleChannelSeconds)
      : this.staleChannelSeconds
    let nextIdentityStorageRetentionSeconds = updatesIdentityPolicy
      ? normalizeIdentityStorageRetentionSeconds(options.identityStorageRetentionSeconds)
      : this.identityStorageRetentionSeconds
    if (userSigner && (userSigner !== this.userSigner || !this.userPubkey)) {
      const userPubkey = await userSigner.getPublicKey?.()
      if (!userPubkey) throw new ValidationError('USER_SIGNER_REQUIRED')
      if (this.userPubkey && userPubkey !== this.userPubkey) throw new ValidationError('USER_SIGNER_MISMATCH')
      this.userSigner = userSigner
    }
    if (contentKeySigner !== this.contentKeySigner || (!this.contentKeyPubkey && contentKeySigner)) {
      this.contentKeyPubkey = await contentKeySigner?.getPublicKey?.() || ''
    }
    this.contentKeySigner = contentKeySigner || null
    this.nymSigner = nymSigner || null
    const nextChannels = await this.normalizeChannels(channels, { relays, mode })
    this.assertOpen()
    const nextPubkeys = new Set(nextChannels.map(channel => channel.pubkey))
    const changedChannel = channel => {
      const previous = this.channels.get(channel.pubkey)
      return !previous || ['signer', 'readerSigner', 'readerPubkey', 'nymSigner', 'mode', 'usesNip65WatchRelays', 'offlineRecoverySeconds', 'autoDeletionCapability'].some(key => previous[key] !== channel[key]) ||
        ['relays', 'sendRelays', 'seeders'].some(key => JSON.stringify(previous[key]) !== JSON.stringify(channel[key]))
    }
    const changed = nextChannels.filter(channel => readerIdentityChanged || updatesStalePolicy || updatesIdentityPolicy || changedChannel(channel))
    const watchPubkeys = changed.filter(channel => !this.channels.has(channel.pubkey) || this.desiredChannels.has(channel.pubkey)).map(channel => channel.pubkey)
    const removedPubkeys = [...this.channels.keys()].filter(pubkey => !nextPubkeys.has(pubkey))
    const updatesStoragePolicy = updatesStalePolicy || updatesIdentityPolicy
    if (!changed.length && !removedPubkeys.length && !updatesStoragePolicy) return [...this.channelBackground.values()].map(job => job.work)

    if (updatesStoragePolicy) {
      const currentPolicy = await this.readStoragePolicySnapshot()
      if (!updatesStalePolicy) nextStaleChannelSeconds = currentPolicy?.staleChannelSeconds ?? this.staleChannelSeconds
      if (!updatesIdentityPolicy) {
        nextIdentityStorageRetentionSeconds = currentPolicy?.identityStorageRetentionSeconds ?? this.identityStorageRetentionSeconds
      }
    }

    if (removedPubkeys.length) {
      await this.stampChannelActivity(removedPubkeys)
      for (const pubkey of removedPubkeys) this.dropRecoveryState(pubkey)
    }

    const storageSnapshot = await activatePrivateMessengerStorage({
      userPubkey: this.userPubkey,
      leaseId: this.storageLeaseId,
      activeChannelPubkeys: [...nextPubkeys],
      storagePolicy: updatesStoragePolicy
        ? {
            staleChannelSeconds: nextStaleChannelSeconds,
            identityStorageRetentionSeconds: nextIdentityStorageRetentionSeconds
          }
        : undefined,
      indexedDB: this._indexedDB
    })
    this.applyStoragePolicySnapshot(storageSnapshot)
    this.lastStorageTouch = Date.now()
    if (updatesStoragePolicy) this.broadcastStoragePolicyChange()

    await this.unwatch(removedPubkeys)
    // Keep an established reader alive until private-message atomically
    // replaces it. Only obsolete setup/presence work is retired here.
    for (const pubkey of watchPubkeys) {
      this.cancelReloadGap(pubkey)
      this.cancelLiveRecovery(pubkey)
      this.channelBackground.get(pubkey)?.controller.abort()
      this.channelBackground.delete(pubkey)
      this.watchRevisionByChannel.set(pubkey, (this.watchRevisionByChannel.get(pubkey) || 0) + 1)
      this.stopPresencePublisher(pubkey)
    }
    for (const pubkey of removedPubkeys) this.channels.delete(pubkey)
    for (const [key, value] of this.sendRelayExclusions) if (removedPubkeys.includes(value.channelPubkey)) this.sendRelayExclusions.delete(key)
    for (const channel of changed) this.channels.set(channel.pubkey, channel)

    // Incremental registration must not prune persisted channels whose signers
    // are still being prepared. Retention cleanup belongs to maintenance.
    await this.applyRecoveryPolicies(changed, { prune: !this.seedStorage })
    this.assertOpen()
    for (const pubkey of watchPubkeys) {
      this.desiredChannels.add(pubkey)
      this.recordRecoveryWindow(pubkey)
    }
    if (this.pauseReasons.size) this.recordInterruption(watchPubkeys)
    await this.flushStateWrites()
    this.ensureNetworkWatchers()
    if (this.storagePolicyRevision === storageSnapshot.policyRevision) {
      this.storagePolicyNeedsApply = false
    }
    return this.startChannelBackground(watchPubkeys)
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
      if (!pubkey) throw new ValidationError('CHANNEL_PUBKEY_REQUIRED')
      if (!signer && !readerSigner) throw new ValidationError('CHANNEL_SIGNER_REQUIRED')
      const mode = channel.mode || defaults.mode || 'leecher'
      if (!signer && doesModeStoreRecoverySeeds(mode)) throw new ValidationError('PRIVATE_CHANNEL_WRITER_REQUIRED')
      const readerPubkey = channel.readerPubkey || channel.privateChannelReaderPubkey || (readerSigner === signer ? pubkey : await readerSigner?.getPublicKey?.()) || pubkey
      const autoDeletionCapability = channel.autoDeletionCapability === undefined
        ? undefined
        : normalizeAutoDeletionCapability(channel.autoDeletionCapability)
      const offlineRecoverySeconds = normalizeOfflineRecoverySeconds(
        channel.offlineRecoverySeconds ?? this.offlineRecoverySeconds
      )
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
        seeders: uniq(channel.seeders),
        offlineRecoverySeconds,
        autoDeletionCapability
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

  async readRelaysForPubkey (pubkey, options) {
    const relaysByPubkey = await this._getRelaysByPubkey([pubkey], options)
    const readRelays = uniq(relaysByPubkey?.[pubkey]?.read)
    if (readRelays.length) return readRelays
    return relayMapRelays(this._pickRelaysForPubkeys([pubkey], relaysByPubkey, { relayType: 'read' }))
  }

  async recoveryMirrorRelays (channelPubkey) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return []
    const seeders = this.recoverySeeders(channelPubkey)
    if (!seeders.length) return []
    try {
      return relayMapRelays(await this.readRelayToReceivers(seeders))
    } catch (err) {
      this.onError?.(err)
      return []
    }
  }

  async resolveWatchRelays (channel, options) {
    const primary = !channel.usesNip65WatchRelays && channel.relays.length ? channel.relays : await this.readRelaysForPubkey(this.userPubkey, options)
    return uniq([...primary, ...this.fallbackRelays].map(normalizeRelayUrl))
  }

  async resolveSendRouting ({ channel, receiverPubkeys, relays, relayToReceivers, signal }) {
    const recoveryRelays = await this.recoveryMirrorRelays(channel.pubkey)
    if (relayToReceivers && !this.fallbackRelays.length) return { relayToReceivers, recoveryRelays }
    const fixed = relayToReceivers ? null : relays?.length ? uniq(relays) : channel.sendRelays.length ? channel.sendRelays : channel.relays.length ? channel.relays : null
    if (fixed && !this.fallbackRelays.length) return { relays: fixed, recoveryRelays }
    const recipients = uniq(receiverPubkeys?.length ? receiverPubkeys : relayMapReceivers(relayToReceivers))
    const relaysByPubkey = fixed || relayToReceivers ? undefined : await this._getRelaysByPubkey(recipients)
    const key = `${channel.pubkey}:${[...recipients].sort().join(',')}`
    if (!this.sendRelayExclusions.has(key)) {
      if (this.sendRelayExclusions.size >= 256) this.sendRelayExclusions.delete(this.sendRelayExclusions.keys().next().value)
      this.sendRelayExclusions.set(key, { channelPubkey: channel.pubkey, exclusions: new Map() })
    }
    const routing = createSendRelayRouting({
      peers: fixed && !recipients.length ? [this.userPubkey] : recipients, relaysByPubkey, recoveryRelays, primaryRelays: fixed?.map(normalizeRelayUrl), primaryRelayToReceivers: relayToReceivers, fallbackRelays: this.fallbackRelays,
      signal: signal ? AbortSignal.any([this.sendRoutingLifetime.signal, signal]) : this.sendRoutingLifetime.signal,
      exclusions: this.sendRelayExclusions.get(key).exclusions,
      fallbackDelayMs: this.fallbackDelayMs, pauseSignal: this.sendRoutingPause.signal,
      setTimer: this._setTimeout, clearTimer: this._clearTimeout,
      pickRelays: this._pickRelaysForPubkeys,
      publish: this._privateChannel.publish || privateChannel.publish,
      publishNymEvent: this._privateChannel.publishNymEvent || privateChannel.publishNymEvent,
      sendEvent: (...args) => relayPool.sendEvent(...args),
      isOnline: this._isOnline,
      isCurrent: () => !this.closePromise && !this.pauseReasons.size && this.channels.has(channel.pubkey)
    })
    if (!relayMapRelays(routing.relayToReceivers).length) throw new ValidationError('NO_RELAYS')
    // Fixed lists address the original receiver set as a whole, preserving the
    // existing encryption grouping instead of inventing per-recipient routes.
    if (fixed) return { relays: fixed, recoveryRelays: routing.recoveryRelays, _publish: routing._publish }
    return routing
  }

  readState () {
    return structuredClone(this.state)
  }

  writeState (state, { touchStorage = true } = {}) {
    if (touchStorage) this.touchStorageActivity().catch(err => this.onError?.(err))
    const previous = this.state?.channels || {}
    const next = {
      channels: isPlainObject(state?.channels) ? structuredClone(state.channels) : {}
    }
    const changed = Object.fromEntries(Object.entries(next.channels)
      .filter(([pubkey, value]) => !areStateValuesEqual(previous[pubkey], value)))
    const removed = Object.keys(previous).filter(pubkey => !Object.hasOwn(next.channels, pubkey))
    this.state = next
    if (!Object.keys(changed).length && !removed.length) return this.stateWriteTail
    const snapshot = structuredClone(changed)
    const write = this.stateWriteTail.then(() => this.stateStore.update(snapshot, removed))
    this.stateWriteTail = write.catch(err => {
      this.stateWriteError = err
      try { this.onError?.(err) } catch {}
    })
    return write
  }

  async flushStateWrites () {
    await this.stateWriteTail
    if (!this.stateWriteError) return
    const repair = this.stateWriteTail.then(async () => {
      const stored = await this.stateStore.load()
      const snapshot = structuredClone(this.state.channels)
      await this.stateStore.update(snapshot, Object.keys(stored).filter(pubkey => !Object.hasOwn(snapshot, pubkey)))
      this.stateWriteError = null
    })
    this.stateWriteTail = repair.catch(err => { this.stateWriteError = err })
    await repair
  }

  async stampChannelActivity (pubkeys) {
    if (!this.stateStore || !this.state) return false
    pubkeys = [...new Set(pubkeys || [])].filter(pubkey => this.state.channels[pubkey])
    if (!pubkeys.length) return false
    const lastWatchedAt = nowSeconds()
    for (const pubkey of pubkeys) {
      this.state.channels[pubkey] = {
        ...this.state.channels[pubkey],
        lastWatchedAt
      }
    }
    const write = this.stateWriteTail.then(() => this.stateStore.touch(pubkeys, lastWatchedAt))
    this.stateWriteTail = write.catch(err => {
      this.stateWriteError = err
      try { this.onError?.(err) } catch {}
    })
    await write
    return true
  }

  stampActiveChannelActivity () {
    return this.stampChannelActivity([...this.channels.keys()])
  }

  updateChannelState (pubkey, patch) {
    const state = this.readState()
    const current = state.channels[pubkey] || {}
    state.channels[pubkey] = { ...current, ...patch }
    this.writeState(state)
    return state.channels[pubkey]
  }

  removeChannelState (pubkey) {
    this.removeChannelStates([pubkey])
  }

  removeChannelStates (pubkeys) {
    pubkeys = [...new Set(pubkeys || [])]
    for (const pubkey of pubkeys) delete this.state.channels[pubkey]
    if (!pubkeys.length) return this.stateWriteTail
    this.touchStorageActivity().catch(err => this.onError?.(err))
    const write = this.stateWriteTail.then(() => this.stateStore.update({}, pubkeys))
    this.stateWriteTail = write.catch(err => {
      this.stateWriteError = err
      try { this.onError?.(err) } catch {}
    })
    return write
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
    if (!this.offlineRecoverySecondsFor(pubkey)) return []
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
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return false
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
    return true
  }

  trackSeederActivity (channelPubkey, message) {
    if (message.provenance === 'hearsay') return false
    const senderPubkey = message.senderPubkey || message.event?.pubkey
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
    const recoverySeconds = this.offlineRecoverySecondsFor(pubkey)
    if (!recoverySeconds) return
    const now = nowSeconds()
    const minStart = now - recoverySeconds
    const normalized = {
      start: Math.max(0, Math.floor(start)),
      end: Math.floor(end)
    }
    if (normalized.end < normalized.start || normalized.end < minStart) return
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

  closeOpenOfflineRanges (channels = [...this.desiredChannels]) {
    const state = this.readState()
    const end = nowSeconds()
    for (const pubkey of channels) {
      const current = state.channels[pubkey]
      if (current?.openOfflineStart == null) continue
      const recoverySeconds = this.offlineRecoverySecondsFor(pubkey)
      if (!recoverySeconds) {
        delete current.openOfflineStart
        current.offlineRanges = []
        state.channels[pubkey] = current
        continue
      }
      const minStart = end - recoverySeconds
      const start = Math.max(minStart, Math.max(0, current.openOfflineStart))
      if (end >= start) {
        current.offlineRanges = mergeRanges((current.offlineRanges || []).concat([{ start, end }]))
      }
      delete current.openOfflineStart
      state.channels[pubkey] = current
    }
    this.writeState(state)
  }

  async watch (channels = [...this.channels.keys()], { scheduleReloadGap = true, signal } = {}) {
    this.assertOpen()
    const channelPubkeys = uniq(channels)
    for (const pubkey of channelPubkeys) {
      this.requireChannel(pubkey)
      this.desiredChannels.add(pubkey)
    }
    this.ensureNetworkWatchers()
    if (this.pauseReasons.size) {
      this.recordInterruption(channelPubkeys)
      await this.flushStateWrites()
      return this
    }
    for (const pubkey of channelPubkeys) {
      const channel = this.channels.get(pubkey)
      if (!channel) throw new ValidationError('UNKNOWN_CHANNEL')
      const revision = (this.watchRevisionByChannel.get(pubkey) || 0) + 1
      this.watchRevisionByChannel.set(pubkey, revision)
      // Persist the pending interval before live delivery can advance lastSeenAt.
      // Neither a crash nor a newly received message may erase the initial scan.
      this.recordRecoveryWindow(pubkey)
      await this.flushStateWrites()
      let stop; let watchRelays
      try {
        signal?.throwIfAborted()
        watchRelays = await this.resolveWatchRelays(channel, { signal })
        this.assertOpen()
        if (this.pauseReasons.size || !this.desiredChannels.has(pubkey) || revision !== (this.watchRevisionByChannel.get(pubkey) || 0)) continue
        stop = await this._privateMessage.watch({
          channels: [pubkey],
          relays: watchRelays,
          receiverSigner: this.userSigner,
          receiverPubkey: this.userPubkey,
          iykcSigner: this.contentKeySigner,
          privateChannelSigner: channel.signer,
          privateChannelReaderSigner: channel.readerSigner,
          privateChannelReaderPubkey: channel.readerPubkey,
          mode: channel.mode,
          onAsk: message => this.receive(pubkey, () => this.handleAsk(pubkey, message), message),
          onReply: message => this.receive(pubkey, () => this.handleReply(pubkey, message), message),
          onTell: message => this.receive(pubkey, () => this.handleTell(pubkey, message), message),
          onYell: message => this.receive(pubkey, () => this.handleYell(pubkey, message), message),
          onNym: message => this.receive(pubkey, () => this.handleNym(pubkey, message), message),
          onMessage: message => this.receive(pubkey, () => this.handleMessage(pubkey, message), message),
          onSeed: seed => this.receive(pubkey, () => this.enqueueSeed(pubkey, seed), seed),
          onContentKeyUsage: usage => this.handleContentKeyUsage(pubkey, usage),
          receivedChunkTtlMs: this.receivedChunkTtlMsFor(channel),
          receivedChunkIndexedDB: this._indexedDB,
          onSubscriptionState: state => this.handleSubscriptionState(pubkey, state),
          onError: err => this.onError?.(err)
        })
      } catch (error) {
        if (signal?.aborted) continue
        const transient = isRetryableRelayFailure(error)
        const remote = error?.name !== 'Nip42AuthenticationError' && (error?.category === 'relay' || isReplaceableRelayFailure(error))
        if (!transient && !remote) throw error
        this.onError?.(error)
        if (!transient) continue
        if (!this.watchRecoveries.has(pubkey)) {
          const task = this.recoveryTask(async signal => {
            signal.throwIfAborted()
            if (this.closePromise || this.pauseReasons.size || !this.desiredChannels.has(pubkey)) return
            await this.watch([pubkey])
            if (!this.stopByChannel.has(pubkey)) throw error
          }, { network: true, retryable: isRetryableRelayFailure })
          this.watchRecoveries.set(pubkey, task)
        }
        this.watchRecoveries.get(pubkey).start({ retryAt: error.retryAt })
        continue
      }
      this.watchRecoveries.get(pubkey)?.stop()
      this.watchRecoveries.delete(pubkey)
      if (this.closePromise || this.pauseReasons.size || !this.desiredChannels.has(pubkey) || revision !== (this.watchRevisionByChannel.get(pubkey) || 0)) {
        await stop?.()
        this.assertOpen()
        continue
      }
      this.stopByChannel.set(pubkey, stop)
      this.watchRevisionByChannel.set(pubkey, revision)
      this.updateChannelState(pubkey, {
        lastWatchedAt: nowSeconds(),
        mode: channel.mode,
        relays: watchRelays,
        seeders: channel.seeders,
        offlineRecoverySeconds: channel.offlineRecoverySeconds
      })
      this.debug('watch', {
        channelPubkey: pubkey,
        relays: watchRelays,
        mode: channel.mode,
        seeders: channel.seeders,
        seederCount: channel.seeders.length
      })
      if (scheduleReloadGap) {
        this.closeOpenOfflineRanges([pubkey])
        this.scheduleReloadGap(pubkey)
      }
    }
    this.ensureNetworkWatchers()
    this.ensureRelayListWatcher()
    return this
  }

  async handleSubscriptionState (pubkey, { state, relay, since, until }) {
    if (this.closePromise || this.pauseReasons.size || !this.desiredChannels.has(pubkey)) return
    if (!this.offlineRecoverySecondsFor(pubkey)) return
    let interrupted = this.liveInterruptions.get(pubkey)
    if (state === 'interrupted') {
      if (!interrupted) { interrupted = new Map(); this.liveInterruptions.set(pubkey, interrupted) }
      const start = Math.min(interrupted.get(relay) ?? Infinity, since)
      interrupted.set(relay, start)
      this.watchRecoveries.get(pubkey)?.stop()
      this.watchRecoveries.delete(pubkey)
      this.cancelReloadGap(pubkey)
      for (const controller of this.recoveryControllers) {
        if (controller.channelPubkey === pubkey) { controller.liveInterrupted = true; controller.abort() }
      }
      this.recordInterruption([pubkey], start)
      this.addOfflineRange(pubkey, Math.max(0, start - this.offlineSkewSeconds), nowSeconds())
      // This rejection prevents the session from reopening the shared relay.
      await this.flushStateWrites()
    } else if (state === 'ready' && interrupted?.has(relay)) {
      const start = interrupted.get(relay)
      this.addOfflineRange(pubkey, Math.max(0, start - this.offlineSkewSeconds), until)
      if (interrupted.size === 1) this.closeOpenOfflineRanges([pubkey])
      await this.flushStateWrites()
      if (this.closePromise || this.pauseReasons.size || !this.desiredChannels.has(pubkey) || this.liveInterruptions.get(pubkey) !== interrupted) return
      interrupted.delete(relay)
      if (!interrupted.size) this.liveInterruptions.delete(pubkey)
      this.scheduleLiveRecovery(pubkey)
    }
  }

  cancelLiveRecovery (pubkey) {
    const work = this.liveRecoveryTimers.get(pubkey)
    if (work?.timer != null) this._clearTimeout(work.timer)
    this.liveRecoveryTimers.delete(pubkey)
  }

  async checkRecoveryOnline () {
    const now = Date.now()
    if (now - this.recoveryOnline.at < RECOVERY_ONLINE_CACHE_MS) return this.recoveryOnline.value
    let value = true
    try {
      if (globalThis.navigator?.onLine === false) value = false
      else if (this._isOnline !== isOnline || typeof globalThis.document !== 'undefined') {
        value = (await this._isOnline()) !== false
      }
    } catch { value = false }
    this.recoveryOnline = { at: Date.now(), value }
    return value
  }

  recoveryRangeKey (channelPubkey, range) {
    return `${channelPubkey}:${range.start}:${range.end}`
  }

  recoveryRangeRecord (channelPubkey, range) {
    const key = this.recoveryRangeKey(channelPubkey, range)
    let record = this.recoveryRanges.get(key)
    if (!record) {
      record = {
        key,
        channelPubkey,
        start: range.start,
        end: range.end,
        covered: [],
        claimed: [],
        oldestCreatedAt: null,
        relays: new Map(),
        tier: '',
        everEose: false,
        everEoseWithEvents: false,
        attempts: 0,
        lastErrors: []
      }
      this.recoveryRanges.set(key, record)
    }
    return record
  }

  reconcileRecoveryRelays (record, relays) {
    const current = new Set(relays)
    for (const relay of [...record.relays.keys()]) {
      if (!current.has(relay)) record.relays.delete(relay)
    }
    for (const relay of current) {
      if (record.relays.has(relay)) continue
      record.relays.set(relay, {
        pending: [{ start: record.start, end: record.end }],
        covered: [],
        attempts: 0,
        dead: false,
        eose: false,
        events: 0,
        error: null,
        consecutiveFailures: 0,
        latencyMs: null,
        lastEoseAt: 0,
        lastErrorAt: 0
      })
    }
  }

  orderedRecoveryRelays (record) {
    return [...record.relays.entries()]
      .filter(([, state]) => !state.dead && !state.eose && state.pending?.length)
      .sort((a, b) => {
        const failures = (a[1].consecutiveFailures || 0) - (b[1].consecutiveFailures || 0)
        if (failures) return failures
        const latency = (a[1].latencyMs ?? Infinity) - (b[1].latencyMs ?? Infinity)
        if (latency) return latency
        return (b[1].lastEoseAt || 0) - (a[1].lastEoseAt || 0)
      })
  }

  recoveryRelayPlan (record) {
    const plan = {}
    const ordered = this.orderedRecoveryRelays(record)
    for (const [relay, state] of ordered) {
      plan[relay] = state.pending
    }
    return ordered.length ? plan : null
  }

  hasActiveRecoveryRelays (channelPubkey) {
    for (const record of this.recoveryRanges.values()) {
      if (record.channelPubkey !== channelPubkey) continue
      if (this.recoveryRecordActive(record)) return true
    }
    return false
  }

  recoveryRecordActive (record) {
    for (const state of record.relays.values()) {
      if (!state.dead && !state.eose && state.pending?.length) return true
    }
    return false
  }

  hasPendingRecovery (channelPubkey) {
    return (this.readState().channels[channelPubkey]?.offlineRanges || []).length > 0
  }

  dropRecoveryRanges (channelPubkey) {
    for (const [key, record] of this.recoveryRanges) {
      if (record.channelPubkey === channelPubkey) this.recoveryRanges.delete(key)
    }
  }

  pruneRecoveryRanges (channelPubkey, ranges) {
    const keep = new Set((ranges || []).map(range => this.recoveryRangeKey(channelPubkey, range)))
    for (const [key, record] of this.recoveryRanges) {
      if (record.channelPubkey === channelPubkey && !keep.has(key)) this.recoveryRanges.delete(key)
    }
  }

  dropRecoveryState (channelPubkey) {
    this.dropRecoveryRanges(channelPubkey)
    this.recoveryPriority.delete(channelPubkey)
  }

  recoveryPriorities (channelPubkey) {
    const entries = this.recoveryPriority.get(channelPubkey) || []
    if (!entries.length) return []
    const now = Date.now()
    const active = entries.filter(entry => entry.expiresAt > now)
    if (active.length !== entries.length) {
      if (active.length) this.recoveryPriority.set(channelPubkey, active)
      else this.recoveryPriority.delete(channelPubkey)
    }
    return active
  }

  async prioritizeRange (channelPubkey, { since, until, type = 'unread-page', ttlMs } = {}) {
    if (typeof channelPubkey !== 'string' || !/^[0-9a-f]{64}$/i.test(channelPubkey)) throw new ValidationError('INVALID_CHANNEL_PUBKEY')
    if (type !== 'unread-page' && type !== 'tail') throw new ValidationError('INVALID_PRIORITY_TYPE')
    if (since !== undefined && !Number.isSafeInteger(since)) throw new ValidationError('INVALID_PRIORITY_RANGE')
    if (until !== undefined && !Number.isSafeInteger(until)) throw new ValidationError('INVALID_PRIORITY_RANGE')
    if (type === 'unread-page' && since === undefined) throw new ValidationError('PRIORITY_SINCE_REQUIRED')

    const now = nowSeconds()
    const ranges = (this.readState().channels[channelPubkey]?.offlineRanges || []).slice()
    if (!ranges.length) return false

    let start = since
    let end = until
    if (type === 'unread-page') {
      const range = ranges.filter(candidate => candidate.end >= start).sort((a, b) => a.start - b.start)[0]
      if (!range) return false
      start = Math.max(start, range.start)
      if (end === undefined) end = Math.min(range.end, now)
    } else {
      const range = ranges.sort((a, b) => b.end - a.end)[0]
      if (end === undefined) end = Math.min(range.end, now)
      if (start === undefined) start = Math.max(range.start, end - this.priorityTailSeconds)
    }

    const recoverySeconds = this.offlineRecoverySecondsFor(channelPubkey)
    if (recoverySeconds) {
      start = Math.max(start, now - recoverySeconds)
      end = Math.min(end, now)
    }
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || end < start) return false

    const ttl = Number.isSafeInteger(ttlMs) && ttlMs > 0 ? ttlMs : this.priorityTtlMs
    const entries = this.recoveryPriority.get(channelPubkey) || []
    const createdAt = Date.now()
    if (!entries.some(entry => entry.start === start && entry.end === end && entry.type === type)) {
      entries.push({ start, end, type, createdAt, expiresAt: createdAt + ttl })
      this.recoveryPriority.set(channelPubkey, entries)
    }
    return true
  }

  updateRecoveryRecord (record, history, { completeRelays = new Set((history.relays || []).map(entry => entry.relay)) } = {}) {
    record.covered = mergeRanges([
      ...record.covered,
      ...(history.relays || []).flatMap(entry => entry.covered || [])
    ])
    if (history.oldestCreatedAt != null) {
      record.oldestCreatedAt = record.oldestCreatedAt == null
        ? history.oldestCreatedAt
        : Math.min(record.oldestCreatedAt, history.oldestCreatedAt)
    }
    const byRelay = new Map((history.relays || []).map(entry => [entry.relay, entry]))
    for (const [relay, state] of record.relays) {
      const entry = byRelay.get(relay)
      if (!entry) continue
      const relayErrors = [...(entry.errors || [])]
      if (entry.error) relayErrors.push(entry.error)
      state.error = relayErrors[0] || null
      const done = entry.status === 'eose' && !(entry.pending?.length) && completeRelays.has(relay)
      state.covered = mergeRanges([...(state.covered || []), ...(entry.covered || [])])
      state.eose = done
      state.pending = done
        ? []
        : subtractRanges({ start: record.start, end: record.end }, mergeRanges([...state.covered, ...record.claimed]))
      state.events += entry.events || 0
      if (Number.isFinite(entry.elapsedMs)) {
        state.latencyMs = state.latencyMs == null
          ? entry.elapsedMs
          : Math.round(state.latencyMs * 0.7 + entry.elapsedMs * 0.3)
      }
      if (done) {
        state.attempts = 0
        state.consecutiveFailures = 0
        state.lastEoseAt = nowSeconds()
      } else if (entry.status === 'eose') {
        state.consecutiveFailures = 0
        state.lastEoseAt = nowSeconds()
      } else {
        state.consecutiveFailures = (state.consecutiveFailures || 0) + 1
        state.lastErrorAt = nowSeconds()
      }
      if (entry.status === 'eose') {
        record.everEose = true
        if (state.events > 0) record.everEoseWithEvents = true
      }
    }
    record.tier = record.everEoseWithEvents ? 'partial' : record.everEose ? 'emptyEose' : 'allFailed'
    record.lastErrors = (history.relays || []).flatMap(entry => [...(entry.errors || []), ...(entry.error ? [entry.error] : [])])
  }

  applyRecoveryCoverage (record, ranges) {
    if (!ranges?.length) return
    record.claimed = mergeRanges([...record.claimed, ...ranges])
    record.covered = mergeRanges([...record.covered, ...ranges])
    for (const state of record.relays.values()) {
      if (state.eose) continue
      state.pending = subtractRanges({ start: record.start, end: record.end }, mergeRanges([...state.covered, ...record.claimed]))
    }
  }

  activeRecoverySeeders (channelPubkey) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return []
    const channel = this.channels.get(channelPubkey)
    if (!channel?.signer) return []
    const activity = this.readState().channels[channelPubkey]?.seederActivity || {}
    const cutoff = nowSeconds() - this.seederOnlineSeconds
    const candidates = new Set([...(channel.seeders || []), ...Object.keys(activity)])
    return [...candidates].filter(seeder => {
      if (!seeder || seeder === this.userPubkey) return false
      const entry = activity[seeder]
      const recent = Math.max(entry?.announcedAt || 0, entry?.lastActiveAt || 0)
      return recent >= cutoff
    })
  }

  prunePriorityEntries (channelPubkey) {
    const entries = this.recoveryPriorities(channelPubkey)
    if (!entries.length) return
    const records = [...this.recoveryRanges.values()].filter(record => record.channelPubkey === channelPubkey)
    const next = entries.filter(entry => {
      let intersects = false
      for (const record of records) {
        const parts = intersectRanges(
          [{ start: entry.start, end: entry.end }],
          [{ start: record.start, end: record.end }]
        )
        if (!parts.length) continue
        intersects = true
        if (subtractRanges(parts[0], record.covered).length) return true
      }
      return !intersects
    })
    if (next.length) this.recoveryPriority.set(channelPubkey, next)
    else this.recoveryPriority.delete(channelPubkey)
  }

  applyRecoveryRetryBudget (record) {
    const limit = RECOVERY_RELAY_RETRY_LIMITS[record.tier] ?? RECOVERY_RELAY_RETRY_LIMITS.allFailed
    for (const state of record.relays.values()) {
      if (state.eose || state.dead || !state.pending?.length) continue
      state.attempts++
      if (state.attempts >= limit || isPermanentRecoveryError(state.error)) state.dead = true
    }
    record.attempts++
  }

  recoveryIncompleteError (record, history, range) {
    const errors = []
    for (const entry of history.relays || []) {
      for (const reason of entry.errors || []) errors.push({ relay: entry.relay, reason })
      if (entry.error) errors.push({ relay: entry.relay, reason: entry.error })
    }
    const error = incompleteFetchError({
      errors,
      report: (history.relays || []).map(entry => ({
        relay: entry.relay,
        status: entry.status,
        ...(entry.error ? { error: entry.error } : {})
      })),
      receivedEventCount: history.receivedEventCount || 0,
      elapsedMs: history.elapsedMs || 0,
      request: {
        relays: [...record.relays.keys()],
        channelPubkeys: [record.channelPubkey],
        receiverPubkey: this.userPubkey,
        since: range.start,
        until: range.end,
        limit: 16,
        timeoutMs: 5000
      }
    })
    error.operation = 'private-channel.fetchHistory'
    return error
  }

  isPartialRecoveryHistory (history) {
    return Boolean(history) && !Array.isArray(history) &&
      Array.isArray(history.relays) &&
      history.pendingByRelay !== undefined &&
      typeof history.anyEose === 'boolean'
  }

  async finalizeRecoveryRange ({ channelPubkey, range, record }) {
    if (!await this.checkRecoveryOnline()) return { complete: false, failures: [], offline: true }
    const uncovered = subtractRanges({ start: range.start, end: range.end }, record.covered)
    const oldest = record.oldestCreatedAt
    const leftEdge = oldest == null
      ? [{ start: range.start, end: range.end }]
      : oldest > range.start
        ? [{ start: range.start, end: Math.min(oldest, range.end) }]
        : []
    const seedRanges = mergeRanges([...uncovered, ...leftEdge])
    const failures = []
    for (const seedRange of seedRanges) {
      const attempt = await this.#askSeedersForMissingRangeAttempt(channelPubkey, seedRange.start, seedRange.end)
      failures.push(...attempt.failures.map(failure => failure.error))
    }
    record.lastErrors = failures
    return { complete: failures.length === 0, failures }
  }

  scheduleLiveRecovery (pubkey) {
    return this.scheduleOfflineRecovery(pubkey)
  }

  scheduleOfflineRecovery (pubkey, { startDelayMs = 0 } = {}) {
    const current = this.liveRecoveryTimers.get(pubkey)
    if (current) { current.again = true; return }
    const work = { delay: 1000, timer: null, again: false }
    this.liveRecoveryTimers.set(pubkey, work)
    const isCurrent = () => this.liveRecoveryTimers.get(pubkey) === work && !this.closePromise &&
      !this.pauseReasons.size && this.desiredChannels.has(pubkey)
    const schedule = delay => {
      work.timer = this._setTimeout(async () => {
        work.timer = null
        if (!isCurrent()) return
        work.again = false
        let errors
        try {
          // A ready notification may overtake cancellation of the previous scan.
          // Let that scan retain its range, then start a fresh recovery attempt.
          await this.recoveries.get(pubkey)
          if (!isCurrent()) return
          errors = await this.recoverOfflineRanges([pubkey])
        } catch (error) {
          this.onError?.(error)
          errors = [error]
        }
        if (!isCurrent()) return
        const pending = this.readState().channels[pubkey]?.offlineRanges?.length
        let retry = null
        if (pending) {
          retry = recoveryRetryDelay(errors, Math.min(30000, work.delay * (0.8 + Math.random() * 0.4)))
          if (retry === null && this.hasActiveRecoveryRelays(pubkey)) retry = Math.min(30000, work.delay)
        }
        if (work.again || retry !== null) {
          schedule(work.again ? 0 : retry)
          work.delay = Math.min(30000, work.delay * 2)
        } else this.liveRecoveryTimers.delete(pubkey)
      }, delay)
      work.timer?.unref?.()
    }
    schedule(startDelayMs)
  }

  recordInterruption (channels, at = nowSeconds()) {
    const state = this.readState()
    for (const pubkey of channels) {
      if (!this.offlineRecoverySecondsFor(pubkey)) continue
      const current = state.channels[pubkey] || {}
      const start = Math.max(0, at - this.offlineSkewSeconds)
      current.openOfflineStart = Math.min(current.openOfflineStart ?? start, start)
      state.channels[pubkey] = current
    }
    this.writeState(state)
  }

  stopWatches (channels = [...this.stopByChannel.keys()]) {
    const closing = []
    for (const pubkey of channels) {
      this.channelBackground.get(pubkey)?.controller.abort()
      this.channelBackground.delete(pubkey)
      this.cancelReloadGap(pubkey)
      this.cancelLiveRecovery(pubkey)
      this.liveInterruptions.delete(pubkey)
      this.watchRevisionByChannel.set(pubkey, (this.watchRevisionByChannel.get(pubkey) || 0) + 1)
      const close = this.stopByChannel.get(pubkey)?.()
      if (close?.then) closing.push(close)
      this.stopByChannel.delete(pubkey)
      this.stopPresencePublisher(pubkey)
    }
    this.ensureRelayListWatcher()
    return Promise.allSettled(closing)
  }

  unwatch (channels) {
    const pubkeys = channels ? uniq(Array.isArray(channels) ? channels : [channels]) : [...this.desiredChannels]
    for (const extension of this.extensions) extension.unwatch?.(pubkeys)
    for (const pubkey of pubkeys) this.desiredChannels.delete(pubkey)
    for (const [key, pending] of this.pendingStorageWrites) if (pubkeys.includes(pending.record.channelPubkey)) this.pendingStorageWrites.delete(key)
    this.syncPauseRecovery()
    for (const pubkey of pubkeys) this.dropRecoveryState(pubkey)
    this.recordInterruption(pubkeys)
    for (const controller of this.recoveryControllers) {
      if (pubkeys.includes(controller.channelPubkey)) controller.abort()
    }
    return Promise.all([this.stopWatches(pubkeys), this.flushStateWrites()])
  }

  readStatus () {
    return { closed: Boolean(this.closePromise), paused: this.pauseReasons.size > 0, pauseReasons: [...this.pauseReasons].sort() }
  }

  notifyStatus () {
    const status = this.readStatus()
    const fingerprint = JSON.stringify(status)
    if (fingerprint !== this.statusFingerprint) {
      this.statusFingerprint = fingerprint
      try { this.onStateChanged?.(status) } catch (error) { this.onError?.(error) }
    }
    this.syncPauseRecovery()
  }

  pausedError () {
    return Object.assign(new Error('PRIVATE_MESSENGER_PAUSED'), { pauseReasons: this.readStatus().pauseReasons })
  }

  recoveryTask (attempt, { network = false, retryable = isRecoverableStorageFailure } = {}) {
    return createPauseRecovery({
      attempt, retryable, onError: error => this.onError?.(error),
      ...(network ? { online: () => this.checkPauseOnline(), onOnline: this._onOnline } : {}),
      setTimer: this._setTimeout, clearTimer: this._clearTimeout, random: this._random
    })
  }

  checkPauseOnline () {
    if (!this.pauseOnlineWork) {
      const controller = new AbortController()
      this.pauseOnlineController = controller
      const work = Promise.resolve().then(() => this._isOnline({ signal: controller.signal })).finally(() => {
        if (this.pauseOnlineWork === work) { this.pauseOnlineWork = null; this.pauseOnlineController = null }
      })
      this.pauseOnlineWork = work
    }
    return this.pauseOnlineWork
  }

  syncPauseRecovery () {
    if (!this.pauseRecoveries) return
    const blocked = this.closePromise || !this.desiredChannels.size ||
      [...this.pauseReasons].some(reason => !['network', 'storage', 'capacity', 'session-storage'].includes(reason))
    for (const [reason, task] of this.pauseRecoveries) {
      if (blocked || !this.automaticPauses.has(reason) || !this.pauseReasons.has(reason)) task.stop()
    }
    if (blocked) {
      this.pauseOnlineController?.abort()
      this.pauseOnlineWork = null; this.pauseOnlineController = null
      return
    }
    for (const reason of this.automaticPauses) {
      if (!this.pauseReasons.has(reason)) continue
      let task = this.pauseRecoveries.get(reason)
      if (!task) {
        task = this.recoveryTask(async signal => {
          if (reason === 'storage') await this.retryStorageWrites(signal)
          await this.flushStateWrites()
          signal.throwIfAborted()
          await this.resume(reason)
        }, {
          network: reason === 'network', retryable: error => {
            const retry = reason === 'network' ? isRetryableRelayFailure(error) : isRecoverableStorageFailure(error)
            if (!retry) this.automaticPauses.delete(reason)
            return retry
          }
        })
        this.pauseRecoveries.set(reason, task)
      }
      task.start()
    }
  }

  async retryStorageWrites (signal) {
    for (const [key, pending] of this.pendingStorageWrites) {
      signal.throwIfAborted()
      const pubkey = pending.record.channelPubkey
      if (!this.channels.has(pubkey) || !this.desiredChannels.has(pubkey)) { this.pendingStorageWrites.delete(key); continue }
      try {
        if (!pending.dedupeKey || !await this.queue.someBy('byChannelTypeEventId', pending.dedupeKey)) await this.queue.enqueue(pending.record)
      } catch (error) { if (error?.name !== 'ConstraintError') throw error }
      signal.throwIfAborted()
      this.pendingStorageWrites.delete(key)
      this.markSeen(pubkey, pending.at || nowSeconds())
      this.wakeDeliveries()
      this.onMessageQueued?.()
    }
  }

  pauseInternally (reason) {
    if (!this.pauseReasons.has(reason) || this.automaticPauses.has(reason)) this.automaticPauses.add(reason)
    return this.applyPause(reason)
  }

  pause (reason) {
    // An explicit caller takes ownership, even if this cause already exists.
    this.automaticPauses.delete(reason)
    return this.applyPause(reason)
  }

  applyPause (reason) {
    if (typeof reason !== 'string' || !reason.trim()) throw new ValidationError('PAUSE_REASON_REQUIRED')
    this.assertOpen()
    this.pauseReasons.add(reason)
    this.pauseRevisions.set(reason, (this.pauseRevisions.get(reason) || 0) + 1)
    this.sendRoutingPause.abort(this.pausedError())
    this.notifyStatus()
    for (const extension of this.extensions) extension.pause?.()
    this.recordInterruption(this.desiredChannels)
    for (const controller of this.recoveryControllers) controller.abort()
    return Promise.all([this.stopWatches([...this.desiredChannels]), this.flushStateWrites()])
  }

  async resume (reason, { waitForBackground = false } = {}) {
    if (typeof reason !== 'string' || !reason.trim()) throw new ValidationError('PAUSE_REASON_REQUIRED')
    if (typeof waitForBackground !== 'boolean') throw new ValidationError('INVALID_WAIT_FOR_BACKGROUND')
    this.assertOpen()
    const revision = this.pauseRevisions.get(reason)
    // Repair durable state before announcing that publication can resume.
    if (reason === 'storage' && this.pendingStorageWrites.size) await this.retryStorageWrites(this.sendRoutingLifetime.signal)
    await this.flushStateWrites()
    this.assertOpen()
    if (revision !== this.pauseRevisions.get(reason)) return
    const removed = this.pauseReasons.delete(reason)
    this.automaticPauses.delete(reason)
    if (!this.pauseReasons.size && this.sendRoutingPause.signal.aborted) this.sendRoutingPause = new AbortController()
    this.notifyStatus()
    if (reason === 'capacity' && removed) {
      clearInterval(this.capacityTimer)
      this.capacityTimer = null
      this.capacityRequiredBytes = 0
    }
    if (this.pauseReasons.size) return
    const channels = [...this.desiredChannels]
    if (removed) {
      this.closeOpenOfflineRanges(channels)
      await this.flushStateWrites()
    }
    if (this.pauseReasons.size || this.closePromise) return
    const jobs = removed ? this.startChannelBackground(channels, { recover: true }) : [...this.channelBackground.values()].map(job => job.work)
    if (removed) {
      for (const extension of this.extensions) {
        const work = Promise.resolve().then(() => { if (!this.pauseReasons.size && !this.closePromise) return extension.resume?.() })
        work.catch(error => this.onError?.(error))
        jobs.push(work)
      }
    }
    if (waitForBackground) await Promise.all(jobs)
  }

  startChannelBackground (channels, { recover = false } = {}) {
    const jobs = []
    for (const pubkey of channels) {
      if (this.closePromise || this.pauseReasons.size || !this.desiredChannels.has(pubkey)) continue
      let job = this.channelBackground.get(pubkey)
      if (job) { jobs.push(job.work); continue }
      const controller = new AbortController()
      const channel = this.channels.get(pubkey)
      job = { controller, work: null }
      this.channelBackground.set(pubkey, job)
      const active = () => this.initialized && !controller.signal.aborted && !this.closePromise && !this.pauseReasons.size && this.channels.get(pubkey) === channel && this.desiredChannels.has(pubkey)
      let phase = 'subscribe'
      let stopAbort
      const cancelled = new Promise(resolve => {
        stopAbort = resolve
        controller.signal.addEventListener('abort', stopAbort, { once: true })
      })
      const work = Promise.resolve().then(async () => {
        if (!this.initialized) await this.initSettledPromise
        if (!active()) return
        const release = await this.channelSetupAdmission.acquire(controller.signal)
        try {
          if (!active()) return
          await this.watch([pubkey], { scheduleReloadGap: !recover, signal: controller.signal })
        } finally { release() }
        if (!active()) return
        phase = 'presence'
        if (doesModeStoreRecoverySeeds(channel.mode)) await this.startPresencePublisher(pubkey)
        if (!active()) return
        phase = 'history'
        if (recover) await this.recoverOfflineRanges([pubkey])
        if (!active()) return
        if (recover && this.hasPendingRecovery(pubkey)) this.scheduleOfflineRecovery(pubkey, { startDelayMs: 1000 })
      }).catch(error => {
        if (!active()) return
        const diagnostic = Object.assign(new Error(error?.message || 'PRIVATE_MESSENGER_BACKGROUND_FAILED', { cause: error }), {
          name: error?.name || 'Error', ...(error?.code ? { code: error.code } : {}),
          ...(error?.category ? { category: error.category } : {}),
          operation: 'private-messenger.background', phase
        })
        this.onError?.(diagnostic)
        throw error
      })
      job.work = Promise.race([work, cancelled]).finally(() => {
        controller.signal.removeEventListener('abort', stopAbort)
        if (this.channelBackground.get(pubkey) === job) this.channelBackground.delete(pubkey)
      })
      job.work.catch(() => {})
      jobs.push(job.work)
    }
    return jobs
  }

  receive (pubkey, operation, message = {}) {
    if (this.closePromise) {
      if (this.storageActive) this.recordInterruption([pubkey], messageTime(message))
      return Promise.reject(new Error('PRIVATE_MESSENGER_CLOSED'))
    }
    return this.queueIncoming(async () => {
      if (this.pauseReasons.size || !this.desiredChannels.has(pubkey)) {
        this.recordInterruption([pubkey], messageTime(message))
        throw this.pausedError()
      }
      return operation()
    })
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
    for (const pubkey of channelPubkeys) {
      if (this.hasPendingRecovery(pubkey)) this.scheduleOfflineRecovery(pubkey, { startDelayMs: 1000 })
    }
  }

  async handleAsk (channelPubkey, message) {
    if (message.provenance === 'hearsay') return this.enqueueRumor('message', channelPubkey, message)
    this.trackSeederActivity(channelPubkey, message)
    for (const extension of this.extensions) if (await extension.handleAsk?.(channelPubkey, message)) return
    if (doesModeStoreRecoverySeeds(this.channels.get(channelPubkey)?.mode) && messageCode(message) === MISSING_MESSAGES_ASK_CODE) {
      await this.replyWithStoredSeeds(channelPubkey, message)
      return
    }
    await this.enqueueRumor('ask', channelPubkey, message)
  }

  async handleReply (channelPubkey, message) {
    if (message.provenance === 'hearsay') return this.enqueueRumor('message', channelPubkey, message)
    this.trackSeederActivity(channelPubkey, message)
    if (messageCode(message) === MISSING_MESSAGES_REPLY_CODE) {
      await this.consumeMissingMessagesReply(channelPubkey, message)
      return
    }
    await this.enqueueRumor('reply', channelPubkey, message)
  }

  async handleTell (channelPubkey, message) {
    this.trackSeederActivity(channelPubkey, message)
    await this.enqueueRumor('tell', channelPubkey, message)
  }

  async handleYell (channelPubkey, message) {
    if (message.provenance === 'hearsay') return this.enqueueRumor('message', channelPubkey, message)
    this.trackSeederActivity(channelPubkey, message)
    if (messageCode(message) === SEEDER_PRESENCE_CODE) return
    await this.enqueueRumor('yell', channelPubkey, message)
  }

  async handleNym (channelPubkey, message) {
    await this.enqueueRumor('nym', channelPubkey, message)
  }

  async handleMessage (channelPubkey, message) {
    if (eventType(message.event) !== 'message' && message.provenance !== 'hearsay' && (!message.senderPubkey || message.senderPubkey === message.event.pubkey)) return
    this.trackSeederActivity(channelPubkey, message)
    await this.enqueueRumor('message', channelPubkey, message)
  }

  async enqueueRumor (type, channelPubkey, message) {
    const channel = this.channels.get(channelPubkey)
    if (channel?.mode === 'watchtower' && type !== 'ask') return
    const info = deliveryInfo(message.event, message.senderPubkey ?? message.meta?.senderPubkey ?? message.meta?.router?.tags?.find(tag => tag[0] === 'f')?.[1])
    const eventId = message.event?.id || ''
    // Stronger provenance must reach the app even while hearsay is pending.
    const dedupeKey = eventId ? [channelPubkey, type, eventId, info.provenance] : null
    if (dedupeKey && await this.queue.someBy('byChannelTypeEventId', dedupeKey)) {
      this.markSeen(channelPubkey, message.outer?.created_at || message.event?.created_at || nowSeconds())
      this.debug('dedupe', debugMessageInfo(type, channelPubkey, message))
      return
    }
    const record = {
      type,
      channelPubkey,
      receivedAt: nowSeconds(),
      event: message.event,
      ...info,
      payload: message.payload,
      question: message.question || null,
      questionId: message.questionId || null,
      outer: message.outer || null,
      meta: message.meta || null
    }
    try {
      await this.queue.enqueue(record)
    } catch (err) {
      // The unique index closes the small cross-instance race after `someBy`.
      if (dedupeKey && err?.name === 'ConstraintError') {
        this.markSeen(channelPubkey, message.outer?.created_at || message.event?.created_at || nowSeconds())
        this.debug('dedupe', debugMessageInfo(type, channelPubkey, message))
        return
      }
      this.recordInterruption([channelPubkey], message.outer?.created_at || message.event?.created_at || nowSeconds())
      // Do not await stop from inside its own transport callback.
      const atCapacity = err.message === 'QUEUE_CAPACITY_EXCEEDED'
      if (!atCapacity && isRecoverableStorageFailure(err)) this.pendingStorageWrites.set(JSON.stringify(dedupeKey || [channelPubkey, type]), { record, dedupeKey, at: message.outer?.created_at || message.event?.created_at })
      ;(atCapacity ? this.pause('capacity') : isRecoverableStorageFailure(err) ? this.pauseInternally('storage') : this.pause('storage')).catch(error => this.onError?.(error))
      if (atCapacity) {
        this.capacityRequiredBytes = Math.max(this.capacityRequiredBytes, err.requiredBytes || 0)
        if (!this.capacityTimer) {
          this.capacityTimer = setInterval(() => this.checkCapacity().catch(error => this.onError?.(error)), 1000)
          this.capacityTimer.unref?.()
        }
      }
      await this.flushStateWrites()
      throw err
    }
    this.markSeen(channelPubkey, message.outer?.created_at || message.event?.created_at || nowSeconds())
    this.wakeDeliveries()
    this.debug('enqueue', debugMessageInfo(type, channelPubkey, message))
    this.onMessageQueued?.()
  }

  outgoingSeedHandler (channelPubkey) {
    if (!doesModeStoreRecoverySeeds(this.channels.get(channelPubkey)?.mode) || !this.offlineRecoverySecondsFor(channelPubkey)) return undefined
    return seed => this.enqueueSeed(channelPubkey, seed)
  }

  async enqueueSeed (channelPubkey, seed) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return
    const receivedAt = nowSeconds()
    if (this.seedStorage) {
      const rows = seed.recordType === NYM_CARRIER_SEED_RECORD_TYPE || seed.carriers?.length
        ? [{ recordType: NYM_CARRIER_SEED_RECORD_TYPE, carriers: compactSeedNymCarriers(seed.carriers) }]
        : compactSeedRouterRows(seed)
      for (const row of rows) {
        const time = seedRecordTime(row) || receivedAt
        await this.seedStorage.put({ ...row, channelPubkey, receivedAt, expiresAt: time + this.offlineRecoverySecondsFor(channelPubkey) })
      }
      return
    }
    if (seed.recordType === NYM_CARRIER_SEED_RECORD_TYPE || seed.carriers?.length) {
      const carriers = compactSeedNymCarriers(seed.carriers)
      const recordTime = nymCarrierRecordTime({ carriers }) || seed.outer?.created_at || receivedAt
      const key = nymCarrierSeedKey({ channelPubkey, carriers })
      const seedKey = key ? `nym:${key}` : ''
      if (seedKey && await this.seedQueue.someBy('bySeedKey', seedKey)) return
      await this.seedQueue.enqueue({
        type: 'seed',
        recordType: NYM_CARRIER_SEED_RECORD_TYPE,
        channelPubkey,
        receivedAt,
        carriers,
        meta: { channelPubkey: seed.channelPubkey },
        [SEED_KEY]: seedKey || undefined,
        [SEED_TIME]: recordTime
      })
      await this.pruneStoredSeeds(channelPubkey)
      return
    }

    const rows = compactSeedRouterRows(seed)
    for (const row of rows) {
      const rowTime = row.lastSeenAt || row.router?.created_at || receivedAt
      const rowKey = routerSeedRowKey({ ...row, channelPubkey })
      const seedKey = `router:${rowKey}`
      const [previous] = await this.seedQueue.removeBy('bySeedKey', seedKey)
      const firstSeenAt = Math.min(previous?.firstSeenAt ?? rowTime, row.firstSeenAt ?? rowTime)
      const lastSeenAt = Math.max(previous?.lastSeenAt ?? rowTime, row.lastSeenAt ?? rowTime)
      await this.seedQueue.enqueue({
        ...row,
        type: 'seed',
        recordType: ROUTER_SEED_RECORD_TYPE,
        channelPubkey,
        receivedAt,
        firstSeenAt,
        lastSeenAt,
        meta: { channelPubkey: seed.channelPubkey },
        [SEED_KEY]: seedKey,
        [SEED_TIME]: lastSeenAt || rowTime
      })
    }
    await this.pruneStoredSeeds(channelPubkey)
  }

  checkCapacity () {
    if (this.capacityCheck) return this.capacityCheck
    const work = (async () => {
      if (this.closePromise || !this.pauseReasons.has('capacity')) return
      const { usedBytes, maxBytes } = await this.queue.getCapacity()
      if (!this.closePromise && usedBytes + this.capacityRequiredBytes <= maxBytes) await this.resume('capacity')
    })()
    this.capacityCheck = work
    work.then(() => { this.capacityCheck = null }, () => { this.capacityCheck = null })
    return work
  }

  wakeDeliveries () {
    for (const wake of this.deliveryWaiters) wake()
    this.deliveryWaiters.clear()
  }

  // A cancellable iterator, including while next() is waiting on an empty queue.
  messages () {
    let cancelled = false
    const held = new Set()
    let tail = Promise.resolve()
    const next = async () => {
      while (true) {
        if (cancelled || this.closePromise) break
        const delivery = await this.nextMessage()
        if (delivery) {
          if (cancelled || this.closePromise) { await delivery.nack(); break }
          held.add(delivery)
          delivery.settled.then(() => held.delete(delivery))
          return { value: delivery, done: false }
        }
        await new Promise(resolve => {
          const wake = () => { clearTimeout(timer); this.deliveryWaiters.delete(wake); resolve() }
          const timer = setTimeout(wake, 250)
          this.deliveryWaiters.add(wake)
          if (cancelled || this.closePromise) wake()
        })
      }
      return { done: true }
    }
    return {
      [Symbol.asyncIterator] () { return this },
      next: () => { const work = tail.then(next); tail = work.catch(() => {}); return work },
      return: async () => {
        cancelled = true
        this.wakeDeliveries()
        await tail
        await Promise.all([...held].map(delivery => delivery.nack()))
        return { done: true }
      }
    }
  }

  nextMessage () {
    const work = this.reserveMessage()
    this.deliveryReads.add(work)
    work.then(() => this.deliveryReads.delete(work), () => this.deliveryReads.delete(work))
    return work
  }

  async reserveMessage () {
    this.assertOpen()
    this.touchStorageActivity().catch(err => this.onError?.(err))
    await this.queueOperationTail
    const reservation = await this.queue.reserve()
    if (!reservation) return null
    if (this.closePromise) { await reservation.nack(); return null }
    let finish
    const settled = new Promise(resolve => { finish = resolve })
    const done = () => {
      clearInterval(timer)
      this.deliveries.delete(delivery)
      finish()
      this.wakeDeliveries()
    }
    const settle = async method => {
      const result = await reservation[method]()
      done()
      if (method === 'ack' && result && !this.closePromise && this.pauseReasons.has('capacity')) {
        this.checkCapacity().catch(err => this.onError?.(err))
      }
      return result
    }
    const delivery = {
      message: withoutQueueMetadata(reservation.item),
      ack: () => settle('ack'),
      nack: () => settle('nack')
    }
    Object.defineProperty(delivery, 'settled', { value: settled })
    this.deliveries.add(delivery)
    const timer = setInterval(() => {
      reservation.renew().then(ok => { if (!ok) done() }, err => { done(); this.onError?.(err) })
    }, 10000)
    timer.unref?.()
    return delivery
  }

  async ask ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkey, relays, relayToReceivers, message, code, payload, error, content, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      message,
      code,
      payload,
      error,
      content,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async reply ({ channelPubkey = this.defaultChannelPubkey(), question, receiverPubkey, relays, relayToReceivers, message, code, payload, error, content, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      message,
      code,
      payload,
      error,
      content,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async tell ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkey, relays, relayToReceivers, message, code, payload, error, content, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      message,
      code,
      payload,
      error,
      content,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async yell ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, message, code, payload, error, content, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      message,
      code,
      payload,
      error,
      content,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastRumor ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, rumor, deletionPubkey, signal }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers, signal })
    this.debugSend('broadcastRumor', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastRumor({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      rumor,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastEvent ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, event, deletionPubkey, signal }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers, signal })
    this.debugSend('broadcastEvent', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastEvent({
      senderSigner: this.userSigner,
      imkcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      event,
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastNymRumor ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, rumor, nymSigner, deletionPubkey }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const resolvedNymSigner = this.requireNymSigner(channel, nymSigner)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastNymRumor', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastNymRumor({
      nymSigner: resolvedNymSigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      ...routing,
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      rumor
    })
  }

  async broadcastNymEvent ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, event, nymSigner, deletionPubkey }) {
    const channel = this.requireWritableChannel(channelPubkey)
    const resolvedNymSigner = this.requireNymSigner(channel, nymSigner)
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, relays, relayToReceivers })
    this.debugSend('broadcastNymEvent', channelPubkey, { receiverPubkeys })
    return this._privateMessage.broadcastNymEvent({
      nymSigner: resolvedNymSigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderPubkey: channel.readerPubkey,
      ...routing,
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      event
    })
  }

  async publishSeederPresence (channelPubkey = this.defaultChannelPubkey(), { signal } = {}) {
    const channel = this.requireWritableChannel(channelPubkey)
    if (!this.offlineRecoverySecondsFor(channel)) return null
    const receiverPubkeys = uniq([...this.knownSeeders(channelPubkey), this.userPubkey])
    const routing = await this.resolveSendRouting({ channel, receiverPubkeys, signal })
    this.debugSend('yell', channelPubkey, { code: SEEDER_PRESENCE_CODE, receiverPubkeys })
    return this._privateMessage.yell({
      senderSigner: backgroundSigner(this.userSigner, this._waitForForeground, signal),
      imkcSigner: backgroundSigner(this.contentKeySigner, this._waitForForeground, signal),
      privateChannelSigner: backgroundSigner(channel.signer, this._waitForForeground, signal),
      privateChannelReaderPubkey: channel.readerPubkey,
      receiverPubkeys,
      ...routing,
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      code: SEEDER_PRESENCE_CODE,
      payload: {},
      onPreparedSeed: this.outgoingSeedHandler(channelPubkey),
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async startPresencePublisher (channelPubkey) {
    if (this.pauseReasons.size || !this.desiredChannels.has(channelPubkey) || !this.offlineRecoverySecondsFor(channelPubkey)) return
    if (this.presenceTimers.has(channelPubkey)) return
    if (this.presenceJobs.has(channelPubkey)) return this.presenceJobs.get(channelPubkey).work
    const job = { controller: new AbortController(), work: null }
    const active = () => this.presenceJobs.get(channelPubkey) === job && !this.pauseReasons.size && !this.closePromise && this.desiredChannels.has(channelPubkey)
    let publishing = false
    const publish = async () => {
      if (!active() || publishing) return
      publishing = true
      let release
      try {
        release = await this.presenceAdmission.acquire(job.controller.signal)
        job.controller.signal.addEventListener('abort', release, { once: true })
        if (active()) await this.publishSeederPresence(channelPubkey, { signal: job.controller.signal })
      } catch (error) {
        if (active()) this.onError?.(Object.assign(new Error(error?.message, { cause: error }), { operation: 'private-messenger.background', phase: 'presence' }))
      } finally {
        if (release) { job.controller.signal.removeEventListener('abort', release); release() }
        publishing = false
      }
    }
    const work = Promise.resolve().then(async () => {
      await publish()
      if (!active()) return
      const timer = this._setInterval(publish, this.seederPresenceIntervalMs)
      timer?.unref?.()
      this.presenceTimers.set(channelPubkey, timer)
    })
    job.work = work
    this.presenceJobs.set(channelPubkey, job)
    return work
  }

  stopPresencePublisher (channelPubkey) {
    this.presenceJobs.get(channelPubkey)?.controller.abort()
    this.presenceJobs.delete(channelPubkey)
    const timer = this.presenceTimers.get(channelPubkey)
    if (timer) this._clearInterval(timer)
    this.presenceTimers.delete(channelPubkey)
  }

  async reconcilePresencePublishers () {
    const starts = []
    for (const pubkey of [...this.presenceTimers.keys()]) {
      if (this.pauseReasons.size || !this.desiredChannels.has(pubkey) || !doesModeStoreRecoverySeeds(this.channels.get(pubkey)?.mode) || !this.offlineRecoverySecondsFor(pubkey)) this.stopPresencePublisher(pubkey)
    }
    for (const [pubkey, channel] of this.channels) {
      if (!this.pauseReasons.size && this.desiredChannels.has(pubkey) && doesModeStoreRecoverySeeds(channel.mode) && this.offlineRecoverySecondsFor(channel)) starts.push(this.startPresencePublisher(pubkey))
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
    if (!channel) throw new ValidationError('UNKNOWN_CHANNEL')
    return channel
  }

  requireWritableChannel (pubkey) {
    this.assertOpen()
    if (this.pauseReasons.size) throw this.pausedError()
    const channel = this.requireChannel(pubkey)
    if (!channel.signer) throw new ValidationError('PRIVATE_CHANNEL_WRITER_REQUIRED')
    return channel
  }

  autoDeletionCapabilityFor (channel) {
    return channel.autoDeletionCapability ?? this.autoDeletionCapability
  }

  requestedOfflineRecoverySecondsFor (channelOrPubkey) {
    const channel = typeof channelOrPubkey === 'string'
      ? this.channels.get(channelOrPubkey)
      : channelOrPubkey
    if (channel?.offlineRecoverySeconds !== undefined) return channel.offlineRecoverySeconds
    const pubkey = typeof channelOrPubkey === 'string' ? channelOrPubkey : channelOrPubkey?.pubkey
    const persisted = pubkey ? this.state.channels[pubkey]?.offlineRecoverySeconds : undefined
    return persisted === undefined
      ? this.offlineRecoverySeconds
      : normalizeOfflineRecoverySeconds(persisted)
  }

  offlineRecoverySecondsFor (channelOrPubkey) {
    return Math.min(
      this.requestedOfflineRecoverySecondsFor(channelOrPubkey),
      this.staleChannelSeconds,
      this.identityStorageRetentionSeconds
    )
  }

  staleChannelSecondsForCleanup () {
    return Math.min(this.staleChannelSeconds, this.identityStorageRetentionSeconds)
  }

  eventExpirationSecondsFor (channel) {
    return this.offlineRecoverySecondsFor(channel) || privateChannel.EXPIRATION_SECONDS
  }

  receivedChunkTtlMsFor (channel) {
    const seconds = this.offlineRecoverySecondsFor(channel)
    return seconds ? seconds * 1000 : DEFAULT_RECEIVED_CHUNK_TTL_MS
  }

  async applyRecoveryPolicies (channels, { prune = true } = {}) {
    return this.runQueueOperation(async () => {
      const state = this.readState()
      const now = nowSeconds()
      for (const channel of channels) {
        const current = state.channels[channel.pubkey] || {}
        const requestedSeconds = channel.offlineRecoverySeconds
        const effectiveSeconds = this.offlineRecoverySecondsFor(channel)
        current.offlineRecoverySeconds = requestedSeconds
        if (!effectiveSeconds) {
          delete current.openOfflineStart
          current.offlineRanges = []
        } else {
          const cutoff = now - effectiveSeconds
          current.offlineRanges = mergeRanges((current.offlineRanges || [])
            .filter(range => range.end >= cutoff)
            .map(range => ({ ...range, start: Math.max(range.start, cutoff) })))
          if (current.openOfflineStart) current.openOfflineStart = Math.max(current.openOfflineStart, cutoff)
        }
        state.channels[channel.pubkey] = current
      }
      this.writeState(state)
      await this.flushStateWrites()
      for (const channel of channels) {
        if (!this.offlineRecoverySecondsFor(channel)) this.dropRecoveryState(channel.pubkey)
      }
      if (prune) for (const channel of channels) await this.pruneStoredSeeds(channel.pubkey)
    })
  }

  requireNymSigner (channel, override) {
    const signer = override || channel?.nymSigner || this.nymSigner
    if (!signer?.getPublicKey) throw new ValidationError('NYM_SIGNER_REQUIRED')
    return signer
  }

  contentKeyLookup () {
    return this.useContentKeys ? undefined : noContentKeys
  }

  recordRecoveryWindow (pubkey) {
    const seconds = this.offlineRecoverySecondsFor(pubkey)
    if (!seconds) return
    const now = nowSeconds()
    const current = this.readState().channels[pubkey] || {}
    const checkpoint = Math.max(current.lastSeenAt || 0, current.recoveredThrough || 0)
    const since = checkpoint ? checkpoint - this.offlineSkewSeconds : now - seconds
    this.addOfflineRange(pubkey, Math.max(0, now - seconds, since), now)
  }

  scheduleReloadGap (pubkey) {
    this.cancelReloadGap(pubkey)
    if (!this.offlineRecoverySecondsFor(pubkey)) return
    const current = this.readState().channels[pubkey]
    const start = current?.openOfflineStart ?? current?.lastSeenAt
    if ((start == null && !current?.offlineRanges?.length) || this.pauseReasons.size) return
    const revision = this.watchRevisionByChannel.get(pubkey) || 0
    const token = {}
    const timer = this._setTimeout(async () => {
      const scheduled = this.reloadGapTimers.get(pubkey)
      if (scheduled?.token !== token) return
      this.reloadGapTimers.delete(pubkey)
      if (this.closePromise || !this.channels.has(pubkey) || !this.stopByChannel.has(pubkey)) return
      if ((this.watchRevisionByChannel.get(pubkey) || 0) !== revision) return
      if (start != null) this.addOfflineRange(pubkey, Math.max(0, start - this.offlineSkewSeconds), nowSeconds())
      try { await this.recoverOfflineRanges([pubkey]) } catch (err) { this.onError?.(err) }
      if (!this.closePromise && !this.pauseReasons.size && this.desiredChannels.has(pubkey) && this.hasPendingRecovery(pubkey)) {
        this.scheduleOfflineRecovery(pubkey, { startDelayMs: 1000 })
      }
    }, this.reloadGapDelayMs)
    this.reloadGapTimers.set(pubkey, { timer, token, revision })
  }

  cancelReloadGap (pubkey) {
    const scheduled = this.reloadGapTimers.get(pubkey)
    if (!scheduled) return
    this.reloadGapTimers.delete(pubkey)
    this._clearTimeout(scheduled.timer)
  }

  ensureNetworkWatchers () {
    if (typeof window === 'undefined') return
    if (!this.stopOffline) {
      const offline = () => { this.pauseInternally('network').catch(err => this.onError?.(err)) }
      window.addEventListener('offline', offline)
      this.stopOffline = () => window.removeEventListener('offline', offline)
    }
    if (!this.stopOnline) {
      const online = () => { this.syncPauseRecovery(); this.pauseRecoveries.get('network')?.wake() }
      window.addEventListener('online', online)
      this.stopOnline = () => window.removeEventListener('online', online)
    }
  }

  async askSeedersForMissingRange (channelPubkey, since, until) {
    const { asks } = await this.#askSeedersForMissingRangeAttempt(channelPubkey, since, until)
    return asks
  }

  async #askSeedersForMissingRangeAttempt (channelPubkey, since, until, { seeders } = {}) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return { asks: [], failures: [] }
    if (!this.channels.get(channelPubkey)?.signer) return { asks: [], failures: [] }
    const candidates = seeders ?? this.recoverySeeders(channelPubkey)
    if (!candidates.length || until < since) return { asks: [], failures: [] }

    const asks = []
    const failures = []
    for (const seeder of candidates) {
      try {
        const ask = await this.ask({
          channelPubkey,
          receiverPubkey: seeder,
          code: MISSING_MESSAGES_ASK_CODE,
          payload: { since, until }
        })
        asks.push(ask)
        const reports = ask?.delivery?.reports
        if (!Array.isArray(reports) || !reports.length || reports.some(report => report?.success !== true)) {
          throw new Error('PRIVATE_MESSAGE_NOT_PUBLISHED')
        }
      } catch (err) {
        failures.push({ seeder, error: err })
        console.warn('private-messenger seeder recovery ask failed', seeder, err?.message ?? err)
      }
    }
    return { asks, failures }
  }

  async askSeedersForRelayLeftEdge (channelPubkey, range, fetchedEvents) {
    const { asks } = await this.#askSeedersForRelayLeftEdgeAttempt(channelPubkey, range, oldestCreatedAt(fetchedEvents))
    return asks
  }

  async #askSeedersForRelayLeftEdgeAttempt (channelPubkey, range, oldest) {
    const until = oldest == null ? range.end : Math.min(range.end, oldest)
    if (until < range.start) return { asks: [], failures: [] }
    return this.#askSeedersForMissingRangeAttempt(channelPubkey, range.start, until)
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
      until,
      sendEmptyReply: !this.offlineRecoverySecondsFor(channelPubkey)
    })

    if (this.offlineRecoverySecondsFor(channelPubkey)) {
      for await (const seed of this.seedStorage ? this.seedStorage.iterate({ channelPubkey, receiverPubkey: message.event?.pubkey, since, until }) : this.seedQueue.storedItemsBy('byChannel', channelPubkey)) {
        if (seedRecordTime(seed) < nowSeconds() - this.offlineRecoverySecondsFor(channelPubkey)) continue
        if (this.seedStorage && !await this.seedStorage.has(seed)) continue
        await packer.update(seed)
      }
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
      await this.enqueueRumor(recovered.type, channelPubkey, {
        event: recovered.event,
        outer: recovered.outer,
        ...deliveryInfo(recovered.event, recovered.meta?.senderPubkey),
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
        meta: { channelPubkey, carriers: record.carriers, ...deliveryInfo(event, record.carriers[0]?.pubkey) },
        payload: parseEventContent(event)
      }
    }

    const routerRecord = record?.recordType === ROUTER_SEED_RECORD_TYPE ? record.router : null
    if (!isPrivateChannelRouter(routerRecord)) return null
    if (!this._privateChannel.unwrapEvent) throw new ValidationError('PRIVATE_CHANNEL_UNWRAP_UNSUPPORTED')

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
      meta: { channelPubkey, ...deliveryInfo(event, router.tags.find(tag => tag[0] === 'f')?.[1]) },
      payload: parseEventContent(event)
    }
  }

  async recoverOfflineRanges (channels = [...this.stopByChannel.keys()]) {
    const failures = []
    for (const pubkey of uniq(channels)) {
      if (this.pauseReasons.size || this.closePromise || !this.desiredChannels.has(pubkey)) continue
      let work = this.recoveries.get(pubkey)
      if (!work) {
        work = this.recoverChannelRanges([pubkey])
        this.recoveries.set(pubkey, work)
      }
      try { failures.push(...await work) } finally { if (this.recoveries.get(pubkey) === work) this.recoveries.delete(pubkey) }
    }
    return failures
  }

  recoveryFetchOptions ({ channelPubkey, channel, relays, since, until, signal, partial = false, resume = null }) {
    return {
      _acquirePage: pageSignal => this.recoveryAdmission.acquire(pageSignal),
      signal,
      receivedChunkScope: this.storageLeaseId,
      receiverSigner: this.userSigner,
      iykcSigner: this.contentKeySigner,
      privateChannelSigner: channel.signer,
      privateChannelReaderSigner: channel.readerSigner,
      privateChannelReaderPubkey: channel.readerPubkey,
      privateChannelPubkeys: [channelPubkey],
      receiverPubkey: this.userPubkey,
      relays,
      since: Math.max(0, since),
      until,
      partial,
      resume,
      mode: channel.mode,
      modeByPubkey: { [channelPubkey]: channel.mode },
      receivedChunkTtlMs: this.receivedChunkTtlMsFor(channel),
      receivedChunkIndexedDB: this._indexedDB,
      onEvent: (event, outer, meta) => this.receive(channelPubkey, () => this.enqueueRumor(eventType(event), channelPubkey, { event, outer, meta, payload: parseEventContent(event) }), { event, outer }),
      onNymEvent: (event, outer, meta) => this.receive(channelPubkey, () => this.enqueueRumor('nym', channelPubkey, { event, outer, meta, payload: parseEventContent(event) }), { event, outer }),
      onSeedEvent: seed => this.receive(channelPubkey, () => this.enqueueSeed(channelPubkey, seed), seed),
      onContentKeyUsage: usage => this.handleContentKeyUsage(channelPubkey, usage),
      onError: err => { throw err }
    }
  }

  async hedgedPriorityFetch ({ channelPubkey, channel, candidates, since, until }) {
    const launched = []
    let hedgeTimer = null
    let secondStarted = false
    const launch = candidate => {
      const controller = new AbortController()
      const promise = this._privateChannel
        .fetchHistory(this.recoveryFetchOptions({
          channelPubkey,
          channel,
          relays: [candidate.relay],
          since,
          until,
          signal: controller.signal,
          partial: true,
          resume: { [candidate.relay]: candidate.pending }
        }))
        .then(
          history => ({ ok: true, relay: candidate.relay, controller, history }),
          error => ({ ok: false, relay: candidate.relay, controller, error })
        )
      promise.catch(() => {})
      launched.push({ promise, controller })
      return promise
    }
    try {
      const first = launch(candidates[0])
      const hedge = candidates.length > 1
        ? new Promise(resolve => {
          hedgeTimer = this._setTimeout(() => {
            secondStarted = true
            resolve(launch(candidates[1]))
          }, this.priorityHedgeDelayMs)
        })
        : null
      let settled = hedge ? await Promise.race([first, hedge]) : await first
      if (!settled.ok) {
        if (secondStarted && launched.length > 1) settled = await launched[launched.length - 1].promise
        else if (candidates.length > 1) settled = await launch(candidates[1])
      }
      if (!settled.ok) return null
      for (const entry of launched) {
        if (entry.controller === settled.controller) continue
        try { entry.controller.abort() } catch { /* aborted */ }
      }
      return settled
    } finally {
      if (hedgeTimer) this._clearTimeout(hedgeTimer)
    }
  }

  async askPrioritySeeders (channelPubkey, intervals) {
    if (!intervals.length) return []
    if (!await this.checkRecoveryOnline()) return []
    const seeders = this.activeRecoverySeeders(channelPubkey)
    if (!seeders.length) return []
    const failures = []
    const claimed = []
    for (const interval of mergeRanges(intervals)) {
      const attempt = await this.#askSeedersForMissingRangeAttempt(channelPubkey, interval.start, interval.end, { seeders })
      failures.push(...attempt.failures.map(failure => failure.error))
      if (!attempt.failures.length) claimed.push(interval)
    }
    if (claimed.length) {
      for (const record of this.recoveryRanges.values()) {
        if (record.channelPubkey !== channelPubkey) continue
        const applied = intersectRanges(claimed, [{ start: record.start, end: record.end }])
        if (applied.length) this.applyRecoveryCoverage(record, applied)
      }
    }
    return failures
  }

  async runPriorityLane (channelPubkey, channel, ranges) {
    const priorities = this.recoveryPriorities(channelPubkey)
    if (!priorities.length) return
    const fetchRelays = await this.resolveWatchRelays(channel)
    for (const range of ranges) {
      const record = this.recoveryRangeRecord(channelPubkey, range)
      this.reconcileRecoveryRelays(record, fetchRelays)
      const priorityIntervals = intersectRanges(
        priorities.map(entry => ({ start: entry.start, end: entry.end })),
        [{ start: range.start, end: range.end }]
      )
      if (!priorityIntervals.length) continue
      const pending = subtractRanges({ start: range.start, end: range.end }, record.covered)
      const wanted = intersectRanges(priorityIntervals, pending)
      if (!wanted.length) continue
      const candidates = this.orderedRecoveryRelays(record)
        .map(([relay, state]) => ({ relay, pending: intersectRanges(state.pending, wanted) }))
        .filter(candidate => candidate.pending.length)
        .slice(0, this.priorityHedgeRelays)
      if (candidates.length) {
        const winner = await this.hedgedPriorityFetch({
          channelPubkey,
          channel,
          candidates,
          since: wanted[0].start,
          until: wanted[wanted.length - 1].end
        })
        if (winner?.history) {
          if (this.isPartialRecoveryHistory(winner.history)) this.updateRecoveryRecord(record, winner.history, { completeRelays: new Set() })
          else this.applyRecoveryCoverage(record, wanted)
        }
      }
      const uncovered = intersectRanges(wanted, subtractRanges({ start: range.start, end: range.end }, record.covered))
      if (uncovered.length) await this.askPrioritySeeders(channelPubkey, uncovered)
    }
    this.prunePriorityEntries(channelPubkey)
  }

  async recoverChannelRanges (channels) {
    const failures = []
    const state = this.readState()
    const now = nowSeconds()
    const online = await this.checkRecoveryOnline()

    for (const pubkey of uniq(channels)) {
      const channel = this.channels.get(pubkey)
      const current = state.channels[pubkey]
      if (!channel || !current?.offlineRanges?.length) {
        this.dropRecoveryState(pubkey)
        continue
      }
      const recoverySeconds = this.offlineRecoverySecondsFor(channel)
      if (!recoverySeconds) {
        this.dropRecoveryState(pubkey)
        continue
      }
      const minStart = now - recoverySeconds
      const processedRanges = new Set(current.offlineRanges.map(range => `${range.start}:${range.end}`))

      if (online) {
        try { await this.runPriorityLane(pubkey, channel, current.offlineRanges) } catch (error) { this.onError?.(error) }
      }

      const remaining = []
      let recoveredThrough = current.recoveredThrough || 0
      for (const range of current.offlineRanges) {
        if (range.end < minStart) continue
        if (!online) { remaining.push(range); continue }
        const watchRevision = this.watchRevisionByChannel.get(pubkey) || 0
        const controller = new AbortController()
        controller.channelPubkey = pubkey
        this.recoveryControllers.add(controller)
        try {
          if (this.pauseReasons.size || this.closePromise || !this.desiredChannels.has(pubkey)) throw this.pausedError()
          const fetchRelays = await this.resolveWatchRelays(channel)
          controller.signal.throwIfAborted()
          const record = this.recoveryRangeRecord(pubkey, range)
          this.reconcileRecoveryRelays(record, fetchRelays)
          const plan = this.recoveryRelayPlan(record)
          if (plan) {
            const history = await this._privateChannel.fetchHistory(this.recoveryFetchOptions({
              channelPubkey: pubkey,
              channel,
              relays: Object.keys(plan),
              since: range.start,
              until: range.end,
              signal: controller.signal,
              partial: true,
              resume: plan
            }))
            controller.signal.throwIfAborted()
            if (this.isPartialRecoveryHistory(history)) {
              this.updateRecoveryRecord(record, history)
              this.applyRecoveryRetryBudget(record)
              if ((history.relays || []).some(entry => entry.status !== 'eose' || entry.pending?.length)) {
                const error = this.recoveryIncompleteError(record, history, range)
                this.onError?.(error)
                failures.push(error)
              }
            } else {
              // Legacy fetch seam (tests/custom channels): resolves only when
              // the whole range was read, so treat it as fully relay-covered.
              record.covered = mergeRanges([...record.covered, { start: range.start, end: range.end }])
              if (history?.oldestCreatedAt != null) {
                record.oldestCreatedAt = record.oldestCreatedAt == null
                  ? history.oldestCreatedAt
                  : Math.min(record.oldestCreatedAt, history.oldestCreatedAt)
              }
              for (const state of record.relays.values()) {
                state.eose = true
                state.pending = []
              }
              record.tier = 'partial'
              record.lastErrors = []
            }
          }
          const lifecycleChanged = controller.signal.aborted || this.closePromise || !this.channels.has(pubkey) || !this.stopByChannel.has(pubkey) ||
            (this.watchRevisionByChannel.get(pubkey) || 0) !== watchRevision
          if (lifecycleChanged) {
            remaining.push(range)
            continue
          }
          if (this.recoveryRecordActive(record)) {
            remaining.push(range)
            continue
          }
          const result = await this.finalizeRecoveryRange({ channelPubkey: pubkey, range, record })
          failures.push(...result.failures)
          if (result.complete) recoveredThrough = Math.max(recoveredThrough, range.end)
          else remaining.push(range)
        } catch (err) {
          const record = this.recoveryRanges.get(this.recoveryRangeKey(pubkey, range))
          if (record && isPermanentRecoveryError(err)) {
            for (const state of record.relays.values()) state.dead = true
          }
          if (!(controller.liveInterrupted && err === controller.signal.reason)) this.onError?.(err)
          failures.push(err)
          remaining.push(range)
        } finally { this.recoveryControllers.delete(controller) }
      }
      const fresh = this.readState()
      const concurrentRanges = (fresh.channels[pubkey]?.offlineRanges || [])
        .filter(range => !processedRanges.has(`${range.start}:${range.end}`))
      fresh.channels[pubkey] = {
        ...(fresh.channels[pubkey] || {}),
        recoveredThrough: Math.max(fresh.channels[pubkey]?.recoveredThrough || 0, recoveredThrough),
        offlineRanges: mergeRanges(concurrentRanges.concat(remaining))
      }
      this.pruneRecoveryRanges(pubkey, remaining)
      this.writeState(fresh)
      await this.flushStateWrites()
    }
    return failures
  }

  async clearChannel (pubkey) {
    await this.unwatch(pubkey)
    await this._privateMessage.clearChannelState?.(pubkey)
    this.dropRecoveryState(pubkey)
    return this.runQueueOperation(async () => {
      this.channels.delete(pubkey)
      this.removeChannelState(pubkey)
      await this.flushStateWrites()
      await this.queue.removeBy('byChannel', pubkey)
      await (this.seedStorage ? this.seedStorage.removeLocal({ channelPubkey: pubkey }) : this.seedQueue.removeBy('byChannel', pubkey))
      await this.touchStorageActivity({ force: true })
      this.ensureRelayListWatcher()
    })
  }

  async clearQueue () {
    return this.runQueueOperation(() => this.queue.clear())
  }

  async cleanupStaleChannels ({ storageSnapshot } = {}) {
    if (!this.prefix) return
    storageSnapshot ||= await this.readStoragePolicySnapshot()
    if (!storageSnapshot) return
    const activeChannelPubkeys = new Set(storageSnapshot.activeChannelPubkeys || [])
    return this.runQueueOperation(async () => {
      await this.flushStateWrites()
      const state = { channels: await this.stateStore.load() }
      const cutoff = nowSeconds() - this.staleChannelSecondsForCleanup()
      const stalePubkeys = []
      for (const [pubkey, channel] of Object.entries(state.channels)) {
        if (activeChannelPubkeys.has(pubkey)) continue
        if ((channel.lastWatchedAt || 0) >= cutoff) continue
        delete state.channels[pubkey]
        stalePubkeys.push(pubkey)
        await this.queue?.removeBy('byChannel', pubkey)
        await (this.seedStorage ? this.seedStorage.removeLocal({ channelPubkey: pubkey }) : this.seedQueue?.removeBy('byChannel', pubkey))
      }
      for (const pubkey of stalePubkeys) this.dropRecoveryState(pubkey)
      this.state = state
      if (stalePubkeys.length) await this.removeChannelStates(stalePubkeys)
    })
  }

  async pruneStoredSeeds (channelPubkey) {
    if (this.seedStorage) { await this.seedStorage.prune({ now: nowSeconds() }); return }
    if (!this.seedQueue) return
    const keyRange = globalThis.IDBKeyRange
    if (!channelPubkey) {
      const pubkeys = new Set([...Object.keys(this.state.channels), ...this.channels.keys()])
      for (const pubkey of pubkeys) await this.pruneStoredSeeds(pubkey)
      await this.seedQueue.removeWhere(item => !pubkeys.has(item.channelPubkey))
      return
    }
    const recoverySeconds = this.offlineRecoverySecondsFor(channelPubkey)
    if (!recoverySeconds) {
      await this.seedQueue.removeBy('byChannel', channelPubkey)
      return
    }
    const cutoff = nowSeconds() - recoverySeconds
    if (cutoff <= 0) return
    if (keyRange?.bound) {
      await this.seedQueue.removeBy('byChannelTime', keyRange.bound([channelPubkey, 0], [channelPubkey, cutoff], false, true))
      return
    }
    await this.seedQueue.removeWhere(item => {
      if (item.channelPubkey !== channelPubkey) return false
      return (seedRecordTime(item) || item.receivedAt || 0) < cutoff
    })
  }

  close () {
    if (this.closePromise) return this.closePromise
    const initSettledPromise = this.initSettledPromise
    this.sendRoutingLifetime.abort()
    this.sendRelayExclusions.clear()
    this.pendingStorageWrites.clear()
    for (const task of this.pauseRecoveries.values()) task.stop()
    for (const task of this.watchRecoveries.values()) task.stop()
    let unwatchPromise
    try {
      unwatchPromise = Promise.resolve(this.unwatch())
    } catch (err) {
      unwatchPromise = Promise.reject(err)
    }
    for (const pubkey of [...this.presenceTimers.keys()]) this.stopPresencePublisher(pubkey)
    this.stopRelayListWatcher?.()
    this.stopRelayListWatcher = null
    this.relayListWatcherPubkey = ''
    this.stopOffline?.()
    this.stopOnline?.()
    this.stopOffline = null
    this.stopOnline = null
    this.stopStorageMaintenance()
    this.stopStoragePolicyBroadcast()
    clearInterval(this.capacityTimer)
    this.capacityTimer = null
    this.wakeDeliveries()

    this.closePromise = (async () => {
      let unwatchError
      try { await unwatchPromise } catch (err) { unwatchError = err }
      await initSettledPromise
      await this.updateTail.catch(() => {})
      await Promise.all([...this.extensions].map(extension => extension.close?.()))
      await this.stampActiveChannelActivity()
      await this.queueOperationTail
      await Promise.allSettled([...this.recoveries.values()])
      await Promise.allSettled([...this.deliveryReads])
      await Promise.all([...this.deliveries].map(delivery => delivery.nack()))
      await this.stateWriteTail
      try { await this.storageTouchPromise } catch {}
      await this.storageMaintenancePromise
      try { await this.storagePolicyRefreshTail } catch {}
      await Promise.all([
        this.queue?.close?.(),
        this.seedQueue?.close?.(),
        this.stateStore?.close?.()
      ])
      if (this.storageActive) {
        await releasePrivateMessengerStorage({
          userPubkey: this.userPubkey,
          leaseId: this.storageLeaseId,
          indexedDB: this._indexedDB
        })
      }
      this.storageActive = false
      if (this.identityStorageRetentionSeconds === 0) {
        await PrivateMessenger.maintainStorage({
          indexedDB: this._indexedDB,
          temporaryStorageArea: this.temporaryStorageArea
        })
      }
      this.recoveryRanges.clear()
      this.recoveryPriority.clear()
      if (unwatchError) throw unwatchError
    })()
    this.notifyStatus()
    return this.closePromise
  }
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

function relayMapReceivers (relayToReceivers) {
  if (!relayToReceivers) return []
  const values = relayToReceivers instanceof Map ? [...relayToReceivers.values()] : Object.values(relayToReceivers)
  return uniq(values.flat())
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

function withoutQueueMetadata (item) {
  if (!item) return null
  const value = { ...item }
  delete value[SEED_KEY]
  delete value[SEED_TIME]
  return value
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
