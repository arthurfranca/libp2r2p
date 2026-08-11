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
// await messenger.clearChannel(channelPubkey)
//
// Missed-message recovery:
// - Each watched channel stores lastSeenAt/lastWatchedAt in IndexedDB.
// - Re-watching after reload fetches the gap from lastSeenAt to now.
// - Browser offline/online events add explicit offline ranges with a small skew.
// - Recovery defaults to 7 days and can be overridden per channel; zero disables durable recovery.
// - Channel state not actively leased or watched within the configured stale window is pruned.
// - Seeders announce presence every 10min and are used for the relay-uncovered left edge of a missed range.
// - Configured seeders are all asked; auto-discovered seeders are capped to the 8 most recently active.
// - Seeder/watchtower channels store reconstructed router events in a separate IndexedDB queue and auto-reply to recovery asks.
// - Seeder replies stream compact routers with createMissingMessageReplyPacker({ messenger, question }).update(seed), then finalize(optionalLastSeed).
// - For other event-list replies, use createEventReplyPacker({ messenger, question, code }).update(event).

import * as privateMessage from '../private-message/index.js'
import { bytesToBase64 } from '../base64/index.js'
import { ValidationError } from '../error/index.js'
import { getRelaysByPubkey, pickRelaysForPubkeys, subscribeRelayListUpdates } from '../relay/index.js'
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
    keyPath: ['channelPubkey', 'type', 'event.id'],
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
    offlineRecoverySeconds = DEFAULT_OFFLINE_RECOVERY_SECONDS,
    staleChannelSeconds = DEFAULT_STALE_CHANNEL_SECONDS,
    identityStorageRetentionSeconds = DEFAULT_IDENTITY_STORAGE_RETENTION_SECONDS,
    offlineSkewSeconds = DEFAULT_OFFLINE_SKEW_SECONDS,
    reloadGapDelayMs = DEFAULT_RELOAD_GAP_DELAY_MS,
    seederPresenceIntervalMs = DEFAULT_SEEDER_PRESENCE_INTERVAL_MS,
    seederOnlineSeconds = DEFAULT_SEEDER_ONLINE_SECONDS,
    maxDynamicRecoverySeeders = DEFAULT_MAX_DYNAMIC_RECOVERY_SEEDERS,
    messageQueueMaxBytes = DEFAULT_MESSAGE_QUEUE_MAX_BYTES,
    seedQueueMaxBytes = DEFAULT_SEED_QUEUE_MAX_BYTES,
    temporaryStorageArea = globalThis.sessionStorage,
    autoDeletionCapability = true,
    _indexedDB = globalThis.indexedDB,
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
    _clearTimeout = globalThis.clearTimeout.bind(globalThis),
    _setInterval = globalThis.setInterval.bind(globalThis),
    _clearInterval = globalThis.clearInterval.bind(globalThis),
    _storageSetInterval = globalThis.setInterval.bind(globalThis),
    _storageClearInterval = globalThis.clearInterval.bind(globalThis),
    _BroadcastChannel = _indexedDB === globalThis.indexedDB ? globalThis.BroadcastChannel : undefined
  } = {}) {
    this.offlineRecoverySeconds = normalizeOfflineRecoverySeconds(offlineRecoverySeconds)
    this.staleChannelSeconds = normalizeStaleChannelSeconds(staleChannelSeconds)
    this.identityStorageRetentionSeconds = normalizeIdentityStorageRetentionSeconds(identityStorageRetentionSeconds)
    this.offlineSkewSeconds = offlineSkewSeconds
    this.reloadGapDelayMs = reloadGapDelayMs
    this.seederPresenceIntervalMs = seederPresenceIntervalMs
    this.seederOnlineSeconds = seederOnlineSeconds
    this.maxDynamicRecoverySeeders = maxDynamicRecoverySeeders
    this.messageQueueMaxBytes = messageQueueMaxBytes
    this.seedQueueMaxBytes = seedQueueMaxBytes
    this.temporaryStorageArea = temporaryStorageArea
    this.autoDeletionCapability = normalizeAutoDeletionCapability(autoDeletionCapability)
    this._indexedDB = _indexedDB
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
    this.channels = new Map()
    this.stopByChannel = new Map()
    this.reloadGapTimers = new Map()
    this.watchRevisionByChannel = new Map()
    this.presenceTimers = new Map()
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
        evictionPolicy: 'fifo',
        indexedDB: this._indexedDB
      })
      this.assertOpen()
      this.seedQueue = await createQueue({
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
      await this.pruneStoredSeeds()
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
      const pubkeys = [...this.channels.keys()]
      if (pubkeys.length) {
        await this.unwatch(pubkeys)
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
    return this.runQueueOperation(operation).catch(() => undefined)
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

  async update (options = {}) {
    this.assertOpen()
    const {
      userSigner = this.userSigner,
      contentKeySigner = this.contentKeySigner,
      nymSigner = this.nymSigner,
      channels = [...this.channels.values()],
      relays = [],
      mode = 'leecher'
    } = options
    const updatesStalePolicy = Object.hasOwn(options, 'staleChannelSeconds')
    const updatesIdentityPolicy = Object.hasOwn(options, 'identityStorageRetentionSeconds')
    let nextStaleChannelSeconds = updatesStalePolicy
      ? normalizeStaleChannelSeconds(options.staleChannelSeconds)
      : this.staleChannelSeconds
    let nextIdentityStorageRetentionSeconds = updatesIdentityPolicy
      ? normalizeIdentityStorageRetentionSeconds(options.identityStorageRetentionSeconds)
      : this.identityStorageRetentionSeconds
    if (userSigner) {
      const userPubkey = await userSigner.getPublicKey?.()
      if (!userPubkey) throw new ValidationError('USER_SIGNER_REQUIRED')
      if (this.userPubkey && userPubkey !== this.userPubkey) throw new ValidationError('USER_SIGNER_MISMATCH')
      this.userSigner = userSigner
    }
    this.contentKeySigner = contentKeySigner || null
    this.nymSigner = nymSigner || null
    this.contentKeyPubkey = await this.contentKeySigner?.getPublicKey?.() || ''
    const nextChannels = await this.normalizeChannels(channels, { relays, mode })
    this.assertOpen()
    const nextPubkeys = new Set(nextChannels.map(channel => channel.pubkey))
    const removedPubkeys = [...this.channels.keys()].filter(pubkey => !nextPubkeys.has(pubkey))
    const updatesStoragePolicy = updatesStalePolicy || updatesIdentityPolicy

    if (updatesStoragePolicy) {
      const currentPolicy = await this.readStoragePolicySnapshot()
      if (!updatesStalePolicy) nextStaleChannelSeconds = currentPolicy?.staleChannelSeconds ?? this.staleChannelSeconds
      if (!updatesIdentityPolicy) {
        nextIdentityStorageRetentionSeconds = currentPolicy?.identityStorageRetentionSeconds ?? this.identityStorageRetentionSeconds
      }
    }

    if (removedPubkeys.length) {
      await this.stampChannelActivity(removedPubkeys)
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
    for (const pubkey of removedPubkeys) this.channels.delete(pubkey)
    for (const channel of nextChannels) this.channels.set(channel.pubkey, channel)

    await this.cleanupStaleChannels({ storageSnapshot })
    await this.applyRecoveryPolicies(nextChannels)
    await this.watch([...nextPubkeys])
    await this.reconcilePresencePublishers()
    if (this.storagePolicyRevision === storageSnapshot.policyRevision) {
      this.storagePolicyNeedsApply = false
    }
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
      if (!pubkey) throw new ValidationError('CHANNEL_PUBKEY_REQUIRED')
      if (!signer && !readerSigner) throw new ValidationError('CHANNEL_SIGNER_REQUIRED')
      const mode = channel.mode || defaults.mode || 'leecher'
      if (!signer && doesModeStoreRecoverySeeds(mode)) throw new ValidationError('PRIVATE_CHANNEL_WRITER_REQUIRED')
      const readerPubkey = channel.readerPubkey || channel.privateChannelReaderPubkey || await readerSigner?.getPublicKey?.() || pubkey
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

  async readRelaysForPubkey (pubkey) {
    const relaysByPubkey = await this._getRelaysByPubkey([pubkey])
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
    if (!relayMapRelays(derived).length) throw new ValidationError('NO_RELAYS')
    return { relayToReceivers: derived, recoveryRelays }
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
      try { this.onError?.(err) } catch {}
    })
    return write
  }

  async flushStateWrites () {
    await this.stateWriteTail
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
    const recoverySeconds = this.offlineRecoverySecondsFor(pubkey)
    if (!recoverySeconds) return
    const now = nowSeconds()
    const minStart = now - recoverySeconds
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
    for (const pubkey of Object.keys(state.channels)) {
      const current = state.channels[pubkey]
      if (!current.openOfflineStart) continue
      const recoverySeconds = this.offlineRecoverySecondsFor(pubkey)
      if (!recoverySeconds) {
        delete current.openOfflineStart
        current.offlineRanges = []
        state.channels[pubkey] = current
        continue
      }
      const minStart = end - recoverySeconds
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
    this.assertOpen()
    const channelPubkeys = uniq(channels)
    for (const pubkey of channelPubkeys) {
      const channel = this.channels.get(pubkey)
      if (!channel) throw new ValidationError('UNKNOWN_CHANNEL')
      const watchRelays = await this.resolveWatchRelays(channel)
      this.assertOpen()
      const stop = await this._privateMessage.watch({
        channels: [pubkey],
        relays: watchRelays,
        receiverSigner: this.userSigner,
        iykcSigner: this.contentKeySigner,
        privateChannelSigner: channel.signer,
        privateChannelReaderSigner: channel.readerSigner,
        privateChannelReaderPubkey: channel.readerPubkey,
        mode: channel.mode,
        onAsk: message => this.queueIncoming(() => this.handleAsk(pubkey, message)),
        onReply: message => this.queueIncoming(() => this.handleReply(pubkey, message)),
        onTell: message => this.queueIncoming(() => this.handleTell(pubkey, message)),
        onYell: message => this.queueIncoming(() => this.handleYell(pubkey, message)),
        onNym: message => this.queueIncoming(() => this.handleNym(pubkey, message)),
        onMessage: message => this.queueIncoming(() => this.handleMessage(pubkey, message)),
        onSeed: seed => this.queueIncoming(() => this.enqueueSeed(pubkey, seed)),
        onContentKeyUsage: usage => this.handleContentKeyUsage(pubkey, usage),
        receivedChunkTtlMs: this.receivedChunkTtlMsFor(channel),
        receivedChunkIndexedDB: this._indexedDB,
        onError: err => this.onError?.(err)
      })
      if (this.closePromise) {
        await stop?.()
        this.assertOpen()
      }
      this.stopByChannel.set(pubkey, stop)
      this.watchRevisionByChannel.set(pubkey, (this.watchRevisionByChannel.get(pubkey) || 0) + 1)
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
      if (scheduleReloadGap) this.scheduleReloadGap(pubkey)
    }
    this.ensureNetworkWatchers()
    this.ensureRelayListWatcher()
    return this
  }

  unwatch (channels) {
    const channelPubkeys = channels ? uniq(Array.isArray(channels) ? channels : [channels]) : [...this.stopByChannel.keys()]
    const closing = []
    for (const pubkey of channelPubkeys) {
      this.cancelReloadGap(pubkey)
      this.watchRevisionByChannel.set(pubkey, (this.watchRevisionByChannel.get(pubkey) || 0) + 1)
      const close = this.stopByChannel.get(pubkey)?.()
      if (close && typeof close.then === 'function') closing.push(close)
      this.stopByChannel.delete(pubkey)
      this.stopPresencePublisher(pubkey)
    }
    this.ensureRelayListWatcher()
    return Promise.allSettled(closing)
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
    this.trackSeederActivity(channelPubkey, message)
    if (doesModeStoreRecoverySeeds(this.channels.get(channelPubkey)?.mode) && messageCode(message) === MISSING_MESSAGES_ASK_CODE) {
      await this.replyWithStoredSeeds(channelPubkey, message)
      return
    }
    await this.enqueueRumor('ask', channelPubkey, message)
  }

  async handleReply (channelPubkey, message) {
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
    this.trackSeederActivity(channelPubkey, message)
    if (messageCode(message) === SEEDER_PRESENCE_CODE) return
    await this.enqueueRumor('yell', channelPubkey, message)
  }

  async handleNym (channelPubkey, message) {
    await this.enqueueRumor('nym', channelPubkey, message)
  }

  async handleMessage (channelPubkey, message) {
    if (eventType(message.event) !== 'message') return
    this.trackSeederActivity(channelPubkey, message)
    await this.enqueueRumor('message', channelPubkey, message)
  }

  async enqueueRumor (type, channelPubkey, message) {
    const channel = this.channels.get(channelPubkey)
    if (channel?.mode === 'watchtower' && type !== 'ask') return
    this.markSeen(channelPubkey, message.outer?.created_at || message.event?.created_at || nowSeconds())
    const eventId = message.event?.id || ''
    const dedupeKey = eventId ? [channelPubkey, type, eventId] : null
    if (dedupeKey && await this.queue.someBy('byChannelTypeEventId', dedupeKey)) {
      this.debug('dedupe', debugMessageInfo(type, channelPubkey, message))
      return
    }
    try {
      await this.queue.enqueue({
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
    } catch (err) {
      // The unique index closes the small cross-instance race after `someBy`.
      if (dedupeKey && err?.name === 'ConstraintError') {
        this.debug('dedupe', debugMessageInfo(type, channelPubkey, message))
        return
      }
      throw err
    }
    this.debug('enqueue', debugMessageInfo(type, channelPubkey, message))
    this.onMessageQueued?.()
  }

  async enqueueSeed (channelPubkey, seed) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return
    const receivedAt = nowSeconds()
    if (seed.recordType === NYM_CARRIER_SEED_RECORD_TYPE || seed.carriers?.length) {
      const carriers = compactSeedNymCarriers(seed.carriers)
      const recordTime = nymCarrierRecordTime({ carriers }) || seed.outer?.created_at || receivedAt
      this.markSeen(channelPubkey, recordTime)
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
    let newest = seed.outer?.created_at || receivedAt
    for (const row of rows) {
      const rowTime = row.lastSeenAt || row.router?.created_at || receivedAt
      newest = Math.max(newest, rowTime)
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
    this.markSeen(channelPubkey, newest)
    await this.pruneStoredSeeds(channelPubkey)
  }

  async * messages () {
    // seedQueue is retained replay material for recovery replies, not an app-message stream.
    for await (const item of this.queue.items()) yield withoutQueueMetadata(item)
  }

  async nextMessage () {
    // seedQueue is retained replay material for recovery replies, not an app-message stream.
    this.touchStorageActivity().catch(err => this.onError?.(err))
    await this.queueOperationTail
    return withoutQueueMetadata(await this.queue.shift())
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
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastRumor ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, rumor, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      rumor,
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async broadcastEvent ({ channelPubkey = this.defaultChannelPubkey(), receiverPubkeys, relays, relayToReceivers, event, deletionPubkey }) {
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      deletionPubkey,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      event,
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

  async publishSeederPresence (channelPubkey = this.defaultChannelPubkey()) {
    const channel = this.requireWritableChannel(channelPubkey)
    if (!this.offlineRecoverySecondsFor(channel)) return null
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
      expirationSeconds: this.eventExpirationSecondsFor(channel),
      temporaryStorageArea: this.temporaryStorageArea,
      autoDeletionCapability: this.autoDeletionCapabilityFor(channel),
      code: SEEDER_PRESENCE_CODE,
      payload: {},
      _getIykcProofs: this.contentKeyLookup()
    })
  }

  async startPresencePublisher (channelPubkey) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return
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
      if (!doesModeStoreRecoverySeeds(this.channels.get(pubkey)?.mode) || !this.offlineRecoverySecondsFor(pubkey)) this.stopPresencePublisher(pubkey)
    }
    for (const [pubkey, channel] of this.channels) {
      if (doesModeStoreRecoverySeeds(channel.mode) && this.offlineRecoverySecondsFor(channel)) starts.push(this.startPresencePublisher(pubkey))
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

  async applyRecoveryPolicies (channels) {
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
      for (const channel of channels) await this.pruneStoredSeeds(channel.pubkey)
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

  scheduleReloadGap (pubkey) {
    this.cancelReloadGap(pubkey)
    if (!this.offlineRecoverySecondsFor(pubkey)) return
    const current = this.readState().channels[pubkey]
    const start = current?.openOfflineStart || current?.lastSeenAt
    if (!start) return
    const revision = this.watchRevisionByChannel.get(pubkey) || 0
    const token = {}
    const timer = this._setTimeout(async () => {
      const scheduled = this.reloadGapTimers.get(pubkey)
      if (scheduled?.token !== token) return
      this.reloadGapTimers.delete(pubkey)
      if (this.closePromise || !this.channels.has(pubkey) || !this.stopByChannel.has(pubkey)) return
      if ((this.watchRevisionByChannel.get(pubkey) || 0) !== revision) return
      this.addOfflineRange(pubkey, Math.max(0, start - this.offlineSkewSeconds), nowSeconds())
      await this.recoverOfflineRanges([pubkey])
    }, this.reloadGapDelayMs)
    this.reloadGapTimers.set(pubkey, { timer, token, revision })
  }

  cancelReloadGap (pubkey) {
    const scheduled = this.reloadGapTimers.get(pubkey)
    if (!scheduled) return
    this.reloadGapTimers.delete(pubkey)
    this._clearTimeout(scheduled.timer)
  }

  // Browser-offline recovery owns durable gaps. Stop only the child live reads;
  // unwatch() would also stop seeder-presence publishing and alter channel state.
  #pauseLiveWatches () {
    for (const [pubkey, stop] of this.stopByChannel) {
      this.cancelReloadGap(pubkey)
      this.watchRevisionByChannel.set(pubkey, (this.watchRevisionByChannel.get(pubkey) || 0) + 1)
      stop?.()
    }
    this.stopByChannel.clear()
  }

  async #resumeLiveWatches () {
    const channelPubkeys = [...this.channels.keys()]
    await this.watch(channelPubkeys, { scheduleReloadGap: false })
    return channelPubkeys
  }

  ensureNetworkWatchers () {
    if (typeof window === 'undefined') return
    if (!this.stopOffline) {
      const offline = () => {
        const state = this.readState()
        const start = Math.max(0, nowSeconds() - this.offlineSkewSeconds)
        for (const pubkey of this.channels.keys()) {
          if (!this.offlineRecoverySecondsFor(pubkey)) continue
          const current = state.channels[pubkey] || {}
          current.openOfflineStart ||= start
          state.channels[pubkey] = current
        }
        this.writeState(state)
        this.#pauseLiveWatches()
      }
      window.addEventListener('offline', offline)
      this.stopOffline = () => window.removeEventListener('offline', offline)
    }
    if (!this.stopOnline) {
      const online = async () => {
        this.closeOpenOfflineRanges()
        const channelPubkeys = await this.#resumeLiveWatches()
        await this.recoverOfflineRanges(channelPubkeys)
      }
      window.addEventListener('online', online)
      this.stopOnline = () => window.removeEventListener('online', online)
    }
  }

  async askSeedersForMissingRange (channelPubkey, since, until) {
    const { asks } = await this.#askSeedersForMissingRangeAttempt(channelPubkey, since, until)
    return asks
  }

  async #askSeedersForMissingRangeAttempt (channelPubkey, since, until) {
    if (!this.offlineRecoverySecondsFor(channelPubkey)) return { asks: [], failures: [] }
    if (!this.channels.get(channelPubkey)?.signer) return { asks: [], failures: [] }
    const seeders = this.recoverySeeders(channelPubkey)
    if (!seeders.length || until < since) return { asks: [], failures: [] }

    const asks = []
    const failures = []
    for (const seeder of seeders) {
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
    const { asks } = await this.#askSeedersForRelayLeftEdgeAttempt(channelPubkey, range, fetchedEvents)
    return asks
  }

  async #askSeedersForRelayLeftEdgeAttempt (channelPubkey, range, fetchedEvents) {
    const oldest = oldestCreatedAt(fetchedEvents)
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
      for await (const seed of this.seedQueue.storedItemsBy('byChannel', channelPubkey)) {
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
      meta: { channelPubkey },
      payload: parseEventContent(event)
    }
  }

  async recoverOfflineRanges (channels = [...this.stopByChannel.keys()]) {
    const state = this.readState()
    const now = nowSeconds()

    for (const pubkey of uniq(channels)) {
      const channel = this.channels.get(pubkey)
      const current = state.channels[pubkey]
      if (!channel || !current?.offlineRanges?.length) continue
      const recoverySeconds = this.offlineRecoverySecondsFor(channel)
      if (!recoverySeconds) continue
      const minStart = now - recoverySeconds
      const processedRanges = new Set(current.offlineRanges.map(range => `${range.start}:${range.end}`))

      const remaining = []
      for (const range of current.offlineRanges) {
        if (range.end < minStart) continue
        const watchRevision = this.watchRevisionByChannel.get(pubkey) || 0
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
            receivedChunkTtlMs: this.receivedChunkTtlMsFor(channel),
            receivedChunkIndexedDB: this._indexedDB,
            onEvent: (event, outer, meta) => this.queueIncoming(() => this.enqueueRumor(eventType(event), pubkey, { event, outer, meta, payload: parseEventContent(event) })),
            onNymEvent: (event, outer, meta) => this.queueIncoming(() => this.enqueueRumor('nym', pubkey, { event, outer, meta, payload: parseEventContent(event) })),
            onSeedEvent: seed => this.queueIncoming(() => this.enqueueSeed(pubkey, seed)),
            onContentKeyUsage: usage => this.handleContentKeyUsage(pubkey, usage),
            onError: err => { throw err }
          }) || []
          const attempt = await this.#askSeedersForRelayLeftEdgeAttempt(pubkey, range, fetchedEvents)
          const lifecycleChanged = this.closePromise || !this.channels.has(pubkey) || !this.stopByChannel.has(pubkey) ||
            (this.watchRevisionByChannel.get(pubkey) || 0) !== watchRevision
          if (lifecycleChanged || attempt.failures.length) remaining.push(range)
        } catch (err) {
          this.onError?.(err)
          remaining.push(range)
        }
      }
      const fresh = this.readState()
      const concurrentRanges = (fresh.channels[pubkey]?.offlineRanges || [])
        .filter(range => !processedRanges.has(`${range.start}:${range.end}`))
      fresh.channels[pubkey] = {
        ...(fresh.channels[pubkey] || {}),
        offlineRanges: mergeRanges(concurrentRanges.concat(remaining))
      }
      this.writeState(fresh)
    }
  }

  async clearChannel (pubkey) {
    return this.runQueueOperation(async () => {
      await this.unwatch(pubkey)
      await this._privateMessage.clearChannelState?.(pubkey)
      this.channels.delete(pubkey)
      this.removeChannelState(pubkey)
      await this.flushStateWrites()
      await this.queue.removeBy('byChannel', pubkey)
      await this.seedQueue.removeBy('byChannel', pubkey)
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
        await this.seedQueue?.removeBy('byChannel', pubkey)
      }
      this.state = state
      if (stalePubkeys.length) await this.removeChannelStates(stalePubkeys)
    })
  }

  async pruneStoredSeeds (channelPubkey) {
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

    this.closePromise = (async () => {
      let unwatchError
      try { await unwatchPromise } catch (err) { unwatchError = err }
      await initSettledPromise
      await this.stampActiveChannelActivity()
      await this.queueOperationTail
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
      if (unwatchError) throw unwatchError
    })()
    return this.closePromise
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
