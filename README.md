# libp2r2p

Peer-to-relay-to-peer utilities for Nostr apps.

libp2r2p focuses on flows where one peer talks to another peer with Nostr
relays in the middle. It is not pure peer-to-peer networking; relays provide
the transport and discovery surface. The package was born to distribute the
private messenger reference implementation, and it also carries a few Nostr
power-ups used by that messenger.

## Private Messenger

The main API is `createPrivateMessenger` from `libp2r2p/private-messenger`.
It coordinates private-channel wrapping, relay watching, recovery state, and
content-key lookup for direct or group-style private app messages.

```js
import { createPrivateMessenger } from 'libp2r2p/private-messenger'

const messenger = await createPrivateMessenger({
  userSigner,
  contentKeySigner,
  offlineRecoverySeconds: 7 * 24 * 60 * 60,
  staleChannelSeconds: 45 * 24 * 60 * 60,
  identityStorageRetentionSeconds: 60 * 24 * 60 * 60,
  channels: [{
    signer: privateChannelSigner,
    relays: ['wss://relay.example'],
    mode: 'leecher',
    offlineRecoverySeconds: 30 * 24 * 60 * 60
  }],
  onError: err => console.warn('private messenger failed', err)
})

async function logMessages () {
  for await (const message of messenger.messages()) {
    console.log(message.type, message.payload)
  }
}

logMessages().catch(err => console.warn('private messenger messages failed', err))

await messenger.tell({
  receiverPubkey,
  payload: { text: 'hello' }
})
```

### Deleting Private Broadcasts

By default, each high-level private-message send creates one fresh deletion
keypair for its logical message. Every outer kind `3560` event produced for that
send, including router chunks, recipient subsets, and nym carriers, carries the
same public key in its `s` tag. The result always contains `delivery.reports`.
When libp2r2p generated the keypair, it also contains
`delivery.deletionSeckey`; the public key can be derived from that secret.

This shared `s` value deliberately makes the outer events for one logical send
linkable to relay operators and other observers. Disable automatic capabilities
when that tradeoff is not acceptable. The messenger-wide setting defaults to
`true`, and a channel setting takes precedence:

```js
const messenger = await createPrivateMessenger({
  userSigner,
  autoDeletionCapability: false,
  channels: [{
    signer: privateChannelSigner,
    relays: ['wss://relay.example'],
    autoDeletionCapability: true
  }]
})
```

With automatic capabilities disabled and no caller-supplied key, the outer
events have no `s` tag. They are not deliberately linkable through this
extension, but cannot later be deleted with it. A caller that already owns a
deletion key can supply its public key on an individual send; libp2r2p then
does not generate or return a key. Use a fresh caller-owned key for each
logical message unless cross-message linkability is intentional:

```js
import { generateKeypair } from 'libp2r2p/key'

const deletionKey = generateKeypair()
await messenger.tell({
  receiverPubkey,
  payload: { text: 'remove this later' },
  deletionPubkey: deletionKey.pubkey
})
```

```js
import { finalizeEvent } from 'libp2r2p/event'
import { keypairFromSeckey } from 'libp2r2p/key'
import { relayPool } from 'libp2r2p/relay'

const sent = await messenger.tell({
  receiverPubkey,
  payload: { text: 'remove this later' }
})

if (sent.delivery.deletionSeckey) {
  const deletionKey = keypairFromSeckey(sent.delivery.deletionSeckey)
  // Persist this secret with the application's copy of the logical message.

  const { result: outerEvents } = await relayPool.getEvents({
    kinds: [3560],
    authors: [channelPubkey],
    '#s': [deletionKey.pubkey]
  }, relays)
  for (let offset = 0; offset < outerEvents.length; offset += 100) {
    const deletion = finalizeEvent({
      kind: 5,
      created_at: Math.floor(Date.now() / 1000),
      tags: [['k', '3560'], ...outerEvents.slice(offset, offset + 100).map(event => ['e', event.id])],
      content: ''
    }, deletionKey.secretKey)
    await relayPool.sendEvent(deletion, relays)
  }
}
```

The `s` tag is public metadata and a deletion capability, not the channel key
or sender identity. libp2r2p does not delete anything automatically.

Relay support for this capability is not universal. A relay that implements it
should accept only a kind `5` request signed by the matching `s` key with exactly
one `['k', '3560']` tag, explicit matching `e` targets, and no `a` tags. A
regular NIP-09 kind `5` request signed by the outer event's private-channel key
must not delete a kind `3560` event, whether or not that event has an `s` tag.

### Storage Maintenance

While an outgoing private message is being assembled, the messenger keeps
encrypted envelope rows and router chunks in `sessionStorage`. They are
removed when the send finishes, but an interrupted browser operation can leave
them behind until cleanup runs.

`PrivateMessenger.init()` awaits storage maintenance automatically. Call
`PrivateMessenger.maintainStorage()` during app startup when messenger
initialization may be delayed, such as while an account is locked:

```js
import { PrivateMessenger } from 'libp2r2p/private-messenger'

PrivateMessenger.maintainStorage().catch(console.warn)
```

Maintenance removes interrupted-send staging, expired receive chunks, and
storage belonging to inactive principal identities. It also
resumes any interrupted storage-set deletion. An application does not need to
know database names or enumerate IndexedDB. Pass `temporaryStorageArea` only
when the messenger was configured to use a Storage area other than the default
`sessionStorage`.

Each principal signer owns an internal storage set containing message,
recovery-seed, and channel-state databases. The messenger updates its activity
lease while it is open and closes all handles in `await messenger.close()`.
The complete set is removed after `identityStorageRetentionSeconds` without
use (60 days by default), including messages that were never consumed.
Maintenance runs on every initialization and every six hours while a messenger
is active; failed deletions remain journaled and retry automatically.

Channel recovery state inside an otherwise active identity uses the separate
`staleChannelSeconds` cleanup policy (45 days by default). Active instances
record the channels they administer, so a channel remains protected while any
instance or tab still uses it. Offline recovery defaults to seven days.
Set `offlineRecoverySeconds` on an individual channel to override the
messenger default for its recovery seeds, offline ranges, new outer-event
expiration, and new incomplete receive groups. Updating a channel applies a
shorter window immediately to its stored seeds and ranges; increasing it does
not recreate data already removed.

The effective recovery duration is capped by both `staleChannelSeconds` and
`identityStorageRetentionSeconds`. The requested per-channel value remains
stored separately, so raising a cap affects new retention decisions without
recreating data already removed. Both retention policies can be changed by an
instance at runtime; omitted fields retain their current persisted values:

```js
await messenger.update({
  staleChannelSeconds: 30 * 24 * 60 * 60,
  identityStorageRetentionSeconds: 90 * 24 * 60 * 60
})
```

The policies are persisted per principal identity. If multiple instances use
the same identity, the last confirmed policy update wins and is propagated to
the others. A zero policy disables durable recovery immediately. Identity
storage itself remains protected until the final active lease closes.

Set a channel's `offlineRecoverySeconds` to `0` to disable durable recovery for
that channel. The messenger then stores no recovery seeds, tracks no offline
ranges, contacts no seeders, publishes no seeder presence, and uses no recovery
mirror relays. Live delivery remains usable: new outer events retain the
private-channel two-day technical expiration and incomplete receive groups use
the one-hour technical TTL. Existing signed events and receive groups retain
the deadlines chosen when they were created.

Recovery metadata is separate from that temporary send staging. Per-channel
`lastSeenAt`, offline ranges, and related state are stored in IndexedDB. Raw
incomplete receive chunks are also stored in IndexedDB and share a 16 MiB
logical budget. Direct private-channel calls give each new group a one-hour
TTL; PrivateMessenger groups use the effective recovery window of their
channel, or one hour when durable recovery is disabled. The TTL is persisted
per group, so another caller opening the shared database or a later channel
configuration update cannot change it. Capacity
eviction removes whole
least-recently-used message groups so a partial group is never mistaken for a
complete one. `receivedChunkTtlMs`, `receivedChunkMaxBytes`, and
`receivedChunkIndexedDB` may be supplied to the private-channel APIs when an
embedding environment needs different limits or an injected IDB factory.
Legacy Web Storage recovery records are neither read nor migrated.

The recovery-seed queue has a shared 64 MiB logical budget by default and uses
FIFO eviction. A channel recovery duration is therefore a maximum retention
window, not a guarantee that every seed remains available until its deadline.

Signers are expected to expose the Nostr-style methods used by the messenger,
including `getPublicKey()`, `signEvent(event)`, and the NIP-44 v3 methods
needed by private channels. For double-DH content-key use, pass a
`contentKeySigner` or a signer implementation that handles content keys
internally.

Messages are stored in a bounded, durable IndexedDB queue until consumed or
until the principal identity has been inactive for 60 days:

```js
async function handleMessages () {
  for await (const message of messenger.messages()) {
    if (message.type === 'message') {
      console.log(message.payload)
    }
  }
}

handleMessages().catch(err => console.warn('private messenger messages failed', err))
```

For one-at-a-time consumption, use `await messenger.nextMessage()`. Queue
clearing is asynchronous too: `await messenger.clearChannel(channelPubkey)`.

Use explicit subpath imports for bundle size. The package root re-exports the
main messenger API for convenience, but applications that only need one piece
should import that subpath directly.

## Nostr primitives

The modern stack can use the package without `nostr-tools`. Its intentionally
small public surface includes strict, non-caching NIP-01 helpers, NIP-04 for
legacy interoperability, NIP-44 v2, key helpers, event-kind classification,
NIP-05 lookup, NIP-96 compatibility, NIP-98 authorization, Nostr Web Tokens,
and relay URL normalization:

```js
import {
  assertSerializableEvent,
  assertValidEvent,
  finalizeEvent,
  isSerializableEvent,
  isValidEvent
} from 'libp2r2p/event'
import { generateSecretKey, getPublicKey } from 'libp2r2p/key'
import { eventKinds, classifyKind } from 'libp2r2p/kind'
import * as nip44 from 'libp2r2p/nip44'
import { assertValidPublicRelayUrl, normalizeRelayUrl } from 'libp2r2p/url'
```

`classifyEvent()` from `libp2r2p/event` combines the exact NIP-01 kind
ranges with tag-defined behavior. The first `d` tag may add `replaceable` or
`addressable`, while an `expiration` tag equal to `created_at` adds
`ephemeral`. An event is also regular when it is neither replaceable nor
addressable. Classifications are additive and callers can disable the legacy
kind ranges with `{ includeLegacyKindRanges: false }`.

NIP-44 v2 uses the interoperable `nip44-v2` salt by default. A custom UTF-8
salt of at most 32 bytes may be passed to `getConversationKey()`, but messages
derived with it are not interoperable with standard NIP-44 implementations.

NIP-46 clients and bunker signers use a 30-second operation timeout by
default. Set `timeout` in the `Nip46Client`/`BunkerSigner` constructor to
choose another default, override it for an individual `connect()` or RPC, or
pass `timeout: null` explicitly when an operation is intentionally allowed to
wait indefinitely.

Nostr Web Tokens are available from `libp2r2p/nwt`. Creation returns a signed
kind `27519` event, while transport encoding is kept separate:

```js
import { createToken, encodeToken, validateToken } from 'libp2r2p/nwt'

const event = await createToken({
  signEvent,
  audience: ['api.example.com'],
  expiration: Math.floor(Date.now() / 1000) + 300,
  claims: [['action', 'upload']],
  content: 'Authorize an upload'
})
const authorization = encodeToken(event, { includeAuthorizationScheme: true })
const claims = validateToken(authorization, { audience: 'api.example.com' })
```

Transport decoding requires canonical unpadded Base64URL. Validation verifies
the Nostr signature on every call, enforces registered-claim cardinality and
time bounds, and requires the verifier to provide its identity whenever an
`aud` claim is present. Tokens without `aud` or `exp` retain the draft
specification's public/unbounded defaults; servers can reject those forms with
`requireAudience` and `requireExpiration`.

The NIP-96 module is provided only for interoperability with older file
servers. New applications should prefer NIP-B7. Its upload API accepts an
`AbortSignal` and a ProgressEvent-compatible callback; browsers use XHR for
real upload progress when available, while the fetch fallback reports only
estimated start and successful completion.

`isSerializableEvent()` checks only the NIP-01 fields used during
serialization. `isValidEvent()` additionally recalculates the ID and verifies
the Schnorr signature on every call; it never adds a cache marker to the
event. Their `assert…` counterparts return the original event or throw a
`ValidationError` with a stable code.

Public validity checks consistently use a non-throwing `is…` predicate plus an
`assert…` counterpart when callers need the exact reason. Strict codecs,
decoders, token validation, and malformed public arguments also throw
`ValidationError` from `libp2r2p/error`. Network, timeout, abort, quota, and
closed-state failures remain ordinary operational errors.

NIP-04 remains available at
`libp2r2p/nip04` only for compatibility with older Nostr applications.
Low-level relay sockets, subscriptions, message parsing, and serialization are
internal implementation details; use `RelayPool` or the `relayPool` singleton
from `libp2r2p/relay`.

## Binary encodings

Base16, Base36, Base62, Base64/Base64URL, and Base93 helpers are available
through their matching `libp2r2p/<encoding>` subpaths. Base36 exposes both a
binary-safe variable-width codec and the canonical 32-byte/50-character
Base36Nsite representation from NIP-5A. Base62 uses the same case-sensitive
alphabet as app NIP-19 entities; its default byte mode preserves leading zero
bytes, while integer mode supports fixed-width identifiers.

In NIP-5A, "no padding" means that no separate padding character such as `=`
is used. Leading `0` digits are nevertheless required to make every Nsite
Base36 value exactly 50 characters long.
