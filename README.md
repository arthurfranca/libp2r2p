# libp2r2p

Peer-to-relay-to-peer utilities for Nostr apps.

libp2r2p focuses on flows where one peer talks to another peer with Nostr
relays in the middle. It is not pure peer-to-peer networking; relays provide
the transport and discovery surface. The package was born to distribute the
private messenger reference implementation, and it also carries a few Nostr
power-ups used by that messenger.

For remote-work scheduling, see [`libp2r2p/network`](network/README.md):
`isOnline` probes connectivity and `onOnline` shares recovery monitoring,
including retries when the browser omits its native `online` event.

## Relay reads and lifecycle

All three `RelayPool` read generators emit typed envelopes. The Nostr event is
never decorated with `meta`; relay provenance belongs to its enclosing item:

```js
{ type: 'event', event, relay }
{ type: 'error', relay, error }
{ type: 'eose', relays: [{ relay, status, error }] } // error is optional
```

`eose` means the **initial read attempt is complete**, including empty reads and
partial results. Its report distinguishes actual relay EOSE from other outcomes:

| Status | Meaning |
| --- | --- |
| `eose` | The relay sent EOSE. |
| `satisfied` | The requested limit or IDs were satisfied, so the read closed early. |
| `timeout` | The initial relay attempt deadline elapsed. Also emits an error item. |
| `cutoff` | The grace period after the first qualifying EOSE/early completion elapsed. |
| `closed` | The subscription ended without EOSE or an explicit error. |
| `error` | Connection or subscription failed. Also emits an error item. |

Each normalized relay URL gets one report entry, preserving the first spelling
provided by the caller. An empty relay list emits `{ type: 'eose', relays: [] }`.
Caller cancellation never fabricates an initial completion. Individual relay
failures do not prevent other relays from delivering; malformed arguments and
general operation failures reject the call or iterator read.

`getEvents(filter, relays, options)` resolves to
`{ result: [{ event, relay }], errors: [{ relay, reason }], success, relays }`.
Its optional `callback` receives the same event/error/EOSE envelopes immediately.
`success` retains its existing meaning: an event was received or at least one
relay completed without an error; it does not mean every relay sent EOSE.
`getEventsGenerator` emits those envelopes and retains that report as its final
iterator return value (not visible inside a `for await` loop).

`getEventsFeedGenerator(filter, relays, options)` combines history and live
subscription by default: history items, one historical `eose`, buffered live
items, then ongoing live delivery. `live: false` ends after history and its marker;
`filter.limit: 0` skips history and forwards the live stream's initial marker.
Internal live/reconnect EOSEs do not produce additional feed markers.

`getLiveEventsGenerator` discards retained events received before each relay's
EOSE. A ready relay can deliver live events before the aggregate marker. Initial
`timeout` defaults to 5000 ms (`null` disables it); expiration reports pending
relays without stopping their connections or subsequent recovery. The grace
period `timeoutAfterFirstEose` defaults to 500 ms (`null` disables it). Historical
queries use these same defaults; their grace period starts only when a relay
with events EOSEs or satisfies its filter. Reconnect-gap reads keep their separate
`timeoutForReconnectGap` and `timeoutAfterFirstReconnectGapEose` options.

The live iterator retains `ready: Promise<{ relays, errors }>` and the
`readyRelays` getter. `ready` is the initial readiness snapshot, derived from the
same outcomes as the marker; its errors use `{ relay, reason }`. `readyRelays`
tracks currently ready relays and can change after that snapshot. Timeouts and
cutoffs are not acknowledgements of readiness. Reconnections do not repeat the
initial marker. Both APIs require consuming the iterator to start its work.

Read iterators have a synchronous, idempotent `stopAndDrain()` method.
It closes subscription input and cancels reconnections and outstanding historical
queries, while retaining items already accepted by receive callbacks. Continue
consuming to obtain those items and completion. This includes initial history,
buffered live events, and reconnect recovery buffers. Calling it before the
first `next()` prevents subscriptions from opening. It does not synthesize EOSE.

Aborting `options.signal`, calling `return()` (including a `for await` break), or
calling `throw()` cancels input and pending delivery instead. An item already
delivered cannot be recalled. These operations can also interrupt a drain.
`stopAndDrain()` does not consume the iterator or return a completion promise:
completion is the iterator's `{ done: true }` result.

```js
const stream = relayPool.getEventsFeedGenerator({ authors: [pubkey] }, [relay], { signal })
// When this relay is removed, call stream.stopAndDrain() from the list handler.
for await (const item of stream) {
  if (item.type === 'event') await store(item.event)
  else if (item.type === 'error') reportError(item.relay, item.error)
  else if (item.type === 'eose') initialReadFinished(item.relays)
}
```

Migration from 0.10.18: replace raw live/feed event reads with `item.event` after
checking `item.type`, replace `event.meta.relay` with the envelope's `relay`, and
unwrap each `getEvents().result` entry. There is no compatibility flag. Query,
content-key, private-channel and NIP-46 helpers unwrap pool results internally
and retain their higher-level event contracts. Count, publication and disconnect
return formats are unchanged.

### Read admission and bounded snapshots (0.10.21)

Each pool coordinates event reads by normalized connection URL. Constructor
options are `maxSubscriptionsPerRelay: 28`,
`maxConcurrentHistoryPerRelay: 2`, and `maxQueuedReadsPerRelay: 256`.
These are local budgets, not negotiated relay limits. A stricter relay can still
reject subscriptions. Count and publication contracts are unchanged and do not
consume event-read slots.

Admission is FIFO per connection. A history+live feed reserves two slots
atomically before either REQ opens; completing history releases only its slot.
One-shot queries and reconnect gap reads share this budget. Reconnection reserves
live+history together when recovery is needed. Caller cancellation removes queued
work; `disconnect(url)` also rejects queued work for that connection. Existing
live streams retain their reconnect behavior until cancelled by their owner.

All event reads accept `queueTimeout: 30000` (milliseconds, `null` disables).
The network `timeout` starts **after admission for each relay**, including its
connection setup; time in the queue does not consume it. The first-EOSE grace
can still cut short other relay attempts. Admission failures are ordinary error
envelopes/report entries, with `error.code`, `error.relay` and
`error.phase === 'admission'`: `RELAY_READ_QUEUE_FULL`,
`RELAY_READ_QUEUE_TIMEOUT`, `RELAY_READ_CAPACITY`, or `RELAY_DISCONNECTED`.
Cancellation rejects an outstanding direct `getEvents()` call; cancelled iterators
stop delivering. Failures on one relay continue to allow results from others.

Live and feed readers limit each pending live buffer to 1,000 envelopes and
8 MiB of serialized UTF-8 data. Options `maxBufferedLiveEvents` and
`maxBufferedLiveBytes` customize those positive limits. They cover consumer
backlog and reconnect buffers as well as live events waiting for history. These
are bounds on queued data, not a measurement of total JavaScript heap usage.
Overflow fails the attempt with `RELAY_LIVE_BUFFER_FULL` (`phase: 'live-buffer'`),
closes input and discards pending live data. Callers decide whether to retry;
events already delivered are not recalled. Ordinary relay failures retain the
existing partial-result behavior; they do not automatically discard the feed.

With `snapshot: true`, a feed waits for its initial live readiness window before
capturing the historical `until`. `filter.since` defaults to zero; `filter.until`
can provide an earlier cutoff but cannot extend a snapshot into the future.
The historical completion includes the actual inclusive bounds:

```js
const stream = relayPool.getEventsFeedGenerator(
  { authors: [pubkey], kinds: [1], since: lastConfirmed - 600, limit: 200 },
  [relay],
  { snapshot: true, timeoutAfterFirstEose: null, signal }
)
// Historical completion:
// { type: 'eose', relays: [...], snapshot: { since, until } }
```

Only history is bounded: the live REQ has no `until` in this mode. Events around
the boundary can appear in both sources and are deduplicated by ID. Empty history
still reports its bounds. With `live: false`, the cutoff is captured when the
iterator starts; `limit: 0` remains live-only, without a historical snapshot.
Default `snapshot: false` retains the existing filter time-range behavior.

This is a bounded time interval, not a transactional relay snapshot or proof of
complete coverage. Check each relay's outcome, exhaust pagination and commit
received events before checkpointing. `satisfied` can mean a truncated page;
EOSE can also terminate a limited response. A consumer that rejects a historical
attempt can call `return()`/abort when it receives its report, before the buffered
live events are released. Use `stopAndDrain()` only when those pending events
should be preserved. Slow/failed live readiness is still reported by the live
stream according to the existing partial-failure rules.

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
  for await (const { message, ack } of messenger.messages()) {
    console.log(message.type, message.payload)
    await ack()
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
      tags: [['k', '3560'], ...outerEvents.slice(offset, offset + 100).map(({ event }) => ['e', event.id])],
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

Messages are stored in a bounded, durable IndexedDB queue until acknowledged or
until the principal identity has been inactive for 60 days:

```js
async function handleMessages () {
  for await (const { message, ack } of messenger.messages()) {
    await persistMessageIdempotently(message)
    await ack() // only after the destination commits
  }
}

handleMessages().catch(err => console.warn('private messenger messages failed', err))
```

For one-at-a-time consumption, `await messenger.nextMessage()` returns
`{ message, ack, nack }` or `null` if no record is currently available. `ack()`
removes the reserved record; `nack()` releases it for another attempt. Both
return a boolean and are idempotent for the same reservation. An expired or
superseded reservation returns `false` and cannot delete a later delivery.

Delivery leases last 30 seconds and renew every 10 seconds while held. Closing
the messenger or returning the iterator releases its unacknowledged deliveries;
returning an iterator also cancels an empty pending read. A crashed/suspended
consumer's expired leases are reclaimable by another instance. Apps must save
idempotently: a crash between their commit and `ack()` can repeat delivery.
The queue is shared by identity, so concurrent consumers claim distinct records.

The app-message queue defaults to 16 MiB and rejects writes at capacity, without
evicting pending messages. Capacity pressure pauses ingestion and records a
recovery gap; successful acknowledgments resume it when possible. While capacity-paused, a
one-second capacity check also detects space released by another instance. Oversized
records and storage failures report errors and pause with reason `storage`;
after correcting the cause, call `resume('storage')`. Retention cleanup and
explicit `clearQueue()`/`clearChannel()` remain destructive. The queue's budget
bounds serialized storage, not the size of every intermediate decoded message.

### Forwarded authors and isolated consumers

`broadcastRumor({ rumor, ... })` accepts an optional `rumor.pubkey`. Omission
means the sender; a different pubkey forwards that claimed author and requires
the original `created_at`. A supplied ID must match the reconstructed event.
The wire retains the original fields; recipients never overwrite an explicit
author with the transport sender. Signed events retain their signature.

Received messages expose `senderPubkey` and `provenance` alongside `event`,
`outer`, `meta` and `payload`. Provenance is `direct`, `hearsay`, or `signed`,
calculated by the receiver. Pending deduplication includes provenance, so an original can upgrade a queued
hearsay copy of the same event. Private-channel callbacks also expose these fields
in their metadata. Nothing is attached to the Nostr event. A rumor naming the
recipient is still hearsay unless independently matched to a known original.
Forwarded controls cannot invoke ask/reply/presence/recovery handlers in the
name of another author. Direct rumors are not transferable signed proof.

Each PrivateMessenger owns a `createPrivateMessageSession()` from
`libp2r2p/private-message`. A session groups relay reads for one receiver and
owns its watches, callbacks, and scoped receive-fragment progress. Independent
sessions can watch the same channel, including with the same receiver, without
overwriting or closing each other's reads. The module-level watch helpers are
a convenience scope per receiver; use explicit sessions for separate consumers.
`receivedChunkScope` can also scope lower-level private-channel reads; receiver
pubkeys are included in fragment keys. Expiration/cleanup still use the shared
receive-chunk database and budget.

### Pause, resume and recovery

`pause(reason)` and `resume(reason)` accept nonempty strings. Reasons compose:
removing `network` cannot override an application's `vault` pause. Pause stops
live watches, recovery fetches and presence publishing, but retains channel
configuration and pending deliveries. Already-started operations can finish;
failed ingestion remains recoverable. Send attempts while paused reject.

`watch(channels)` declares desired subscriptions. `unwatch(channels)` removes
that intent while retaining configuration and history. Browser `online` only
clears the internal `network` pause, never restarts explicitly unwatched
channels. Updating retained channel configuration preserves explicit unwatch;
new channels are watched unless paused. Applications may explicitly watch again.

Pauses, unwatch and close record the start of the gap, even for channels that
have never received a message. Rewatch/resume uses live delivery plus historical
recovery with overlap. A failed resume remains retryable under its pause reason.
Before opening a watch, the messenger persists a pending recovery interval.
A first watch scans the configured recovery window (seven days by default),
including messages sent before this identity first opened the channel. Successful
scans persist `recoveredThrough`, even when empty; later watches resume from the
latest persisted message or scan checkpoint with overlap. Pending older ranges
remain until successfully recovered, even if newer live messages arrive.
This also covers abrupt termination without `close()` and channels with no
messages. Applications still supply signers and desired channels on reopening;
storage is scoped by origin and user pubkey, not the transient session ID.
Failed ingestion or incomplete relay fetch reports never complete the gap.
`lastSeenAt` tracks messages persisted in the incoming queue, not app commits.
The default recovery window is seven days; recovery still depends on available
relay/seeder data, and retention limits cannot guarantee indefinite delivery.

Queue clearing remains asynchronous: `await messenger.clearChannel(channelPubkey)`.

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
import {
  assertValidPublicBlossomServerUrl,
  assertValidPublicRelayUrl,
  normalizeBlossomServerUrl,
  normalizeRelayUrl
} from 'libp2r2p/url'
```

Blossom server normalization accepts root `http:` origins for local
development. Public Blossom validation additionally requires `https:` and a
public host, and rejects credentials, paths, query strings and fragments.

`classifyEvent()` from `libp2r2p/event` combines the exact NIP-01 kind
ranges with tag-defined behavior. The first `d` tag may add `replaceable` or
`addressable`, while an `expiration` tag equal to `created_at` adds
`ephemeral`. An event is also regular when it is neither replaceable nor
addressable. Classifications are additive and callers can disable the legacy
kind ranges with `{ includeLegacyKindRanges: false }`.

NIP-44 v2 uses the interoperable `nip44-v2` salt by default. A custom UTF-8
salt of at most 32 bytes may be passed to `getConversationKey()`, but messages
derived with it are not interoperable with standard NIP-44 implementations.

NIP-44 v3 separates binary (`encryptBytes`/`decryptBytes`), UTF-8 text
(`encrypt`/`decrypt`), and standard-Base64 plaintext
(`encryptBase64`/`decryptBase64`) interfaces.
The suffix describes plaintext; ciphertext is always
standard Base64. NIP-46 uses the Base64 helpers, while browser NIP-07 APIs use
`ArrayBuffer` plaintext; see [the interface guide](nip44-v3/README.md) before
adapting a signer.

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

NIP-27 text references live in `libp2r2p/nip27`. `extractMedia()` splits
content into text, URL, profile, event, relay, NIP-05, app and hashtag items in
occurrence order, accepting the optional `@` and `nostr:` mention prefixes
plus NIP-05 in its standard, root and custom compact spellings.
`decodeReference()` parses a single reference, and `decodeMediaMetadata()`
reads the file/media metadata carried in a URL fragment
(`#m=image/png&dim=640x480&...`).
URLs can contain literal `+` in their paths, including launcher links such as
`https://44billion.net/+apps?by=fiatjaf.com`. They produce a single URL item
with the full address, rather than an app item for the embedded reference.

App references are a library extension to NIP-27. `extractMedia()` recognizes
encoded `+…` app entities and named references such as `+hallway@fiatjaf.com`,
`++myapp@bob@example.com` and `+++myapp@npub1…`, with optional `nostr:` before
the entire reference. Named authors accept the NIP-05 spellings above, `npub`,
`nprofile` and hex pubkeys; app names may be URL-encoded. The one-to-three `+`
prefix selects `main`, `next` or `draft` respectively.

Bare app names use the `defaultAppAuthor` option, which defaults to
`'44billion.net'`: `+apps` identifies the same app as `+apps@44billion.net`.
The option accepts a NIP-05 reference (including compact spellings), `npub`,
`nprofile` or hex pubkey and is validated locally on every call. An invalid
value throws `ValidationError` with code `INVALID_DEFAULT_APP_AUTHOR`.
Explicit authors and encoded app entities are never overridden. Malformed
explicit authors remain text instead of falling back to the default.

```js
extractMedia('+apps', { defaultAppAuthor: 'bob@example.com' })
// app.user: { type: 'nip05', local: 'bob', domain: 'example.com', raw: 'bob.example.com' }
// app.original remains '+apps'.
```

These references produce `{ key: 'app', app: { original, ...decoded } }`, where
`decoded` is the result of `decodeAppUrl()` from `libp2r2p/url`, with a missing
named author filled from `defaultAppAuthor` as described above. Named apps
include `type: 'named'`, `prefix`, `channel`, `appName` (the manifest's `d` tag)
and the decoded `user`; entities include `type: 'entity'` and `entity`, usable
with `appDecode()` from `libp2r2p/nip19`. Extraction performs no network lookup
and does not verify that the app exists. For example:

```js
extractMedia('Open nostr:++myapp@bob@example.com')
// [
//   { key: 'text', text: { value: 'Open ' } },
//   { key: 'app', app: {
//     original: 'nostr:++myapp@bob@example.com', type: 'named',
//     prefix: '++', channel: 'next', appName: 'myapp',
//     user: { type: 'nip05', local: 'bob', domain: 'example.com', raw: 'bob.example.com' }
//   } }
// ]
```

App references must be standalone inline tokens, not concatenated to event
pointers or embedded in URL paths. Invalid app candidates remain text.
Bare names can use letters, numbers, dots, underscores, hyphens and tildes;
URL-encode spaces, `@` and other punctuation in them.
`+naddr1…` is recognized as an app only for site-manifest kinds; unprefixed
`naddr1…` and `nostr:naddr1…` keep their existing `event` item shape.

`compactWhitespace(text, options?)` is also exported from `libp2r2p/nip27` as an
opt-in display helper. It removes carriage returns, collapses spaces and
tabs, removes spaces around line breaks, limits consecutive line breaks to
two, keeps the first eight line breaks (replacing subsequent runs with a
space), and trims the result. Empty strings are accepted; non-string inputs
throw `ValidationError` with code `INVALID_WHITESPACE_TEXT`. It is not
applied automatically by `extractMedia()`.

The option `maxLineBreaks` (default `8`) sets the total limit and accepts a
non-negative safe integer. Setting it to `0` replaces all line-break runs
with spaces. `consecutiveLineBreakThreshold` (default `3`) sets the minimum
run length that collapses to **two** line breaks; shorter runs are preserved.
It accepts safe integers of at least `3`. Either option accepts `Infinity`
to disable its rule. Runs are collapsed before applying the total limit,
and trimming happens last. Invalid options throw `ValidationError` with
code `INVALID_WHITESPACE_OPTIONS`.

```js
import { compactWhitespace } from 'libp2r2p/nip27'

compactWhitespace(text, { maxLineBreaks: 12, consecutiveLineBreakThreshold: 5 })
```

User references (`npub`, `nprofile`, hex pubkeys and every NIP-05 spelling)
are handled by `libp2r2p/nip27`: `decodeUserReference()` returns the decoded
form with its canonical compact spelling, `encodeUserReference()` returns
that canonical spelling, and `resolveUserReference()` resolves it to a
pubkey. `libp2r2p/nip05` keeps only `queryProfile()`, the NIP-05 lookup, which
accepts the compact custom forms directly. The decoders (`decodeReference`,
`decodeMediaMetadata`, `decodeUserReference`, `decodeAppUrl`) throw
`ValidationError` with a stable code; each has a `tryDecode…` counterpart
that returns `null` when the value cannot be decoded.

Public validity checks consistently use a non-throwing `is…` predicate plus an
`assert…` counterpart when callers need the exact reason. Strict codecs,
decoders, token validation, and malformed public arguments also throw
`ValidationError` from `libp2r2p/error`; probing code can use the
non-throwing `tryDecode…` variants instead of catching. Network, timeout,
abort, quota, and closed-state failures remain ordinary operational errors.

NIP-04 remains available at
`libp2r2p/nip04` only for compatibility with older Nostr applications.
Low-level relay sockets, subscriptions, message parsing, and serialization are
internal implementation details; use `RelayPool` or the `relayPool` singleton
from `libp2r2p/relay`.

`getEvents` and `getEventsGenerator` accept `deduplicateAcrossRelays` (boolean,
default `true`). With `false`, a matching event is delivered once per relay,
while repeated IDs from the same relay remain suppressed. Each occurrence owns
its envelope's `relay`; callbacks still run immediately and per-relay filter
limits are unchanged. The callback/generator event item is
`{ type: 'event', event, relay }`; the completed query is
`{ result: [{ event, relay }], errors, success, relays }`. The option does not
extend to the live or feed generators. Callers that need replication coverage can aggregate the returned
copies by event ID; missing responses do not prove absence from a relay.

Publication errors retain their existing `reason` objects and may expose
`category`: `connection` (WebSocket establishment), `transport` (socket send or
close), `relay` (an explicit negative `OK`), or `timeout` (missing confirmation).
Native messages, codes, nested causes and aggregate errors remain available;
WebSocket closure details use `closeCode`, `closeReason`, and `wasClean` rather
than overwriting a native `code`. A timeout can retain a preceding socket error
as its cause without claiming that the relay rejected the event. Authentication
wrappers preserve this context. Local failures can remain uncategorized.
Event metadata is internal and is removed by `sendEvent` before serialization.

The same public subpath exports `getRelaysByPubkey(pubkeys)`, which discovers
the latest NIP-65 relay list for every requested pubkey through `seedRelays`,
normalizes and deduplicates its public relay URLs, and falls back to the first
two `freeRelays` when no list is available. Its result can be passed directly
to `pickRelaysForPubkeys(pubkeys, relaysByPubkey)` to batch authors that share
read or write relays.

`getRelaysByPubkey` accepts a few options:

- `includeEvents` returns each pubkey's latest kind `10002` event alongside its
  parsed relays (`{ read, write, event }`), for consumers that need the original
  event (storage, re-signing, freshness tracking).
- `forceRefresh` re-queries `seedRelays` even when the pubkey is cached, without
  regressing a newer cached event when the relay returns an older one.
- `timeout` / `timeoutAfterFirstEose` tune the relay-list query timing. The
  default opens a short grace window after the first EOSE with events, matching
  `RelayPool.getEvents`.
- `emptyRelaysFallback` sets the relays returned when a pubkey has no relay
  list (default: the first two `freeRelays`); pass `[]` to return empty
  `read`/`write` arrays instead, keeping the free-relay decision with the
  caller.
- `relayUrlPolicy` opts into non-default URL validation: `onion` allows
  `ws://`/`wss://` `.onion` hosts, `localRelay` allows the standardized
  `ws://localhost:4869` local relay, and `nostrEntityUrls` stops rejecting
  public URLs that contain `npub1`/`nprofile1`. All other non-public or
  insecure URLs stay rejected.

`parseRelayListEvent(event, relayUrlPolicy)` is exported from the same subpath
for consumers that parse relay-list events they receive outside
`getRelaysByPubkey`, so routing and persistence always agree on the parsed
shape.

`pickRelaysForPubkeys` also accepts `excludeRelaysByPubkey` (relays already
queried per pubkey, as an object or `Map`) and `emptyRelaysFallback` (the
relays used when a pubkey has no typed relays; pass `[]` to leave that pubkey
unrouted). Together with `maxPerPubkey: Infinity` these express a second,
exhaustive pass over the relays that were not yet tried for each author.

For callers that want the whole two-pass flow, `getLatestEventsByPubkey`
fetches the latest replaceable events (or addressable events when
`dTagsByPubkey` maps pubkeys to their `d` tags) through NIP-65 write relays:
the first pass routes through up to `maxPerPubkey` shared relays, and missing
authors are retried on every remaining relay plus `fallbackRelays` (default:
the first three `freeRelays`), excluding what was already queried. It accepts
`relaysByPubkey` to reuse a previous discovery (only missing pubkeys are then
discovered) and returns `{ events, byPubkey, relaysByPubkey }` so the merged
relay map can be passed back on later calls.

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

## Files

[`libp2r2p/irfs`](irfs/README.md) prepares retryable chunk templates from files,
with explicit cancellation and resource release. [`libp2r2p/nip94`](nip94/README.md)
builds/interprets file metadata, including the local IRFS profile. NIP-27 extracts
`https://nostr.alt/nfile1…?localOnly=1` up to the NIP-19 codec's 5,000-character
limit, retaining the full URL and exposing decoded `url.nfile` and MIME `url.m`.

The NIP-94 extension also carries optional `download` intent; see
[nip94/README.md](nip94/README.md#download-intent) for event and inline URL forms.

## Reliable IndexedDB queue reservations

`createQueue({ prefix, evictionPolicy: 'reject', maxBytes })` opts out of
capacity eviction. Oversized items reject with `QUEUE_ITEM_TOO_LARGE`, full
queues with `QUEUE_CAPACITY_EXCEEDED`; browser quota errors propagate without
trimming existing records. Other eviction policies retain their cache behavior.
Opening a reject-policy queue under a smaller budget preserves existing items.

`queue.reserve({ leaseMs = 30000 })` returns `null` or
`{ item, ack, nack, renew }`. Reservation token/deadline are internal record
metadata, persisted atomically in the same IndexedDB transaction as selection.
`renew()` extends a still-valid lease. Low-level callers own renewal and release;
PrivateMessenger supplies both. Do not mix reservations with destructive or
position-shifting queue operations on the same queue except explicit removal.
A stale token cannot acknowledge a newly reserved or replaced record.

`queue.getCapacity()` returns `{ usedBytes, maxBytes }`; capacity rejections
also expose `requiredBytes` and `maxBytes`, without retaining the rejected item.
