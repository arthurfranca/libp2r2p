# Private message sessions

`createPrivateMessageSession(options)` coordinates one identity's DM inbox,
outbox and IRFS attachments without presentation state. It builds on
PrivateMessenger and the file coordinator, not another recovery protocol.

`retryPendingDecryptions({ force = false } = {})` forwards a background pass to
the account messenger while available. Call after committed content-key changes;
ordinary availability recovery already requests a pass. Ciphertext stays in the
separate bounded FIFO until admitted to the normal inbox, evicted or explicitly
cleared. This does not retry application storage/permission failures as crypto.

Construction accepts `pendingDecryptionReady` (default `true`). Pass `false` to
hold pending queue reads/replay until `setPendingDecryptionReady(true)` releases
them after initial presentation. This never pauses live reception, sends or
channel preparation. Recovery waits for 250 ms of channel inactivity; a five-second
maximum admits one packet during sustained activity. It yields between records,
with at most four consecutive preferred records before another eligible channel.
`setPendingDecryptionPriority(peer | null)` retains a peer preference through
preparation; self-chat/`null` or removal clear it. Deadlines prevent preference
from starving other channels. Account unavailability/close still cancel recovery;
`force` bypasses only the MAC attempt limit. Awaiting the retry API awaits its
scheduled pass, so do not await it on the interface's critical startup path.


Provide `owner`, a byte-based NIP-44 v3 `signer` with `withSharedKey`, and either
`eventStore` or a `messageStorage.save(event, { peerPubkey, hearsay })` adapter.
Optional `chunkStorage` and `recoveryStorage` (`seeds`, `authorizations`) select
persistence; absent file storage uses the library cache. `mode` defaults to
`seeder`, `seedersForPeer(peer)` to `[peer]`, and `allowedKinds` to
`[5, 9, 1063, 34601]`. `useContentKeys` defaults to `true`: the coordinator looks
up recipients' published content-key proofs (kind 18716) while sending and falls
back to sender-content whenever a proof is missing or the lookup fails. Misses
are cached for 5 minutes (found proofs for 40), so a later prefetch picks up a
key published after an earlier miss. Set it to `false` when the signer owns that discovery. The DM context is `dm`; file contexts are `dm:media:<root>`.
`createMessengerSigner` adapts this byte signer to the transport's Base64 API.
Each adapter belongs to an immutable account/channel identity: it shares the
first successful public-key lookup and retries failed lookups only on a new call.
Create a new adapter when changing identity. Signing/encryption still uses the
real signer and its current permissions on every operation.

Methods: `setPeers`, `setAvailable(boolean)`, `preparePeer(peer)`, `prefetchContentKeys(peers?)`,
`prioritizeRange(peer, options)`, `enqueue`, `retry`, `cancel`, `download`,
`cancelDownload`, and `close`.

The session creates one messenger for the account, independently of contact
revisions. `setPeers` and `setAvailable` reconcile local configuration without
awaiting all contacts or remote recovery. Channel preparation is incremental and
deduplicated: at most four preparations run, with at most three in the background.
`preparePeer(peer)` promotes an existing contact and resolves when its channel is
locally writable. Call it when opening a conversation; enqueue, retry and remote
download also request preparation automatically. An unprepared conversation does
not block local saves or sends to a ready contact. Resolved identities are reused.
The session also defers the next optional presence signer call while user outbox
writes/publications are active. Existing signer calls, subscriptions and historical
coverage remain intact; releasing foreground work resumes the waiting presence.
Successful sends remove the final outbox record directly instead of encrypting a
redundant final checkpoint; intermediate context/file checkpoints remain durable.

Known transient preparation failures retry with bounded offline-aware backoff;
unknown, permission and validation failures remain isolated to that contact until
explicit preparation/retry or a real availability transition. Removing a contact,
locking or closing discards obsolete signer results and cancels its queued work.
Contact changes never invent a `signer` pause; only `setAvailable` controls that
reason. Storage, capacity, network and explicit pauses keep their own owners.
`prefetchContentKeys` warms the content-key lookup cache for the owner and the
given peers (default: current peers); it is best-effort and never rejects.
`prioritizeRange` registers an in-memory priority window for a conversation:
`type: 'unread-page'` requires `since` (the read anchor), while `type: 'tail'`
defaults to the newest persisted range minus `priorityTailSeconds` (6 hours).
The coordinator resolves the peer's channel and queues the request until the
channel exists. Priorities expire after `priorityTtlMs` (10 minutes) or once
covered, and are dropped with recovery state. They are process-local and reset
on reload.
Callbacks are optional `onDebug`, `onOutbox`, `onMedia`,
`onError`, and optional `onSendError(error, { id, peer })`. The caller maps its
account permissions/connectivity/lock state to availability and owns view subscriptions. There are no DOM or UI imports.

`onSendError` identifies the main outbox message even when its quote, metadata,
file chunks, or self-chat persistence failed. Remote local-persistence failures
use onError and the copied localPersistence summary, never a send failure. The original error is also reported to
`onError` unchanged. It fires once per failed send attempt (including retries),
not when persisted failures are read, nor for inbox/download/deletion operations
or cancelled/closed sends. Apps own route visibility and user-intent filtering;
automatic retries may also fail. Callbacks must not throw. Errors are not copied
into persisted outbox records.

Sends that only failed because the messenger was paused stay pending without
`onSendError`. `PrivateMessenger.readStatus()` and optional `onStateChanged`
provide isolated `{ closed, paused, pauseReasons }` snapshots, initially and on
actual transitions. Paused errors retain their `pauseReasons` and native cause.
The session parks remote sends and wakes them immediately on release; local
personal copies and self-chat remain writable.

A custom `Messenger(options)` factory must provide synchronous `readStatus()`
and honor `options.onStateChanged`: deliver an initial snapshot before the factory
resolves and notify each effective state change. Snapshots contain boolean
`closed`/`paused` and a `pauseReasons` string array; `paused` reflects whether that
array is nonempty. The session copies snapshots and rejects missing observation
with `MESSENGER_STATE_CONTRACT_REQUIRED`, or malformed state with
`INVALID_MESSENGER_STATUS` (`ValidationError`). Rejected instances are closed and
obsolete callbacks are ignored. Wrappers can forward the complete options to
`createPrivateMessenger(options)` unchanged.

Observed pauses never fall back to polling sends. Other transient availability
failures without an active pause retain bounded 1..30s send retries; a pause
superseding such a retry parks it until the state notification releases it.

Network pauses observed internally use shared `isOnline`/`onOnline` monitoring,
including recovery when no second native online event arrives. Offline waiting
spends no backoff. Retryable watch failures recover by channel without globally
pausing publication. Background historical recovery does not gate text sends.
Internal storage pauses repair pending state writes before release; explicit
external pauses remain owned by their caller. Operational recovery retries use
1, 2, 4... seconds, capped at 30 seconds with 20% jitter; permanent relay and
local validation/auth/permission failures await explicit recovery.

A failed session inbox save uses the separate `session-storage` reason. The
session retries the actual reserved record and releases only after save and ACK.
No message eviction or false historical coverage is introduced. Account
unavailability stops its recovery timers; availability or explicit retry can
reevaluate a local failure without clearing another owner's storage pause.

`enqueue({ peer, event, context, requiredFiles, deletion })` preserves prepared
identities. Local message commits and remote stages have independent checkpoints.
It sends original/thumbnail chunks before announcements, yields for small messages,
and never interprets relay rejection text as signer permission denial. Inbox ACK
follows durable storage; failed storage nacks and pauses ingestion. Hearsay cannot
execute an authoritative deletion. Completion of file recovery means verified,
persisted indices, not a relay ACK.

`openOutbox` and `openDownloads` inject work storage. Their default is exported as
`createPrivateSessionStorage({ owner, signer, indexedDB, namespace, prefix })`:
IDB queue with unique IDs, encrypted payloads, reject-on-capacity and atomic
existing-only checkpoints. Default prefix is `libp2r2p:private-messenger:session`;
namespace is `outbox` or `downloads`. These contain pending user work, not a cache;
no automatic age eviction or synchronization is performed. `close()` releases
connections without deleting pending work. Apps can preserve existing storage by
passing their established factories, as Zillion does.

External recovery factories are owned by their caller and must be closed after
the session. Work storage opened by the session is closed by the session.
This is a DM profile; group membership/key derivation is not implemented.

## Relay fallback

Both `createPrivateMessenger` and `createPrivateMessageSession` accept an optional
`fallbackRelays` array (default `[]`) at construction. URLs are normalized,
deduplicated and copied; invalid values throw `ValidationError`. For example:

```js
const session = createPrivateMessageSession({
  owner, signer, eventStore,
  fallbackRelays: ['wss://relay.44billion.net']
})
```

By default, automatic sends try each recipient's NIP-65 read relays first, at most two per
recipient per attempt, then the configured fallbacks in pairs. Explicit
per-call `relays`, channel `sendRelays`/`relays` and global `relays` remain primary
and also support configured fallbacks, including fixed multi-recipient sends.
Explicit lists retain their requested initial fanout; no recipient relay lookup
is added. A relay present in both lists is attempted only once per outer event.
Automatic multi-recipient sends and explicit `relayToReceivers` maps also support
fallbacks. Explicit maps retain their precedence over fixed lists, initial fanout
and encrypted recipient subsets, without consulting recipient NIP-65 lists.
The session coordinator remains a DM API; multi-recipient channels use
`createPrivateMessenger` directly.

Optional `fallbackDelayMs` defaults to `null` (sequential primary exhaustion).
A nonnegative integer up to 2147483647 opts automatic NIP-65 routing into early
fallback; explicit relay lists/maps retain the sequential policy. With `3000`,
start at most two eligible fallback relays after three seconds without complete
acceptance, or immediately after primary exhaustion. Optional `enqueue({ ..., attemptStartedAt })`
uses `performance.now()` from the caller's realm to include preparation in the
budget. Without it, the deadline starts at publication. Later batches do not
renew it; retries/reloads start a new attempt and uploads budget each chunk.
Primary operations retain
their deadlines and can still win. Failed fallback operations leave primary
alternatives available; no relay is tried twice. Multiple recipients require
accepted coverage of every encrypted recipient subset. No wrapper, ID, recipient
tag or deletion capability is recreated. Confirmed connectivity, exclusions and
connection cooldowns remain authoritative; a waiting primary is not penalized
merely because fallback started. Late native failures may inform later sends.

On `blocked`, `restricted`, `auth-required`, `pow`, `rate-limited`, or `error`
rejections, or connection/transport/timeout failures, replacement requires
`isOnline` to confirm connectivity. Invalid messages, local signer/authentication
failures and unknown error text retain their original diagnosis.

Receive routing always adds the configured fallback relays to the effective
primary list (explicit `relays` or the owner's NIP-65 read relays). Listen there
from the start, even when primary relays are healthy: a sender may have used a
fallback after publication was refused elsewhere. Live watches, historical
recovery and file-channel reads share this policy. NIP-65 refreshes retain the
fallbacks; outgoing exclusions never remove them from receive subscriptions.
No public recipient tags or changes to a user's NIP-65 event are introduced.

Each signed outer event is reused verbatim, including router/carrier fragments
and deletion capabilities. Native errors from exhausted attempts remain in its
publication report. A shared relay's first ACK covers all members assigned to
that publication batch. If replacements use different relays for different
members, every pending member must be covered before the outer event succeeds;
a partial ACK cannot hide another member's failure. Remaining primary routes are
exhausted before configured fallbacks with the default null delay or explicit
routes. Redundant acknowledgements may update
future routing preferences without delaying a successfully covered send. Preferences are channel/recipient-scoped, in-memory,
expire after five minutes, and never alter receive subscriptions. A fresh attempt
can recheck an exhausted list. Additional fallback relays come only from the
caller configuration; absent NIP-65 lists retain the existing discovery defaults.

When offline/interrupted with alternatives remaining, reports carry
`retryWhenAvailable: true`. The outbox remains pending and emits no `onSendError`.
Offline reports also carry `retryWhenOnline: true`: the session owns a temporary
`onOnline` subscription to retry those entries after confirmed connectivity,
including brief outages the app's existing monitor might have missed. Cancel,
close and signer unavailability release the subscription. Account recovery still
uses `setAvailable(true)`; network recovery never overrides signer availability.
`onSendError` is emitted only after final failure, including exhausted eligible
relays. Validation/authorization failures are terminal without relay rotation.
`broadcastRumor`/`broadcastEvent` accept a `signal` to stop further replacement
publications; the session supplies it and aborts it on cancel/close. An already
published event cannot be recalled by cancellation.

`RelayPool.sendEvent(event, relays, { signal })` supports optional cancellation.
It returns the existing result/report envelopes with the original abort reason
for unfinished relays, preserving partial acceptance and avoiding synthetic
timeouts. Cancellation releases publication waiters and deferred sends without
closing shared sockets or cancelling another subscriber for the same event ID.
Operations already transmitted cannot be recalled from relays. A messenger pause
cancels its outstanding publication work and leaves the outbox pending; resume
creates a fresh publication lifetime, without treating the pause as a user abort.

## Independent local persistence and publication

One encrypted outbox feeds two independent serial processors. Local persistence
never waits for peer preparation or an in-flight publication. After the initial encrypted intent commits, remote preparation and publication
never wait for event-store persistence. Only the exact prepared-envelope
checkpoint gates transmission. Text continues passing uploads between chunks;
its local stage need not have completed. Both stages use the admitted inner ID.

Self entries emit `status: 'saved'` as soon as local persistence succeeds. They
do not write a redundant localSaved checkpoint before removal. Remote entries
emit that status after the first valid relay ACK for every required publication,
without waiting for redundant relays. Outbox cleanup runs independently; failures
retain saved status and retry only removal with the session backoff, never
`onSendError`. Consumers must accept saved entries while removal is pending.
A crash before removal replays the original event identity into idempotent storage.
Account shutdown fences late admissions and waits for both processors and cleanup.

`onDebug(record)` is disabled by default. Records carry fixed stage names, public
event/peer identifiers and elapsed milliseconds for admission, local processing,
channel preparation, publication acceptance and cleanup. Callback failures never
change delivery. Connect only in development; never log events or crypto buffers.
Custom outbox factories also receive this optional callback for storage timings.

## Durable prepared publications

Each remote entry may contain optional `prepared` progress in its existing
encrypted JSON. It holds the signed envelope fragments, compact recovery seeds,
recipient/channel and inner-event identities, event/chunk position, retention
and accepted/archived flags. Old entries without it retain `localSaved`, index
and file/chunk progress and enter the same preparation path; there is no database
migration or alternate publisher. Replays validate signatures and identity and
reuse the exact envelopes without repeating cryptography or extending expiry.

The existing `onPreparedSeed` callback now also supplies `publication`. The
session commits that minimal journal before transmitting; direct messengers
retain their local seed-store durability. Only after that commit can relay
publication proceed. One independent serial worker converts recovery seeds into
their configured archive. Pending durable rows participate in recovery reads
immediately, including during a blocked archive write. No new seeds are enabled
for modes which did not previously retain them.

First acceptance of every required envelope emits saved independently of archive
or cleanup. ACK progress checkpoints serialize with other entry writes. A crash
between ACK and its checkpoint may replay the same envelope; acknowledged durable
progress only archives. Archive failure never becomes `onSendError` or changes
saved status. Removal waits for local persistence, archival and outstanding entry writes.

At most one unit awaits archival per entry; a stalled upload yields its remote
slot to other texts rather than accumulating the entire file. Cancel stops wire
work but retains potentially transmitted seeds until archival finishes. Close
drains active local operations and leaves durable backoff work for restart.
Diagnostics add publication/seed IDs, checkpoint time, archive wait/execution and
first-ACK timing, without message or ciphertext contents.

## Independent delivery status (0.11.27)

`deliveryConfirmed` is an optional boolean in encrypted outbox JSON. It records
relay confirmation separately from `localSaved`. Old entries normalize from
saved delivery status, never from local progress; their event/file indices and
prepared envelopes remain unchanged. No database migration is needed.

`onOutbox` adds `localPersistence: { state, retryable }`, where state is pending,
saved or failed. It is a presentation summary with no native error objects.
A saved remote entry may still have pending/failed local work. Keep it visible
through reload until the local store catches up. `retry(id)` on a confirmed
entry repeats local/archival cleanup only and never renews network urgency.
Recoverable local failures retain bounded backoff without clearing terminal
remote errors or releasing network pauses. Self-chat still confirms on commit.

Entry updates serialize through existing-only checkpoints; not-yet-started
writes coalesce and capture the latest monotonic progress at transaction start.
Prepared-publication callers wait for the resulting durable snapshot. An ACK
checkpoint may merge local progress but never requires it. Shutdown/lock waits
for admitted local writes; cancellation cannot recreate a removed intent.

The local failure summary remains failed while a retry is queued or running;
unrelated snapshots cannot hide its warning before commit. Retry admission is
runtime-only and temporarily makes that summary non-retryable. A failed retry
restores retryability after its operation ends; a successful commit clears the
failure. This does not change the already confirmed remote status.

Legacy single-stage failure flags with incomplete local progress normalize into
local failure state, preserving manual-only retry refusals. The new delivery
marker distinguishes those records from a modern remote failure racing local
persistence. Both formats enter the same processors after normalization.
