# AGENTS.md

## Project Shape

libp2r2p is an ESM package with no build step. Public package exports should
come from their own folders, each with an `index.js` entry point. Prefer
singular public subpaths, such as `libp2r2p/key` and `libp2r2p/relay`.

Public export folders may organize implementation details with `constants/`,
`helpers/`, and `services/` subfolders. Additional custom public subfolders
are fine when they are part of the API, such as `private-messenger/recovery`
or `content-key/event`, and those should also expose an `index.js`.

Root-level `constants/`, `helpers/`, and `services/` folders are reserved
for shared internal code that is not itself a public export. Do not expose
internal files directly through `package.json`; add or adjust an export-folder
`index.js` instead.

Keep the `exports` object in `package.json` sorted, with `.` first.
Do not keep backwards-compatible alias exports unless explicitly requested.

## Validation APIs

Boolean predicates must use an interrogative prefix such as `is`, `has`,
`does`, or `are`, and return `false` rather than throwing for a simply invalid
candidate. Public validity predicates should have an `assert…` counterpart
when callers benefit from a detailed reason; both must share one checker so
their accepted inputs cannot drift.

Strict public decoders, codecs, validators, and malformed public arguments
throw `ValidationError` from `libp2r2p/error` with a stable uppercase
snake-case code. Preserve ordinary operational errors for network, timeout,
abort, quota, unavailable storage, and closed-state failures. Permissive
parsers that use `null` for an expected mismatch keep that contract.

## Tests

Preserve relay API envelopes and immediate delivery when changing transport
behavior. Read generators emit `event`, `error`, and one initial aggregate
`eose`, plus automatic `live-progress` controls during ready live input. The initial
marker reports actual EOSE, satisfaction, timeout, cutoff, normal
closure, or error per relay. Never attach provenance to the Nostr event itself.
`getEvents().result` contains `{ event, relay }` entries. Internal consumers must
unwrap event envelopes and handle/ignore control items explicitly. Preserve
`ready`, `readyRelays`, cancellation and drain behavior used by NIP-46. `deduplicateAcrossRelays` belongs only to `getEvents` and
`getEventsGenerator`; disabling it still deduplicates IDs within each relay.
Replication tests must exercise the real pool with a controlled transport.
Operational error categories supplement native errors; do not replace their
messages, codes or causes, or reinterpret a timeout as an explicit rejection.

During implementation, run the smallest directly related test files with
`npm run test:files -- <relative-test-path> [...]`. Before completing a change,
run the full deterministic suite with `npm test`.

Tests that contact real external services use the `*.network-test.js` suffix,
which keeps them out of the standard `*.test.js` suite. Run them explicitly
with `npm run test:files -- <relative-network-test-path>` when changing network
behavior, changing relay constants, or investigating service availability.
External network tests are diagnostic and do not gate unrelated changes.

If a full-suite failure appears unrelated to the current change, investigate
enough to distinguish a regression from a pre-existing failure or external
instability. Do not change unrelated production behavior merely to make the
suite pass; report independently scoped problems separately.

## IRFS and file metadata

- `irfs` is storage/signer/network independent. Use 51,000-byte NMMR blocks,
  deterministic identifiers and Base93 proofs/content. Retain a seekable immutable
  input and hash tree; no temporary IndexedDB is needed. Every preparation owns
  explicit `close()` and abort cleanup; retry iterators retain stable timestamps
  supplied by callers. Empty files are currently rejected.
- `nip94` documents an extended kind-1063 profile: optional SHA-256, `r` root,
  `service`, and unchanged Base64 `thumbhash`. Never put MMR roots in `x` tags.
  Validate nfile/root/MIME agreement. Renderer consumers read tags directly.
- NIP-27 nfile URLs retain `localOnly=1` and metadata up to the codec's limit.

- The optional download extension decodes to string '0'/'1'. Kind-1063 absence
  means '0', a bare tag means '1'; inline URL fragments require explicit values.
  Reject invalid/duplicate flags. Builders never emit the tag by default.

## Private messaging lifecycle

- `idb-queue.putBy` performs an atomic upsert through a declared unique,
  non-multiEntry index. `existingOnly` checkpoints must never recreate removed
  items. Preserve queue position on replacement, transactional byte accounting,
  capacity policy and reservation-token invalidation. Indexed methods are
  extensions beyond the common Web Storage queue API; keep parity tests scoped
  to that common API.

- Rumors preserve an explicit author distinct from the transport sender. Keep
  `senderPubkey` and `provenance` outside the event through live and recovery
  paths; forwarded controls must not execute as direct sender commands.
- PrivateMessenger owns an isolated private-message session. Keep callback and
  fragment progress scoped by consumer and receiver, including shared DM keys.
- `messages()` / `nextMessage()` deliver `{ message, ack, nack }`. Persist before
  acknowledgment; cancellation releases reservations. Capacity pressure must
  not evict pending app messages or complete an unpersisted recovery interval.
- Desired watches and pause reasons are separate. Browser online clears only
  the network reason; it never undoes explicit unwatch or another pause.
- Persist the initial recovery interval before starting a watch. Empty successful
  scans advance recoveredThrough; live progress never clears pending ranges.
  First watches use the bounded recovery window, and crashes need no close hook.

## Read admission

Event readers share per-connection admission. Reserve feed/reconnect history and
live slots atomically; network deadlines start after admission, separately from
queueTimeout. Release leases on every completion/cancellation path and preserve
partial relay outcomes. Snapshot bounds describe only the historical attempt;
they neither end live input nor prove complete persisted coverage. Keep live
buffers bounded and surface overflow explicitly, never as successful EOSE.

## Live coverage controls

Live readers use a hard-coded ten-minute opening/recovery overlap and per-relay
recovery cursors (including duplicates, capped at receipt time). Empty relays use
opening time; failed recovery never advances its pending baseline. Automatic
60-second `live-progress` controls follow earlier events through the same bounded
queues, with a per-attempt epoch. They describe client-observed continuous live
input, never historical completeness or disconnected periods. Stop timers on
close/cancellation/drain; do not emit while waiting for EOSE or recovery. Report
involuntary closes even without a remote error. Consumers must explicitly select
`type: 'event'` and tolerate unknown control types; test private-channel and
NIP-46 against the real pool with controlled transport and injected controls.

- Incomplete private-channel reads preserve per-relay outcomes, native errors and
  request metadata under `PRIVATE_CHANNEL_FETCH_INCOMPLETE`. Keep status-only
  failures visible even when AggregateError.errors is empty. Never attach event
  payloads/signers to diagnostics or relax recovery completeness for one healthy
  relay. Read elapsed time includes admission, not decryption or storage.

- Private-channel subscription error notifications retain the generator's relay
  URL and `private-channel.subscribe` operation. Do not mutate shared native
  transport errors; keep the original as cause and preserve message/code/category,
  close details and AggregateError children. Do not guess absent relay metadata.

## Private file channels

- The public `private-messenger/file` coordinator owns file transfer/recovery;
  callers provide resolved signers and persisted IRFS chunk adapters. All data,
  including thumbnails, uses `dm:media:<canonical root>`; requests use the parent
  conversation, compact replies use the file channel. Do not create permanent
  watched seeder channels per file or alter ordinary DM temporal recovery.
- Router 26300 uses `p`, never legacy `r`, for recipient routing; carrier 26400
  has no recipient tag. Inner control `r` and NIP-94 root `r` keep their meanings.
  Router `i` identifies the IRFS index; `c` identifies transport fragmentation.
- File seeders store durable per-recipient authorizations and serve local verified
  chunks as `irfsChunk_v1`; only file watchtowers retain ciphertext seeds (64 MiB
  FIFO). Explicit sharing timestamps bound grants; retries cannot renew them.
  Include both file DBs in identity maintenance; catalog entries never pin chunks.
  Completion depends on validated persisted chunks, never reply terminal markers.
- Parse NIP-94 root extensions by field name, not ordinal position. Only the root
  is fixed at tag[1]. Preserve ThumbHash Base64 and optional thumbnail SHA-256.

## Pluggable recovery persistence

- External seed/grant stores use semantic methods, never the IDB queue interface.
  Keep local queue indices, byte accounting and atomic upserts intact. External
  adapters are caller-owned; default stores are closed by their consumers.
- Event-store recovery records are immutable 30078 inners in self personal copies
  with empty content and unversioned d namespaces. Preserve ciphertext/tag order,
  validate snapshot hashes and escape original CRDT-like values. Revocations use
  private kind 5, not a custom state field. Never renew expiry on sync/retry.
- Day candidates use D and literal '*', then exact snapshot overlap. Do not filter
  wrapper created_at as if it were the recovery time. No cross-snapshot hull merge.
- Keep sync transport seeds out of the event store being synchronized. Never
  automatically migrate unpublished local file seeds or pin roots with grants.
- The optional session coordinator is UI-independent; apps own presentation and
  account-state interpretation. Keep pending work encrypted and existing-only
  checkpoints cancellation-safe. File cache budgets count useful bytes, evict
  inactive roots and preserve active reservations/stream backpressure.

- Session `onSendError(error, { id, peer })` reports the owning outbox item,
  including failures of its context/file events. Preserve native diagnostics;
  do not persist errors or report inbox/download/deletion/cancelled work through
  this callback. Apps decide which user attempts and routes warrant feedback.

- Automatic NIP-65 sends retry a finite candidate snapshot in pairs per recipient,
  preserving the exact signed outer event. Configured `fallbackRelays`
  follow primary exhaustion by default and also support explicit per-call `relays`, channel
  `sendRelays` and channel/global `relays`, preserving explicit initial fanout and
  receiver sets. Automatic multi-recipient routing and explicit `relayToReceivers`
  maps use the same fallback policy. Maps keep precedence over fixed lists and
  do not trigger recipient discovery. Do not promote fallbacks through recovery mirrors.
- Private-channel publication passes local recipient/primary-route context to its
  injected sender. Retry each encrypted subset unchanged; never recreate wrappers
  or widen their recipients. Split replacement batches by the members they serve
  and require coverage of every member, even when some batches have ACKs. Exhaust
  remaining primary routes before configured fallbacks unless automatic routing
  opts into fallbackDelayMs. Preserve first-ACK latency
  within each batch and native errors in partially successful final reports.
- `fallbackRelays` defaults to `[]` on the messenger/session constructors. Validate,
  normalize, deduplicate and snapshot URLs. Receive routing unions fallbacks with
  both explicit and NIP-65-derived primaries from the start, including history and
  files; keep NIP-65 subscriptions active for automatic channels. Never add public
  recipient tags or rewrite relay-list metadata for this policy.
- Classify failures by native category and leading NIP prefix, never arbitrary
  prose. Invalid events, local signer/auth failures and unknown errors do not
  rotate relays. Confirm connectivity through `isOnline` before replacement;
  session failures with `retryWhenAvailable` stay pending and do not invoke
  `onSendError`. `retryWhenOnline` also owns a temporary shared `onOnline`
  subscription, released on cancel/close/unavailability. Retry only those waiting
  entries and never override signer availability; account recovery still uses
  `setAvailable(true)`.
- Relay exclusions live only in memory, expire after five minutes, and are scoped
  by channel/recipient (at most 256 scopes). Removing channels or closing clears
  them. Track attempted relays per outer event independently of this shared state.
  Aggregate native diagnostics from all attempts and retain the first-ACK path.
- Outbox cancellation and close abort fallback work for messages and file chunks.
  Shared publication diagnostics live in private-messenger/helpers/publication.js.

- Complete private-channel recovery uses `fetchHistory`: 16-event temporal pages,
  oldest leaves first, 16..256 for a dense second, at most 4 MiB per response.
  `satisfied` is saturation, never exhaustion. A dense/oversized response leaves
  the interval pending with `PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT` inside
  `PRIVATE_CHANNEL_FETCH_INCOMPLETE`; never checkpoint an incomplete scan.
  Preserve one fragment processor per call and summary-only results. Recovery
  gates cover reads plus processing (two per messenger, one per relay per library
  instance) without blocking live watches. Keep the original `fetch` contract.
- Messenger recovery uses `fetchHistory`'s opt-in partial mode with an in-memory
  per-range/per-relay tracker: retry only pending subranges (3 partial, 6
  empty-EOSE, 10 all-failed attempts per relay), then accept coverage from EOSE
  relays and ask seeders for uncovered subranges plus the pre-oldest left edge.
  Never advance ranges, consume budgets or contact seeders while offline. Trackers
  are process-local and pruned on range completion, relay-set change, channel
  clear/stale cleanup and close. Permanent per-relay errors stop that relay
  without spending the range budget.
- Priority recovery is an in-memory, app-directed window: `unread-page` requires
  `since`, `tail` derives a bounded window. Fetch priority subranges before older
  history, order relays by recent EOSE/latency, hedge only the first priority page,
  and use seeders only as fallback with recent presence; a successful ask counts as
  coverage and probes never consume tier budgets. Keep presence payloads and
  persisted ranges unchanged.

- Share `parseRelayRetryAdvice` through the public relay export. Accept only
  temporal advice on leading rate-limited CLOSED/OK: finite positive `retry_at`
  Unix seconds wins over `retry_after`, even after expiry. Never derive origin,
  retry eligibility or routing from extra fields.
- Honor optional absolute/relative relay timing on rate-limited CLOSED/OK, bounded
  to five minutes, without blocking CLOSE or sending cancelled work later.
  Preserve retryAfterMs/retryAt through subscription diagnostics. Existing
  operation deadlines remain authoritative; do not silently republish failures.
- Reply packers cap UTF-8 JSONL at 128 KiB and 100 records, except one indivisible
  oversized record. Stream outgoing fragments from prepared rows without a
  second Web Storage copy. Cleanup partial writes and failed tracking entries.

- Private-channel subscriptions expose a non-rejecting `done` result; private-message
  retires only the matching generation. Overflow retries await every affected
  channel's durable interruption callback before reopening one relay, preserving
  healthy subscriptions, fragment scope and cancellation. Readiness triggers paged
  gap recovery; live progress must never erase that interval. Keep bounded backoff,
  permanent-refusal handling and content-free buffer diagnostics.


## Public relay read diagnostics

- The relay export owns distinct retry/replacement predicates. Keep send routing
  behavior unchanged when sharing their implementation: policy refusals can
  select another relay without allowing repeated attempts on the same relay.
- `getLatestEventsByPubkey` preserves per-query `requests` reports for discovery,
  primary and fallback phases, plus immediate `onQueryResult` reports. Preserve
  native errors, retryAt/retryAfterMs and partial events; no event payloads in
  diagnostics. Cancellation must stop later routing passes.
- Shared NIP-65 discovery retains per-consumer cancellation and reporting.
  Exclusion sets scope shared requests. Failed/incomplete negative results must
  not enter the forty-minute cache; valid relay lists and complete empty EOSE
  reads retain normal caching. Never extend cooldown timestamps on report replay.
- Public exclusions apply to both event passes and to seed discovery through
  relayListOptions. Missing events do not turn a native refusal into success.

## Failure-aware live recovery

- Live retries use the public same-relay predicate plus explicit local admission
  recovery codes. Never retry validation, local authentication, policy refusals
  or unknown failures. Stop only the affected reader/relay; drain accepted items
  and finish naturally when all routes are terminal. Preserve readiness reports.
- Share bounded connectivity checks and one online wait per pool through
  `ReadRetry`. Cancellation is per consumer. Offline spends no backoff or read
  slots; use max(backoff deadline, retryAt) and reset only after EOSE and gap
  recovery succeed. Do not extend deadlines on online notifications.
- A failed gap retains its original baseline. Transient gaps retry; definitive
  gaps release the route while preserving accepted history/buffered live. No
  progress may certify that disconnected interval. Keep overflow/drain contracts.
- Timeouts retain native messages and category `timeout`. Internal connectivity
  hooks belong to controlled tests; ordinary relay failures keep original errors.

## Pause recovery and publication readiness

- `readStatus()`/`onStateChanged` expose copied `{ closed, paused, pauseReasons }`
  snapshots initially and on transitions; paused errors retain reason snapshots.
- Custom session `Messenger` factories must implement `readStatus()` and emit
  `options.onStateChanged` initially before resolving and on effective changes.
  Reject missing/malformed state, close rejected instances and fence callbacks.
  There is no polling compatibility for unobserved pauses; retain bounded retries
  for other transient availability failures without an active pause.
- Sessions park paused remote sends and wake on release independently of presence
  and historical completion. Keep personal saves/self-chat independent.
- Internally observed network/storage pauses own cancellable recovery jobs with
  1..30s exponential backoff and 20% jitter. Offline waits share onOnline and spend
  no step. Retryable watch setup failures recover their channel, not every writer.
- Explicit external pauses do not gain automatic recovery by name. Session inbox
  saves use `session-storage`, retry the actual reservation, and release after ACK.
  Account loss/close stop jobs and stale continuations. Unchanged channels, peers
  and availability must not recreate healthy watches.

## Optional early send fallback

- `fallbackDelayMs: null` preserves sequential fallback. Only automatic NIP-65
  routing opts into a single absolute deadline per signed outer event; explicit
  routes/maps keep their fanout/policy. At most two fallback relays per subset
  start after confirmation of connectivity, or immediately on primary exhaustion.
- Concurrent lanes share coverage/tried sets and stop new work on first complete
  acceptance, cancellation or pause. Preserve original bytes/IDs/deletion keys,
  native reports, 30s deadlines, rate advice and recipient-subset coverage. Waiting
  three seconds is not failure; only actual eligible failures inform preferences.
- Send signals settle unfinished reports with their original reason, never a
  timeout, and release per-consumer waiters. Same-ID publishers share one wire
  operation; cancelling one must preserve others. Deferred cooldown sends are
  removed on last cancellation. Physical sockets and bridge APIs are unchanged.
