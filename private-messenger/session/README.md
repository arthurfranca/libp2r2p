# Private message sessions

`createPrivateMessageSession(options)` coordinates one identity's DM inbox,
outbox and IRFS attachments without presentation state. It builds on
PrivateMessenger and the file coordinator, not another recovery protocol.

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

Methods: `setPeers`, `setAvailable(boolean)`, `prefetchContentKeys(peers?)`,
`enqueue`, `retry`, `cancel`, `download`, `cancelDownload`, and `close`.
`prefetchContentKeys` warms the content-key lookup cache for the owner and the
given peers (default: current peers); it is best-effort and never rejects.
Callbacks are `onOutbox`, `onMedia`,
`onError`, and optional `onSendError(error, { id, peer })`. The caller maps its
account permissions/connectivity/lock state to availability and owns view subscriptions. There are no DOM or UI imports.

`onSendError` identifies the main outbox message even when its quote, metadata,
file chunks, or local persistence failed. The original error is also reported to
`onError` unchanged. It fires once per failed send attempt (including retries),
not when persisted failures are read, nor for inbox/download/deletion operations
or cancelled/closed sends. Apps own route visibility and user-intent filtering;
automatic retries may also fail. Callbacks must not throw. Errors are not copied
into persisted outbox records.

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

Automatic sends try each recipient's NIP-65 read relays first, at most two per
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
exhausted before configured fallbacks. Redundant acknowledgements may update
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
