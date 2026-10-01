# Private file transfer

`createPrivateFileTransfer({ messenger, resolveChannel, storage, onError })`
creates one account coordinator attached to an initialized PrivateMessenger.
It does not create permanent watched channels for files. The application resolves
channel signers. An optional adapter supplies `storage.read(root, index, descriptor)`
and `storage.save(event, descriptor)`; otherwise the library uses its bounded local
cache. `save` must resolve only after persistence.
The descriptor is `{ controlChannelPubkey, peerPubkey, root, size?, sharedAt? }`.
`sharedAt` is a stable sharing timestamp, never a read/retry timestamp.

For one-to-one chats, resolve the signer with
`withSharedKey(peerPubkey, fileChannelInfo(root))`: the canonical context is
`dm:media:<lowercase MMR root>`. Original and thumbnail roots use separate
channels under the same convention. Do not put this context in a public tag or
append it to the thumbnail URL. This is not a group-key derivation scheme.

- `register(descriptor)` resolves and checks the file channel without watching it.
- `authorizeSeeding(descriptor, { receiverPubkeys, sharedAt })` durably grants
  the listed identities indexed recovery of this root on this control/data pair.
  The application calls it for an explicit share, using its persisted message
  timestamp (standalone 1063 uses its own timestamp). Registration, cache hits
  and incoming requests never grant access. Missing router `p` is not a wildcard.
- `publishChunk(descriptor, event, { signal } = {})` checks a 34601 proof and the descriptor,
  captures ciphertext only in watchtower mode, and publishes to the recipient's relays.
  It shares the parent channel's automatic relay fallback and full publication
  diagnostics; `signal` stops further replacement attempts.
  Every wrapper must receive at least one relay acceptance. Yield between calls
  so text/control sends can run. Publish all file chunks before announcing 1063/9.
- `download(descriptor, { manual, thumbnail, signal })` returns the verified,
  persisted file descriptor. Automatic originals are limited to 1 MiB and
  automatic thumbnails to 51,000 bytes. Larger/unknown originals require action;
  already complete local files remain accessible. Unknown-size manual downloads
  bootstrap their total from a verified chunk and learn size from the last one.
- `observe(callback)` returns an unsubscribe function. Updates include control
  channel, root, status, persisted useful bytes (`completed`), total and errors.
- `cancel(controlChannelPubkey, root)` cancels the shared transfer; individual
  caller signals only stop that caller waiting. Partial chunks are retained.
- `close()` releases subscriptions, timers and storage. Messenger pause/unwatch/
  close also stop work; resume restarts paused transfers, never explicit cancels.

There are at most two active downloads per coordinator; thumbnails precede
manual originals, then small automatic originals. Open live input before reading
history or asking seeders. A failed historical read does not block peer recovery.
Deduplication and completion depend on verified, persisted chunk indices, not
publication ACKs or a terminal response. Ordinary DM left-edge temporal recovery
is unchanged.

## Recovery protocol

The authenticated direct request travels on the conversation's `dm` channel:

```js
{ code: 'fileChunksRequest_p5cc', payload: {
  fileChannelPubkey,
  missingRanges: [[0, 15]] // inclusive, sorted, disjoint; at most 16 indices
} }
```

A seeder replies from `storage.read`, skipping unavailable indices and validating
local proofs before sending. Each JSONL record is:

```js
{ recordType: 'irfsChunk_v1', index, total, proof, content }
```

`proof` and `content` use Base93. The receiver reconstructs a 34601 template,
checks its proof against the expected root, and persists it without claiming the
original announcer authored the reconstructed event. The outer reply remains
individually encrypted for its recipient. Watchtowers continue replying with
`routerEnvelopeRow_v1`; they need not decrypt original bytes.

The reply travels on the **file** channel, with code `fileChunksReply_p5cc` and
the existing compact-record JSONL packer's `index`, `isLast`, and `jsonl` fields.
The inner reply's `q` references the request event. No new terminal availability
message is required and `isLast` never establishes file completeness. Late
responses from authorized seeders remain useful during an active download.
Seed replies run outside the DM dispatcher, with at most two reply workers and
sixteen queued requests; workers yield between records so control traffic can
continue. Requests discarded at the queue bound can be retried.

Request one seeder first and another after two seconds, recalculating missing
indices. A new persisted chunk resets the inactivity clock; duplicates do not.
After contacting all selected seeders, 30 seconds without progress makes the
transfer retryable. Each new attempt rechecks local storage and relay history.

Router 26300 uses `p` for its optional recipient, `f` for transport sender,
optional `imkc`, `c` for outer fragmentation, and `i` for the IRFS chunk index.
Repeat `i` across every fragment. It survives seed compaction, and must agree
with the validated 34601. It is encrypted within the router, never a public tag.
Carrier 26400 continues to use only `id`/`c`; inner control recipient `r` and
NIP-94 file-reference `r` are unchanged. There is no router `r` read fallback.
The existing 65,536-byte event ceiling includes the additional router overhead.

## Storage and scope

Seeder authorizations live in
`libp2r2p:private-messenger:<owner>:file-authorizations:idb-queue`, with `key`
(unique control/data/recipient) and `channel` indexes. Records hold root, peer,
optional size, recipient, `sharedAt`, and absolute `expiresAt`; no ciphertext,
chunk bytes, signer or file-retention reference. The queue rejects capacity
failures instead of silently evicting grants. Grants survive restart and expire
at the original sharing time plus parent retention (default seven days), subject
to a subsequently shorter parent policy. Only a newer explicit share renews them.

Direct validated, persisted deliveries may authorize their actual local receiver
using the descriptor's `sharedAt`. Cache reads and recovery replies never renew
or create grants. Applications must not infer authorization for a third-party
announcer from its knowledge of a root. Group membership/revocation is not part
of this DM implementation; future group requests also need current membership.

Serving requires both live authorization and locally available chunks. The
catalog never pins NostrDB roots or recreates missing bytes. Checks run before
reads and again before each publication. Shared cleanup removes invalid grants
at startup and every minute while running; expired grants are immediately
unusable even before physical cleanup. Identity cleanup includes the catalog.

Watchtower ciphertext remains in
`libp2r2p:private-messenger:<owner>:file-seeds:idb-queue`, with its existing
64 MiB FIFO budget and parent retention. The authorization catalog starts empty;
there is no migration or compatibility path for unpublished seeder file seeds.

Seed responses require the matching control/data channel pair and requester.
A channel holder can group traffic by its public channel author; no plaintext
root/index tag is exposed. This does not hide timing or repeated transfers of
the same pair/root.

Multi-device seed synchronization and group-channel derivation remain separate
follow-ups. Applications own durable manual-download intent, metadata retention
and UI policy. Removing one message must not delete a root retained by another.

## Optional persistence (0.11.1)

`storage`, `authorizationStorage`, and `seedStorage` are optional. The latter two
use the semantic contracts in `private-messenger/event-store`; omitted stores
retain their existing indexed local implementation. External stores are owned by
the caller. Synced grants are checked against the current channel mode/retention
before serving, and are never deleted globally because a channel is not loaded.

With no chunk adapter, `createFileCache` owns
`<messenger prefix>:file-chunks:idb` (files and chunks stores). Its default
`cacheMaxBytes` is 256 MiB of useful decoded bytes; physical IndexedDB overhead is
additional. Deduplication uses root/index. FIFO root eviction discards inactive
files to admit new data. Durable renewable reservations protect active transfers
and readers across tabs; stale reservations expire after two minutes. A blocked
transfer waits with cancellation. Unknown sizes reserve the budget exclusively;
an individual oversized file raises `FILE_EXCEEDS_CACHE_CAPACITY`. Physical quota
failures try discarding additional inactive roots before propagating.

Outgoing chunks are saved before publication; this does not authorize recipients.
`readChunk(root, index)` reads local data. `stream(descriptor, { signal })` uses
bounded sequential reads and backpressure, not a whole-file Blob. Cancel streams
when unused. Cache eviction can remove a previously completed file, so new
requests recheck persisted indices. The cache is included in identity cleanup.
