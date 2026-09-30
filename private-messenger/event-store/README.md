# Event-store adapters

`createPersonalCopyRecoveryStorage({ eventStore, signer })` returns `seeds`,
`authorizations`, and `close()`. Pass these to `PrivateMessenger` (`seedStorage`)
and `createPrivateFileTransfer` (`seedStorage`, `authorizationStorage`). The owner
signer and event store must address the same identity. There is no `window` access
and no launcher-specific private database API.

The signer exposes `getPublicKey`, `obfuscate(value, kind, scope)`, and byte-based
`nip44v3.decrypt(peer, kind, scope, ciphertext)`. The store exposes the existing
`query`, `addPersonalCopy`, and `remove` API. Queries require `&tags`, `!ids`,
`search:sort:desc`, and the personal-copy mirrors used by 44billion. Returned
wrappers remain encrypted until the adapter decrypts them. The adapter bounds memoized obfuscations (256) and decrypted grants (128); every
grant use still queries the store, so removal is immediately effective. Permission, lock,
read-only, quota and operational failures propagate; they are not empty results.
Expiry cleanup uses the signed wrapper expiration preserved by personal copies,
without decrypting the archive; the store may also purge expired wrappers itself.

## Contracts and ownership

- Seeds: `put(record)`, `iterate(query)`, `has(record)`, `removeLocal(query)`,
  `prune({ now })`, and `revoke(records)`.
- Authorizations: the same operations plus `find(query)`, choosing the newest
  valid share, then the shorter expiry and lexicographically smaller record ID.
- `iterate` is an async generator. Queries can name `recordId`, `channelPubkey`,
  `receiverPubkey`, `since`/`until`, or the file tuple `controlChannelPubkey`,
  `fileChannelPubkey`, `receiverPubkey`, and `indices`. `signal` cancels reads.
- The messenger/coordinator never closes caller-owned adapters. Close their
  owner factory after the consumers. No ciphertext is duplicated into a local
  library queue. Limits and storage admission belong to the event store.
- `createEventStoreChunkStorage({ eventStore })` implements `read`/`save` for
  IRFS chunks. Reconstruction saves an owner template, not a forged announcer
  identity. NostrDB keeps the usual deduplicated payloads and root references.
- `createEventStoreMessageStorage({ eventStore })` implements
  `save(event, { peerPubkey, hearsay })`, preserving the DM context/provenance.

Local defaults remain available when the corresponding adapter is omitted.
Registration, cache reads and recovery requests never grant file access. A grant
neither pins chunks nor contains signer material. Synced timestamps never renew
retention. Missing channel configuration is not a distributed revocation.

## Immutable inner events

Recovery data uses self-authored kind 30078 templates, `content: ''`, personal
copy context `''`, and no hearsay. Coordinates are
`libp2r2p:recovery:seed:<id>` / `libp2r2p:recovery:grant:<id>` (no schema version
in `d`). Data changes create another snapshot. Imports preserve authored times;
identical writes query their coordinate and do not re-encrypt/revise it.

`id` is lowercase SHA-256 of UTF-8 `JSON.stringify([type, bodyTags])`, where
`type` is `seed` or `grant` and body tags follow the fixed encoder order below.
`d`, `t`, `s`, `D`, and CRDT decorations are derived, not hash inputs. The body
includes expiration. Original strings starting with `%` or `~` escape that first
character as `%25` or `%7E`; decoding reverses exactly one prefix. This protects
preserved strings from CRDT metadata interpretation. Numbers are canonical
nonnegative decimal strings. The decoder rebuilds and verifies all tags and the
coordinate, tolerating only the store's reserved CRDT decorations.

Grant body, in order: `c` (control channel), `channel` (data channel), `peer`,
`p` (recipient), `root`, optional `size`, `shared`, `expiration`. There is no `r`
root reference: a grant must not retain file bytes. Creation time is `shared`.

Router seed body: `c` (data/ordinary channel), `type=routerEnvelopeRow_v1`, `p`,
`router` (pubkey, original timestamp), numbered `router-tag` entries (original
transport `c` removed), `payload`, `key`, optional `iykc` (remaining envelope
fields), optional `event` (known inner ID), `range` (first/last), optional `file`
(control channel, peer, root) plus `received`, and `expiration`. Kind 26300 is
implied by the record type. The original router's `i` supplies the file index.
The wire reply still reconstructs the existing JSONL rows; no network codec was
replaced. File seed creation time is `received`; ordinary seed time is range end.

Carrier seed body: `c`, `type=nymCarrier_v1`, numbered `carrier` entries containing
kind/pubkey/timestamp/ID/signature/content, their numbered `carrier-tag` entries
with `carrierIndex:tagIndex` identities, `range`, `expiration`. Signatures and
original tag order are verified/preserved. Range/creation time uses the maximum
carrier timestamp, as the existing recovery codec does.

All snapshots have `t=libp2r2p:recovery:<type>` and selector `s`. Selector hashes
use the same UTF-8 JSON/SHA-256 operation on:

- `['grant', control, data, recipient]`
- `['seed', channel, recipientOrEmpty]`
- `['file', control, data, recipient, chunkIndex]`

Ordinary seeds also carry every UTC Unix day in their interval (`D`, 86400 seconds
per day). More than 32 days uses the literal `D='*'`. Queries include their days
and `*`, then verify exact interval overlap. Wide/unbounded queries skip the day
prefilter. They never filter wrapper `created_at` with the recovery interval.
Snapshots do not invent an interval between independent observations. The local
IDB implementation retains its existing logical min/max compaction.

## Deletion and synchronization

`revoke(records)` writes owner-authored kind-5 personal copies in context `''`,
with `a` targets for the **inner** 30078 coordinates and advisory `k=30078`.
Batches contain at most 100 targets. Their timestamp covers stored versions and
expiration is the latest target expiry. The normal NostrDB private-deletion path
applies local removal and synced tombstones. There is no custom `state` tag.
Local eviction/removal and expiry do not publish a deletion. Once a tombstone
expires, its original targets are expired too. Do not reauthor synced snapshots.

Sync transports must keep **their own recovery seeds local**. Persisting seeds
of the transport that synchronizes seeds into that same event store creates a
recursive stream of new synchronization data. Consumer chat sessions may use
these adapters while the vault's synchronization messenger keeps its default.

No legacy seed migration or automatic local-to-event-store copying is performed.
A newly configured adapter records new captures/shares. Paired devices still
need the same channel configuration, identity keys, mode and locally available
chunks to serve a synced grant. Watchtowers need only their preserved envelopes.
