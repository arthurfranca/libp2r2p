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
`eose`; the latter reports actual EOSE, satisfaction, timeout, cutoff, normal
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
