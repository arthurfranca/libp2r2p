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
