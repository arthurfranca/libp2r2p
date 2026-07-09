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
