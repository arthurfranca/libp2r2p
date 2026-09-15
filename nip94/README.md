# File metadata

`libp2r2p/nip94` exports `createFileMetadata(options)` and
`decodeFileMetadata(event)`. The former creates an unsigned kind-1063 template;
the latter reads and validates its tags without parsing the caption as a URL.

This is an **adaptation/extension of NIP-94**, not a claim that every emitted
event meets its SHA-256 requirements. `url` and `mime` are required. Options
include `caption`, `created_at`, `size`, `width`/`height`, `alt`, `root`, `service`,
`thumbhash`, `sha256`, `originalSha256`, and additional `tags` (e.g. replies).
The caption becomes `.content`. `root` maps to `r`, dimensions to `dim`, hashes
to `x`/`ox`; hashes are optional and never synthesized. ThumbHash is transported
unchanged as Base64, independently of image decoding. No `blurhash` is generated.

For local IRFS files use `service: 'irfs'` and
`https://nostr.alt/nfile1…?localOnly=1`. Encode the MMR root, MIME and filename
with `nfileEncode`; relay/author hints are unnecessary. The decoder verifies
agreement between `r`/`m` and nfile metadata and exposes its `filename`.
The `r` reference allows storage owners to retain the associated local chunks.
The module does not encrypt, sign, store or download data.

## Download intent

`download` is an optional extension expressing the author's intended action:
`'1'` asks clients to download on activation rather than open/play the media.
It does not prevent thumbnails or enforce server behavior.

For kind 1063, no tag and `['download', '0']` both decode to `download: '0'`;
`['download']` and `['download', '1']` both decode to `download: '1'`.
Empty/other values, extra fields and duplicate download tags are invalid.
`createFileMetadata({ ..., download: '0' | '1' })` emits an explicit value;
omitting the option emits no download tag. Boolean/numeric options are invalid.

For inline URLs, `nip27.decodeMediaMetadata()` requires an explicit
`#download=0` or `#download=1` (or `&download=...` after other fragment fields).
A bare/empty/duplicate/invalid value throws `INVALID_MEDIA_METADATA_DOWNLOAD`;
`tryDecodeMediaMetadata()` returns null. No fragment means no download property.
`extractMedia()` carries the string flag for ordinary URLs and nfile URLs,
including long nfile entities and `?localOnly=1`. Metadata fragments do not
change the bytes or root of a file; clients strip them from download routes.
