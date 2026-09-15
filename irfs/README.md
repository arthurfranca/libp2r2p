# IRFS preparation

`libp2r2p/irfs` prepares an immutable browser `File`/`Blob` or `Uint8Array`.
It has no signer, publisher, relay pool or launcher dependency.

```js
import { prepareIrfsFile, decodeIrfsChunk } from 'libp2r2p/irfs'
const prepared = await prepareIrfsFile(file, { signal })
try {
  const created_at = Math.floor(Date.now() / 1000)
  for await (const template of prepared.chunks({ created_at, signal })) {
    // Sign/store/publish according to the consuming application's policy.
    const verified = decodeIrfsChunk(template)
  }
  // prepared.chunks({ created_at }) may be iterated again for retry.
} finally {
  prepared.close()
}
```

The result exposes `root` (hex MMR root), `size`, `total`, `chunks()` and
idempotent `close()`. Blocks contain at most 51,000 bytes; only the final block
may be smaller. Templates use kind 34601, deterministic `d` identifiers, an
`mmr` tag containing decimal index/total and Base93 proof, and Base93 content.
Use a stable timestamp when identical templates are required across retries.
`decodeIrfsChunk()` validates the proof, identifier, root and block size.

Preparation retains the immutable input and an in-memory NMMR hash tree, not
a second copy of file bytes. It creates **no temporary IndexedDB database**.
`close()` drops its input/tree references and abort-listener registration;
abort also closes preparation. In-flight Blob reads finish but cannot yield
another chunk after cancellation. Consumers own any object URLs they create.
Hash memory scales with chunk count. `onProgress({ completed, total })` reports
bytes hashed; preparation yields regularly for input/cancellation.
Empty input raises `ValidationError('EMPTY_IRFS_FILE')` in this version.
