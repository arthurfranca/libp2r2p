# secp256k1 WebAssembly

The runtime consumes the generated base64 module at
`helpers/secp256k1-wasm/generated.js`. npm consumers do not need a native
toolchain or a bundler-specific `.wasm` loader.

Rebuild after changing `wasm/Containerfile` or updating the pinned
`vendor/libsecp256k1` submodule:

```sh
npm run build:wasm
```

The command runs `wasm/build.js`.

The script uses `podman` or `docker`, or the executable named by
`WASM_CONTAINER_ENGINE`. The Emscripten image is pinned by digest and the
submodule is pinned to libsecp256k1 v0.8.0
(`6e2c8bc4ecdc6e71dbe7a368f360d8d453ce435d`). The build exports its internal
incremental SHA-256 functions and uses a fixed 256 KiB linear memory.

The wrapper and build process are derived from
https://github.com/fiatjaf/nostr-wasm, which is distributed under the MIT
license. libsecp256k1 is also distributed under the MIT license; see its
`COPYING` file in the submodule.
