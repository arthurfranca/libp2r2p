# NIP-44 v3 interfaces

These public functions separate bytes, UTF-8 text and Base64 convenience.
Suffixes describe the plaintext input/output format; ciphertexts are standard
Base64 strings throughout.

| Functions | Plaintext input/output | Scope |
| --- | --- | --- |
| `encryptBytes` / `decryptBytes` | `Uint8Array` | UTF-8 bytes (`Uint8Array`) |
| `encryptWithConversationKeyBytes` / `decryptWithConversationKeyBytes` | `Uint8Array` | UTF-8 bytes (`Uint8Array`) |
| `encrypt` / `decrypt` | UTF-8 text (`string`) | UTF-8 text (`string`) |
| `encryptWithConversationKey` / `decryptWithConversationKey` | UTF-8 text (`string`) | UTF-8 text (`string`) |
| `encryptBase64` / `decryptBase64` | Standard Base64 (`string`) | UTF-8 text (`string`) |

The Base64 helpers decode plaintext before encryption and encode decrypted
bytes before returning them. Base64URL is not this
format, and arbitrary bytes must not pass through a UTF-8 text helper.

The [NIP-07 extension](https://github.com/nostr-land/nip44v3/blob/master/extensions/nip07.md)
defines `window.nostr.nip44v3.encrypt` with an `ArrayBuffer` plaintext and
`decrypt` with an `ArrayBuffer` result. Browser signer bridges should adapt
that boundary to their binary implementation or to the Base64
[NIP-46 transport](https://github.com/nostr-land/nip44v3/blob/master/extensions/nip46.md).
The library's text convenience functions are not implementations of that
browser interface. Do not guess whether a string is literal text or Base64.

The [implementation guide](https://github.com/nostr-land/nip44v3/blob/master/implementing.md)
requires binary support and explicit expected context. Callers choose kind and
scope from their protocol/event; decrypt verifies them. The
[NIP-17 extension](https://github.com/nostr-land/nip44v3/blob/master/extensions/nip17.md)
specifies kind 1059 for gift wraps and 13 for seals, each with empty scope;
these are protocol choices, not automatic defaults for other event types.
