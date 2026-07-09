# libp2r2p

Peer-to-relay-to-peer utilities for Nostr apps.

libp2r2p focuses on flows where one peer talks to another peer with Nostr
relays in the middle. It is not pure peer-to-peer networking; relays provide
the transport and discovery surface. The package was born to distribute the
private messenger reference implementation, and it also carries a few Nostr
power-ups used by that messenger.

## Private Messenger

The main API is `createPrivateMessenger` from `libp2r2p/private-messenger`.
It coordinates private-channel wrapping, relay watching, recovery state, and
content-key lookup for direct or group-style private app messages.

```js
import { createPrivateMessenger } from 'libp2r2p/private-messenger'

const messenger = await createPrivateMessenger({
  userSigner,
  contentKeySigner,
  channels: [{
    signer: privateChannelSigner,
    relays: ['wss://relay.example'],
    mode: 'leecher'
  }],
  onMessageQueued: () => {
    for (const message of messenger.messages()) {
      console.log(message.type, message.payload)
    }
  },
  onError: err => console.warn('private messenger failed', err)
})

await messenger.tell({
  receiverPubkey,
  payload: { text: 'hello' }
})
```

### Temporary Send Storage

While an outgoing private message is being assembled, the messenger keeps
encrypted envelope rows and router chunks in `sessionStorage`. They are
removed when the send finishes, but an interrupted browser operation can leave
them behind until cleanup runs.

`PrivateMessenger.init()` performs cleanup automatically. Call
`PrivateMessenger.cleanupTemporaryStorage()` once during app startup when
messenger initialization may be delayed, such as while an account is locked:

```js
import { PrivateMessenger } from 'libp2r2p/private-messenger'

PrivateMessenger.cleanupTemporaryStorage()
```

Call it before any private-message send using that storage area starts. It
does not clear persisted messages, recovery material, or channel state. Pass
`temporaryStorageArea: localStorage` when constructing a messenger to opt into
a different Storage area.

Signers are expected to expose the Nostr-style methods used by the messenger,
including `getPublicKey()`, `signEvent(event)`, and the NIP-44 v3 methods
needed by private channels. For double-DH content-key use, pass a
`contentKeySigner` or a signer implementation that handles content keys
internally.

Messages are stored in the messenger queue until consumed:

```js
for (const message of messenger.messages()) {
  if (message.type === 'message') {
    console.log(message.payload)
  }
}
```

Use explicit subpath imports for bundle size. The package root re-exports the
main messenger API for convenience, but applications that only need one piece
should import that subpath directly.
