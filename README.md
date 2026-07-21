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
  onError: err => console.warn('private messenger failed', err)
})

async function logMessages () {
  for await (const message of messenger.messages()) {
    console.log(message.type, message.payload)
  }
}

logMessages().catch(err => console.warn('private messenger messages failed', err))

await messenger.tell({
  receiverPubkey,
  payload: { text: 'hello' }
})
```

### Deleting Private Broadcasts

By default, each high-level private-message send creates one fresh deletion
keypair for its logical message. Every outer kind `3560` event produced for that
send, including router chunks, recipient subsets, and nym carriers, carries the
same public key in its `s` tag. The result always contains `delivery.reports`.
When libp2r2p generated the keypair, it also contains
`delivery.deletionSeckey`; the public key can be derived from that secret.

This shared `s` value deliberately makes the outer events for one logical send
linkable to relay operators and other observers. Disable automatic capabilities
when that tradeoff is not acceptable. The messenger-wide setting defaults to
`true`, and a channel setting takes precedence:

```js
const messenger = await createPrivateMessenger({
  userSigner,
  autoDeletionCapability: false,
  channels: [{
    signer: privateChannelSigner,
    relays: ['wss://relay.example'],
    autoDeletionCapability: true
  }]
})
```

With automatic capabilities disabled and no caller-supplied key, the outer
events have no `s` tag. They are not deliberately linkable through this
extension, but cannot later be deleted with it. A caller that already owns a
deletion key can supply its public key on an individual send; libp2r2p then
does not generate or return a key. Use a fresh caller-owned key for each
logical message unless cross-message linkability is intentional:

```js
import { generateKeypair } from 'libp2r2p/key'

const deletionKey = generateKeypair()
await messenger.tell({
  receiverPubkey,
  payload: { text: 'remove this later' },
  deletionPubkey: deletionKey.pubkey
})
```

```js
import { finalizeEvent } from 'nostr-tools'
import { keypairFromSeckey } from 'libp2r2p/key'
import { relayPool } from 'libp2r2p/relay'

const sent = await messenger.tell({
  receiverPubkey,
  payload: { text: 'remove this later' }
})

if (sent.delivery.deletionSeckey) {
  const deletionKey = keypairFromSeckey(sent.delivery.deletionSeckey)
  // Persist this secret with the application's copy of the logical message.

  const { result: outerEvents } = await relayPool.getEvents({
    kinds: [3560],
    authors: [channelPubkey],
    '#s': [deletionKey.pubkey]
  }, relays)
  for (let offset = 0; offset < outerEvents.length; offset += 100) {
    const deletion = finalizeEvent({
      kind: 5,
      created_at: Math.floor(Date.now() / 1000),
      tags: [['k', '3560'], ...outerEvents.slice(offset, offset + 100).map(event => ['e', event.id])],
      content: ''
    }, deletionKey.secretKey)
    await relayPool.sendEvent(deletion, relays)
  }
}
```

The `s` tag is public metadata and a deletion capability, not the channel key
or sender identity. libp2r2p does not delete anything automatically.

Relay support for this capability is not universal. A relay that implements it
should accept only a kind `5` request signed by the matching `s` key with exactly
one `['k', '3560']` tag, explicit matching `e` targets, and no `a` tags. A
regular NIP-09 kind `5` request signed by the outer event's private-channel key
must not delete a kind `3560` event, whether or not that event has an `s` tag.

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

Messages are stored in a bounded, durable IndexedDB queue until consumed:

```js
async function handleMessages () {
  for await (const message of messenger.messages()) {
    if (message.type === 'message') {
      console.log(message.payload)
    }
  }
}

handleMessages().catch(err => console.warn('private messenger messages failed', err))
```

For one-at-a-time consumption, use `await messenger.nextMessage()`. Queue
clearing is asynchronous too: `await messenger.clearChannel(channelPubkey)`.

Use explicit subpath imports for bundle size. The package root re-exports the
main messenger API for convenience, but applications that only need one piece
should import that subpath directly.

## Internationalization

The dependency-free `libp2r2p/i18n` subpath exposes locale detection and a
small translator suitable for keeping each component's translations beside
that component. Translation keys are literal strings, not dotted paths.

```js
import { getT } from 'libp2r2p/i18n'

const t = getT({
  'Allow {{size}}': {
    en: 'Allow {{size}}',
    'pt-BR': 'Permitir {{size}}'
  },
  'Delete {{count}} items': {
    en: {
      one: 'Delete {{count}} item',
      other: 'Delete {{count}} items'
    }
  }
})

t('Allow {{size}}', { size: '10 MiB' })
```

`getCurrentDeviceLocale()` prefers the locale resolved by `Intl`, then browser
language hints, and preserves the full canonical BCP 47 locale. `getT()`
matches exact and compatible language variants, falls back to English and then
to the key itself, interpolates `{{name}}` values, and uses `Intl.PluralRules`
when a locale value supplies plural forms. Missing interpolation values remain
visible in the returned string.

Catalog validation is opt-in. `validateLocales()` can be used independently,
or `getT()` can validate once when creating the translator:

```js
import { getT, validateLocales } from 'libp2r2p/i18n'

const validation = {
  requiredLocales: ['en', 'pt-BR'],
  referenceLocale: 'en',
  requireReferenceKey: true
}

validateLocales(locales, validation)
const t = getT(locales, { validation })
```

Validation checks catalog structure, required locales, plural `other` forms,
and placeholder parity across every translation. `requireReferenceKey` also
requires the reference locale's string, or its plural `other` form, to equal
the literal translation key. Without `validation`, sparse catalogs continue
to use the normal locale fallback behavior.

## Binary encodings

Base16, Base36, Base62, Base64/Base64URL, and Base93 helpers are available
through their matching `libp2r2p/<encoding>` subpaths. Base36 exposes both a
binary-safe variable-width codec and the canonical 32-byte/50-character
Base36Nsite representation from NIP-5A. Base62 uses the same case-sensitive
alphabet as app NIP-19 entities; its default byte mode preserves leading zero
bytes, while integer mode supports fixed-width identifiers.

In NIP-5A, "no padding" means that no separate padding character such as `=`
is used. Leading `0` digits are nevertheless required to make every Nsite
Base36 value exactly 50 characters long.
