import { test } from 'node:test'
import assert from 'node:assert/strict'

test('new public Nostr subpaths expose only the used surface', async () => {
  const expected = {
    event: ['finalizeEvent', 'getEventHash', 'validateEvent', 'verifyEvent'],
    key: [
      'generateKeypair', 'generateSecretKey', 'getPublicKey', 'keypairFromSeckey',
      'npubFromPubkey', 'nsecFromHex', 'parseProfileEvent', 'profileEventTemplate',
      'pubkeyFromNpub', 'signProfileEvent', 'signRelayListEvent'
    ],
    nip04: ['decrypt', 'encrypt'],
    nip44: ['decrypt', 'encrypt', 'getConversationKey'],
    url: ['normalizeUrl']
  }

  for (const [subpath, names] of Object.entries(expected)) {
    assert.deepEqual(Object.keys(await import(`libp2r2p/${subpath}`)).sort(), names.sort())
  }
})

test('serialization and RelayConnection remain package-internal', async () => {
  await assert.rejects(import('libp2r2p/event/helpers/serialize.js'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
  await assert.rejects(import('libp2r2p/relay/services/relay-connection.js'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
})
