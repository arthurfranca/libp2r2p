import { test } from 'node:test'
import assert from 'node:assert/strict'

test('new public Nostr subpaths expose only the used surface', async () => {
  const expected = {
    'content-key/event': [
      'CONTENT_KEY_KIND', 'assertValidContentKeyProof', 'assertValidIykcProof',
      'isValidContentKeyProof', 'isValidIykcProof', 'makeContentKeyEvent',
      'makeContentKeyEventForPubkey', 'makeContentKeyProof', 'makeIykcProof',
      'parseContentKeyEvent'
    ],
    event: [
      'assertSerializableEvent', 'assertValidEvent', 'classifyEvent', 'finalizeEvent',
      'getEventHash', 'isAddressableEvent', 'isEphemeralEvent', 'isRegularEvent',
      'isReplaceableEvent', 'isSerializableEvent', 'isValidEvent'
    ],
    error: ['ValidationError'],
    key: [
      'generateKeypair', 'generateSecretKey', 'getPublicKey', 'keypairFromSeckey',
      'npubFromPubkey', 'nsecFromHex', 'parseProfileEvent', 'profileEventTemplate',
      'pubkeyFromNpub', 'signProfileEvent', 'signRelayListEvent'
    ],
    kind: [],
    nip04: ['decrypt', 'encrypt'],
    nip05: ['queryProfile'],
    nip44: ['decrypt', 'encrypt', 'getConversationKey'],
    nip96: [
      'assertValidDelayedProcessingResponse', 'assertValidFileUploadResponse',
      'assertValidServerConfiguration',
      'calculateFileHash', 'checkFileProcessingStatus', 'deleteFile', 'generateDownloadUrl',
      'generateFSPEventTemplate', 'readServerConfig', 'uploadFile',
      'isValidDelayedProcessingResponse', 'isValidFileUploadResponse', 'isValidServerConfiguration'
    ],
    nip98: ['getToken'],
    nwt: ['createToken', 'decodeToken', 'encodeToken', 'validateToken'],
    url: ['assertValidPublicRelayUrl', 'isValidPublicRelayUrl', 'normalizeRelayUrl']
  }

  for (const [subpath, names] of Object.entries(expected)) {
    const actual = Object.keys(await import(`libp2r2p/${subpath}`)).sort()
    if (subpath === 'kind') {
      assert.ok(actual.includes('eventKinds'))
      assert.ok(actual.includes('classifyKind'))
      assert.ok(actual.includes('isRegularKind'))
      assert.ok(actual.includes('PERSONAL_COPY'))
      continue
    }
    assert.deepEqual(actual, names.sort())
  }

  const root = await import('libp2r2p')
  assert.equal(root.error.ValidationError, (await import('libp2r2p/error')).ValidationError)
})

test('serialization and RelayConnection remain package-internal', async () => {
  await assert.rejects(import('libp2r2p/event/helpers/serialize.js'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
  await assert.rejects(import('libp2r2p/relay/services/relay-connection.js'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
})
