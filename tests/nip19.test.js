import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { bech32 } from '@scure/base'
import {
  NAPP_ENTITY_REGEX,
  appDecode,
  appEncode,
  nfileDecode,
  nfileEncode,
  npubDecode,
  npubEncode,
  nsecDecode,
  nsecEncode
} from '../nip19/index.js'

const root = '00112233445566778899aabbccddeeff'.repeat(2)
const author = 'ffeeddccbbaa99887766554433221100'.repeat(2)

function rawNfile (bytes) {
  return bech32.encode('nfile', bech32.toWords(Uint8Array.from(bytes)), 5000)
}

describe('nfile', () => {
  it('has a deterministic complete vector', () => {
    const value = {
      root,
      relays: ['wss://one.example', 'wss://two.example'],
      author,
      mime: 'image/webp',
      filename: 'café.webp'
    }
    const encoded = nfileEncode(value)
    assert.equal(
      encoded,
      'nfile1qqsqqyfzxdz92enh3zv64w7vmhh07qq3yge5g4txw7yfn24menw7alcpz9mhxue69uhk7mn99ejhsctdwpkx2qg3waehxw309a68wmewv4uxzmtsd3jsyg8lamwuewa2nxy8wej4gsejyygqllhdmn9m42vcsamx24zrxgs3qqps56tdv9nk2tmhv438qpq2vdskdsaf9emk2cnsc8rqhc'
    )
    assert.deepEqual(nfileDecode(encoded), value)
  })

  it('supports the root-only form and repeated relay hints', () => {
    assert.deepEqual(nfileDecode(nfileEncode({ root })), { root, relays: [] })
    const relays = ['wss://same.example', 'wss://same.example']
    assert.deepEqual(nfileDecode(nfileEncode({ root, relays })).relays, relays)
  })

  it('ignores unknown TLV types', () => {
    const rootBytes = Buffer.from(root, 'hex')
    assert.deepEqual(
      nfileDecode(rawNfile([0, 32, ...rootBytes, 99, 3, 1, 2, 3])),
      { root, relays: [] }
    )
  })

  it('rejects truncated, duplicate, malformed and non-canonical inputs', () => {
    const rootBytes = [...Buffer.from(root, 'hex')]
    assert.throws(() => nfileDecode(rawNfile([0])), /Truncated TLV header/)
    assert.throws(() => nfileDecode(rawNfile([0, 32, ...rootBytes.slice(0, 31)])), /Truncated TLV/)
    assert.throws(() => nfileDecode(rawNfile([0, 32, ...rootBytes, 0, 32, ...rootBytes])), /Duplicate MMR root/)
    assert.throws(() => nfileDecode(rawNfile([
      0, 32, ...rootBytes,
      2, 32, ...rootBytes,
      2, 32, ...rootBytes
    ])), /Duplicate author hint/)
    assert.throws(() => nfileDecode(rawNfile([
      0, 32, ...rootBytes,
      3, 1, 97,
      3, 1, 98
    ])), /Duplicate MIME/)
    assert.throws(() => nfileDecode(rawNfile([
      0, 32, ...rootBytes,
      4, 1, 97,
      4, 1, 98
    ])), /Duplicate filename/)
    assert.throws(() => nfileDecode(rawNfile([0, 32, ...rootBytes, 3, 2, 0xc3, 0x28])), /UTF-8/)
    assert.throws(() => nfileDecode(nfileEncode({ root }).toUpperCase()), /canonical lowercase/)
  })

  it('rejects invalid field lengths and values', () => {
    assert.throws(() => nfileEncode({ root: '00' }), /32 bytes/)
    assert.throws(() => nfileEncode({ root, author: '00' }), /32 bytes/)
    assert.throws(() => nfileEncode({ root, mime: '' }), /non-empty/)
    assert.throws(() => nfileEncode({ root, filename: 'x'.repeat(256) }), /too big/)
    assert.throws(() => nfileEncode({ root, relays: ['\ud800'] }), /UTF-8/)
  })
})

describe('existing entities', () => {
  const hex = 'a0a810b0fa6499358355d353884e5633c1a237c81e58044c531639590817dfa5'

  it('round-trips app entities without changing their format', () => {
    const value = { dTag: '', pubkey: hex, channel: 'main', relays: [] }
    const entity = appEncode(value)
    assert.ok(NAPP_ENTITY_REGEX.test(entity))
    assert.deepEqual(appDecode(entity), { ...value, kind: 35128 })
  })

  it('round-trips npub and nsec entities', () => {
    assert.equal(npubDecode(npubEncode(hex)), hex)
    assert.equal(nsecDecode(nsecEncode(hex)), hex)
  })
})
