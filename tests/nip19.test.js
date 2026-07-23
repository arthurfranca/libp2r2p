import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { bech32 } from '@scure/base'
import { ValidationError } from '../error/index.js'
import {
  NAPP_ENTITY_REGEX,
  appDecode,
  appEncode,
  naddrDecode,
  naddrEncode,
  neventDecode,
  neventEncode,
  nfileDecode,
  nfileEncode,
  noteDecode,
  noteEncode,
  nprofileDecode,
  nprofileEncode,
  npubDecode,
  npubEncode,
  nrelayDecode,
  nrelayEncode,
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
    const value = {
      dTag: 'my-app',
      pubkey: hex,
      channel: 'main',
      relays: ['wss://one.example']
    }
    const entity = appEncode(value)
    assert.equal(entity, '+qYizSSBIIRhTSKm6lRDGAFOSrO1KgUm2l6TMXJvogyMTKagXypXriUE4v2Q2bqqHZRDsDv2hPHzNru1PO')
    assert.ok(NAPP_ENTITY_REGEX.test(entity))
    assert.deepEqual(appDecode(entity), { ...value, kind: 35128 })
  })

  it('appDecode reports stable validation codes', () => {
    for (const [entity, code] of [
      ['', 'INVALID_APP_CHANNEL'],
      ['+0', 'TRUNCATED_TLV_HEADER'],
      ['+', 'MISSING_APP_D_TAG']
    ]) {
      assert.throws(() => appDecode(entity), error => (
        error instanceof ValidationError && error.code === code
      ))
    }
  })

  it('round-trips npub and nsec entities', () => {
    assert.equal(npubDecode(npubEncode(hex)), hex)
    assert.equal(nsecDecode(nsecEncode(hex)), hex)
  })
})

describe('standard pointer entities', () => {
  it('decodes the NostrHub naddr vector and re-encodes it exactly', () => {
    const entity = 'naddr1qvzqqqrcvypzplrsshpc8wn3w3tsf0wpcmhu7latqxt4q809nrz7d3fh4s9n9fxtqqd8gct894jx2enfdejkgtt9wejkuapdvfjksctkd9hhyapjete'
    const value = {
      identifier: 'tag-defined-event-behavior',
      pubkey: 'fc7085c383ba71745704bdc1c6efcf7fab0197501de598c5e6c537ac0b32a4cb',
      kind: 30817,
      relays: []
    }
    assert.deepEqual(naddrDecode(entity), value)
    assert.equal(naddrEncode(value), entity)
  })

  it('round-trips note, profile, event, relay and empty-address identifiers', () => {
    const id = '12'.repeat(32)
    const author = '34'.repeat(32)
    const relays = ['wss://one.example', 'wss://two.example']
    assert.equal(noteDecode(noteEncode(id)), id)
    assert.deepEqual(nprofileDecode(nprofileEncode({ pubkey: author, relays })), { pubkey: author, relays })
    assert.deepEqual(neventDecode(neventEncode({ id, relays, author, kind: 65535 })), { id, relays, author, kind: 65535 })
    assert.equal(nrelayDecode(nrelayEncode(relays[0])), relays[0])
    assert.deepEqual(naddrDecode(naddrEncode({ identifier: '', pubkey: author, kind: 30000 })), {
      identifier: '', pubkey: author, kind: 30000, relays: []
    })
  })

  it('rejects non-canonical and duplicate unique fields while ignoring unknown TLVs', () => {
    const id = [...Buffer.from('12'.repeat(32), 'hex')]
    const duplicate = bech32.encode('nevent', bech32.toWords(Uint8Array.from([0, 32, ...id, 0, 32, ...id])), 5000)
    assert.throws(() => neventDecode(duplicate), /Duplicate event ID/)
    assert.throws(() => noteDecode(noteEncode('12'.repeat(32)).toUpperCase()), /canonical lowercase/)
    assert.throws(() => neventEncode({ id: '12'.repeat(32), kind: 0x100000000 }), /Invalid kind/)
    const unknown = bech32.encode('nprofile', bech32.toWords(Uint8Array.from([0, 32, ...id, 99, 1, 1])), 5000)
    assert.deepEqual(nprofileDecode(unknown), { pubkey: '12'.repeat(32), relays: [] })
  })
})
