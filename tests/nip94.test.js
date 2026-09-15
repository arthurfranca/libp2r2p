import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createFileMetadata, decodeFileMetadata } from '../nip94/index.js'
import { nfileEncode } from '../nip19/index.js'
import { extractMedia } from '../nip27/index.js'

test('local NIP-94 profile round-trips captions, roots, dimensions and ThumbHash without SHA tags', () => {
  const root = 'ab'.repeat(32)
  const filename = '長い'.repeat(40) + '.png'
  const url = `https://nostr.alt/${nfileEncode({ root, mime: 'image/png', filename })}?localOnly=1`
  const event = createFileMetadata({ url, root, mime: 'image/png', caption: '', size: 17, width: 640, height: 480, thumbhash: 'AQID', service: 'irfs' })
  assert.equal(event.kind, 1063)
  assert.equal(event.tags.some(t => ['x', 'ox', 'blurhash'].includes(t[0])), false)
  assert.deepEqual(decodeFileMetadata(event), { url, root, mime: 'image/png', caption: '', download: '0', size: 17, width: 640, height: 480, thumbhash: 'AQID', service: 'irfs', filename })
  const [item] = extractMedia(url)
  assert.equal(item.url.value, url)
  assert.equal(item.url.nfile.root, root)
  assert.equal(item.url.m, 'image/png')
  assert.equal(extractMedia(`(${url}).`)[2].text.value, ').')
  assert.throws(() => createFileMetadata({ url, root: 'cd'.repeat(32), mime: 'image/png' }), { code: 'FILE_METADATA_NFILE_MISMATCH' })
  assert.throws(() => createFileMetadata({ url, mime: 'image/png', width: 0, height: 1 }), { code: 'INVALID_FILE_METADATA_DIMENSIONS' })
  assert.throws(() => decodeFileMetadata({ ...event, tags: [...event.tags, ['url', url]] }), { code: 'INVALID_FILE_METADATA_TAG' })
})

test('nfile URLs near the codec limit preserve every relay hint and localOnly', () => {
  const root = 'ac'.repeat(32)
  const relays = Array.from({ length: 12 }, (_, i) => `wss://relay${i}.example/${'a'.repeat(220)}`)
  const entity = nfileEncode({ root, relays, mime: 'video/mp4', filename: 'movie.mp4' })
  assert.ok(entity.length > 4500 && entity.length <= 5000)
  const url = `https://nostr.alt/${entity}?localOnly=1`
  const items = extractMedia(url)
  assert.equal(items.length, 1)
  assert.equal(items[0].url.value, url)
  assert.deepEqual(items[0].url.nfile.relays, relays)
  const [download] = extractMedia(url + '#download=1&dim=32x16')
  assert.equal(download.url.value, url + '#download=1&dim=32x16')
  assert.equal(download.url.download, '1')
  assert.equal(download.url.width, '32')
  assert.equal(download.url.m, 'video/mp4')
})

test('download intent defaults to 0, supports a bare event tag and rejects ambiguity', () => {
  const base = createFileMetadata({ url: 'https://example.com/a.mp4', mime: 'video/mp4' })
  assert.equal(base.tags.some(tag => tag[0] === 'download'), false)
  for (const [tags, expected] of [[[], '0'], [[['download']], '1'], [[['download', '0']], '0'], [[['download', '1']], '1']]) {
    assert.equal(decodeFileMetadata({ ...base, tags: [...base.tags, ...tags] }).download, expected)
  }
  for (const download of ['0', '1']) {
    const event = createFileMetadata({ url: 'https://example.com/a', mime: 'audio/mpeg', download })
    assert.deepEqual(event.tags.find(tag => tag[0] === 'download'), ['download', download])
    assert.equal(decodeFileMetadata(event).download, download)
  }
  for (const tags of [[['download', '']], [['download', '2']], [['download', 1]], [['download', 'true']], [['download', '1', '0']], [['download'], ['download', '0']]]) {
    assert.throws(() => decodeFileMetadata({ ...base, tags: [...base.tags, ...tags] }), { code: 'INVALID_FILE_METADATA_DOWNLOAD' })
  }
  assert.throws(() => createFileMetadata({ url: 'https://example.com/a', mime: 'image/png', download: true }), { code: 'INVALID_FILE_METADATA_DOWNLOAD' })
})
