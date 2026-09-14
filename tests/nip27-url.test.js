import { test } from 'node:test'
import assert from 'node:assert/strict'
import { npubEncode } from '../nip19/index.js'
import { extractMedia } from '../nip27/index.js'

const npub = npubEncode('ab'.repeat(32))
const appUrls = [
  { value: 'https://44billion.net/+3swFhu23QNl8er5yOtc8bf9ueHCdwF8CzoDUiSSwwKIWoS8Ki5TwMyeA3Js1' },
  { value: 'https://44billion.net/+apps?by=fiatjaf.com' },
  { value: 'https://44billion.net/+++testing@npub1...?by=fiatjaf.com' },
  { value: `https://44billion.net/+++testing@${npub}?by=fiatjaf.com` },
  { value: 'https://example.com/+hallway@fiatjaf.com', ext: '.com' },
  { value: 'https://example.com/++myapp@bob@example.com/route+one?by=fiatjaf.com#section+two' }
]

test('extractMedia recognizes complete launcher URLs with plus signs in their paths', () => {
  for (const url of appUrls) {
    for (const bareNip05 of [false, true]) {
      assert.deepEqual(extractMedia(url.value, { bareNip05 }), [
        { key: 'url', url }
      ], url.value)
    }
  }
})

test('extractMedia preserves prose punctuation around URLs containing plus signs', () => {
  for (const url of appUrls) {
    for (const [before, after] of [['(', ').'], ['before\n', ', after'], ['"', '"'], ['', '...']]) {
      assert.deepEqual(extractMedia(before + url.value + after), [
        ...(before ? [{ key: 'text', text: { value: before } }] : []),
        { key: 'url', url },
        { key: 'text', text: { value: after } }
      ])
    }
  }
})

test('extractMedia preserves media metadata and MIME inference for paths containing plus signs', () => {
  const value = 'https://example.com/photos+shared/image+one.png#dim=640x480'
  const calls = []
  assert.deepEqual(extractMedia(value, {
    getMimeType: input => {
      calls.push(input)
      return 'image/png'
    }
  }), [{ key: 'url', url: { value, ext: '.png', dim: '640x480', width: '640', height: '480', m: 'image/png' } }])
  assert.deepEqual(calls, [{ url: value, ext: '.png' }])
})

test('extractMedia also recognizes scheme-less URLs and encoded plus signs', () => {
  for (const value of ['44billion.net/+apps?by=fiatjaf.com', '44billion.net/%2Bapps?by=fiatjaf.com']) {
    assert.deepEqual(extractMedia(value), [
      { key: 'url', url: { value: 'https://' + value } }
    ])
  }
})
