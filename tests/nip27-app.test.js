import { test } from 'node:test'
import assert from 'node:assert/strict'
import { extractMedia, decodeUserReference } from 'libp2r2p/nip27'
import { ValidationError } from 'libp2r2p/error'
import { appEncode, naddrEncode, noteEncode, npubEncode, nprofileEncode } from 'libp2r2p/nip19'
import { decodeAppUrl, encodeAppUrl } from 'libp2r2p/url'

const pubkey = 'ab'.repeat(32)
const npub = npubEncode(pubkey)
const nprofile = nprofileEncode({ pubkey, relays: ['wss://relay.example.com'] })
const example = '+32Wp7Qdz5XIbjlzHht7GdEP8IDxk6CC90ee8lRxYjx0stjsVD8yzTguF'

function assertApp (value) {
  const decoded = decodeAppUrl(value)
  for (const prefix of ['', 'nostr:']) {
    const original = prefix + value
    assert.deepEqual(extractMedia(original), [{ key: 'app', app: { original, ...decoded } }], original)
  }
}

test('extractMedia recognizes encoded app entities in all channels with optional nostr:', () => {
  assertApp(example)
  for (const channel of ['main', 'next', 'draft']) {
    assertApp(appEncode({ dTag: 'myapp', pubkey, channel, relays: ['wss://relay.example.com'] }))
  }
})

test('extractMedia recognizes named apps with every supported author spelling and channel', () => {
  for (const prefix of ['+', '++', '+++']) {
    for (const user of [
      'fiatjaf.com', '_@fiatjaf.com.br', 'bob@example.com',
      'bob.example.com', 'bob.xyz.abc.example.com', npub, nprofile, pubkey
    ]) assertApp(`${prefix}myapp@${user}`)
  }
  const app = extractMedia('nostr:++myapp@bob@example.com')[0].app
  assert.equal(app.type, 'named')
  assert.equal(app.appName, 'myapp')
  assert.equal(app.channel, 'next')
  assert.deepEqual(app.user, { type: 'nip05', local: 'bob', domain: 'example.com', raw: 'bob.example.com' })
  assert.deepEqual(extractMedia(`+myapp@${nprofile}`)[0].app.user.relays, ['wss://relay.example.com'])
})

test('extractMedia uses 44billion.net as the author of a bare app name', () => {
  for (const channel of ['+', '++', '+++']) {
    for (const scheme of ['', 'nostr:']) {
      for (const name of ['apps', '10', 'i', 'app-store', 'app_name', 'café', 'app%20store', 'my%40app']) {
        const original = scheme + channel + name
        const [item] = extractMedia(original)
        const decoded = decodeAppUrl(channel + name)
        assert.deepEqual(item, { key: 'app', app: { original, ...decoded, user: decodeUserReference('44billion.net') } })
      }
    }
  }
  const bare = extractMedia('+apps')[0].app
  const explicit = extractMedia('+apps@44billion.net')[0].app
  assert.deepEqual({ ...bare, original: explicit.original }, explicit)
})

test('extractMedia accepts a custom default app author in every user-reference format', () => {
  for (const defaultAppAuthor of [
    'fiatjaf.com', '_@fiatjaf.com.br', 'bob@example.com', 'bob.example.com',
    'bob.xyz.abc.example.com', npub, nprofile, pubkey, '@bob@example.com', `nostr:${npub}`
  ]) {
    const options = { defaultAppAuthor }
    const [item] = extractMedia('nostr:++apps', options)
    assert.equal(item.key, 'app')
    assert.equal(item.app.original, 'nostr:++apps')
    assert.equal(item.app.appName, 'apps')
    assert.equal(item.app.channel, 'next')
    assert.deepEqual(item.app.user, decodeUserReference(defaultAppAuthor))
    assert.deepEqual(extractMedia('+apps@44billion.net', options), extractMedia('+apps@44billion.net'))
    assert.deepEqual(extractMedia(example, options), extractMedia(example))
  }
  assert.equal(extractMedia('+apps')[0].app.user.raw, '44billion.net', 'per-call defaults never leak into other calls')
})

test('extractMedia validates the default app author even when content contains no app', () => {
  for (const defaultAppAuthor of [null, '', false, 1, {}, [], 'not a user', '+apps', 'npub1broken', 'nprofile1broken', noteEncode(pubkey)]) {
    for (const content of ['+apps', 'plain text', '']) {
      assert.throws(() => extractMedia(content, { defaultAppAuthor }), error => {
        assert.ok(error instanceof ValidationError)
        assert.equal(error.code, 'INVALID_DEFAULT_APP_AUTHOR')
        assert.ok(error.cause instanceof ValidationError)
        return true
      })
    }
  }
})

test('extractMedia preserves prose around bare apps without consuming punctuation or existing references', () => {
  const items = extractMedia('Try (+apps), nostr:+++app-store. Then +café and @bob@example.com.')
  assert.deepEqual(items.filter(item => item.key === 'app').map(item => item.app.original), ['+apps', 'nostr:+++app-store', '+café'])
  assert.equal(items.find(item => item.key === 'nip05').nip05.value, 'bob.example.com')
  assert.equal(items.filter(item => item.key === 'text').map(item => item.text.value).join(''), 'Try (), . Then  and .')
})

test('extractMedia accepts encoded app names and names containing @', () => {
  for (const appName of ['my@app', 'app+store', 'app store', 'café 日本', "hello!(app), 'test'"]) {
    for (const user of ['bob@example.com', 'fiatjaf.com', npub]) {
      assertApp(encodeAppUrl({ appName, user, channel: 'draft' }))
    }
  }
  assertApp('+my%40app@bob@example.com')
})

test('extractMedia recognizes prefixed site-manifest naddr while retaining ordinary naddr event items', () => {
  for (const kind of [35128, 35129, 35130]) {
    const value = naddrEncode({ pubkey, identifier: 'myapp', kind, relays: [] })
    for (const prefix of ['+', '++', '+++']) assertApp(prefix + value)
    for (const prefix of ['', 'nostr:']) {
      const [item] = extractMedia(prefix + value)
      assert.equal(item.key, 'event')
      assert.equal(item.event.kind, kind)
    }
  }
})

test('extractMedia preserves punctuation, whitespace and order around app references', () => {
  for (const value of [example, '++myapp@bob@example.com', '+myapp@' + nprofile]) {
    for (const [before, after] of [['(', ').'], ['«', '»'], ['"', '"'], ['before\n', ', after'], ['', '...']]) {
      const original = 'nostr:' + value
      assert.deepEqual(extractMedia(before + original + after), [
        ...(before ? [{ key: 'text', text: { value: before } }] : []),
        { key: 'app', app: { original, ...decodeAppUrl(value) } },
        { key: 'text', text: { value: after } }
      ])
    }
  }
})

test('extractMedia keeps app recognition separate from URLs, profiles, NIP-05 and event references', () => {
  const note = noteEncode(pubkey)
  const content = `+hallway@fiatjaf.com https://example.com/app @bob@example.com ${npub} ${note} #apps`
  for (const bareNip05 of [false, true]) {
    const items = extractMedia(content, { bareNip05 }).filter(item => item.key !== 'text')
    assert.deepEqual(items.map(item => item.key), ['app', 'url', 'nip05', 'profile', 'event', 'hashtag'])
    assert.equal(items[1].url.value, 'https://example.com/app')
  }
})

test('extractMedia separates adjacent parenthesized app references', () => {
  const values = ['+apps', '++site', '+hallway@fiatjaf.com', example, 'nostr:++myapp@bob@example.com']
  const items = extractMedia(values.map(value => `(${value})`).join(''))
  assert.deepEqual(items.filter(item => item.key === 'app').map(item => item.app.original), values)
  assert.equal(items.filter(item => item.key === 'text').map(item => item.text.value).join(''), '()'.repeat(values.length))
})

test('extractMedia keeps invalid, ambiguous and concatenated app candidates as text', () => {
  const note = noteEncode(pubkey)
  const nonApp = naddrEncode({ pubkey, identifier: 'article', kind: 30023, relays: [] })
  for (const value of [
    '+', '++++myapp@bob@example.com', '++++apps', '+myapp@invalid',
    '+myapp@', '+myapp@npub1broken', '+myapp@nprofile1broken', '+myapp@bob..example.com',
    '+my%ZZapp@bob@example.com', '+my%2Fapp@bob@example.com', '+my%00app@bob@example.com',
    `+${'a'.repeat(48)}`, `+${nonApp}`, `${note}${example}`, `${npub}${example}`,
    '+myapp@bob@example.com/route', '+myapp@bob@example.com+extra',
    'https://example.com/+hallway@fiatjaf.com', '+my%ZZapp', '+my%2Fapp', '+my%00app'
  ]) {
    for (const prefix of ['', 'nostr:']) {
      const content = `before ${prefix}${value} after`
      const items = extractMedia(content)
      assert.ok(items.every(item => item.key === 'text'), content)
      assert.equal(items.map(item => item.text.value).join(''), content)
    }
  }
})

test('extractMedia does not discard a malformed app token between valid references', () => {
  const items = extractMedia(`+hallway@fiatjaf.com +${'a'.repeat(48)} ${example}`)
  assert.deepEqual(items.filter(item => item.key === 'app').map(item => item.app.original), ['+hallway@fiatjaf.com', example])
  assert.equal(items.filter(item => item.key === 'text').map(item => item.text.value).join(''), ` +${'a'.repeat(48)} `)
})
