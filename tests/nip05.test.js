import assert from 'node:assert/strict'
import { test } from 'node:test'

import { queryProfile } from '../nip05/index.js'

test('NIP-05 resolves a canonical profile and normalizes relay URLs', async () => {
  let requested
  const pubkey = 'ab'.repeat(32)
  const result = await queryProfile('Alice@Example.com', {
    fetch: async (url, options) => {
      requested = { url: String(url), options }
      return new Response(JSON.stringify({
        names: { alice: pubkey },
        relays: { [pubkey]: ['relay.example.com/', 'wss://relay.example.com'] }
      }), { status: 200, headers: { 'content-type': 'application/json' } })
    }
  })
  assert.equal(requested.url, 'https://example.com/.well-known/nostr.json?name=alice')
  assert.equal(requested.options.redirect, 'error')
  assert.deepEqual(result, { pubkey, relays: ['wss://relay.example.com'] })
})

test('NIP-05 returns null for malformed or unverified responses', async () => {
  assert.equal(await queryProfile('wrong', { fetch: async () => { throw new Error('unused') } }), null)
  assert.equal(await queryProfile('a@example.com', {
    fetch: async () => new Response(JSON.stringify({ names: { a: 'AB'.repeat(32) } }), { status: 200 })
  }), null)
  assert.equal(await queryProfile('a@example.com', {
    fetch: async () => new Response('', { status: 404 })
  }), null)
})

test('NIP-05 propagates caller cancellation', async () => {
  const controller = new AbortController()
  const promise = queryProfile('a@example.com', {
    signal: controller.signal,
    fetch: (_url, { signal }) => new Promise((resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true })
    })
  })
  controller.abort(new DOMException('cancelled', 'AbortError'))
  await assert.rejects(promise, { name: 'AbortError' })
})

test('queryProfile accepts root and compact custom NIP-05 forms', async () => {
  const pubkey = 'ab'.repeat(32)
  let requested
  const fetchJson = names => async url => {
    requested = String(url)
    return new Response(JSON.stringify({ names, relays: {} }), { status: 200 })
  }

  await queryProfile('bob.fiatjaf.com', { fetch: fetchJson({ bob: pubkey }) })
  assert.equal(requested, 'https://fiatjaf.com/.well-known/nostr.json?name=bob')

  await queryProfile('fiatjaf.com', { fetch: fetchJson({ _: pubkey }) })
  assert.equal(requested, 'https://fiatjaf.com/.well-known/nostr.json?name=_')

  await queryProfile('_@fiatjaf.com.br', { fetch: fetchJson({ _: pubkey }) })
  assert.equal(requested, 'https://fiatjaf.com.br/.well-known/nostr.json?name=_')
})
