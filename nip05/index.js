import { normalizeRelayUrl } from '../url/index.js'
import { decodeNip05Identifier } from './helpers/nip05-identifier.js'

const PUBKEY = /^[0-9a-f]{64}$/

export async function queryProfile (identifier, {
  fetch: fetchImpl = globalThis.fetch,
  signal,
  timeoutMs = 5000
} = {}) {
  if (typeof identifier !== 'string' || typeof fetchImpl !== 'function') return null
  const nip05 = decodeNip05Identifier(identifier)
  if (!nip05) return null
  const name = nip05.local
  const domain = nip05.domain

  let url
  try {
    url = new URL(`https://${domain}/.well-known/nostr.json`)
    if (url.hostname !== domain || url.username || url.password) return null
    url.searchParams.set('name', name)
  } catch {
    return null
  }

  const controller = new AbortController()
  const onAbort = () => controller.abort(signal.reason)
  if (signal?.aborted) throw signal.reason ?? new DOMException('This operation was aborted', 'AbortError')
  signal?.addEventListener('abort', onAbort, { once: true })
  const timeout = Number.isFinite(timeoutMs) && timeoutMs >= 0
    ? setTimeout(() => controller.abort(new DOMException('NIP-05 request timed out', 'TimeoutError')), timeoutMs)
    : null

  try {
    const response = await fetchImpl(url, {
      headers: { Accept: 'application/json' },
      redirect: 'error',
      signal: controller.signal
    })
    if (!response.ok) return null
    const data = await response.json()
    const pubkey = data?.names?.[name]
    if (!PUBKEY.test(pubkey)) return null
    const relays = []
    for (const relay of data?.relays?.[pubkey] ?? []) {
      try {
        const normalizedRelay = normalizeRelayUrl(relay)
        if (!relays.includes(normalizedRelay)) relays.push(normalizedRelay)
      } catch {}
    }
    return { pubkey, relays }
  } catch (error) {
    if (signal?.aborted) throw signal.reason ?? error
    return null
  } finally {
    if (timeout !== null) clearTimeout(timeout)
    signal?.removeEventListener('abort', onAbort)
  }
}
