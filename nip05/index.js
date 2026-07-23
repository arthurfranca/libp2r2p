import { normalizeRelayUrl } from '../url/index.js'

const PUBKEY = /^[0-9a-f]{64}$/
const LOCAL_PART = /^[a-z0-9._-]+$/

export async function queryProfile (identifier, {
  fetch: fetchImpl = globalThis.fetch,
  signal,
  timeoutMs = 5000
} = {}) {
  if (typeof identifier !== 'string' || typeof fetchImpl !== 'function') return null
  const normalized = identifier.trim().toLowerCase()
  const separator = normalized.lastIndexOf('@')
  if (separator <= 0 || separator === normalized.length - 1) return null
  const name = normalized.slice(0, separator)
  const domain = normalized.slice(separator + 1)
  if (!LOCAL_PART.test(name) || /[/?#@]/.test(domain)) return null

  let url
  try {
    url = new URL(`https://${domain}/.well-known/nostr.json`)
    if (url.hostname !== domain.split(':')[0] || url.username || url.password) return null
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
