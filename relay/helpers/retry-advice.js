// Nostr params use relative milliseconds or @Unix milliseconds. Advice never
// supplies an error category, provenance or a routing/retry policy.
export function parseRelayRetryAdvice (reason, params, { now = Date.now() } = {}) {
  if (typeof reason !== 'string' || !reason.startsWith('rate-limited:') ||
      typeof params !== 'string' || params.match(/^@?[1-9]\d*$/)?.[0] !== params || !Number.isFinite(now)) return null

  const absolute = params.startsWith('@')
  const value = Number(absolute ? params.slice(1) : params)
  if (!Number.isSafeInteger(value)) return null
  if (absolute) return { retryAt: Math.min(value, now + 300000) }
  const retryAfterMs = Math.min(value, 300000)
  return { retryAfterMs, retryAt: now + retryAfterMs }
}
