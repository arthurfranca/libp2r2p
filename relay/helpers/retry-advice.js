// Nostr params use relative milliseconds. Advice never supplies an error
// category, provenance or a routing/retry policy.
export function parseRelayRetryAdvice (reason, params, { now = Date.now() } = {}) {
  if (typeof reason !== 'string' || !reason.startsWith('rate-limited:') ||
      typeof params !== 'string' || params.match(/^[1-9]\d*$/)?.[0] !== params || !Number.isFinite(now)) return null

  const value = Number(params)
  if (!Number.isSafeInteger(value)) return null
  const retryAfterMs = Math.min(value, 300000)
  return { retryAfterMs, retryAt: now + retryAfterMs }
}
