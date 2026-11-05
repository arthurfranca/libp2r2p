export const DEFAULT_RATE_LIMIT_CAP_MS = 300_000

// Nostr params use relative milliseconds. Advice never supplies an error
// category, provenance or a routing/retry policy.
export function parseRelayRetryAdvice (reason, params, { now = Date.now(), capMs = DEFAULT_RATE_LIMIT_CAP_MS } = {}) {
  if (typeof reason !== 'string' || !reason.startsWith('rate-limited:') ||
      typeof params !== 'string' || params.match(/^[1-9]\d*$/)?.[0] !== params || !Number.isFinite(now) ||
      !Number.isSafeInteger(capMs) || capMs < 1) return null

  const value = Number(params)
  if (!Number.isSafeInteger(value)) return null
  const maxRetryAfterMs = Math.min(capMs, Number.MAX_SAFE_INTEGER - now)
  if (maxRetryAfterMs < 1) return null
  const retryAfterMs = Math.min(value, maxRetryAfterMs)
  return { retryAfterMs, retryAt: now + retryAfterMs }
}
