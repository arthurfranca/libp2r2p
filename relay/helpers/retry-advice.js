// Nostr transport extensions, in seconds. Timing advice is bounded and never
// supplies an error category, provenance or a routing/retry policy.
export function parseRelayRetryAdvice (reason, extra, { now = Date.now() } = {}) {
  if (typeof reason !== 'string' || !reason.startsWith('rate-limited:') ||
      !extra || typeof extra !== 'object' || Array.isArray(extra) || !Number.isFinite(now)) return null

  const after = extra.retry_after
  const retryAfterMs = typeof after === 'number' && Number.isFinite(after) && after > 0
    ? Math.min(after, 300) * 1000
    : undefined
  const at = extra.retry_at
  const absolute = typeof at === 'number' && Number.isFinite(at) && at > 0 && Number.isFinite(at * 1000)
    ? Math.min(at * 1000, now + 300000)
    : undefined
  const retryAt = absolute ?? (retryAfterMs === undefined ? undefined : now + retryAfterMs)
  if (retryAt === undefined) return null
  return { ...(retryAfterMs === undefined ? {} : { retryAfterMs }), retryAt }
}
