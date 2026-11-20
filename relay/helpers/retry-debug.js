// Intermediate automatic retries are intentionally visible only as structured
// debug data. Never include relay messages, filters, event IDs or payloads.
export function debugRateLimitRetry ({
  relay,
  operation,
  retryAttempt,
  maxRetries,
  retryAt,
  effectiveWaitMs,
  maxWaitMs
}) {
  try {
    console.debug('relay rate-limit retry', {
      relay,
      operation,
      retryAttempt,
      maxRetries,
      retryAt,
      delayMs: Math.max(0, retryAt - Date.now()),
      effectiveWaitMs,
      maxWaitMs
    })
  } catch {}
}

function debugToken (value, fallback) {
  return typeof value === 'string' && /^[a-zA-Z0-9_.:-]{1,80}$/.test(value)
    ? value
    : fallback
}

// Connection failures are summarized with bounded protocol fields only.
// Native error messages and relay close reasons may contain arbitrary prose.
export function debugRelayConnectionFailure ({
  relay,
  event = 'connect-failed',
  code,
  category,
  closeCode,
  wasClean,
  elapsedMs,
  timeoutMs,
  online
}) {
  try {
    console.debug('relay connection failure', {
      relay,
      event: debugToken(event, 'unknown'),
      code: debugToken(code, 'CONNECTION_FAILED'),
      category: debugToken(category, 'unknown'),
      ...(Number.isSafeInteger(closeCode) ? { closeCode } : {}),
      ...(typeof wasClean === 'boolean' ? { wasClean } : {}),
      elapsedMs: Number.isFinite(elapsedMs) ? Math.max(0, Math.round(elapsedMs)) : null,
      ...(Number.isFinite(timeoutMs) ? { timeoutMs } : {}),
      ...(typeof online === 'boolean' ? { online } : {})
    })
  } catch {}
}
