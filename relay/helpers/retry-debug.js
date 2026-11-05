// Intermediate automatic retries are intentionally visible only as structured
// debug data. Never include relay messages, filters, event IDs or payloads.
export function debugRateLimitRetry ({
  relay,
  operation,
  retryAttempt,
  maxRetries,
  retryAt
}) {
  try {
    console.debug('relay rate-limit retry', {
      relay,
      operation,
      retryAttempt,
      maxRetries,
      retryAt,
      delayMs: Math.max(0, retryAt - Date.now())
    })
  } catch {}
}
