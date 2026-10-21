const TRANSPORT_CATEGORIES = new Set(['connection', 'transport', 'timeout'])
const RETRY_PREFIXES = new Set(['rate-limited', 'error'])
const REPLACEMENT_PREFIXES = new Set(['banned', 'blocked', 'restricted', 'auth-required', 'pow', ...RETRY_PREFIXES])

// Retry and replacement are different policies: a refusal can justify another
// relay without making the same operation retryable on the refusing relay.
function matches (error, prefixes) {
  if (error?.name === 'Nip42AuthenticationError') return false
  if (TRANSPORT_CATEGORIES.has(error?.category)) return true
  if (error?.category && error.category !== 'relay') return false
  const name = /^([a-z-]+):/.exec(error?.message || '')?.[1] || /^([a-z-]+):/.exec(error?.code || '')?.[1]
  return prefixes.has(name)
}

export const isRetryableRelayFailure = error => matches(error, RETRY_PREFIXES)
export const isReplaceableRelayFailure = error => matches(error, REPLACEMENT_PREFIXES)
