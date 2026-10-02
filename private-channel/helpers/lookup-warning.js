export const LOOKUP_WARNING_INTERVAL_MS = 30_000

// Best-effort discovery can fail on every message while a relay or the
// network is unavailable. Keep the first failure visible, then debounce
// repeats and report how many were suppressed on the next log.
export function createLookupWarningLogger ({
  intervalMs = LOOKUP_WARNING_INTERVAL_MS,
  label = 'private-messenger content-key lookup failed',
  warn = (...args) => console.warn(...args),
  now = Date.now
} = {}) {
  let warnedAt = null
  let suppressed = 0
  return (error, context = '') => {
    const current = now()
    if (warnedAt !== null && current - warnedAt < intervalMs) {
      suppressed++
      return false
    }
    const count = suppressed
    warnedAt = current
    suppressed = 0
    warn(
      `${label}${context ? ` (${context})` : ''}${count ? ` [${count} repeated failures suppressed]` : ''}`,
      error?.message ?? error
    )
    return true
  }
}
