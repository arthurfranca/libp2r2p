// Retry operational interruptions, never dense-page limits or permanent refusals.
export function recoveryRetryDelay (errors, delay) {
  let retry = false
  let blocked = false
  let retryAt = 0
  const seen = new Set()
  function visit (error) {
    if (!error || seen.has(error)) return
    seen.add(error)
    if (error.code === 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT' || error.name === 'ValidationError' ||
        /^(auth-required:|restricted:|blocked:|invalid:|pow:)/.test(error.message || '')) blocked = true
    if (['transport', 'timeout'].includes(error.category) ||
        ['QuotaExceededError', 'InvalidStateError', 'UnknownError'].includes(error.name) ||
        /^(rate-limited:|error:)/.test(error.message || '') ||
        ['RELAY_LIVE_BUFFER_FULL', 'RELAY_LIVE_NOT_READY'].includes(error.code)) retry = true
    if (Number.isFinite(error.retryAt)) retryAt = Math.max(retryAt, Math.min(Date.now() + 300000, error.retryAt))
    for (const child of error.errors || []) visit(child)
    for (const entry of error.relays || []) {
      if (['timeout', 'cutoff', 'closed'].includes(entry.status)) retry = true
      visit(entry.error)
    }
    visit(error.cause)
  }
  errors.forEach(visit)
  return retry && !blocked ? Math.max(delay, retryAt - Date.now()) : null
}
