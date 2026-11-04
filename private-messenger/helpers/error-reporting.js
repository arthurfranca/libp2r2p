const DEFAULT_CANCELLATION_WINDOW_MS = 30_000
const MAX_CANCELLATION_KEYS = 32
const SAFE_TOKEN = /^[A-Za-z0-9][A-Za-z0-9:._-]{0,159}$/

function safeToken (value) {
  return typeof value === 'string' && SAFE_TOKEN.test(value) ? value : undefined
}

function cancellationKey (error) {
  return [
    safeToken(error?.name) || 'AbortError',
    Number.isSafeInteger(error?.code) ? String(error.code) : safeToken(error?.code) || '',
    safeToken(error?.operation) || '',
    safeToken(error?.phase) || ''
  ].join(':')
}

// AbortError is the normal cancellation primitive used while pauses tear down
// subscriptions and recovery work. Keep the first occurrence visible, suppress
// repeats for a bounded window, then report how many were omitted.
export function createPrivateMessengerErrorReporter ({
  report,
  now = Date.now,
  intervalMs = DEFAULT_CANCELLATION_WINDOW_MS,
  status = () => ({ paused: false, pauseReasons: [] })
} = {}) {
  if (typeof report !== 'function') throw new TypeError('report must be a function')
  const reported = new Map()

  return error => {
    if (error?.name !== 'AbortError') return report(error)

    const current = now()
    const key = cancellationKey(error)
    const previous = reported.get(key)
    if (previous && current - previous.at < intervalMs) {
      previous.suppressed++
      return false
    }

    const runtime = status() || {}
    const pauseReasons = Array.isArray(runtime.pauseReasons)
      ? runtime.pauseReasons.filter(reason => safeToken(reason)).slice(0, 8)
      : []
    const diagnostic = Object.assign(new Error('PRIVATE_MESSENGER_CANCELLED'), {
      name: 'AbortError',
      code: 'PRIVATE_MESSENGER_CANCELLED',
      operation: safeToken(error.operation) || 'private-messenger',
      phase: safeToken(error.phase) || (runtime.paused === true ? 'paused' : 'operation'),
      expectedCancellation: runtime.paused === true,
      cause: error,
      ...(pauseReasons.length ? { pauseReasons } : {}),
      ...(previous?.suppressed ? { suppressedCount: previous.suppressed } : {})
    })

    reported.delete(key)
    reported.set(key, { at: current, suppressed: 0 })
    if (reported.size > MAX_CANCELLATION_KEYS) reported.delete(reported.keys().next().value)
    return report(diagnostic)
  }
}
