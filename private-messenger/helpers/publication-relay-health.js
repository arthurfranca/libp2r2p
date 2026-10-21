import { normalizeRelayUrl } from '../../url/index.js'

const PERMANENT_PREFIXES = new Set(['banned', 'blocked', 'restricted', 'auth-required', 'pow'])
const TRANSIENT_CATEGORIES = new Set(['connection', 'transport', 'timeout'])
const RELAY_LIMIT = 256
const INITIAL_COOLDOWN_MS = 5 * 60 * 1000
const MAX_COOLDOWN_MS = 30 * 60 * 1000
const TIMEOUT_THRESHOLD = 3

const prefix = error => {
  for (const value of [error?.message, error?.code]) {
    const match = typeof value === 'string' && /^([a-z-]+):/.exec(value)
    if (match) return match[1]
  }
  return ''
}
const relayUrl = value => {
  try { return normalizeRelayUrl(value) } catch { return '' }
}
const cooldown = level => Math.min(MAX_COOLDOWN_MS, INITIAL_COOLDOWN_MS * 2 ** Math.max(0, level - 1))

// Publication-only, runtime relay health shared by one PrivateMessenger.
// Permanent refusals open immediately; transport failures open after three
// consecutive attempts. Expired entries become one half-open probe.
export function createPublicationRelayHealth ({ now = Date.now, limit = RELAY_LIMIT } = {}) {
  const relays = new Map()

  function save (relay, state) {
    relays.delete(relay)
    relays.set(relay, state)
    while (relays.size > limit) relays.delete(relays.keys().next().value)
  }

  function isExcluded (relay, at = now()) {
    relay = relayUrl(relay)
    const state = relay ? relays.get(relay) : undefined
    return Boolean(state?.until && state.until > at)
  }

  function retryAt (candidates, at = now()) {
    let earliest = 0
    for (const candidate of candidates || []) {
      const relay = relayUrl(candidate)
      const until = relay ? relays.get(relay)?.until : 0
      if (until > at && (!earliest || until < earliest)) earliest = until
    }
    return earliest
  }

  function recordFailure (relay, reason, at = now()) {
    relay = relayUrl(relay)
    if (!relay) return 0
    const previous = relays.get(relay) || { failures: 0, level: 0, until: 0 }
    const halfOpen = previous.until > 0 && previous.until <= at
    const permanent = PERMANENT_PREFIXES.has(prefix(reason))
    const transient = TRANSIENT_CATEGORIES.has(reason?.category)
    const retryAfter = Number.isSafeInteger(reason?.retryAt)
      ? reason.retryAt
      : Number.isSafeInteger(reason?.retryAfterMs)
        ? at + Math.max(0, reason.retryAfterMs)
        : 0
    let next = previous
    if (permanent || retryAfter > at) {
      const level = halfOpen ? Math.min(previous.level + 1, 5) : Math.max(1, previous.level)
      next = { failures: 0, level, until: Math.min(at + cooldown(level), retryAfter > at ? retryAfter : Infinity) }
    } else if (transient || prefix(reason) === 'error') {
      const failures = previous.failures + 1
      if (halfOpen) {
        const level = Math.min(previous.level + 1, 5)
        next = { failures: 0, level, until: at + cooldown(level) }
      } else if (failures >= TIMEOUT_THRESHOLD) {
        next = { failures: 0, level: Math.max(1, previous.level), until: at + cooldown(Math.max(1, previous.level)) }
      } else next = { failures, level: previous.level, until: 0 }
    }
    save(relay, next)
    return next.until > at ? next.until : 0
  }

  function recordSuccess (relay) {
    relay = relayUrl(relay)
    if (relay) relays.delete(relay)
  }

  function clear () {
    relays.clear()
  }

  return { isExcluded, retryAt, recordFailure, recordSuccess, clear }
}

export {
  INITIAL_COOLDOWN_MS as PUBLICATION_RELAY_INITIAL_COOLDOWN_MS,
  MAX_COOLDOWN_MS as PUBLICATION_RELAY_MAX_COOLDOWN_MS,
  RELAY_LIMIT as PUBLICATION_RELAY_HEALTH_LIMIT,
  TIMEOUT_THRESHOLD as PUBLICATION_TIMEOUT_THRESHOLD
}
