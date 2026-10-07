import { normalizeRelayUrl } from '../../url/index.js'
import { ValidationError } from '../../error/index.js'
import { publishWithEarlyFallback } from './fallback-race.js'
import { isReplaceableRelayFailure } from '../../relay/helpers/failure.js'

const EXCLUSION_MS = 5 * 60 * 1000
const normalized = relay => { try { return normalizeRelayUrl(relay) } catch { return '' } }

export { isReplaceableRelayFailure }

export function normalizeFallbackRelays (value = []) {
  if (!Array.isArray(value)) throw new ValidationError('INVALID_FALLBACK_RELAYS')
  return [...new Set(Array.from(value, normalizeRelayUrl))]
}

export function normalizeFallbackDelay (value = null) {
  if (value !== null && (!Number.isSafeInteger(value) || value < 0 || value > 2147483647)) throw new ValidationError('INVALID_FALLBACK_DELAY')
  return value
}

function routeEntries (routes) {
  return routes instanceof Map ? [...routes] : Object.entries(routes || {})
}

function routeGroups (routes) {
  const groups = new Map()
  for (const [relay, receivers] of routes) {
    const key = [...receivers].sort().join(',')
    if (!groups.has(key)) groups.set(key, { receivers, relays: [] })
    groups.get(key).relays.push(relay)
  }
  return [...groups.values()]
}

export function createSendRelayRouting ({ peer, peers = [peer], relaysByPubkey, primaryRelays, primaryRelayToReceivers, fallbackRelays = [], fallbackDelayMs = null, exclusions, pickRelays, recoveryRelays, publish, publishNymEvent, sendEvent, isOnline, isCurrent, signal, pauseSignal, now = Date.now, setTimer = setTimeout, clearTimer = clearTimeout }) {
  // Keep discovery's defaults when NIP-65 is unavailable. Explicit maps/lists
  // retain their fanout; configured fallbacks are a separate last-resort stage.
  fallbackDelayMs = normalizeFallbackDelay(fallbackDelayMs)
  const transportSignal = signal && pauseSignal ? AbortSignal.any([signal, pauseSignal]) : signal || pauseSignal
  const explicit = Boolean(primaryRelays || primaryRelayToReceivers)
  const primaryRoutes = primaryRelayToReceivers || (primaryRelays
    ? new Map(primaryRelays.map(relay => [relay, peers]))
    : pickRelays(peers, relaysByPubkey, { relayType: 'read', maxPerPubkey: Infinity }))
  const primaryByPeer = Object.fromEntries(peers.map(pubkey => [pubkey, { read: [] }]))
  for (const [relay, receivers] of routeEntries(primaryRoutes)) {
    for (const pubkey of Array.isArray(receivers) ? receivers : [receivers]) primaryByPeer[pubkey]?.read.push(normalizeRelayUrl(relay))
  }
  const candidates = [...new Set([...Object.values(primaryByPeer).flatMap(value => value.read), ...fallbackRelays])]
  const candidateSet = new Set(candidates)
  for (const [relay, expires] of exclusions) if (expires <= now() || !candidateSet.has(relay)) exclusions.delete(relay)
  // A fresh attempt can recheck an exhausted list; each outer event still owns
  // a tried set so concurrent sends/fragments cannot create a retry loop.
  if (peers.some(pubkey => {
    const available = [...primaryByPeer[pubkey].read, ...fallbackRelays]
    return available.length && available.every(relay => exclusions.has(relay))
  })) exclusions.clear()
  const select = (receivers, excluded, staged = false) => {
    const excludedRelays = [...excluded]
    const excludeRelaysByPubkey = new Map(receivers.map(pubkey => [pubkey, excludedRelays]))
    const options = { relayType: 'read', excludeRelaysByPubkey, emptyRelaysFallback: [] }
    const routes = pickRelays(receivers, primaryByPeer, { ...options, ...(explicit ? { maxPerPubkey: Infinity } : {}) })
    // For one ciphertext, exhaust the remaining members' primary routes before
    // using a common fallback. Initial wrapping must nevertheless cover everyone.
    if (staged && routes.size) return routes
    const covered = new Set([...routes.values()].flat())
    const missing = receivers.filter(pubkey => !covered.has(pubkey))
    const fallback = pickRelays(missing, Object.fromEntries(missing.map(pubkey => [pubkey, { read: fallbackRelays }])), options)
    for (const [relay, pubkeys] of fallback) routes.set(relay, [...new Set([...(routes.get(relay) || []), ...pubkeys])])
    return routes
  }
  const relayToReceivers = primaryRelayToReceivers || select(peers, exclusions.keys())
  const current = () => !transportSignal?.aborted && isCurrent()
  const rejected = report => (report?.errors || []).filter(item => candidateSet.has(normalized(item.relay)) && isReplaceableRelayFailure(item.reason))
  const remember = errors => { for (const item of errors) if (candidateSet.has(normalized(item.relay))) exclusions.set(normalized(item.relay), now() + EXCLUSION_MS) }
  const online = async () => {
    if (!current()) return false
    try { return await isOnline({ signal: transportSignal }) && current() } catch { return false }
  }

  async function send (event, initialRelays, context = {}) {
    // This context describes the already encrypted recipient subset. It stays
    // local: no recipient tags or routing metadata are added to the outer event.
    const receivers = context.receiverPubkeys || peers
    const pending = new Set(receivers)
    const mirrors = initialRelays.filter(relay => !candidateSet.has(normalized(relay)))
    const initialPrimary = context.primaryRelays || (primaryRelays || peers.length === 1 ? routeEntries(relayToReceivers).map(([relay]) => relay) : [])
    const first = [...new Set(initialPrimary.map(normalizeRelayUrl))].filter(relay => !exclusions.has(relay))
    if (!explicit && fallbackDelayMs !== null && fallbackRelays.length) {
      const tried = new Set()
      const pick = (pending, fallback) => {
        const excluded = [...new Set([...exclusions.keys(), ...tried])]
        const routes = pickRelays([...pending], fallback
          ? Object.fromEntries([...pending].map(pubkey => [pubkey, { read: fallbackRelays }]))
          : primaryByPeer, { relayType: 'read', maxPerPubkey: 2, excludeRelaysByPubkey: new Map([...pending].map(pubkey => [pubkey, excluded])), emptyRelaysFallback: [] })
        return routeGroups(routes)[0]
      }
      const result = await publishWithEarlyFallback({
        event, receivers, first: first.length ? { receivers, relays: first } : null, mirrors, tried,
        nextPrimary: pending => pick(pending, false), nextFallback: pending => pick(pending, true),
        sendEvent, isOnline, isCurrent, signal, pauseSignal, remember,
        delay: fallbackDelayMs, now, setTimer, clearTimer
      })
      if (!result.success && current()) exclusions.clear()
      return result
    }
    let batch = first.length ? { receivers, relays: first } : null
    const tried = new Set()
    const errors = []
    const summaries = []
    const next = () => routeGroups(select([...pending], new Set([...exclusions.keys(), ...tried]), true))[0]
    const finish = result => ({
      ...result,
      success: summaries.length > 0 && pending.size === 0,
      total: tried.size,
      promise: Promise.all(summaries).then(reports => ({
        total: tried.size,
        success: summaries.length > 0 && pending.size === 0,
        fulfilled: reports.reduce((total, report) => total + (report.fulfilled || 0), 0),
        succeededRelays: [...new Set(reports.flatMap(report => report.succeededRelays || []))],
        errors: reports.flatMap(report => report.errors || [])
      }))
    })
    // A lifecycle change (signer/pause) during a batch is not a user abort:
    // every failure summary stays retryable so the session can re-attempt it.
    const finished = result => current()
      ? finish(result)
      : { ...finish(result), retryWhenAvailable: !signal?.aborted, retryWhenOnline: !signal?.aborted }
    let result = { success: false }
    while ((batch ||= next())) {
      signal?.throwIfAborted()
      const relays = [...new Set([...batch.relays, ...(summaries.length ? [] : mirrors)])]
      for (const relay of relays) tried.add(normalized(relay))
      // Every relay in this batch serves the same pending recipient subset.
      // One ACK covers that subset, never unrelated members of the ciphertext.
      result = await sendEvent(event, relays, { signal: transportSignal })
      const settled = Promise.resolve(result.promise)
      summaries.push(settled)
      if (result.success) {
        for (const pubkey of batch.receivers) pending.delete(pubkey)
        // Keep the first ACK immediate; learn about redundant failures later.
        settled.then(async report => {
          const failed = rejected(report)
          if (failed.length && await online()) remember(failed)
        }).catch(() => {})
        if (!pending.size) return finish(result)
      } else {
        const report = await settled
        const failed = rejected(report)
        errors.push(...(report?.errors || []))
        if (!failed.length || errors.some(item => !isReplaceableRelayFailure(item.reason))) return finished(result)
        if (!next()) { exclusions.clear(); return finished(result) }
        if (!await online()) return { ...finish(result), retryWhenAvailable: !signal?.aborted, retryWhenOnline: !signal?.aborted }
        remember(failed)
      }
      if (!current()) return finished(result)
      batch = null
    }
    return finished(result)
  }

  return {
    relayToReceivers,
    // A recovery mirror must not reintroduce refused relays or promote a fallback.
    recoveryRelays: recoveryRelays.filter(relay => !candidateSet.has(normalized(relay)) || !exclusions.has(normalized(relay))),
    _publish: options => (options.nymSigner ? publishNymEvent : publish)({ ...options, _publish: send })
  }
}
