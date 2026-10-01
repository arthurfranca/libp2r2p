import { normalizeRelayUrl } from '../../url/index.js'

const REJECTION_PREFIXES = new Set(['blocked', 'restricted', 'auth-required', 'pow', 'rate-limited', 'error'])
const TRANSPORT_CATEGORIES = new Set(['connection', 'transport', 'timeout'])
const EXCLUSION_MS = 5 * 60 * 1000
const normalized = relay => { try { return normalizeRelayUrl(relay) } catch { return '' } }

// Only machine-readable relay policy/server failures qualify. Invalid events,
// local signer denials and unknown prose must not rotate relays.
export function isReplaceableRelayFailure (error) {
  if (error?.name === 'Nip42AuthenticationError') return false
  if (TRANSPORT_CATEGORIES.has(error?.category)) return true
  if (error?.category && error.category !== 'relay') return false
  return REJECTION_PREFIXES.has(/^([a-z-]+):/.exec(error?.message || '')?.[1])
}

export function createSendRelayRouting ({ peer, relaysByPubkey, exclusions, pickRelays, recoveryRelays, publish, publishNymEvent, sendEvent, isOnline, isCurrent, signal, now = Date.now }) {
  // One finite snapshot, including the existing default pair if NIP-65 is
  // unavailable. Never invent additional fallback relays.
  const candidates = [...pickRelays([peer], relaysByPubkey, { relayType: 'read', maxPerPubkey: Infinity }).keys()]
  const candidateSet = new Set(candidates)
  for (const [relay, expires] of exclusions) if (expires <= now() || !candidateSet.has(relay)) exclusions.delete(relay)
  // New attempts may recheck an exhausted list. Each in-flight outer event has
  // its own tried set, preventing loops during concurrent sends.
  if (candidates.length && candidates.every(relay => exclusions.has(relay))) exclusions.clear()
  const select = excluded => pickRelays([peer], relaysByPubkey, { relayType: 'read', excludeRelaysByPubkey: new Map([[peer, [...excluded]]]) })
  const relayToReceivers = select(exclusions.keys())
  const current = () => !signal?.aborted && isCurrent()
  const rejected = report => (report?.errors || []).filter(item => candidateSet.has(normalized(item.relay)) && isReplaceableRelayFailure(item.reason))
  const remember = errors => { for (const item of errors) exclusions.set(normalized(item.relay), now() + EXCLUSION_MS) }
  const online = async () => {
    if (!current()) return false
    try { return await isOnline({ signal }) && current() } catch { return false }
  }

  async function send (event, initialRelays) {
    const mirrors = initialRelays.filter(relay => !candidateSet.has(normalized(relay)))
    let relays = [...new Set([...select(exclusions.keys()).keys(), ...mirrors])]
    const tried = new Set()
    const errors = []
    const summaries = []
    const finish = result => {
      if (summaries.length === 1) return result
      return {
        ...result,
        total: tried.size,
        promise: Promise.all(summaries).then(reports => ({
          total: tried.size,
          success: reports.some(report => report.success),
          fulfilled: reports.reduce((total, report) => total + (report.fulfilled || 0), 0),
          succeededRelays: [...new Set(reports.flatMap(report => report.succeededRelays || []))],
          errors: reports.flatMap(report => report.errors || [])
        }))
      }
    }
    while (true) {
      signal?.throwIfAborted()
      for (const relay of relays) tried.add(normalized(relay))
      // Republish the exact signed outer event, preserving encryption,
      // fragments, timestamps and deletion capabilities.
      const result = await sendEvent(event, relays)
      const settled = Promise.resolve(result.promise)
      summaries.push(settled)
      if (result.success) {
        // Keep the first ACK immediate; learn about redundant failures later.
        settled.then(async report => {
          const failed = rejected(report)
          if (failed.length && await online()) remember(failed)
        }).catch(() => {})
        return finish(result)
      }
      const report = await settled
      const failed = rejected(report)
      errors.push(...(report?.errors || []))
      if (!failed.length || errors.some(item => !isReplaceableRelayFailure(item.reason))) return finish(result)
      const next = [...select(new Set([...exclusions.keys(), ...tried])).keys()]
      if (!next.length) { exclusions.clear(); return finish(result) }
      if (!await online()) return { ...finish(result), retryWhenAvailable: !signal?.aborted, retryWhenOnline: current() }
      remember(failed)
      relays = next
    }
  }

  return {
    relayToReceivers,
    // A recovery mirror must not reintroduce the recipient's refused relays.
    recoveryRelays: recoveryRelays.filter(relay => !candidateSet.has(relay) || !exclusions.has(relay)),
    _publish: options => (options.nymSigner ? publishNymEvent : publish)({ ...options, _publish: send })
  }
}
