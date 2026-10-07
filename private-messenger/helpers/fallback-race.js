import { isReplaceableRelayFailure } from '../../relay/helpers/failure.js'

// Each invocation owns one already signed outer event and its encrypted subset.
// Lanes share coverage and the tried set; only started reports outlive the race.
export function publishWithEarlyFallback ({ event, receivers, first, mirrors, tried, nextPrimary, nextFallback, sendEvent, isOnline, isCurrent, signal, pauseSignal, remember, delay, now, setTimer, clearTimer }) {
  const pending = new Set(receivers)
  const summaries = []
  const transportSignal = signal && pauseSignal ? AbortSignal.any([signal, pauseSignal]) : signal || pauseSignal
  const outcome = Promise.withResolvers()
  let primaryRunning = true
  let fallbackRunning = false
  let fallbackScheduled = true
  let done = false
  let blocked = false
  let offline = false
  let timer = null
  let onlineWork
  const current = () => !transportSignal?.aborted && isCurrent()
  const summary = success => Promise.all(summaries).then(reports => ({
    success, total: tried.size,
    fulfilled: reports.reduce((total, report) => total + (report.fulfilled || 0), 0),
    succeededRelays: [...new Set(reports.flatMap(report => report.succeededRelays || []))],
    errors: reports.flatMap(report => report.errors || [])
  }))
  const finish = success => {
    if (done) return
    done = true
    clearTimer(timer); timer = null
    transportSignal?.removeEventListener('abort', interrupted)
    const interruptedWork = !success && (offline || !current())
    outcome.resolve({
      success, total: tried.size, promise: summary(success),
      ...(interruptedWork && !signal?.aborted ? { retryWhenAvailable: true, ...(offline ? { retryWhenOnline: true } : {}) } : {})
    })
  }
  const check = () => {
    if (done) return
    if (!pending.size) finish(true)
    else if (!primaryRunning && !fallbackRunning && !fallbackScheduled) finish(false)
  }
  const interrupted = () => finish(false)
  const online = async () => {
    if (done || !current()) return false
    try {
      if (!onlineWork) {
        const work = Promise.resolve().then(() => isOnline({ signal: transportSignal })).finally(() => { if (onlineWork === work) onlineWork = null })
        onlineWork = work
      }
      const connected = await onlineWork
      if (done || !current()) return false
      if (!connected) offline = true
      return connected
    } catch { if (!done && current()) offline = true; return false }
  }
  const launch = async (batch, extra = []) => {
    const relays = [...new Set([...batch.relays, ...extra])].filter(relay => !tried.has(relay))
    if (!relays.length) return
    for (const relay of relays) tried.add(relay)
    const slot = Promise.withResolvers()
    summaries.push(slot.promise)
    let result
    try { result = await sendEvent(event, relays, { signal: transportSignal }) } catch (reason) {
      result = { success: false, promise: Promise.resolve({ fulfilled: 0, errors: [{ reason }] }) }
    }
    const settled = Promise.resolve(result.promise).catch(reason => ({ fulfilled: 0, errors: [{ reason }] }))
    slot.resolve(settled)
    settled.then(async report => {
      const failed = (report.errors || []).filter(item => isReplaceableRelayFailure(item.reason))
      // Late redundant failures can inform later sends after this race wins.
      if (failed.length && (result.success || (done && !pending.size)) && !transportSignal?.aborted && isCurrent()) {
        try { if (await isOnline({ signal: transportSignal }) && isCurrent()) remember(failed) } catch {}
      }
    }).catch(() => {})
    if (result.success) {
      if (!done) for (const receiver of batch.receivers) pending.delete(receiver)
      check()
      return
    }
    const report = await settled
    if (done) return
    const errors = report.errors || []
    if (!errors.length || errors.some(item => !isReplaceableRelayFailure(item.reason))) blocked = true
    else if (await online()) remember(errors)
  }
  async function fallback () {
    if (done || fallbackRunning || !fallbackScheduled) return
    clearTimer(timer)
    fallbackScheduled = false
    fallbackRunning = true
    try {
      if (blocked || offline || !pending.size || !current()) return
      let batch = nextFallback(pending)
      if (!batch || !await online()) return
      while (batch) {
        if (done || blocked || offline || !current()) break
        await launch(batch)
        batch = nextFallback(pending)
      }
    } finally { fallbackRunning = false; check() }
  }
  async function primary () {
    try {
      let batch = first || nextPrimary(pending)
      let opening = true
      while (batch) {
        if (done || blocked || offline || !current()) break
        await launch(batch, opening ? mirrors : [])
        opening = false
        batch = nextPrimary(pending)
      }
    } finally {
      primaryRunning = false
      // Exhaustion before the deadline starts the fallback immediately.
      if (!done && fallbackScheduled) {
        if (!blocked && !offline && current()) await fallback()
        else { clearTimer(timer); fallbackScheduled = false }
      }
      check()
    }
  }
  if (transportSignal?.aborted || !isCurrent()) { finish(false); return outcome.promise }
  transportSignal?.addEventListener('abort', interrupted, { once: true })
  const deadline = now() + delay
  const failed = reason => {
    if (done) return
    done = true; clearTimer(timer); timer = null
    transportSignal?.removeEventListener('abort', interrupted)
    outcome.reject(reason)
  }
  timer = setTimer(() => { fallback().catch(failed) }, Math.max(0, deadline - now()))
  timer?.unref?.()
  primary().catch(failed)
  return outcome.promise
}
