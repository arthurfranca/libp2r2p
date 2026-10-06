// Local persistence is not a relay failure. Retry operational storage failures,
// but leave validation/permission failures to their owner or an explicit retry.
export const isRecoverableStorageFailure = error => error != null && typeof error === 'object' && error?.name !== 'ValidationError' && error?.category !== 'validation' &&
  !['SecurityError', 'NotAllowedError', 'TypeError', 'RangeError', 'SyntaxError', 'DataError', 'DataCloneError'].includes(error?.name) &&
  !/DENIED|PERMISSION|REVOKED|READ_ONLY|INVALID|NOT_IN_PERSONA/.test(`${error?.code || ''} ${error?.name || ''} ${error?.message || ''}`)

// One cancellable job per cause. Waiting offline does not spend a backoff step.
export function createPauseRecovery ({ attempt, online, onOnline, retryable, onError, now = Date.now, setTimer = setTimeout, clearTimer = clearTimeout, random = Math.random }) {
  let current
  function stop () {
    const job = current
    current = null
    if (!job) return
    job.controller.abort()
    if (job.timer != null) clearTimer(job.timer)
    job.stopOnline?.()
  }
  function start ({ immediate = false, retryAt = 0 } = {}) {
    const advice = Number.isFinite(retryAt) && retryAt > 0 ? retryAt : 0
    if (current) {
      if (advice > current.retryAt) { current.retryAt = advice; current.wake?.() }
      return immediate ? current.retry?.() : current.work
    }
    const job = { controller: new AbortController(), delay: 1000, retryAt: advice, timer: null, nextAt: 0, wake: null, retry: null, stopOnline: null, work: null }
    current = job
    const active = () => current === job && !job.controller.signal.aborted
    const schedule = delay => {
      if (!active()) return
      job.nextAt = Math.max(now() + delay, job.retryAt)
      job.timer = setTimer(() => { job.timer = null; return run() }, Math.max(job.nextAt - now(), 0))
      job.timer?.unref?.()
    }
    const run = () => {
      if (!active() || job.work) return job.work
      const work = (async () => {
        if (online && !await online({ signal: job.controller.signal })) {
          if (!active()) return
          job.stopOnline = onOnline(() => {
            if (!active()) return
            job.stopOnline?.(); job.stopOnline = null
            schedule(0)
          })
          return
        }
        if (!active()) return
        try {
          await attempt(job.controller.signal)
          if (active()) stop()
        } catch (error) {
          if (!active()) return
          onError(error)
          if (!retryable(error)) { stop(); return }
          job.retryAt = Math.max(job.retryAt, Number.isFinite(error?.retryAt) ? error.retryAt : 0)
          schedule(Math.min(30000, job.delay * (0.8 + random() * 0.4)))
          job.delay = Math.min(30000, job.delay * 2)
        }
      })().catch(error => { if (active()) { onError(error); stop() } }).finally(() => { if (job.work === work) job.work = null })
      job.work = work
      return work
    }
    job.wake = () => {
      if (!active() || job.work) return job.work
      if (job.timer != null) clearTimer(job.timer)
      job.timer = null
      if (job.nextAt > now()) { schedule(job.nextAt - now()); return }
      return run()
    }
    job.retry = () => {
      if (!active() || job.work) return job.work
      if (job.timer != null) clearTimer(job.timer)
      job.timer = null
      if (job.retryAt > now()) { schedule(job.retryAt - now()); return }
      return run()
    }
    if (immediate) return run()
    schedule(1000)
    job.delay = 2000
  }
  return { start, stop, wake: () => current?.wake?.() }
}
