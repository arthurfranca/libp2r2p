// Local outbox persistence and user publications take precedence over the next
// optional presence signer call. Already-started signer calls are not recalled.
export function createForegroundWork ({ setTimer = setTimeout, clearTimer = clearTimeout } = {}) {
  let active = 0
  const waiters = new Set()
  const observers = new Set()
  const notify = () => { for (const listener of observers) listener(active > 0) }
  const lifetime = new AbortController()
  return {
    enter () {
      active++
      notify()
      let released = false
      return () => {
        if (released) return
        released = true
        if (--active === 0) for (const wake of [...waiters]) wake()
        notify()
      }
    },
    wait (signal) {
      const combined = signal ? AbortSignal.any([signal, lifetime.signal]) : lifetime.signal
      combined.throwIfAborted()
      if (!active) return Promise.resolve()
      return new Promise((resolve, reject) => {
        const finish = () => { waiters.delete(finish); combined.removeEventListener('abort', finish); combined.aborted ? reject(combined.reason) : resolve() }
        waiters.add(finish)
        combined.addEventListener('abort', finish, { once: true })
      })
    },
    waitUntilIdle ({ maxWaitMs = 5000, signal } = {}) {
      if (!Number.isFinite(maxWaitMs) || maxWaitMs < 0) throw new TypeError('INVALID_FOREGROUND_MAX_WAIT')
      const combined = signal ? AbortSignal.any([signal, lifetime.signal]) : lifetime.signal
      combined.throwIfAborted()
      if (!active) return Promise.resolve(true)
      return new Promise((resolve, reject) => {
        const timeout = { timer: undefined }
        const cleanup = () => {
          waiters.delete(finishIdle)
          if (timeout.timer !== undefined) clearTimer(timeout.timer)
          combined.removeEventListener('abort', finishAbort)
        }
        const finishIdle = () => {
          cleanup()
          resolve(true)
        }
        const finishAbort = () => {
          cleanup()
          reject(combined.reason)
        }
        waiters.add(finishIdle)
        combined.addEventListener('abort', finishAbort, { once: true })
        timeout.timer = setTimer(() => {
          cleanup()
          resolve(false)
        }, maxWaitMs)
        timeout.timer?.unref?.()
      })
    },
    observe (listener) { observers.add(listener); listener(active > 0); return () => observers.delete(listener) },
    close () { lifetime.abort(); waiters.clear(); observers.clear() }
  }
}
