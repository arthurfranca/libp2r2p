// Local outbox persistence and user publications take precedence over the next
// optional presence signer call. Already-started signer calls are not recalled.
export function createForegroundWork () {
  let active = 0
  const waiters = new Set()
  const lifetime = new AbortController()
  return {
    enter () {
      active++
      let released = false
      return () => {
        if (released) return
        released = true
        if (--active === 0) for (const wake of [...waiters]) wake()
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
    close () { lifetime.abort(); waiters.clear() }
  }
}
