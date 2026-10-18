// Admission only: never wait while holding an inbox operation, IDB transaction
// or the cross-instance retry lock. A busy channel cannot stall another one.
export function createDecryptionScheduler ({ ready = true, now = Date.now, setTimer = setTimeout, clearTimer = clearTimeout } = {}) {
  const channels = new Map()
  const readyWaiters = new Set()
  let timer
  let running = false
  let closed
  let foreground = false
  let foregroundAt = now()
  let priority = null
  let previous
  let batch = 0
  const stateFor = pubkey => {
    if (!channels.has(pubkey)) channels.set(pubkey, { active: 0, at: now(), deadline: null, jobs: [] })
    return channels.get(pubkey)
  }
  const quietAt = state => state.active || foreground ? Infinity : Math.max(state.at, foregroundAt) + 250
  const schedule = () => {
    if (timer !== undefined) clearTimer(timer)
    timer = undefined
    if (closed || !ready || running) return
    const pending = [...channels.values()].filter(state => state.jobs.length)
    if (!pending.length) return
    const at = Math.min(...pending.map(state => Math.min(quietAt(state), state.deadline)))
    timer = setTimer(pump, Math.max(0, at - now()))
  }
  async function pump () {
    timer = undefined
    if (closed || !ready || running) return
    const time = now()
    const eligible = [...channels].filter(([, state]) => state.jobs.length && Math.min(quietAt(state), state.deadline) <= time)
    eligible.sort(([a, x], [b, y]) => {
      const overdue = Number(y.deadline <= time) - Number(x.deadline <= time)
      if (overdue) return overdue
      if (x.deadline <= time && x.deadline !== y.deadline) return x.deadline - y.deadline
      if (a === previous && batch >= 4) return 1
      if (b === previous && batch >= 4) return -1
      return Number(b === priority) - Number(a === priority)
    })
    if (!eligible.length) { schedule(); return }
    const [pubkey, state] = eligible[0]
    const job = state.jobs.shift()
    running = true
    if (previous === pubkey) batch++
    else { previous = pubkey; batch = 1 }
    try {
      job.signal?.throwIfAborted()
      job.resolve(await job.work())
    } catch (error) { job.reject(error) } finally {
      job.signal?.removeEventListener('abort', job.abort)
      state.deadline = now() + 5000
      running = false
      // Always use a new task, including between records in the same batch.
      schedule()
    }
  }
  const rejectJobs = (state, error) => {
    for (const job of state.jobs.splice(0)) {
      job.signal?.removeEventListener('abort', job.abort)
      job.reject(error)
    }
  }
  return {
    activity (pubkey) {
      if (closed) return () => {}
      const state = stateFor(pubkey)
      state.active++; state.at = now(); schedule()
      let released = false
      return () => {
        if (released) return
        released = true
        state.active--; state.at = now(); schedule()
      }
    },
    foreground (active) { foreground = active; foregroundAt = now(); schedule() },
    priority (pubkey) { priority = pubkey; schedule() },
    setReady (value) {
      if (ready === value || closed) return
      ready = value
      if (ready) {
        for (const state of channels.values()) { state.at = now(); state.deadline = state.jobs.length ? now() + 5000 : null }
        for (const wake of [...readyWaiters]) wake()
      }
      schedule()
    },
    async waitReady (signal) {
      while (true) {
        signal?.throwIfAborted()
        if (closed) throw closed
        if (ready) return
        await new Promise((resolve, reject) => {
          const finish = () => {
            readyWaiters.delete(finish); signal?.removeEventListener('abort', finish)
            if (closed || signal?.aborted) reject(closed || signal.reason)
            else resolve()
          }
          readyWaiters.add(finish); signal?.addEventListener('abort', finish, { once: true })
        })
      }
    },
    run (pubkey, work, signal) {
      signal?.throwIfAborted()
      if (closed) return Promise.reject(closed)
      const state = stateFor(pubkey)
      state.deadline ??= now() + 5000
      return new Promise((resolve, reject) => {
        const job = { work, resolve, reject, signal }
        job.abort = () => {
          const index = state.jobs.indexOf(job)
          if (index < 0) return
          state.jobs.splice(index, 1); reject(signal.reason); schedule()
        }
        signal?.addEventListener('abort', job.abort, { once: true })
        state.jobs.push(job); schedule()
      })
    },
    remove (pubkey) {
      const state = channels.get(pubkey)
      if (state) rejectJobs(state, new Error('PRIVATE_CHANNEL_CHANGED'))
      channels.delete(pubkey); schedule()
    },
    finish (pubkey) {
      const state = channels.get(pubkey)
      if (state && !state.jobs.length) state.deadline = null
    },
    close () {
      closed ||= new Error('PRIVATE_MESSENGER_CLOSED')
      if (timer !== undefined) clearTimer(timer)
      for (const state of channels.values()) rejectJobs(state, closed)
      channels.clear()
      for (const wake of [...readyWaiters]) wake()
    }
  }
}
