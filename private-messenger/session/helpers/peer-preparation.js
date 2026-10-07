import { createPauseRecovery } from '../../helpers/pause-recovery.js'

// One account owns the queue. Background work may occupy only three of the
// four slots so a newly opened conversation never sits behind the whole list.
export function createPeerPreparation ({ prepare, onReady, onError, isRetryable, online, onOnline, setTimer, clearTimer, random }) {
  const entries = new Map()
  let enabled = false
  let closed = false
  let running = 0
  let background = 0
  const unavailable = () => new Error('CHAT_UNAVAILABLE')
  function entryFor (peer) {
    const deferred = Promise.withResolvers()
    deferred.promise.catch(() => {})
    return { peer, ...deferred, controller: new AbortController(), state: 'queued', priority: false, value: null, error: null, recovery: null }
  }
  const current = entry => !closed && enabled && entries.get(entry.peer) === entry && !entry.controller.signal.aborted
  function cancel (entry) {
    entry.recovery?.stop()
    entry.controller.abort(unavailable())
    entry.reject(entry.controller.signal.reason)
    entry.retryResult?.resolve()
    entry.retryResult = null
  }
  function drain () {
    if (!enabled || closed) return
    while (running < 4) {
      const queued = [...entries.values()].filter(entry => entry.state === 'queued')
      const entry = queued.find(entry => entry.priority) || (background < 3 && queued[0])
      if (!entry) break
      const isBackground = !entry.priority
      entry.state = 'running'
      running++
      if (isBackground) background++
      Promise.resolve().then(() => {
        entry.controller.signal.throwIfAborted()
        return prepare(entry.peer, entry.controller.signal)
      }).then(value => {
        if (!current(entry)) return
        entry.state = 'ready'; entry.value = value
        entry.resolve(value)
        onReady(entry.peer, value)
      }, error => {
        if (!current(entry)) return
        entry.error = error
        onError(error)
        if (!isRetryable(error)) {
          entry.state = 'failed'; entry.reject(error)
          entry.retryResult?.reject(error); entry.retryResult = null
          onReady(entry.peer)
          return
        }
        entry.state = 'waiting'
        entry.recovery ||= createPauseRecovery({
          attempt: async signal => {
            signal.throwIfAborted()
            if (!current(entry)) return
            entry.state = 'queued'; drain()
            // Recovery waits for the actual preparation; no slot is held
            // during backoff or a connectivity wait.
            await new Promise((resolve, reject) => { entry.retryResult = { resolve, reject } })
          },
          retryable: isRetryable, onError: () => {}, online, onOnline, setTimer, clearTimer, random
        })
        if (entry.retryResult) { entry.retryResult.reject(error); entry.retryResult = null } else entry.recovery.start({ retryAt: error.retryAt })
      }).finally(() => {
        if (entry.state === 'ready') { entry.retryResult?.resolve(); entry.retryResult = null }
        if (!current(entry)) { entry.retryResult?.resolve(); entry.retryResult = null }
        running--
        if (isBackground) background--
        drain()
      })
    }
  }
  return {
    get: peer => entries.get(peer)?.value,
    error: peer => entries.get(peer)?.state === 'failed' ? entries.get(peer).error : null,
    reconcile (peers) {
      const wanted = new Set(peers)
      for (const [peer, entry] of entries) if (!wanted.has(peer)) { entries.delete(peer); cancel(entry) }
      for (const peer of wanted) if (!entries.has(peer)) entries.set(peer, entryFor(peer))
      drain()
    },
    setAvailable (value) {
      enabled = value === true
      if (!enabled) {
        for (const [peer, entry] of entries) {
          if (entry.state === 'ready') continue
          cancel(entry)
          const replacement = entryFor(peer)
          replacement.priority = entry.priority
          entries.set(peer, replacement)
        }
      }
      drain()
    },
    request (peer, { retry = false } = {}) {
      if (closed || !entries.has(peer)) return Promise.reject(unavailable())
      let entry = entries.get(peer)
      if (retry && entry.state === 'failed') { cancel(entry); entries.set(peer, (entry = entryFor(peer))) }
      entry.priority = true
      drain()
      return entry.promise
    },
    close () {
      closed = true; enabled = false
      for (const entry of entries.values()) cancel(entry)
      entries.clear()
    }
  }
}
