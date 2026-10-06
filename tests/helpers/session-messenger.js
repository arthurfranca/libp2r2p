// A session factory must observe state even when transport methods are mocked.
export function createSessionMessenger ({ onStateChanged }, methods = {}) {
  const reasons = new Set()
  let closed = false
  let previous
  const readStatus = () => ({ closed, paused: reasons.size > 0, pauseReasons: [...reasons].sort() })
  const notify = () => {
    const state = readStatus()
    const fingerprint = JSON.stringify(state)
    if (fingerprint === previous) return
    previous = fingerprint
    onStateChanged(state)
  }
  const messenger = {
    update () {}, nextMessage: async () => null,
    ...methods,
    readStatus,
    async pause (reason) { reasons.add(reason); notify() },
    async resume (reason) { reasons.delete(reason); notify() },
    async close () { closed = true; notify() }
  }
  notify()
  return messenger
}
