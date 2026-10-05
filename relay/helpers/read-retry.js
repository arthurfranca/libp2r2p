import { isOnline, onOnline } from '../../network/index.js'
import { isRetryableRelayFailure } from './failure.js'
import { maybeUnref } from './timer.js'

const LOCAL_RETRY_CODES = new Set(['RELAY_READ_QUEUE_FULL', 'RELAY_READ_QUEUE_TIMEOUT', 'RELAY_DISCONNECTED'])

export function isRetryableReadFailure (error) {
  if (error?.name === 'ValidationError' || error?.name === 'Nip42AuthenticationError') return false
  if (error?.phase === 'admission') return LOCAL_RETRY_CODES.has(error.code)
  return isRetryableRelayFailure(error)
}

export function needsConnectivityCheck (error) {
  return globalThis.navigator?.onLine === false || (error?.phase !== 'admission' && ['connection', 'transport', 'timeout'].includes(error?.category))
}

export function waitUntil (deadline, signal) {
  signal.throwIfAborted()
  if (deadline <= Date.now()) return Promise.resolve()
  return new Promise((resolve, reject) => {
    const finish = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); resolve() }
    const abort = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); reject(signal.reason) }
    const timer = maybeUnref(setTimeout(finish, Math.max(0, deadline - Date.now())))
    signal.addEventListener('abort', abort, { once: true })
  })
}

function waitFor (promise, signal) {
  signal.throwIfAborted()
  return new Promise((resolve, reject) => {
    const abort = () => { signal.removeEventListener('abort', abort); reject(signal.reason) }
    signal.addEventListener('abort', abort, { once: true })
    promise.then(value => { signal.removeEventListener('abort', abort); resolve(value) }, error => {
      signal.removeEventListener('abort', abort); reject(error)
    })
  })
}

// One connectivity check/online subscription per pool, with independent waits.
// Ending one reader must never cancel recovery still needed by another reader.
export class ReadRetry {
  #checkOnline
  #watchOnline
  #probe
  #monitor
  #waiters = new Set()

  constructor ({ checkOnline = isOnline, watchOnline = onOnline } = {}) {
    this.#checkOnline = checkOnline
    this.#watchOnline = watchOnline
  }

  async confirmOnline (signal) {
    signal.throwIfAborted()
    if (globalThis.navigator?.onLine === false) return false
    if (!this.#probe || this.#probe.controller.signal.aborted) {
      const controller = new AbortController()
      const record = { controller, users: 0, finished: false }
      const stopped = new Promise(resolve => controller.signal.addEventListener('abort', () => resolve(false), { once: true }))
      const timer = maybeUnref(setTimeout(() => controller.abort(), 6000))
      record.promise = Promise.race([
        Promise.resolve().then(() => this.#checkOnline({ signal: controller.signal })).catch(() => false), stopped
      ]).then(value => value === true).finally(() => {
        record.finished = true
        clearTimeout(timer)
        controller.abort()
        if (this.#probe === record) this.#probe = null
      })
      this.#probe = record
    }
    const record = this.#probe
    record.users++
    try { return await waitFor(record.promise, signal) } finally {
      if (--record.users === 0 && !record.finished) record.controller.abort()
    }
  }

  waitUntilOnline (signal) {
    signal.throwIfAborted()
    const ready = Promise.withResolvers()
    this.#waiters.add(ready)
    if (!this.#monitor) {
      const monitor = { stop: () => {}, ended: false }
      this.#monitor = monitor
      monitor.stop = this.#watchOnline(() => {
        if (this.#monitor !== monitor) return
        for (const waiter of this.#waiters) waiter.resolve()
        this.#waiters.clear()
        this.#stopIfIdle()
      })
      if (monitor.ended) monitor.stop()
    }
    return waitFor(ready.promise, signal).finally(() => {
      this.#waiters.delete(ready)
      this.#stopIfIdle()
    })
  }

  #stopIfIdle () {
    if (this.#waiters.size || !this.#monitor) return
    const monitor = this.#monitor
    this.#monitor = null
    monitor.ended = true
    monitor.stop()
  }
}
