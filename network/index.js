import { ValidationError } from '../error/index.js'

const RETRY_DELAYS = [5000, 15000, 30000, 60000]
const CONNECTIVITY_PROBE_URLS = [
  { url: 'https://www.gstatic.com/generate_204' },
  { url: 'https://connectivitycheck.gstatic.com/generate_204' },
  { url: 'https://captive.apple.com/hotspot-detect.html' },
  { method: 'GET', url: 'https://connectivity-check.ubuntu.com' }
]
let sharedCheck

// Treat the browser's offline flag as a fast failure; confirm online status with a probe.
export async function isOnline ({ signal } = {}) {
  if (signal?.aborted) throw signal.reason
  if (globalThis.navigator?.onLine === false) return false
  if (signal) return hasInternetConnectivity(signal)
  sharedCheck ??= hasInternetConnectivity().finally(() => { sharedCheck = null })
  return sharedCheck
}

async function hasInternetConnectivity (signal) {
  for (const candidate of shuffle(CONNECTIVITY_PROBE_URLS)) {
    if (signal?.aborted) throw signal.reason
    try {
      await ping(candidate.url, { method: candidate.method, signal })
      return true
    } catch {
      if (signal?.aborted) throw signal.reason
    }
  }
  return false
}

function shuffle (list) {
  const copy = list.slice()
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[copy[i], copy[j]] = [copy[j], copy[i]]
  }
  return copy
}

// Bound each probe and release its timer and abort listener on every exit path.
async function ping (url, { method = 'HEAD', timeout = 5000, signal } = {}) {
  const controller = new AbortController()
  let timer
  let onAbort
  const stopped = new Promise((_resolve, reject) => {
    onAbort = () => {
      controller.abort(signal.reason)
      reject(signal.reason)
    }
    signal?.addEventListener('abort', onAbort, { once: true })
    timer = setTimeout(() => {
      controller.abort()
      reject(new Error('PING_TIMEOUT'))
    }, timeout)
    if (signal?.aborted) onAbort()
  })
  try {
    await Promise.race([
      fetch(url, { method, mode: 'no-cors', cache: 'no-store', redirect: 'follow', signal: controller.signal }),
      stopped
    ])
  } finally {
    clearTimeout(timer)
    signal?.removeEventListener('abort', onAbort)
  }
}

// A monitor owns one probe loop, regardless of how many consumers subscribe.
export function createConnectivityMonitor ({
  check = isOnline,
  eventTarget = globalThis.window,
  document = globalThis.document,
  _setTimeout = globalThis.setTimeout,
  _clearTimeout = globalThis.clearTimeout,
  _random = Math.random,
  reportError = error => console.error('Online listener failed', error)
} = {}) {
  if (typeof check !== 'function') throw new ValidationError('INVALID_CONNECTIVITY_CHECK')
  const listeners = new Set()
  const setTimer = (...args) => Reflect.apply(_setTimeout, globalThis, args)
  const clearTimer = (...args) => Reflect.apply(_clearTimeout, globalThis, args)
  let session

  function deliver (entry, current) {
    if (entry.delivered || !listeners.has(entry) || session !== current) return
    entry.delivered = true
    try { Promise.resolve(entry.handler()).catch(reportError) } catch (error) { reportError(error) }
  }

  function schedule (current) {
    if (session !== current || !listeners.size) return
    const delay = current.online ? 60000 : RETRY_DELAYS[Math.min(current.retry++, RETRY_DELAYS.length - 1)]
    current.timer = setTimer(() => {
      current.timer = null
      return probe(current)
    }, Math.round(delay * (0.8 + _random() * 0.4)))
    current.timer?.unref?.()
  }

  async function probe (current) {
    if (session !== current || current.pending) return
    if (current.timer != null) clearTimer(current.timer)
    current.timer = null
    current.pending = true
    try {
      const online = await check({ signal: current.controller.signal })
      if (session !== current) return
      current.online = online === true && globalThis.navigator?.onLine !== false
      if (current.online) {
        current.retry = 0
        for (const entry of [...listeners]) deliver(entry, current)
      } else {
        for (const entry of listeners) entry.delivered = false
      }
    } catch {
      if (session !== current) return
      current.online = false
      for (const entry of listeners) entry.delivered = false
    } finally {
      current.pending = false
      schedule(current)
    }
  }

  function start () {
    const current = { online: false, retry: 0, timer: null, pending: false, controller: new AbortController() }
    session = current
    current.wake = () => { probe(current) }
    current.connectionChanged = () => {
      current.online = false
      current.retry = 0
      for (const entry of listeners) entry.delivered = false
      current.wake()
    }
    current.visible = () => { if (document?.visibilityState !== 'hidden') current.wake() }
    eventTarget?.addEventListener('online', current.connectionChanged)
    eventTarget?.addEventListener('offline', current.connectionChanged)
    eventTarget?.addEventListener('focus', current.wake)
    document?.addEventListener('visibilitychange', current.visible)
    queueMicrotask(current.wake)
  }

  function stop () {
    const current = session
    session = null
    if (current.timer != null) clearTimer(current.timer)
    current.controller.abort()
    eventTarget?.removeEventListener('online', current.connectionChanged)
    eventTarget?.removeEventListener('offline', current.connectionChanged)
    eventTarget?.removeEventListener('focus', current.wake)
    document?.removeEventListener('visibilitychange', current.visible)
  }

  // Notify once on confirmed initial connectivity, then after each detected reconnection.
  function onOnline (handler) {
    if (typeof handler !== 'function') throw new ValidationError('INVALID_ONLINE_HANDLER')
    const entry = { handler, delivered: false }
    listeners.add(entry)
    if (!session) start()
    else {
      const current = session
      queueMicrotask(() => { probe(current) })
    }
    return () => {
      if (!listeners.delete(entry)) return
      if (!listeners.size) stop()
    }
  }

  return { onOnline }
}

let defaultMonitor

// Share probes, capped backoff and wake-up listeners across callers in this realm.
export function onOnline (handler) {
  defaultMonitor ??= createConnectivityMonitor()
  return defaultMonitor.onOnline(handler)
}
