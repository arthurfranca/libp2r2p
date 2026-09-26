import { ValidationError } from '../error/index.js'

const RETRY_DELAYS = [5000, 15000, 30000, 60000]
const CONNECTIVITY_PROBE_URLS = [
  { url: 'https://www.gstatic.com/generate_204' },
  { url: 'https://connectivitycheck.gstatic.com/generate_204' },
  { url: 'https://captive.apple.com/hotspot-detect.html' },
  { method: 'GET', url: 'https://connectivity-check.ubuntu.com' }
]
// CORS-enabled endpoints let the strict mode validate the response instead of
// merely proving the host is reachable.
const STRICT_CONNECTIVITY_PROBE_URLS = [
  { url: 'https://captive.apple.com/hotspot-detect.html', method: 'GET', marker: 'Success' },
  { url: 'https://1.1.1.1/cdn-cgi/trace', method: 'GET', marker: 'ip=' },
  { url: 'https://cloudflare.com/cdn-cgi/trace', method: 'GET', marker: 'ip=' }
]
const FIRST_PROBE_TIMEOUT_MS = 2500
const HEDGE_DELAY_MS = 1000
const REMAINING_PROBE_TIMEOUT_MS = 4000
const sharedChecks = new Map()

// Treat the browser's offline flag as a fast failure; confirm online status with
// probes. The default mode only proves the hosts are reachable, while strict mode
// requires CORS responses with an expected body and rejects captive portals.
export async function isOnline ({ signal, strict = false } = {}) {
  if (signal?.aborted) throw signal.reason
  if (globalThis.navigator?.onLine === false) return false
  if (signal) return hasInternetConnectivity(signal, strict)
  const key = strict ? 'strict' : 'lenient'
  if (!sharedChecks.has(key)) {
    sharedChecks.set(key, hasInternetConnectivity(undefined, strict).finally(() => { sharedChecks.delete(key) }))
  }
  return sharedChecks.get(key)
}

// The first candidate answers the common case alone. When it is still pending
// after HEDGE_DELAY_MS, the remaining candidates race in parallel so one slow or
// blocked host cannot serialize every timeout.
async function hasInternetConnectivity (signal, strict) {
  if (signal?.aborted) throw signal.reason
  const candidates = shuffle(strict ? STRICT_CONNECTIVITY_PROBE_URLS : CONNECTIVITY_PROBE_URLS)
  const [first, ...rest] = candidates
  if (!first) return false
  const controller = new AbortController()
  const onAbort = () => controller.abort(signal?.reason)
  signal?.addEventListener('abort', onAbort, { once: true })
  if (signal?.aborted) onAbort()
  let hedgeTimer
  let onHedgeAbort
  try {
    let startHedge
    const firstAttempt = ping(first, { strict, signal: controller.signal, timeout: FIRST_PROBE_TIMEOUT_MS })
    const hedge = new Promise((resolve, reject) => {
      startHedge = resolve
      hedgeTimer = setTimeout(resolve, HEDGE_DELAY_MS)
      if (signal) {
        onHedgeAbort = () => reject(signal.reason)
        signal.addEventListener('abort', onHedgeAbort, { once: true })
        if (signal.aborted) onHedgeAbort()
      }
    }).then(() => Promise.any(rest.map(candidate => ping(candidate, { strict, signal: controller.signal, timeout: REMAINING_PROBE_TIMEOUT_MS }))))
    // A definitive failure releases the hedge immediately; a slow probe waits
    // for the hedge delay so a working network still costs one request.
    firstAttempt.catch(() => startHedge())
    try {
      await Promise.any([firstAttempt, hedge])
      return true
    } catch {
      if (signal?.aborted) throw signal.reason
      return false
    }
  } finally {
    clearTimeout(hedgeTimer)
    if (onHedgeAbort) signal?.removeEventListener('abort', onHedgeAbort)
    controller.abort()
    signal?.removeEventListener('abort', onAbort)
  }
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
async function ping (candidate, { strict = false, timeout, signal } = {}) {
  if (signal?.aborted) throw signal.reason
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
    const response = await Promise.race([
      fetch(candidate.url, {
        method: candidate.method ?? (strict ? 'GET' : 'HEAD'),
        mode: strict ? 'cors' : 'no-cors',
        cache: 'no-store',
        redirect: 'follow',
        signal: controller.signal
      }),
      stopped
    ])
    if (!strict) return
    if (!response.ok) throw new Error('PING_STATUS')
    if (!(await response.text()).includes(candidate.marker)) throw new Error('PING_BODY')
  } finally {
    clearTimeout(timer)
    signal?.removeEventListener('abort', onAbort)
  }
}

// A monitor owns one probe loop per mode, regardless of how many consumers subscribe.
export function createConnectivityMonitor ({
  check,
  strict = false,
  eventTarget = globalThis.window,
  document = globalThis.document,
  _setTimeout = globalThis.setTimeout,
  _clearTimeout = globalThis.clearTimeout,
  _random = Math.random,
  reportError = error => console.error('Online listener failed', error)
} = {}) {
  if (check !== undefined && typeof check !== 'function') throw new ValidationError('INVALID_CONNECTIVITY_CHECK')
  const runCheck = check ?? (options => isOnline({ ...options, strict }))
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
      const online = await runCheck({ signal: current.controller.signal })
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

const defaultMonitors = new Map()

// Share probes, capped backoff and wake-up listeners per mode in this realm.
export function onOnline (handler, { strict = false } = {}) {
  const key = strict ? 'strict' : 'lenient'
  let monitor = defaultMonitors.get(key)
  if (!monitor) {
    monitor = createConnectivityMonitor({ strict })
    defaultMonitors.set(key, monitor)
  }
  return monitor.onOnline(handler)
}
