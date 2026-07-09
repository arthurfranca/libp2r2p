export async function isOnline () {
  if (typeof navigator !== 'undefined' && typeof navigator.onLine === 'boolean') {
    if (!navigator.onLine) return false
  }
  return hasInternetConnectivity()
}

const CONNECTIVITY_PROBE_URLS = [
  { url: 'https://www.gstatic.com/generate_204' },
  { url: 'https://connectivitycheck.gstatic.com/generate_204' },
  { url: 'https://captive.apple.com/hotspot-detect.html' },
  { method: 'GET', url: 'https://connectivity-check.ubuntu.com' }
]

async function hasInternetConnectivity () {
  const candidates = shuffle(CONNECTIVITY_PROBE_URLS)
  for (const candidate of candidates) {
    try {
      await ping(candidate.url, { method: candidate.method })
      return true
    } catch (err) {
      console.warn('connectivity probe failed', candidate.url, err?.message ?? err)
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

async function ping (url, { method = 'HEAD', timeout = 5000 } = {}) {
  const abortController = typeof AbortController === 'function' ? new AbortController() : null
  let timerId = null

  const fetchPromise = fetch(url, {
    method,
    mode: 'no-cors',
    cache: 'no-store',
    redirect: 'follow',
    signal: abortController?.signal
  })

  const completionPromise = fetchPromise.finally(() => {
    if (timerId != null) clearTimeout(timerId)
  })

  const timeoutPromise = new Promise((_resolve, reject) => {
    timerId = setTimeout(() => {
      if (abortController) abortController.abort()
      reject(new Error('PING_TIMEOUT'))
    }, timeout)
  })

  await Promise.race([completionPromise, timeoutPromise])
  return true
}

export function onOnline (handler) {
  const listener = () => handler()
  window.addEventListener('online', listener)
  return () => window.removeEventListener('online', listener)
}
