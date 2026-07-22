export function normalizeUrl (value) {
  if (typeof value !== 'string' || value.length === 0) throw new TypeError('URL_SHOULD_BE_A_STRING')
  let input = value.trim()
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(input)) input = `wss://${input}`
  const url = new URL(input)
  if (url.protocol === 'http:') url.protocol = 'ws:'
  else if (url.protocol === 'https:') url.protocol = 'wss:'
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') throw new Error('INVALID_RELAY_PROTOCOL')
  url.pathname = url.pathname.replace(/\/{2,}/g, '/').replace(/\/$/, '')
  if (url.pathname === '/') url.pathname = ''
  if ((url.protocol === 'ws:' && url.port === '80') || (url.protocol === 'wss:' && url.port === '443')) url.port = ''
  url.searchParams.sort()
  url.hash = ''
  return url.toString()
}
