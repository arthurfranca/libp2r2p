import { ValidationError } from '../error/index.js'

function isIpv4Address (hostname) {
  const parts = hostname.split('.')
  return parts.length === 4 && parts.every(part =>
    /^(?:0|[1-9][0-9]{0,2})$/.test(part) && Number(part) <= 255
  )
}

function isPublicIpv4Address (hostname) {
  const [a, b, c] = hostname.split('.').map(Number)
  if (a === 0 || a === 10 || a === 127 || a >= 224) return false
  if (a === 100 && b >= 64 && b <= 127) return false
  if (a === 169 && b === 254) return false
  if (a === 172 && b >= 16 && b <= 31) return false
  if (a === 192 && b === 168) return false
  if (a === 192 && b === 0 && (c === 0 || c === 2)) return false
  if (a === 192 && b === 88 && c === 99) return false
  if (a === 198 && (b === 18 || b === 19 || (b === 51 && c === 100))) return false
  if (a === 203 && b === 0 && c === 113) return false
  return true
}

function parseIpv6Words (hostname) {
  const value = hostname.replace(/^\[|\]$/g, '').toLowerCase()
  if (!/^[0-9a-f:]+$/.test(value) || value.split('::').length > 2) return null
  const [left = '', right = ''] = value.split('::')
  const leftWords = left ? left.split(':') : []
  const rightWords = right ? right.split(':') : []
  const missing = 8 - leftWords.length - rightWords.length
  if ((value.includes('::') && missing < 1) || (!value.includes('::') && missing !== 0)) return null
  const words = [...leftWords, ...Array(missing).fill('0'), ...rightWords]
  if (words.length !== 8 || words.some(word => !/^[0-9a-f]{1,4}$/.test(word))) return null
  return words.map(word => Number.parseInt(word, 16))
}

function isPublicIpv6Address (hostname) {
  const words = parseIpv6Words(hostname)
  if (!words) return false
  const first = words[0]
  if (first === 0) return false
  if ((first & 0xfe00) === 0xfc00) return false
  if ((first & 0xffc0) === 0xfe80) return false
  if ((first & 0xff00) === 0xff00) return false
  if (first === 0x2001 && words[1] === 0x0db8) return false
  return true
}

function removeEmptyQuerySegments (url) {
  const entries = [...url.searchParams]
  url.search = ''
  for (const [key, value] of entries) url.searchParams.append(key, value)
  url.searchParams.sort()
}

export function normalizeRelayUrl (value) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new ValidationError('INVALID_RELAY_URL', { message: 'URL_SHOULD_BE_A_STRING' })
  }
  let input = value.trim()
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(input)) input = `wss://${input}`
  let url
  try {
    url = new URL(input)
  } catch (cause) {
    throw new ValidationError('INVALID_RELAY_URL', { cause })
  }
  if (url.protocol === 'http:') url.protocol = 'ws:'
  else if (url.protocol === 'https:') url.protocol = 'wss:'
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') throw new ValidationError('INVALID_RELAY_PROTOCOL')
  url.pathname = url.pathname.replace(/\/{2,}/g, '/').replace(/\/+$/, '')
  if (url.pathname === '/') url.pathname = ''
  if ((url.protocol === 'ws:' && url.port === '80') || (url.protocol === 'wss:' && url.port === '443')) url.port = ''
  removeEmptyQuerySegments(url)
  url.hash = ''
  return url.toString().replace(/^(wss?:\/\/[^/?#]+)\/(?=[?#]|$)/, '$1')
}

function publicRelayUrlError (value) {
  if (typeof value !== 'string' || value.trim().length === 0) return 'INVALID_RELAY_URL'
  let input = value.trim()
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(input)) input = `wss://${input}`
  try {
    if (decodeURIComponent(new URL(input).pathname).includes('://')) return 'RELAY_URL_NESTED_SCHEME'
  } catch {
    return 'INVALID_RELAY_URL'
  }

  let normalized
  try {
    normalized = normalizeRelayUrl(value)
  } catch (error) {
    return error instanceof ValidationError ? error.code : 'INVALID_RELAY_URL'
  }

  const url = new URL(normalized)
  if (url.protocol !== 'wss:') return 'INSECURE_RELAY_URL'
  if (url.username || url.password) return 'RELAY_URL_CREDENTIALS_NOT_ALLOWED'

  const hostname = url.hostname.toLowerCase()
  if (
    hostname === 'localhost' ||
    hostname.endsWith('.localhost') ||
    hostname.endsWith('.local') ||
    hostname.endsWith('.onion')
  ) return 'NON_PUBLIC_RELAY_HOST'

  const unwrappedHostname = hostname.replace(/^\[|\]$/g, '')
  if (isIpv4Address(unwrappedHostname)) {
    if (!isPublicIpv4Address(unwrappedHostname)) return 'NON_PUBLIC_RELAY_IP'
  } else if (unwrappedHostname.includes(':')) {
    if (!isPublicIpv6Address(unwrappedHostname)) return 'NON_PUBLIC_RELAY_IP'
  } else if (!hostname.includes('.')) {
    return 'INVALID_RELAY_HOST'
  }

  const lowerValue = normalized.toLowerCase()
  if (lowerValue.includes('npub1') || lowerValue.includes('nprofile1')) return 'RELAY_URL_NOSTR_ENTITY_NOT_ALLOWED'

  return null
}

export function isValidPublicRelayUrl (value) {
  return publicRelayUrlError(value) === null
}

export function assertValidPublicRelayUrl (value) {
  const code = publicRelayUrlError(value)
  if (code) throw new ValidationError(code)
  return value
}
