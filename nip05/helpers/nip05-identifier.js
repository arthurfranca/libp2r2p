const NIP05_LOCAL = /^[a-z0-9._-]+$/
const NIP05_DOMAIN = /^[a-z0-9.-]+$/

export function isValidNip05Local (local) {
  return typeof local === 'string' &&
    (local === '_' || (local.length <= 64 && NIP05_LOCAL.test(local)))
}

export function isValidNip05Domain (domain) {
  return typeof domain === 'string' &&
    domain.length > 0 &&
    domain.length <= 253 &&
    domain.includes('.') &&
    !domain.startsWith('.') &&
    !domain.endsWith('.') &&
    !domain.includes('..') &&
    NIP05_DOMAIN.test(domain)
}

export function nip05FromLocalDomain (local, domain) {
  if (!isValidNip05Local(local) || !isValidNip05Domain(domain)) return null
  return { local, domain }
}

// Accepts:
// - `local@domain` (standard NIP-05)
// - `domain` with exactly one dot -> root `_@domain`
// - `local.domain...` with more than one dot -> local part + domain (custom extension)
export function decodeNip05Identifier (value) {
  if (typeof value !== 'string') return null
  const text = value.trim().toLowerCase()
  if (!text) return null

  const at = text.lastIndexOf('@')
  if (at !== -1) {
    if (at === 0 || at === text.length - 1 || text.includes('@', at + 1)) return null
    return nip05FromLocalDomain(text.slice(0, at), text.slice(at + 1))
  }

  if (!text.includes('.')) return null
  const firstDot = text.indexOf('.')
  if (text.slice(firstDot + 1).includes('.')) {
    return nip05FromLocalDomain(text.slice(0, firstDot), text.slice(firstDot + 1))
  }
  return nip05FromLocalDomain('_', text)
}

// Returns the most compact unambiguous NIP-05 spelling:
// - root (`_@domain`) becomes `domain` only when the domain has one dot;
// - a non-root local part becomes `local.domain` unless the local part
//   itself contains dots, which would make the compact form ambiguous.
export function compactNip05Raw (local, domain) {
  if (local === '_') {
    return domain.split('.').length === 2 ? domain : `_@${domain}`
  }
  return local.includes('.') ? `${local}@${domain}` : `${local}.${domain}`
}
