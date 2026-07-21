const DEFAULT_LOCALE = 'en'
const INTERPOLATION_RE = /{{\s*([A-Za-z0-9_.-]+)\s*}}/g

function assertLocales (locales) {
  if (!locales || typeof locales !== 'object' || Array.isArray(locales)) {
    throw new TypeError('locales should be an object')
  }
}

function placeholderSignature (value) {
  return [...String(value).matchAll(INTERPOLATION_RE)]
    .map(match => match[1])
    .sort()
    .join(',')
}

function validateTranslationValue (key, locale, value, expectedPlaceholders) {
  if (typeof value === 'string') {
    if (placeholderSignature(value) !== expectedPlaceholders) {
      throw new Error(`placeholder mismatch for "${key}" (${locale})`)
    }
    return
  }

  if (!value || typeof value !== 'object' || Array.isArray(value) || typeof value.other !== 'string') {
    throw new TypeError(`translation for "${key}" (${locale}) should be a string or plural object with an other form`)
  }

  for (const form of Object.values(value)) {
    if (typeof form !== 'string') {
      throw new TypeError(`plural forms for "${key}" (${locale}) should be strings`)
    }
    if (placeholderSignature(form) !== expectedPlaceholders) {
      throw new Error(`placeholder mismatch for "${key}" (${locale})`)
    }
  }
}

export function validateLocales (locales, options = {}) {
  assertLocales(locales)
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError('validation options should be an object')
  }
  const {
    requiredLocales = [],
    referenceLocale = DEFAULT_LOCALE,
    requireReferenceKey = false
  } = options
  if (!Array.isArray(requiredLocales) || requiredLocales.some(locale => typeof locale !== 'string' || !locale)) {
    throw new TypeError('requiredLocales should be an array of non-empty strings')
  }
  if (typeof referenceLocale !== 'string' || !referenceLocale) {
    throw new TypeError('referenceLocale should be a non-empty string')
  }

  for (const [key, translations] of Object.entries(locales)) {
    if (!translations || typeof translations !== 'object' || Array.isArray(translations)) {
      throw new TypeError(`translations for "${key}" should be an object`)
    }

    for (const locale of requiredLocales) {
      if (!Object.prototype.hasOwnProperty.call(translations, locale)) {
        throw new Error(`missing translation for "${key}" (${locale})`)
      }
    }

    const expectedPlaceholders = placeholderSignature(key)
    for (const [locale, value] of Object.entries(translations)) {
      validateTranslationValue(key, locale, value, expectedPlaceholders)
    }

    if (requireReferenceKey) {
      if (!Object.prototype.hasOwnProperty.call(translations, referenceLocale)) {
        throw new Error(`missing reference translation for "${key}" (${referenceLocale})`)
      }
      const reference = translations[referenceLocale]
      const referenceValue = typeof reference === 'string' ? reference : reference.other
      if (referenceValue !== key) {
        throw new Error(`reference translation should match key "${key}" (${referenceLocale})`)
      }
    }
  }

  return locales
}

function canonicalizeLocale (locale) {
  if (typeof locale !== 'string' || !locale.trim()) return null
  const value = locale.trim().replace(/_/g, '-')
  try {
    return Intl.getCanonicalLocales(value)[0] ?? null
  } catch {
    return null
  }
}

function getIntlLocale () {
  try {
    return Intl.DateTimeFormat().resolvedOptions().locale
  } catch {
    return null
  }
}

function getNavigator () {
  try {
    return globalThis.navigator
  } catch {
    return null
  }
}

export function getCurrentDeviceLocale () {
  const navigator = getNavigator()
  const candidates = [
    getIntlLocale(),
    navigator?.language,
    navigator?.languages?.[0],
    DEFAULT_LOCALE
  ]

  for (const candidate of candidates) {
    const locale = canonicalizeLocale(candidate)
    if (locale) return locale
  }
  return DEFAULT_LOCALE
}

function localeLanguage (locale) {
  try {
    return new Intl.Locale(locale).language
  } catch {
    return locale.split('-')[0].toLowerCase()
  }
}

function preferredChineseLocale (locale) {
  if (localeLanguage(locale) !== 'zh') return null
  try {
    const { script, region } = new Intl.Locale(locale)
    if (script === 'Hant' || ['TW', 'HK', 'MO'].includes(region)) return 'zh-TW'
    return 'zh-CN'
  } catch {
    return /(?:^|-)(?:hant|tw|hk|mo)(?:-|$)/i.test(locale) ? 'zh-TW' : 'zh-CN'
  }
}

function localeCandidates (translations, requestedLocale, fallbackLocale) {
  const keys = Object.keys(translations)
  const canonicalKeys = new Map()
  for (const key of keys) {
    const canonical = canonicalizeLocale(key)
    if (canonical && !canonicalKeys.has(canonical.toLowerCase())) {
      canonicalKeys.set(canonical.toLowerCase(), { key, canonical })
    }
  }

  const result = []
  const seen = new Set()
  const add = locale => {
    const canonical = canonicalizeLocale(locale)
    const match = canonical && canonicalKeys.get(canonical.toLowerCase())
    if (match && !seen.has(match.key)) {
      seen.add(match.key)
      result.push(match)
    }
  }
  const addCompatible = locale => {
    const canonical = canonicalizeLocale(locale)
    if (!canonical) return
    add(canonical)
    add(preferredChineseLocale(canonical))
    const language = localeLanguage(canonical)
    const match = [...canonicalKeys.values()].find(candidate => localeLanguage(candidate.canonical) === language)
    if (match) add(match.canonical)
  }

  addCompatible(requestedLocale)
  addCompatible(fallbackLocale)
  addCompatible(DEFAULT_LOCALE)
  return result
}

function selectPlural (forms, locale, values) {
  if (!forms || typeof forms !== 'object' || Array.isArray(forms)) return null
  const count = Number(values?.count)
  if (!Number.isFinite(count)) return null

  let category = 'other'
  try {
    category = new Intl.PluralRules(locale).select(count)
  } catch {}
  const value = forms[category] ?? forms.other
  return typeof value === 'string' ? value : null
}

function selectTemplate (translations, requestedLocale, fallbackLocale, values) {
  if (!translations || typeof translations !== 'object' || Array.isArray(translations)) return null
  for (const { key, canonical } of localeCandidates(translations, requestedLocale, fallbackLocale)) {
    const value = translations[key]
    if (typeof value === 'string') return value
    const plural = selectPlural(value, canonical, values)
    if (plural !== null) return plural
  }
  return null
}

function interpolate (template, values) {
  const source = String(template)
  if (!values || typeof values !== 'object') return source
  return source.replace(INTERPOLATION_RE, (token, name) => (
    Object.prototype.hasOwnProperty.call(values, name)
      ? String(values[name])
      : token
  ))
}

export function getT (locales, {
  locale = getCurrentDeviceLocale(),
  fallbackLocale = DEFAULT_LOCALE,
  validation
} = {}) {
  assertLocales(locales)
  if (validation !== undefined) validateLocales(locales, validation)

  const requestedLocale = canonicalizeLocale(locale) ?? DEFAULT_LOCALE
  const canonicalFallback = canonicalizeLocale(fallbackLocale) ?? DEFAULT_LOCALE

  return function t (key, values) {
    if (typeof key !== 'string') throw new TypeError('translation key should be a string')
    const translations = Object.prototype.hasOwnProperty.call(locales, key) ? locales[key] : null
    const template = selectTemplate(translations, requestedLocale, canonicalFallback, values) ?? key
    return interpolate(template, values)
  }
}
