import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'

import { getCurrentDeviceLocale, getT, validateLocales } from '../i18n/index.js'

const originalDateTimeFormat = Intl.DateTimeFormat
const originalNavigatorDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'navigator')

function setIntlLocale (locale) {
  Intl.DateTimeFormat = function () {
    return { resolvedOptions: () => ({ locale }) }
  }
}

function setNavigator (value) {
  Object.defineProperty(globalThis, 'navigator', {
    configurable: true,
    value
  })
}

afterEach(() => {
  Intl.DateTimeFormat = originalDateTimeFormat
  if (originalNavigatorDescriptor) Object.defineProperty(globalThis, 'navigator', originalNavigatorDescriptor)
  else delete globalThis.navigator
})

test('detects and canonicalizes the Intl locale before navigator hints', () => {
  setIntlLocale('pt_br')
  setNavigator({ language: 'fr-FR', languages: ['de-DE'] })
  assert.equal(getCurrentDeviceLocale(), 'pt-BR')
})

test('falls back through navigator language, languages, and English', () => {
  Intl.DateTimeFormat = function () { throw new Error('unavailable') }
  setNavigator({ language: 'zh_hant_tw', languages: ['ja-JP'] })
  assert.equal(getCurrentDeviceLocale(), 'zh-Hant-TW')

  setNavigator({ language: '', languages: ['ja-JP'] })
  assert.equal(getCurrentDeviceLocale(), 'ja-JP')

  setNavigator(undefined)
  assert.equal(getCurrentDeviceLocale(), 'en')
})

const locales = {
  'Sentence.with.dots.': {
    en: 'Sentence.with.dots.',
    fr: 'Phrase.avec.des.points.'
  },
  'Allow {{size}}': {
    en: 'Allow {{size}}',
    'pt-BR': 'Permitir {{size}}'
  },
  'Chinese variant': {
    en: 'Chinese variant',
    'zh-CN': '简体',
    'zh-TW': '繁體'
  },
  'Delete {{count}} items': {
    en: {
      one: 'Delete {{count}} item',
      other: 'Delete {{count}} items'
    },
    ru: {
      one: 'Удалить {{count}} элемент',
      few: 'Удалить {{count}} элемента',
      many: 'Удалить {{count}} элементов',
      other: 'Удалить {{count}} элемента'
    },
    ja: {
      other: '{{count}} 件を削除'
    }
  }
}

test('uses literal keys, interpolates values, and leaves missing tokens visible', () => {
  const t = getT(locales, { locale: 'pt-PT' })
  assert.equal(t('Allow {{size}}', { size: '10 MiB' }), 'Permitir 10 MiB')
  assert.equal(t('Allow {{size}}'), 'Permitir {{size}}')
  assert.equal(t('Unknown {{value}}', { value: 3 }), 'Unknown 3')
  assert.equal(getT(locales, { locale: 'fr-CA' })('Sentence.with.dots.'), 'Phrase.avec.des.points.')
})

test('selects simplified and traditional Chinese aliases', () => {
  assert.equal(getT(locales, { locale: 'zh-Hans-SG' })('Chinese variant'), '简体')
  assert.equal(getT(locales, { locale: 'zh-HK' })('Chinese variant'), '繁體')
  assert.equal(getT(locales, { locale: 'zh-Hant' })('Chinese variant'), '繁體')
  assert.equal(getT(locales, { locale: 'zh' })('Chinese variant'), '简体')
})

test('uses locale plural rules with English, Russian, and CJK forms', () => {
  const en = getT(locales, { locale: 'en-US' })
  assert.equal(en('Delete {{count}} items', { count: 1 }), 'Delete 1 item')
  assert.equal(en('Delete {{count}} items', { count: 0 }), 'Delete 0 items')
  assert.equal(en('Delete {{count}} items', { count: 2 }), 'Delete 2 items')

  const ru = getT(locales, { locale: 'ru-RU' })
  assert.equal(ru('Delete {{count}} items', { count: 1 }), 'Удалить 1 элемент')
  assert.equal(ru('Delete {{count}} items', { count: 2 }), 'Удалить 2 элемента')
  assert.equal(ru('Delete {{count}} items', { count: 5 }), 'Удалить 5 элементов')
  assert.equal(ru('Delete {{count}} items', { count: 21 }), 'Удалить 21 элемент')

  assert.equal(getT(locales, { locale: 'ja-JP' })('Delete {{count}} items', { count: 2 }), '2 件を削除')
})

test('falls back to English, then the source key', () => {
  const t = getT(locales, { locale: 'ar', fallbackLocale: 'invalid locale' })
  assert.equal(t('Allow {{size}}', { size: 4 }), 'Allow 4')
  assert.equal(t('Missing key'), 'Missing key')
})

test('rejects invalid catalogs and keys', () => {
  assert.throws(() => getT(null), /locales should be an object/)
  assert.throws(() => validateLocales({}, null), /validation options should be an object/)
  assert.throws(() => getT({}, { validation: false }), /validation options should be an object/)
  const t = getT({}, { locale: 'en' })
  assert.throws(() => t(null), /translation key should be a string/)
})

test('keeps catalog validation optional', () => {
  const partial = { Greeting: { en: 'Greeting' } }
  assert.equal(getT(partial, { locale: 'fr' })('Greeting'), 'Greeting')
  assert.throws(() => getT(partial, {
    validation: { requiredLocales: ['en', 'fr'] }
  }), /missing translation.*fr/)
})

test('validates required locales and returns the original catalog', () => {
  const catalog = {
    'Allow {{size}}': {
      en: 'Allow {{size}}',
      'pt-BR': 'Permitir {{size}}'
    }
  }
  const validation = {
    requiredLocales: ['en', 'pt-BR'],
    referenceLocale: 'en',
    requireReferenceKey: true
  }

  assert.equal(validateLocales(catalog, validation), catalog)
  assert.equal(getT(catalog, { locale: 'pt-BR', validation })('Allow {{size}}', { size: '2 MiB' }), 'Permitir 2 MiB')
})

test('validates placeholders in strings and every plural form', () => {
  assert.throws(() => validateLocales({
    'Hello {{name}}': { en: 'Hello' }
  }), /placeholder mismatch/)
  assert.throws(() => validateLocales({
    'Delete {{count}} items': {
      en: { one: 'Delete one item', other: 'Delete {{count}} items' }
    }
  }), /placeholder mismatch/)
  assert.throws(() => validateLocales({
    'Delete {{count}} items': {
      en: { one: 'Delete {{count}} item' }
    }
  }), /plural object with an other form/)
})

test('optionally requires the reference translation to equal the key', () => {
  const catalog = { Greeting: { en: 'Hello', fr: 'Bonjour' } }
  assert.equal(validateLocales(catalog), catalog)
  assert.throws(() => validateLocales(catalog, {
    referenceLocale: 'en',
    requireReferenceKey: true
  }), /reference translation should match key/)
  assert.throws(() => validateLocales({ Greeting: { fr: 'Greeting' } }, {
    referenceLocale: 'en',
    requireReferenceKey: true
  }), /missing reference translation/)
})
