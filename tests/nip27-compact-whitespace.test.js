import { test } from 'node:test'
import assert from 'node:assert/strict'
import { compactWhitespace } from 'libp2r2p/nip27'
import { ValidationError } from 'libp2r2p/error'

test('compactWhitespace compacts horizontal whitespace and trims the result', () => {
  assert.equal(compactWhitespace(' \t Hello\t  world!  \t'), 'Hello world!')
  assert.equal(compactWhitespace(''), '')
  assert.equal(compactWhitespace(' \t\r\n\n  '), '')
})

test('compactWhitespace removes carriage returns and spaces around line breaks', () => {
  assert.equal(compactWhitespace(' first \t\r\n \t second\rthird '), 'first\nsecondthird')
})

test('compactWhitespace preserves paragraphs with at most one blank line', () => {
  assert.equal(compactWhitespace('one\n\n\n\n two\nthree\n\n four'), 'one\n\ntwo\nthree\n\nfour')
})

test('compactWhitespace keeps eight line breaks and flattens subsequent runs', () => {
  const firstNineLines = Array.from({ length: 9 }, (_, i) => `line ${i}`).join('\n')
  assert.equal(compactWhitespace(firstNineLines), firstNineLines)
  assert.equal(compactWhitespace(`${firstNineLines}\nline 9\n\n\nline 10`), `${firstNineLines} line 9 line 10`)
  const firstEightLines = Array.from({ length: 8 }, (_, i) => `line ${i}`).join('\n')
  assert.equal(compactWhitespace(`${firstEightLines}\n\nlast`), `${firstEightLines}\n last`)
})

test('compactWhitespace leaves non-whitespace content and internal Unicode spacing intact', () => {
  const text = 'Olá 👋\u00a0mundo https://example.com/#dim=640x480 nostr:note1abc'
  assert.equal(compactWhitespace(text), text)
})

test('compactWhitespace allows the total limit and collapse threshold to be configured independently', () => {
  assert.equal(compactWhitespace('a\n\n\nb\nc\nd', { maxLineBreaks: 3 }), 'a\n\nb\nc d')
  for (const length of [1, 2, 3, 4, 5, 6]) {
    assert.equal(compactWhitespace(`a${'\n'.repeat(length)}b`, {
      consecutiveLineBreakThreshold: 5
    }), `a${'\n'.repeat(length >= 5 ? 2 : length)}b`)
  }
  const text = Array.from({ length: 13 }, (_, i) => `line ${i}`).join('\n')
  assert.equal(compactWhitespace(text, { maxLineBreaks: 12 }), text)
  assert.equal(compactWhitespace(text, {}), compactWhitespace(text))
  assert.equal(compactWhitespace(text, { maxLineBreaks: undefined, consecutiveLineBreakThreshold: undefined }), compactWhitespace(text))
})

test('compactWhitespace collapses consecutive runs before applying the total limit', () => {
  assert.equal(compactWhitespace('a\n\n\nb\n\n\nc\nd', {
    maxLineBreaks: 3,
    consecutiveLineBreakThreshold: 3
  }), 'a\n\nb\n c d')
})

test('compactWhitespace supports zero and unlimited line-break limits', () => {
  assert.equal(compactWhitespace(' a\n\n\nb\nc ', { maxLineBreaks: 0 }), 'a b c')
  const text = 'a\n\n\n\nb\n\n\n\nc\n\n\n\nd'
  assert.equal(compactWhitespace(text, { maxLineBreaks: Infinity }), 'a\n\nb\n\nc\n\nd')
  assert.equal(compactWhitespace(text, { consecutiveLineBreakThreshold: Infinity }), 'a\n\n\n\nb\n\n\n\nc d')
  assert.equal(compactWhitespace(text, { maxLineBreaks: Infinity, consecutiveLineBreakThreshold: Infinity }), text)
})

test('compactWhitespace rejects invalid options with a public ValidationError', () => {
  const invalidOptions = [null, false, 2, 'options', [], ...[0, 1, 2].map(value => ({ consecutiveLineBreakThreshold: value }))]
  for (const option of ['maxLineBreaks', 'consecutiveLineBreakThreshold']) {
    for (const value of [-1, 1.5, NaN, -Infinity, Number.MAX_SAFE_INTEGER + 1, null, '2', true]) {
      invalidOptions.push({ [option]: value })
    }
  }
  for (const options of invalidOptions) {
    assert.throws(() => compactWhitespace('text', options), error => {
      assert.ok(error instanceof ValidationError)
      assert.equal(error.code, 'INVALID_WHITESPACE_OPTIONS')
      return true
    })
  }
})

test('compactWhitespace rejects non-string inputs with a public ValidationError', () => {
  for (const value of [undefined, null, 0, false, {}, [], Symbol('text')]) {
    assert.throws(() => compactWhitespace(value), error => {
      assert.ok(error instanceof ValidationError)
      assert.equal(error.code, 'INVALID_WHITESPACE_TEXT')
      return true
    })
  }
})
