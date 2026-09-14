import { ValidationError } from '../../error/index.js'

// Compacts display text, defaulting to collapsing runs of three or more line
// breaks to two and keeping eight overall. extractMedia preserves input.
export function compactWhitespace (text, options = {}) {
  if (typeof text !== 'string') {
    throw new ValidationError('INVALID_WHITESPACE_TEXT', { message: 'TEXT_SHOULD_BE_A_STRING' })
  }

  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new ValidationError('INVALID_WHITESPACE_OPTIONS', { message: 'OPTIONS_SHOULD_BE_AN_OBJECT' })
  }
  const { maxLineBreaks = 8, consecutiveLineBreakThreshold = 3 } = options
  if (maxLineBreaks !== Infinity && (!Number.isSafeInteger(maxLineBreaks) || maxLineBreaks < 0)) {
    throw new ValidationError('INVALID_WHITESPACE_OPTIONS', { message: 'MAX_LINE_BREAKS_SHOULD_BE_A_NON_NEGATIVE_SAFE_INTEGER_OR_INFINITY' })
  }
  if (consecutiveLineBreakThreshold !== Infinity && (!Number.isSafeInteger(consecutiveLineBreakThreshold) || consecutiveLineBreakThreshold < 3)) {
    throw new ValidationError('INVALID_WHITESPACE_OPTIONS', { message: 'CONSECUTIVE_LINE_BREAK_THRESHOLD_SHOULD_BE_A_SAFE_INTEGER_AT_LEAST_THREE_OR_INFINITY' })
  }

  let remainingLineBreaks = maxLineBreaks
  return text
    .replace(/\r/g, '')
    .replace(/[\t ]+/g, ' ')
    .replace(/ ?\n ?/g, '\n')
    .replace(/\n+/g, lineBreaks => {
      const consecutive = lineBreaks.length >= consecutiveLineBreakThreshold ? 2 : lineBreaks.length
      const kept = Math.min(remainingLineBreaks, consecutive)
      remainingLineBreaks -= kept
      return lineBreaks.slice(0, kept) + (kept < consecutive ? ' ' : '')
    })
    .trim()
}
