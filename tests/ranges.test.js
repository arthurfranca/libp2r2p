import { test } from 'node:test'
import assert from 'node:assert/strict'

import { mergeRanges, subtractRanges, intersectRanges } from '../helpers/ranges.js'

test('range helpers merge, subtract and intersect inclusive ranges', () => {
  assert.deepEqual(mergeRanges([{ start: 5, end: 9 }, { start: 1, end: 4 }, { start: 12, end: 12 }]), [
    { start: 1, end: 9 },
    { start: 12, end: 12 }
  ])
  assert.deepEqual(subtractRanges({ start: 0, end: 10 }, [{ start: 2, end: 3 }, { start: 6, end: 20 }]), [
    { start: 0, end: 1 },
    { start: 4, end: 5 }
  ])
  assert.deepEqual(intersectRanges(
    [{ start: 0, end: 10 }, { start: 20, end: 30 }],
    [{ start: 5, end: 25 }]
  ), [
    { start: 5, end: 10 },
    { start: 20, end: 25 }
  ])
  assert.deepEqual(intersectRanges([{ start: 0, end: 1 }], [{ start: 2, end: 3 }]), [])
})
