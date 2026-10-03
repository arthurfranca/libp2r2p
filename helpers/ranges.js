// Inclusive integer ranges [start, end]. Merge treats adjacent ranges as one.
export function mergeRanges (ranges = []) {
  const sorted = (ranges || [])
    .filter(range => range && Number.isFinite(range.start) && Number.isFinite(range.end) && range.end >= range.start)
    .map(range => ({ start: range.start, end: range.end }))
    .sort((a, b) => a.start - b.start || a.end - b.end)
  const out = []
  for (const range of sorted) {
    const last = out[out.length - 1]
    if (!last || range.start > last.end + 1) out.push(range)
    else last.end = Math.max(last.end, range.end)
  }
  return out
}

// Parts of `range` not covered by `covered`.
export function subtractRanges (range, covered = []) {
  if (!range || range.end < range.start) return []
  const merged = mergeRanges(covered)
  const out = []
  let cursor = range.start
  for (const entry of merged) {
    if (entry.end < cursor) continue
    if (entry.start > range.end) break
    if (entry.start > cursor) out.push({ start: cursor, end: Math.min(entry.start - 1, range.end) })
    cursor = Math.max(cursor, entry.end + 1)
    if (cursor > range.end) break
  }
  if (cursor <= range.end) out.push({ start: cursor, end: range.end })
  return out
}
