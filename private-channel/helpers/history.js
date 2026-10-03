import { ValidationError } from '../../error/index.js'
import { normalizeRelayUrl } from '../../url/index.js'
import { createAbortableSemaphore } from '../../helpers/abortable-semaphore.js'
import { mergeRanges } from '../../helpers/ranges.js'
import { incompleteFetchError } from './fetch-error.js'

const PAGE_SIZE = 16
const MAX_PAGE_SIZE = 256
const MAX_PAGE_BYTES = 4 * 1024 * 1024
const TIMEOUT_MS = 5000
const relayGates = new Map()
const encoder = new TextEncoder()
const pageLimitError = () => Object.assign(new Error('PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT'), { code: 'PRIVATE_CHANNEL_HISTORY_PAGE_LIMIT' })

function acquireRelay (relay, signal) {
  signal?.throwIfAborted()
  let gate = relayGates.get(relay)
  if (!gate) {
    gate = createAbortableSemaphore(1, () => { if (relayGates.get(relay) === gate) relayGates.delete(relay) })
    relayGates.set(relay, gate)
  }
  return gate.acquire(signal)
}

// Queries bounded temporal leaves, oldest first. A saturated one-second leaf
// cannot be split using standard NIP-01; expand it only up to the safe ceiling.
export async function readHistory ({ filter, relays, receiverPubkey, signal, getEvents, processEvent, acquirePage, partial = false, resume = null }) {
  const since = filter.since ?? 0
  const until = filter.until ?? Math.floor(Date.now() / 1000)
  if (![since, until].every(value => Number.isSafeInteger(value) && value >= 0) || since > until) throw new ValidationError('INVALID_PRIVATE_CHANNEL_HISTORY_RANGE')
  if (!Array.isArray(relays) || !relays.length) throw new ValidationError('NO_RELAYS')
  const urls = [...new Set(relays.map(normalizeRelayUrl))]
  const report = []
  const errors = []
  let oldestCreatedAt = null
  let receivedEventCount = 0
  let elapsedMs = 0
  const process = async result => {
    result.sort((a, b) => a.event.created_at - b.event.created_at || String(a.event.id).localeCompare(String(b.event.id)))
    for (const { event } of result) {
      signal?.throwIfAborted()
      await processEvent(event)
      receivedEventCount++
      oldestCreatedAt = Math.min(oldestCreatedAt ?? event.created_at, event.created_at)
    }
  }
  for (const relay of urls) {
    const relayStartCount = receivedEventCount
    const intervals = resumeIntervalsFor({ resume, relay, since, until })
      .map(interval => ({ since: interval.start, until: interval.end, limit: PAGE_SIZE }))
    const covered = []
    let outcome = { relay, status: 'eose' }
    while (intervals.length) {
      signal?.throwIfAborted()
      const interval = intervals.pop()
      const started = performance.now()
      let releaseRelay
      let releasePage
      let page
      try {
        let bytes = 0
        try {
          releaseRelay = await acquireRelay(relay, signal)
          releasePage = await acquirePage?.(signal)
          signal?.throwIfAborted()
          page = await getEvents({ ...filter, ...interval }, [relay], {
            signal, timeout: TIMEOUT_MS, timeoutAfterFirstEose: null,
            callback: item => {
              if (item.type !== 'event') return
              bytes += encoder.encode(JSON.stringify(item.event)).byteLength
              if (bytes > MAX_PAGE_BYTES) throw pageLimitError()
            }
          })
          // Also enforce the contract for injected readers that omit callbacks.
          let resultBytes = 0
          for (const { event } of page.result) {
            resultBytes += encoder.encode(JSON.stringify(event)).byteLength
            if (resultBytes > MAX_PAGE_BYTES) throw pageLimitError()
          }
        } catch (error) {
          signal?.throwIfAborted()
          errors.push({ relay, reason: error })
          outcome = { relay, status: 'error', error }
          intervals.push(interval)
          break
        } finally { elapsedMs += performance.now() - started }
        signal?.throwIfAborted()
        const status = page.relays?.find(item => item.relay === relay)
        if (page.errors?.length || !status || !['eose', 'satisfied'].includes(status.status)) {
          await process(page.result)
          errors.push(...(page.errors ?? []))
          outcome = status ?? { relay, status: 'error' }
          intervals.push(interval)
          break
        }
        if (status.status === 'satisfied' || page.result.length >= interval.limit) {
          if (interval.since < interval.until) {
            const middle = Math.floor(interval.since + (interval.until - interval.since) / 2)
            intervals.push({ since: middle + 1, until: interval.until, limit: PAGE_SIZE }, { since: interval.since, until: middle, limit: PAGE_SIZE })
          } else if (interval.limit < MAX_PAGE_SIZE) {
            intervals.push({ ...interval, limit: interval.limit * 2 })
          } else {
            await process(page.result)
            const error = pageLimitError()
            errors.push({ relay, reason: error })
            outcome = { relay, status: 'error', error }
            intervals.push(interval)
            break
          }
        } else {
          await process(page.result)
          covered.push({ start: interval.since, end: interval.until })
        }
        signal?.throwIfAborted()
      } finally {
        releasePage?.()
        releaseRelay?.()
      }
    }
    report.push({
      ...outcome,
      ...(partial
        ? {
            covered: mergeRanges(covered),
            pending: mergeRanges(intervals.map(interval => ({ start: interval.since, end: interval.until }))),
            events: receivedEventCount - relayStartCount
          }
        : {})
    })
  }
  signal?.throwIfAborted()
  if (partial) {
    const pendingByRelay = {}
    for (const entry of report) {
      if (entry.pending?.length) pendingByRelay[entry.relay] = entry.pending
    }
    return {
      oldestCreatedAt,
      receivedEventCount,
      elapsedMs: Math.round(elapsedMs),
      relays: report,
      pendingByRelay,
      anyEose: report.some(entry => entry.status === 'eose'),
      anyEoseWithEvents: report.some(entry => entry.status === 'eose' && (entry.events ?? 0) > 0),
      allFailed: report.every(entry => entry.status !== 'eose')
    }
  }
  if (errors.length || report.some(item => item.status !== 'eose')) {
    const error = incompleteFetchError({ errors, report, receivedEventCount, elapsedMs: Math.round(elapsedMs), request: { relays: urls, channelPubkeys: filter.authors ?? [], receiverPubkey, since, until, limit: PAGE_SIZE, timeoutMs: TIMEOUT_MS } })
    error.operation = 'private-channel.fetchHistory'
    throw error
  }
  return { oldestCreatedAt, receivedEventCount, relays: report }
}

function resumeIntervalsFor ({ resume, relay, since, until }) {
  const pending = resume && Object.prototype.hasOwnProperty.call(resume, relay) ? resume[relay] : null
  const intervals = Array.isArray(pending) ? pending : [{ start: since, end: until }]
  return mergeRanges(intervals
    .map(interval => ({
      start: Math.max(since, Math.floor(Number(interval?.start))),
      end: Math.min(until, Math.floor(Number(interval?.end)))
    }))
    .filter(interval => Number.isSafeInteger(interval.start) && Number.isSafeInteger(interval.end) && interval.end >= interval.start))
}
