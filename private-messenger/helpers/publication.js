import { getEventHash } from '../../event/index.js'

const safeTokenPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,159}$/
const safeToken = value => typeof value === 'string' && safeTokenPattern.test(value) ? value : undefined
const safeRelay = value => {
  if (typeof value !== 'string') return undefined
  try {
    const url = new URL(value)
    if (url.protocol !== 'ws:' && url.protocol !== 'wss:') return undefined
    return `${url.protocol}//${url.host}`
  } catch {
    return undefined
  }
}

function reasonDiagnostic (reason) {
  if (typeof reason === 'string') {
    const prefix = /^([a-z][a-z0-9-]{0,31}):/i.exec(reason)?.[1]
    return { code: safeToken(reason) ?? safeToken(prefix ? `${prefix}:` : undefined) ?? 'UNKNOWN' }
  }
  const prefix = typeof reason?.message === 'string' && /^([a-z][a-z0-9-]{0,31}):/i.exec(reason.message)?.[1]
  const category = safeToken(reason?.category)
  return {
    code: safeToken(reason?.code) ?? safeToken(prefix ? `${prefix}:` : undefined) ?? category ?? 'UNKNOWN',
    ...(category ? { category } : {}),
    ...(safeToken(reason?.name) ? { name: safeToken(reason.name) } : {}),
    ...(Number.isSafeInteger(reason?.retryAt) ? { retryAt: reason.retryAt } : {}),
    ...(Number.isSafeInteger(reason?.retryAfterMs) ? { retryAfterMs: reason.retryAfterMs } : {})
  }
}

function sanitizedReport (report, index) {
  return {
    index,
    ...(Number.isSafeInteger(report?.total) ? { total: report.total } : {}),
    ...(Number.isSafeInteger(report?.fulfilled) ? { fulfilled: report.fulfilled } : {}),
    ...(report?.retryWhenAvailable === true ? { retryWhenAvailable: true } : {}),
    ...(report?.retryWhenOnline === true ? { retryWhenOnline: true } : {}),
    errors: (Array.isArray(report?.errors) ? report.errors : []).map(entry => {
      const relay = safeRelay(entry?.relay)
      const detail = reasonDiagnostic(entry?.reason ?? entry)
      return {
        ...(relay ? { relay } : {}),
        ...(safeToken(entry?.code) ? { ...detail, code: entry.code } : detail)
      }
    })
  }
}

const contextFields = ['operation', 'channelPubkey', 'seederPubkey', 'receiverPubkey', 'eventId']
const contextNumbers = ['eventKind', 'since', 'until']

export function sanitizePublicationError (error, context = {}) {
  const source = error instanceof Error ? error : new Error('PRIVATE_MESSAGE_NOT_PUBLISHED')
  const reports = Array.isArray(source.reports) ? source.reports : []
  const retryReasons = reports.flatMap(report => (Array.isArray(report?.errors) ? report.errors : [])
    .map(entry => entry?.reason).filter(Boolean))
  if (source.code !== 'MESSAGE_NOT_PUBLISHED' && !reports.length) {
    for (const key of contextFields) if (safeToken(context[key])) source[key] = context[key]
    for (const key of contextNumbers) if (Number.isSafeInteger(context[key])) source[key] = context[key]
    return source
  }

  const sanitized = new Error('PRIVATE_MESSAGE_NOT_PUBLISHED')
  if (source.stack) sanitized.stack = source.stack
  Object.assign(sanitized, {
    code: safeToken(source.code) ?? 'MESSAGE_NOT_PUBLISHED',
    ...(safeToken(source.reason) ? { reason: source.reason } : {}),
    ...(safeToken(source.eventId) ? { eventId: source.eventId } : {}),
    ...(Number.isSafeInteger(source.eventKind) ? { eventKind: source.eventKind } : {}),
    ...(source.retryWhenAvailable === true ? { retryWhenAvailable: true } : {}),
    ...(source.retryWhenOnline === true ? { retryWhenOnline: true } : {}),
    reports: reports.map(sanitizedReport)
  })
  for (const key of contextFields) if (safeToken(context[key])) sanitized[key] = context[key]
  for (const key of contextNumbers) if (Number.isSafeInteger(context[key])) sanitized[key] = context[key]
  Object.defineProperty(sanitized, 'retryReasons', { value: retryReasons })
  return sanitized
}

export function publicationFailureDiagnostic (error) {
  const result = {}
  for (const key of [...contextFields, ...contextNumbers]) {
    const value = error?.[key]
    if (safeToken(value) && contextFields.includes(key)) result[key] = value
    else if (Number.isSafeInteger(value) && contextNumbers.includes(key)) result[key] = value
  }
  for (const key of ['code', 'reason', 'eventId']) if (safeToken(error?.[key])) result[key] = error[key]
  for (const key of ['retryWhenAvailable', 'retryWhenOnline']) if (error?.[key] === true) result[key] = true
  result.reports = (Array.isArray(error?.reports) ? error.reports : []).map(sanitizedReport)
  return result
}

// A report describes one outer event, not one relay. Each outer event needs
// accepted publication covering its routed recipients. Keep the successful fast
// path immediate; a partially covered multi-recipient report remains a failure.
export async function assertMessagePublished (result, event) {
  const reports = result?.delivery?.reports
  const present = Array.isArray(reports) && reports.length > 0
  if (present && reports.every(report => report?.success === true)) return

  const failures = present
    ? await Promise.all(reports.map(async (report, index) => {
      if (report?.success === true) return null
      let settled
      try { settled = await report?.promise } catch (reason) { settled = { errors: [{ reason }] } }
      // Never retain the broadcast result: it includes plaintext and may carry
      // delivery.deletionSeckey. Native relay errors retain transport causes.
      return {
        index,
        total: settled?.total ?? report?.total,
        ...(report?.retryWhenAvailable ? { retryWhenAvailable: true } : {}),
        ...(report?.retryWhenOnline ? { retryWhenOnline: true } : {}),
        fulfilled: settled?.fulfilled,
        errors: (settled?.errors || []).map(({ relay, reason }) => ({ relay, reason }))
      }
    })).then(values => values.filter(Boolean))
    : []
  const reason = present ? 'RELAY_PUBLICATION_FAILED' : 'NO_DELIVERY_REPORTS'
  const details = failures.map(report => {
    const errors = report.errors.map(({ relay, reason }) => {
      const category = reason?.category ? ` [${reason.category}]` : ''
      return `${relay || 'unknown relay'}${category}: ${reason?.message || String(reason)}`
    })
    return `report ${report.index + 1} (${report.total ?? '?'} relays): ${errors.join('; ') || (report.total === 0 ? 'NO_RELAYS' : 'NO_RELAY_ACKNOWLEDGEMENT')}`
  })
  throw Object.assign(new Error(`MESSAGE_NOT_PUBLISHED: ${details.join(' | ') || reason}`), {
    code: 'MESSAGE_NOT_PUBLISHED',
    ...(failures.length && failures.every(report => report.retryWhenAvailable) ? { retryWhenAvailable: true, ...(failures.some(report => report.retryWhenOnline) ? { retryWhenOnline: true } : {}) } : {}),
    reason,
    eventId: getEventHash(event),
    eventKind: event.kind,
    reports: failures
  })
}
