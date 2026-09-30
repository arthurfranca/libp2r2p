import { getEventHash } from '../../../event/index.js'

// A report describes one outer event, not one relay. Each outer event needs
// at least one relay acknowledgement. Keep the successful fast path immediate.
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
    reason,
    eventId: getEventHash(event),
    eventKind: event.kind,
    reports: failures
  })
}
