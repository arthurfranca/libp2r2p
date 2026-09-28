const describe = reason => `${reason?.category ? `[${reason.category}] ` : ''}${reason?.message || String(reason)}`

export function incompleteFetchError ({ errors, report, request, receivedEventCount, elapsedMs }) {
  // Keep only read metadata, never events, decrypted payloads or signers.
  const relayErrors = errors.map(({ relay, reason }) => ({ relay, reason }))
  const relays = report.map(({ relay, status, error }) => ({ relay, status, ...(error ? { error } : {}) }))
  const reasons = [...new Set([...relayErrors.map(entry => entry.reason), ...relays.map(entry => entry.error)].filter(reason => reason !== undefined))]
  const details = relays.map(({ relay, status, error }) => {
    const failures = relayErrors.filter(entry => entry.relay === relay).map(entry => entry.reason)
    if (error && !failures.includes(error)) failures.push(error)
    return `${relay} [${status}]${failures.length ? `: ${failures.map(describe).join('; ')}` : ''}`
  })
  for (const { relay, reason } of relayErrors) {
    if (!relays.some(entry => entry.relay === relay)) details.push(`${relay || 'unknown relay'}: ${describe(reason)}`)
  }
  return Object.assign(new AggregateError(reasons,
    `PRIVATE_CHANNEL_FETCH_INCOMPLETE: ${details.join(' | ')} (received=${receivedEventCount}, readElapsedMs=${elapsedMs}, timeoutMs=${request.timeoutMs})`), {
    code: 'PRIVATE_CHANNEL_FETCH_INCOMPLETE',
    operation: 'private-channel.fetch',
    request,
    receivedEventCount,
    elapsedMs,
    relays,
    relayErrors
  })
}
