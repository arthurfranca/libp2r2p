// Relay errors can be shared by multiple subscriptions or be frozen. Attach
// consumer context to a new error and retain the untouched original as cause.
export function subscriptionError (reason, relay) {
  const message = reason?.message ?? String(reason)
  const error = reason instanceof AggregateError
    ? new AggregateError(reason.errors, message, { cause: reason })
    : new Error(message, { cause: reason })
  for (const key of ['name', 'code', 'category', 'retryAfterMs', 'retryAt', 'closeCode', 'closeReason', 'wasClean']) {
    if (reason?.[key] !== undefined) error[key] = reason[key]
  }
  error.operation = 'private-channel.subscribe'
  if (typeof relay === 'string') error.relay = relay
  return error
}
