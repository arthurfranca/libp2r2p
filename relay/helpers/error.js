// Adds transport context without replacing native messages, codes or causes.
export function categorizeRelayError (reason, category, fallback = 'RELAY_OPERATION_FAILED') {
  const error = reason instanceof Error ? reason : new Error(String(reason || fallback))
  try {
    Object.defineProperty(error, 'category', { value: category, enumerable: true, configurable: true })
    return error
  } catch {
    const wrapped = error instanceof AggregateError
      ? new AggregateError(error.errors, error.message, { cause: error })
      : new Error(error.message, { cause: error })
    wrapped.name = error.name
    if (error.code !== undefined) wrapped.code = error.code
    wrapped.category = category
    return wrapped
  }
}

// A timeout indicates missing confirmation, even when a socket error preceded it.
export function relayTimeoutError (message, cause) {
  return categorizeRelayError(new Error(message, cause ? { cause } : undefined), 'timeout')
}

// WebSocket close codes are distinct from native system error codes.
export function relayCloseError (event, category, cause) {
  const error = new Error(event?.reason || 'CONNECTION_CLOSED', cause ? { cause } : undefined)
  if (event?.code !== undefined) error.closeCode = event.code
  if (event?.reason !== undefined) error.closeReason = event.reason
  if (event?.wasClean !== undefined) error.wasClean = event.wasClean
  return categorizeRelayError(error, category)
}
