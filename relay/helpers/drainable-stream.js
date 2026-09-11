// Stopping closes network input but keeps accepted events available to next().
// Returning/throwing, or aborting the caller signal, cancels consumption as well.
export function drainableStream (create, options = {}) {
  const cancel = new AbortController()
  const stop = new AbortController()
  const signal = options.signal ? AbortSignal.any([options.signal, cancel.signal]) : cancel.signal
  const stopSignal = options._stopSignal ? AbortSignal.any([options._stopSignal, stop.signal]) : stop.signal
  const stream = create({ ...options, signal, _stopSignal: stopSignal })
  const returnStream = stream.return.bind(stream)
  const throwStream = stream.throw.bind(stream)
  Object.defineProperties(stream, {
    stopAndDrain: { value: () => stop.abort() },
    return: { value: value => { cancel.abort(); return returnStream(value) } },
    throw: { value: error => { cancel.abort(); return throwStream(error) } }
  })
  return stream
}
