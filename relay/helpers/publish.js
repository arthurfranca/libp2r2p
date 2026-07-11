import { maybeUnref } from './timer.js'

function publishTimeoutError () {
  return new Error('PUBLISH_TIMEOUT')
}

// Resolves once any relay accepts the event, all relays reject, or the first
// acknowledgement deadline expires. Individual relay attempts keep running.
export function firstFulfillment (promises, timeoutMs) {
  return new Promise((resolve) => {
    let settled = false
    let rejected = 0
    const finish = (success) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(success)
    }
    const timer = maybeUnref(setTimeout(() => finish(false), timeoutMs))
    for (const promise of promises) {
      Promise.resolve(promise).then(
        () => finish(true),
        () => {
          rejected++
          if (rejected === promises.length) finish(false)
        }
      )
    }
  })
}

// Gives each relay one bounded terminal result without cancelling the
// underlying publish, which may still complete after the reporting deadline.
export function settlePublishPromise (promise, timeoutMs, { onSettled } = {}) {
  return new Promise((resolve) => {
    let settled = false
    const settle = (result) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      onSettled?.(result)
      resolve(result)
    }
    const timer = maybeUnref(setTimeout(() => {
      settle({ status: 'rejected', outcome: 'timed-out', reason: publishTimeoutError() })
    }, timeoutMs))

    Promise.resolve(promise).then(
      value => settle({ status: 'fulfilled', value }),
      reason => settle({ status: 'rejected', outcome: 'failed', reason })
    )
  })
}

// Turns ordered relay settlements into a stable, caller-facing report. Failed
// relays retain their Error while optional successful URLs make acknowledgements visible.
export function publishSummary (settlements, relays, { result, includeSucceededRelays = false } = {}) {
  const succeededRelays = []
  const errors = []

  settlements.forEach((settlement, index) => {
    if (settlement.status === 'fulfilled') succeededRelays.push(relays[index])
    else errors.push({ relay: relays[index], reason: settlement.reason })
  })

  const summary = {
    success: succeededRelays.length > 0,
    total: relays.length,
    fulfilled: succeededRelays.length,
    errors
  }
  if (result !== undefined) summary.result = result
  if (includeSucceededRelays) summary.succeededRelays = succeededRelays
  return summary
}
