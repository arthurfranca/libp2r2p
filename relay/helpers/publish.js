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

// Gives each relay a bounded entry in the final report without cancelling the
// underlying publish, which may still complete after the reporting deadline.
export function settlePublishPromise (promise, timeoutMs) {
  let timer = null
  const publishPromise = Promise.resolve(promise).then(
    () => ({ status: 'fulfilled' }),
    reason => ({ status: 'rejected', reason })
  ).finally(() => clearTimeout(timer))
  const timeoutPromise = new Promise(resolve => {
    timer = maybeUnref(setTimeout(() => resolve({ status: 'rejected', reason: publishTimeoutError() }), timeoutMs))
  })
  return Promise.race([publishPromise, timeoutPromise])
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
