import { maybeUnref } from './timer.js'

function publishTimeoutError () {
  return new Error('PUBLISH_TIMEOUT')
}

// Resolves once any relay accepts the event, all relays reject, an optional
// early deadline expires, or the full report has already finished.
export function firstFulfillment (promises, timeout, { fallback } = {}) {
  return new Promise((resolve) => {
    let settled = false
    let rejected = 0
    let timer = null
    const finish = (success) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(success)
    }
    if (timeout !== null) timer = maybeUnref(setTimeout(() => finish(false), timeout))
    for (const promise of promises) {
      Promise.resolve(promise).then(
        () => finish(true),
        () => {
          rejected++
          if (rejected === promises.length) finish(false)
        }
      )
    }
    if (fallback) Promise.resolve(fallback).then(finish, () => finish(false))
  })
}

// Gives every relay one terminal result under a single operation-wide deadline.
// timeout() also lets the early-return path close every pending report at once.
export function createPublishSettlements (promises, timeout, { onSettled } = {}) {
  const settlements = new Array(promises.length)
  let remaining = promises.length
  let timer = null
  let isFinished = false
  let resolve

  const promise = new Promise(nextResolve => { resolve = nextResolve })
  const finish = () => {
    if (isFinished) return
    isFinished = true
    clearTimeout(timer)
    resolve(settlements)
  }

  const settle = (index, settlement) => {
    if (settlements[index]) return
    settlements[index] = settlement
    onSettled?.(settlement, index)
    remaining--
    if (remaining === 0) finish()
  }

  const timeoutPending = () => {
    if (isFinished) return
    for (let index = 0; index < settlements.length; index++) {
      settle(index, { status: 'rejected', outcome: 'timed-out', reason: publishTimeoutError() })
    }
  }

  if (remaining === 0) finish()
  else {
    if (timeout !== null) timer = maybeUnref(setTimeout(timeoutPending, timeout))
    promises.forEach((promise, index) => {
      Promise.resolve(promise).then(
        value => settle(index, { status: 'fulfilled', value }),
        reason => settle(index, { status: 'rejected', outcome: 'failed', reason })
      )
    })
  }

  return { promise, timeout: timeoutPending }
}

// Turns ordered relay settlements into a stable, caller-facing report. Failed
// relays retain their Error while optional successful URLs make acknowledgements visible.
export function publishSummary (settlements, relays, { includeSucceededRelays = false } = {}) {
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
  if (includeSucceededRelays) summary.succeededRelays = succeededRelays
  return summary
}
