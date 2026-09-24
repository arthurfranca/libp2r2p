import { ValidationError } from '../../error/index.js'
import { maybeUnref } from './timer.js'

export function admissionError (code, relay) {
  return Object.assign(new Error(code), { code, relay, phase: 'admission' })
}

// One FIFO per pooled connection. A feed reserves both REQs atomically; its
// history and live leases can subsequently be released independently.
export class ReadAdmission {
  constructor ({ maxSubscriptionsPerRelay = 28, maxConcurrentHistoryPerRelay = 2, maxQueuedReadsPerRelay = 256 } = {}) {
    for (const value of [maxSubscriptionsPerRelay, maxConcurrentHistoryPerRelay, maxQueuedReadsPerRelay]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new ValidationError('INVALID_RELAY_READ_CAPACITY')
    }
    Object.assign(this, { maxSubscriptionsPerRelay, maxConcurrentHistoryPerRelay, maxQueuedReadsPerRelay })
    this.states = new Map()
  }

  acquire (relay, { history = false, live = false, signal, queueTimeout = 30000 } = {}) {
    if (queueTimeout !== null && (!Number.isFinite(queueTimeout) || queueTimeout < 0)) throw new ValidationError('INVALID_RELAY_TIMEOUT')
    const weight = Number(history) + Number(live)
    if (!weight || weight > this.maxSubscriptionsPerRelay) return Promise.reject(admissionError('RELAY_READ_CAPACITY', relay))
    if (signal?.aborted) return Promise.reject(admissionError('RELAY_READ_CANCELLED', relay))
    let state = this.states.get(relay)
    if (!state) {
      state = { active: 0, history: 0, queue: [] }
      this.states.set(relay, state)
    }
    const deferred = Promise.withResolvers()
    let timer
    const job = {
      history, weight, grant: () => {
        cleanup()
        state.active += weight
        state.history += Number(history)
        const leases = {}
        for (const kind of ['history', 'live']) {
          if (!(kind === 'history' ? history : live)) continue
          let released = false
          leases[kind] = {
            release: () => {
              if (released) return
              released = true
              state.active--
              if (kind === 'history') state.history--
              this.drain(relay, state)
            }
          }
        }
        deferred.resolve(leases)
      }, cancel: code => {
        const index = state.queue.indexOf(job)
        if (index < 0) return
        state.queue.splice(index, 1)
        cleanup()
        deferred.reject(admissionError(code, relay))
        this.drain(relay, state)
      }
    }
    const abort = () => job.cancel('RELAY_READ_CANCELLED')
    const cleanup = () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', abort)
    }
    if (state.queue.length >= this.maxQueuedReadsPerRelay) return Promise.reject(admissionError('RELAY_READ_QUEUE_FULL', relay))
    state.queue.push(job)
    signal?.addEventListener('abort', abort, { once: true })
    if (queueTimeout !== null) timer = maybeUnref(setTimeout(() => job.cancel('RELAY_READ_QUEUE_TIMEOUT'), queueTimeout))
    this.drain(relay, state)
    return deferred.promise
  }

  drain (relay, state) {
    while (state.queue.length) {
      const job = state.queue[0]
      if (state.active + job.weight > this.maxSubscriptionsPerRelay || (job.history && state.history >= this.maxConcurrentHistoryPerRelay)) break
      state.queue.shift().grant()
    }
    if (!state.active && !state.queue.length && this.states.get(relay) === state) this.states.delete(relay)
  }

  cancelQueued (relay) {
    const state = this.states.get(relay)
    if (!state) return
    // Remove the entire queue first: cancelling one entry must not admit another.
    const jobs = state.queue.splice(0)
    for (const job of jobs) {
      state.queue.push(job)
      job.cancel('RELAY_DISCONNECTED')
    }
  }
}
