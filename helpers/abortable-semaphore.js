// A bounded FIFO for asynchronous work. Acquired leases release explicitly;
// queued cancellations never consume capacity or keep idle keyed gates alive.
export function createAbortableSemaphore (capacity, onIdle = () => {}) {
  const queue = []
  let active = 0
  const drain = () => {
    while (active < capacity && queue.length) {
      const job = queue.shift()
      job.signal?.removeEventListener('abort', job.abort)
      active++
      let released = false
      job.resolve(() => {
        if (released) return
        released = true
        active--
        drain()
      })
    }
    if (!active && !queue.length) onIdle()
  }
  return {
    acquire (signal) {
      signal?.throwIfAborted()
      if (queue.length >= 256) return Promise.reject(Object.assign(new Error('PRIVATE_CHANNEL_HISTORY_QUEUE_FULL'), { code: 'PRIVATE_CHANNEL_HISTORY_QUEUE_FULL' }))
      return new Promise((resolve, reject) => {
        const job = {
          signal, resolve, abort: () => {
            const index = queue.indexOf(job)
            if (index < 0) return
            queue.splice(index, 1)
            reject(signal.reason)
            drain()
          }
        }
        signal?.addEventListener('abort', job.abort, { once: true })
        queue.push(job)
        drain()
      })
    }
  }
}
