// Bounded diagnostics only: never attach queued payloads or subscription filters.
export function liveBufferError ({ relay, stage, queue, queuedBytes, incoming, incomingBytes, maxEvents, maxBytes, since }) {
  const now = Date.now()
  const boundaries = [since, incoming?.event?.created_at, ...queue.map(entry => entry.item?.event?.created_at)]
    .filter(value => Number.isFinite(value) && value >= 0)
  return Object.assign(new Error('RELAY_LIVE_BUFFER_FULL'), {
    code: 'RELAY_LIVE_BUFFER_FULL', relay, phase: 'live-buffer',
    recoverySince: Math.min(Math.floor(now / 1000), ...boundaries),
    buffer: {
      stage, queuedEvents: queue.length, queuedBytes, incomingBytes,
      limits: { events: maxEvents, bytes: maxBytes },
      oldestQueuedMs: queue.length ? Math.max(0, now - queue[0].receivedAt) : 0
    }
  })
}
