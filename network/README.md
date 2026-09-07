# Connectivity

`isOnline({ signal } = {})` returns a boolean after probing public connectivity
endpoints, or immediately returns `false` when the browser reports offline.
Concurrent calls without a signal share one check. A supplied abort signal
cancels the caller's independent check. Each endpoint has a five-second timeout.

`onOnline(handler)` returns an idempotent unsubscribe function. It notifies
asynchronously after confirmed initial connectivity and each detected recovery.
The native `online` event only triggers a check; it is not sufficient evidence.
Listeners share a monitor with retries at 5, 15, 30, then 60 seconds (20% jitter),
checks on focus/visibility/network changes, and a 60-second interval while online.
Unsubscribing the last listener removes timers/listeners and aborts its probe.
Handler failures are isolated and logged. `createConnectivityMonitor(options)`
creates an independent monitor with an optional custom `check({ signal })`.

Successful probes do not guarantee a particular relay or HTTP server is reachable.
Handle request failures, cancellations, and timeouts separately. Browsers can
suspend background tabs, so recovery notification has no wall-clock guarantee.
Consumers should keep their own work queues but should not duplicate probe loops.
