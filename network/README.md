# Connectivity

`isOnline({ signal, strict = false } = {})` returns a boolean after probing
public connectivity endpoints, or immediately returns `false` when the browser
reports offline. The first candidate probes alone with a 2.5-second timeout; if
it is still pending after 1 second, the remaining candidates start in parallel
with four-second timeouts and the first success wins, keeping the worst case near
five seconds instead of serializing every timeout. Concurrent calls without a
signal share one check per mode. A supplied abort signal cancels the caller's
independent check immediately.

By default a probe only proves the endpoint is reachable (`no-cors`), so HTTP
errors and captive portals still resolve. With `strict: true`, probes use
CORS-enabled endpoints and require `response.ok` plus the expected body marker,
which rejects captive portals and error pages. `createConnectivityMonitor({
strict })` and `onOnline(handler, { strict })` select the same mode; shared
checks, monitors and listeners are kept per mode.

`onOnline(handler)` returns an idempotent unsubscribe function. It notifies
asynchronously after confirmed initial connectivity and each detected recovery.
The native `online` event only triggers a check; it is not sufficient evidence.
Listeners of a mode share a monitor with retries at 5, 15, 30, then 60 seconds
(20% jitter), checks on focus/visibility/network changes, and a 60-second interval
while online. Unsubscribing the last listener removes timers/listeners and aborts
its probe. Handler failures are isolated and logged.
`createConnectivityMonitor(options)` creates an independent monitor with an
optional custom `check({ signal })` and `strict` default.

Successful probes do not guarantee a particular relay or HTTP server is reachable.
Handle request failures, cancellations, and timeouts separately. Browsers can
suspend background tabs, so recovery notification has no wall-clock guarantee.
Consumers should keep their own work queues but should not duplicate probe loops.
