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


## Expedited connectivity detection

`onOnline(handler, { strict = false, expediteForMs = 0, onDebug })` can request
an optional urgent window (clamped to 30,000 ms; finite, nonnegative durations).
The same realm/mode monitor coalesces subscriptions: urgent requests can advance
the next check, at least two seconds after the previous start. Failed checks
repeat at five seconds between starts, without overlapping slow checks. Native
online/focus/visibility wakeups remain available. Removing a subscription removes
its demand without stopping other listeners.

The urgent loop does not spend/reset the ordinary jittered backoff. At expiry it
returns to the ordinary deadline, or a fresh interval of the preserved retry step
if that deadline passed; it never catches up missed checks. For example, a normal
60-second wait remains a 60-second step after an unsuccessful urgent window.
Connectivity returning after the window must await normal recovery or a new user
send/retry; automatic retries never extend urgency. `expediteForMs` is scheduling
advice, not evidence of Internet connectivity or a relay/ACK delivery deadline.

Private-message sessions remember a runtime-only 30-second deadline for new
remote intents and manual retry. Only work actually waiting for network subscribes
with the remaining time, sharing one subscription per session. Self-chat, restored
outbox entries, chunks and automatic retries never renew that deadline. Lock and
close discard urgency. A confirmed monitor result resumes the existing automatic
pause owner without another immediate probe; explicit pauses remain caller-owned.

Optional `onDebug` reports only probe reason, state, remaining window and duration.
Consumers wire it only in development; it is disabled by default. The monitor
retains no diagnostic history. Session diagnostics additionally correlate the
network wait and pause-release duration with the event ID.
