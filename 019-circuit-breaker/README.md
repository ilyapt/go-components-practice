# Circuit breaker

**Difficulty:** hard · **Topics:** state machine, half-open probe, epoch fencing, injected clock, lock scope

Wrap a client so that a dependency that keeps failing stops receiving traffic
for a while, then gets exactly one trial call before traffic resumes. Circuit
breakers sit in front of RPC providers, databases, and third-party APIs in
service meshes, HTTP clients, and job runners, where hammering a broken
dependency only makes recovery slower.

Implement `New` and `Call` in `starter.go`.

## Requirements

### Closed

- Calls are allowed and may execute concurrently.
- A successful call resets the consecutive failure count to zero.
- An error for which the `FailureClassifier` returns `true` increments the
  consecutive failure count.
- Other errors are returned to the caller but do not change breaker health.
- Reaching `failureThreshold` opens the breaker and starts a cooldown.

### Open

- Calls return `ErrCircuitOpen` without invoking the client.
- When `now() >= openedAt + cooldown`, the first caller is admitted as the
  single half-open probe.

### Half-open

- While the probe is running, other calls return `ErrCircuitOpen`.
- Probe success closes the breaker and resets its failure count.
- Any probe error (classified or not) reopens the breaker and starts a new
  cooldown counted from probe completion.

### Concurrency

- Never hold the breaker mutex while calling the wrapped client.
- A pre-cancelled caller invokes no client call, does not change breaker
  state, and receives `ctx.Err()`.
- Closed-state calls admitted in the same period update health in completion
  order.
- Once one of those calls opens the breaker, later completions from the old
  closed period must not close it, reset it, or count toward its new state.
  This prevents a slow success from an earlier period from closing a breaker
  that newer failures have opened.
- Exactly one half-open probe may run per cooldown.
- The clock boundary is inclusive: a probe is allowed when `now()` equals the
  cooldown deadline.
- The breaker reads time only through the injected `now` function.

## Validation

- A nil client returns `ErrNilClient`.
- A nil classifier returns `ErrNilClassifier`.
- A nil clock, a non-positive threshold, or a non-positive cooldown returns
  `ErrInvalidConfig`.

## Before coding

1. What state is captured when a call is admitted, and why is it needed when
   the call completes?
2. Why must the client call happen outside the mutex, and what can change
   while it runs?
3. Which completions are allowed to change the state of a half-open breaker,
   and which must be ignored?
4. What exactly makes the half-open probe single?
