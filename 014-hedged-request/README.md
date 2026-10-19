# Hedged request

**Difficulty:** medium · **Topics:** hedging, tail latency, hedge timer, first success wins, loser cancellation, bounded attempts

Send a request to one provider and, if it has not answered within a short
hedge delay, send a backup request to the next provider while the first one
keeps running; keep whichever answer arrives first. Unlike a fan-out to every
provider, hedging pays for extra requests only when the first one is slow:
with the delay set near the 95th percentile latency, about one request in
twenty sends a second attempt, and the slow tail is cut. Database and storage
clients, gRPC's hedging policy, Cassandra's speculative retry, and read paths
in front of replicated backends all use this pattern.

This task builds on 011-first-success (first success wins, cancel the losers,
late results must not block) and 012-timeout-wrapper (derived contexts under a
parent context), and uses the `Provider` / `Request` / `Response` vocabulary of
077-multi-provider-rpc. The new part is time: attempts are started one by one,
driven by a timer and by failures, instead of all at once.

Implement `New` and `Do` in `starter.go`.

```go
func New(providers []Provider, hedgeDelay time.Duration, maxAttempts int) (*Hedger, error)
func (h *Hedger) Do(ctx context.Context, request Request) (Response, error)
```

## Requirements

### Attempts and the hedge timer

- Attempt `i` of a `Do` call (counting from 0) calls
  `providers[i % len(providers)]`. Providers are tried in slice order; when
  `maxAttempts` is larger than the number of providers, the order wraps around
  and the same providers are asked again.
- `Do` starts attempt 0 immediately and starts at most `maxAttempts` attempts
  in total. Every attempt receives `request` unchanged.
- While fewer than `maxAttempts` attempts have started and none has
  succeeded, each of these events starts the next attempt immediately:
  - the hedge timer fires;
  - an attempt returns an error. Each failed attempt starts at most one new
    attempt.
- The hedge timer is per attempt: every time an attempt starts, whatever
  caused the start, the timer is re-armed to fire `hedgeDelay` later. With
  `hedgeDelay` = 10ms, if attempt 0 fails at 3ms, attempt 1 starts at 3ms and
  attempt 2 at 13ms (not at 10ms or 20ms).
- Starting a new attempt never cancels the attempts already in flight: the
  hedge delay is not a timeout, and an earlier attempt may still win.

### Result

- Return the first successful response in completion order, unchanged,
  whichever attempt produced it.
- As soon as an attempt succeeds, cancel every other attempt and start no
  more.
- If every attempt fails, return the zero `Response` and an error for which
  `errors.Is(err, ErrAllAttemptsFailed)` holds, and `errors.Is(err, e)` holds
  for the error `e` of every attempt (`errors.Join` gives you both). Return it
  as soon as the last started attempt fails, and not earlier: while any
  attempt is still in flight, it may still succeed.

### Context and cancellation

- Every attempt receives a context derived from the caller's context. Whether
  attempts share one derived context or each gets its own is your choice.
- If the caller's context is already done, `Do` returns `ctx.Err()` without
  calling any provider.
- If the caller's context is done before an attempt succeeds, `Do` cancels
  every attempt, starts no more, and returns `ctx.Err()`, not
  `ErrAllAttemptsFailed`, even when the attempts fail because of that
  cancellation.
- If a success and the caller's cancellation race, either outcome is
  acceptable.
- Before `Do` returns, whatever the outcome, the context of every attempt it
  started is cancelled, the winner's included.
- `Do` does not wait for cancelled attempts to return.
- An attempt that finishes after `Do` has returned never blocks while
  publishing its result, and no attempt starts after `Do` has returned.

### Concurrency

- `Do` is safe for concurrent use. Each call has its own attempts, hedge timer
  and cancellation: one call's cancellation or result never affects another
  call.
- One goroutine per attempt is acceptable.

Assume providers return soon after their context is cancelled, and that
`Fetch` is idempotent (a read), so sending the same request twice is safe.

## Validation

- An empty provider list, a non-positive `hedgeDelay`, or a non-positive
  `maxAttempts` returns `ErrInvalidConfig`.
- A nil provider returns `ErrNilProvider`.

Per-attempt timeouts, a hedging budget across calls, and an adaptive hedge
delay are follow-up design questions, not requirements.

## Before coding

1. Which events start an attempt, and which numbers does `Do` need to keep to
   know that every attempt has failed rather than that some are still running?
2. Which context does each attempt receive, who cancels it, and at which
   moments: success, total failure, caller cancellation?
3. Who owns the hedge timer, and what happens to it when an attempt fails
   early, when an attempt wins, and when `Do` returns?
4. An attempt finishes after `Do` has returned. What guarantees that its
   goroutine exits, that it never blocks, and that it cannot cause another
   attempt to start?
