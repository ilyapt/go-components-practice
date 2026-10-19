# Multi-provider fetch with freshness

**Difficulty:** hard · **Topics:** bounded fan-out, global concurrency limit, per-call timeout, freshness policy, partial failure, loser cancellation

Ask several interchangeable providers for the same data and return the first
answer that is fresh enough. Think of read replicas of a database: each
replica applies the primary's changes with some lag, and every response
carries the `Version` of the data it was read at — higher means fresher. A
client that has just written data at version N must not read anything older,
so it sends `Request.MinVersion = N`. The same shape appears with caches in
front of a primary, mirrors of an upstream API, or any set of providers that
can lag behind each other. `Source` names the provider that produced a
response.

This task builds on 011-first-success (fan-out, first result wins, cancel the
losers) and 012-timeout-wrapper (per-call deadline under a parent context),
and adds a component-wide concurrency limit and a freshness policy.

Implement `New` and `Fetch` in `starter.go`.

## Requirements

### Selection

- All providers are candidates for the same logical request.
- Return the first successful response with `Version >= Request.MinVersion`.
- Once a fresh response is selected, cancel the remaining calls of that
  `Fetch`.
- A response below `MinVersion` is successful but stale; remember the freshest
  stale response seen so far.
- If every provider finishes without a fresh response but at least one stale
  response succeeded, return the freshest stale response together with
  `ErrNoFreshResponse`.
- If no provider succeeds, return the zero `Response` with
  `ErrAllProvidersFailed`.
- Stale responses with equal `Version` may be resolved by completion
  order.
- A provider error or per-call timeout is a partial failure: it does not fail
  the whole request while other candidates remain.

### Concurrency and context

- `maxConcurrency` limits provider calls across the whole client, including
  calls from concurrent `Fetch` invocations.
- Waiting for capacity respects request cancellation: a call that has not
  acquired capacity when the request ends is never started.
- Each provider call receives a child context bounded by `perCallTimeout` and
  by the request context. The timeout starts when the call actually starts,
  after capacity is acquired; time spent waiting for capacity does not count.
- A pre-cancelled request invokes no providers and returns `ctx.Err()`.
- Request cancellation returns `ctx.Err()` and cancels this request's
  in-flight and not-yet-started calls.
- Capacity is released when a call returns, whatever its outcome.
- Calls still running after `Fetch` returns must never block while
  publishing their result.
- Do not hold a mutex while calling a provider.

Assume the provider list is small and fixed, and that providers return soon
after their context is cancelled. One goroutine per provider per `Fetch` is
acceptable.

## Validation

- An empty provider list, non-positive `maxConcurrency`, or non-positive
  `perCallTimeout` returns `ErrInvalidConfig`.
- A nil provider returns `ErrNilProvider`.

Providers disagreeing about the data at the same version, provider health,
and circuit breaking are follow-up design questions, not requirements.

## Before coding

1. What exactly allows `Fetch` to return early, and what must happen to the
   other calls at that moment?
2. Who owns cancellation of the losing calls, and which context does each
   provider call receive?
3. In what order do you acquire capacity and start the per-call timeout, and
   why?
4. How does a provider that finishes after `Fetch` has returned avoid
   blocking forever?
