# Reviewer notes: multi-provider-rpc

> Spoilers. Read after your attempt.

## Test coverage notes

- **The capacity semaphore is used in reverse (acquire by receiving from an empty buffered
  channel).** Every call blocks; tests fail with `deadlock: all goroutines in bubble are blocked`.
- **A mutex is held while a provider runs.** Shows up as a hang until the 60s timeout (for example
  in `TestFirstFreshResponseCancelsLoser` or `TestGlobalConcurrencyLimitAcrossFetchCalls`), because
  synctest does not treat a goroutine blocked on `sync.Mutex` as blocked.
- **Losing calls receive the caller's context, so nothing cancels them when a fresh response
  wins; they run until their own timeout and keep holding capacity.** Caught by
  `TestFirstFreshResponseCancelsLoser`, `TestLateResultsDoNotBlockAfterFetchReturns` (late
  providers never finish), and `TestStaleThenFreshReturnsFresh` (the one-hour provider is still
  waiting when the test returns, and virtual time stops once the test body exits, so synctest
  reports `blocked goroutines remain`).
- **A late result is sent on an unbuffered channel nobody reads any more.** Caught by
  `TestFirstFreshResponseCancelsLoser` and `TestLateResultsDoNotBlockAfterFetchReturns`
  (synctest reports `blocked goroutines remain`).
- **The result channel is closed by the receiver or after the first result.** Late senders panic
  with send on closed channel in `TestFirstFreshResponseCancelsLoser` and
  `TestLateResultsDoNotBlockAfterFetchReturns`.
- **`Fetch` waits for every provider even after a fresh response arrived, or treats
  `Version == MinVersion` as stale.** Caught by `TestStaleThenFreshReturnsFresh` (elapsed time)
  and `TestFirstFreshResponseCancelsLoser`.
- **The latest stale response is kept rather than the freshest.** Caught by
  `TestReturnsFreshestStaleResponse`.
- **A failed provider's response data is returned, or a non-zero `Response` comes with
  `ErrAllProvidersFailed`.** Caught by `TestAllProvidersFailedReturnsZeroResponse`.
- **A per-call timeout fails the whole request, or there is no per-call deadline at all.** Caught by
  `TestPerCallTimeoutCountsAsProviderFailure` and `TestTimeoutStartsAfterCapacityIsAcquired`.
- **The per-call timeout starts before capacity is acquired.** Caught by
  `TestTimeoutStartsAfterCapacityIsAcquired` (budget smaller than the full timeout).
- **The provider context is not derived from the request context.** Caught by
  `TestProviderContextIsBoundedByRequestContext` (wrong deadline).
- **The concurrency limit is per `Fetch` rather than per client.** Caught by
  `TestGlobalConcurrencyLimitAcrossFetchCalls`.
- **No pre-cancellation check: with a cancelled context, a `select` between `ctx.Done()` and a free
  slot picks randomly, so providers are still called.** Caught by
  `TestParentCancellationAndPreCancelledRequest` (20 attempts make it reliable in practice).
- **Waiting for capacity ignores cancellation, so a cancelled request starts its call once a slot
  frees.** Caught by `TestCancelledWhileWaitingForCapacityStartsNoCall`.
- **`Fetch` does not watch the request context while collecting results: it hangs on a call that
  gave up waiting for capacity, or returns a provider error where `ctx.Err()` is expected.** Caught by
  `TestCancelledWhileWaitingForCapacityStartsNoCall` and
  `TestProviderContextIsBoundedByRequestContext`.
- **Capacity is released only on some outcomes (success only, or not on cancellation).** Caught by
  `TestCapacityIsReleasedAfterEveryOutcome`.
- **After a fresh response wins, a call of the same `Fetch` still waiting for capacity acquires a
  slot later and invokes its provider with an already-cancelled context.** Not caught by the
  tests (no test has a fresh win while calls of that request are still queued for capacity); look
  for it in review.

## Open choices

- Whether goroutines are started for every provider up front or only as capacity becomes
  available is left to the solver; the README accepts one goroutine per provider per `Fetch`.
  Any consistent choice is fine if the user can justify it.
- The order in which a request's providers acquire capacity is not specified.
- Ties between stale responses with equal `Version` may be resolved by completion order.
- When a fresh response and request cancellation arrive at the same moment, which one `Fetch`
  returns is not specified; the tests do not exercise that race.
- Provider disagreement at the same version, provider health, and circuit breaking are explicitly
  out of scope.

## Follow-up questions

- Two providers return the same `Version` with different payloads. What should `Fetch` do?
- One provider is consistently stale or slow. How do you stop it from taking global capacity on
  every request?
- Fan-out to every provider multiplies provider load and cost by N. When is that acceptable, and
  what would you do instead?
- The caller retries `Fetch` on `ErrNoFreshResponse`. What could go wrong?
- Which metrics would you emit?
