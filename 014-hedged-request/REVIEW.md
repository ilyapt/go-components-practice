# Reviewer notes: hedged-request

> Spoilers. Read after your attempt.

## Test coverage notes

- **The hedge delay acts as a timeout: attempt 0 is cancelled when attempt 1 starts.** Caught by
  `TestEarlierAttemptKeepsRunningAfterHedge`.
- **Attempts follow a fixed schedule (`k * hedgeDelay`, a ticker), or the timer is not re-armed on
  a failure-triggered start (for example, a new timer per start while the old one still fires).**
  Caught by `TestFailureStartsNextAttemptAndRestartsHedgeTimer` (C starts at 10ms, not 13ms).
- **A failure waits for the timer, or only the latest attempt's failure starts the next attempt.**
  Caught by `TestFailureStartsNextAttemptAndRestartsHedgeTimer` and
  `TestFailureOfEarlierAttemptStartsNextAttempt`.
- **Hedging continues past `maxAttempts`, or providers are taken out of order.** Caught by
  `TestHedgesUpToMaxAttemptsInProviderOrder` and `TestSingleAttemptDoesNotHedge`.
- **Index out of range, or attempts capped at `len(providers)`, when `maxAttempts` exceeds the
  number of providers.** Caught by `TestAttemptsWrapAroundProviders`.
- **Total failure is declared when the last-started attempt fails, or when a failure arrives after
  `maxAttempts` started, while an earlier attempt is still in flight.** Caught by
  `TestAllAttemptsFailedReturnsSentinelAfterLastFailure` (returns at 12ms, not 15ms).
- **`Do` never returns after total failure: it keeps waiting on a timer or on a result that never
  comes.** Caught by `TestAllAttemptsFailedReturnsSentinelAfterLastFailure` and
  `TestSingleAttemptDoesNotHedge` (synctest reports the deadlock; with a ticker still running,
  virtual time keeps moving and the test spins until the 60s timeout instead).
- **Only the last error is kept, or a non-zero `Response` comes with the error.** Caught by
  `TestAllAttemptsFailedReturnsSentinelAfterLastFailure`.
- **Failures caused by the caller's cancellation are counted as real failures, so a new attempt
  starts or `ErrAllAttemptsFailed` is returned.** Caught by
  `TestCallerCancellationCancelsAttemptsAndStopsHedging` and
  `TestHedgesUpToMaxAttemptsInProviderOrder`, but only when an attempt's failure is received before
  the cancellation is noticed; a pass is not proof, so look for it in review.
- **Losers are not cancelled (the caller's context passed straight to `Fetch`, or the cancel not
  reached on some return path), or the winner's context stays live.** Caught by
  `TestHedgeStartsNextProviderAfterDelay`, `TestAttemptsRunUnderCallerContextAndAreCancelledOnReturn`,
  and every test that calls `assertAllCancelled`.
- **Attempts are detached from the caller (`context.Background()`), or the request is altered.**
  Caught by `TestAttemptsRunUnderCallerContextAndAreCancelledOnReturn`.
- **A late result blocks on an unbuffered channel, or panics on a channel closed by `Do`.** Caught
  by `TestDoesNotWaitForCancelledAttempts` (synctest reports `blocked goroutines remain`, or send on
  closed channel panics).
- **`Do` waits for cancelled attempts to return before returning.** Caught by
  `TestDoesNotWaitForCancelledAttempts` ("Do has not returned").
- **An attempt starts after `Do` returned (for example, from a `time.AfterFunc` callback).** Caught
  by the late-attempt checks in `TestFastAnswerSendsNoHedge` and `TestAttemptsWrapAroundProviders`;
  the race detector usually reports the shared attempt counter first.
- **Per-call state (attempt counter, provider index, timer) lives on the `Hedger`, so concurrent
  calls interfere.** Caught by `TestConcurrentCallsAreIndependent`.
- **A pre-cancelled caller still gets attempt 0.** Caught by
  `TestPreCancelledContextStartsNoAttempts`.

## Open choices

- Whether attempts share one derived context or each gets its own is explicitly the solver's
  choice. Any consistent choice is fine if the user can justify it.
- When a success and the caller's cancellation race, either outcome is acceptable; the tests do not
  exercise that race.
- Beyond the `errors.Is` requirements, the shape of the total-failure error (order of attempt
  errors, extra context in the message) is left to the solver.
- Per-attempt timeouts, a hedging budget, and an adaptive delay are explicitly out of scope.

## Follow-up questions

- How do you choose `hedgeDelay`?
- During an incident every request is slow, so every request hedges and the load doubles exactly
  when the backend is weakest. How do you bound it?
- What if the request is a write?
- Cancelling an attempt here only cancels a context. What does the backend see?
- What is the difference between a hedge and a retry, and how would you combine this with a
  per-attempt timeout (012) and backoff (017)?
- Should the next attempt skip a provider whose circuit breaker is open or that just failed for
  this request?
- Which metrics show hedging is worth it?
