# Reviewer notes: lease-worker

> Spoilers. Read after your attempt.

## Test coverage notes

- **Renewal is signalled to stop but not waited for before `Complete`,** so a
  `Renew` already past its `select` overlaps or follows `Complete`. Caught only
  sometimes, by `TestNoRenewOverlapsOrFollowsComplete` (handler return and a
  renewal tick coincide for 20 jobs) and by the fake store's per-call checks in
  every test, when the scheduler interleaves badly; look for it in review.
- **A result is completed after the lease was lost,** because the decision
  looks only at the handler's error. Caught by
  `TestStaleHandlerResultIsNeverCompleted` (lost via `ErrLeaseLost`) and
  `TestHungRenewDoesNotExtendLease` (lost via the local deadline); the fake
  store reports the forbidden `Complete`.
- **The local deadline is measured from when `Renew` returned, or extended by a
  failed attempt.** Caught by
  `TestTransientRenewErrorsRetriedUntilLocalDeadline` (job context cancelled at
  15s, or never, where 13s is expected).
- **Any `Renew` error treated as fatal, or transient errors ignored forever
  with no deadline.** Caught by
  `TestTransientRenewErrorsRetriedUntilLocalDeadline`.
- **The deadline is checked only on ticks or only inside the renewal
  goroutine,** so a hung `Renew` keeps the job alive past expiry. Caught by
  `TestHungRenewDoesNotExtendLease` (no cancellation at 10s).
- **The in-flight `Renew` is left running after the handler returns, or not
  waited for:** the next job starts while the old renewal runs, or `Run` never
  claims again. Caught by `TestHungRenewDoesNotExtendLease` (renews still in
  flight, or no second `Claim` at 12s).
- **The job context is passed to `Complete` after being cancelled at handler
  return,** so every `Complete` fails with `context.Canceled`. Caught by
  `TestRenewsAtCadenceThenCompletesWithClaimedGeneration` (outcome error is
  not nil).
- **`Run` returns, or claims the next job, before the handler has returned.**
  Caught by `TestCancellationCancelsJobAndWaitsForHandler` and
  `TestStaleHandlerResultIsNeverCompleted`; the fake store also reports any
  `Claim` issued before the previous job was reported.
- **`time.Sleep` for the poll wait,** so `Run` ignores cancellation for up to
  `pollInterval`. Caught by `TestPollsAfterFailedClaimAndStopsPromptly` (`Run`
  returns later than 22s).
- **`Run` returns on a `Claim` error, or waits `pollInterval` after every job.**
  Caught by `TestPollsAfterFailedClaimAndStopsPromptly` (wrong `Claim`
  timestamps).
- **A store call or the handler gets a context not derived from `ctx`** (for
  example `context.Background()` for `Complete`). Caught by the fake store's
  context check in every test that reaches that call; for the handler, by
  `TestRenewsAtCadenceThenCompletesWithClaimedGeneration`.
- **The job context is cancelled with a plain `cancel()`,** so the handler
  cannot tell a lost lease from a shutdown; or the cause is `ErrLeaseLost`
  after a shutdown. Caught by `TestLostLeaseCancelsHandlerAndSuppressesComplete`,
  `TestTransientRenewErrorsRetriedUntilLocalDeadline`,
  `TestHungRenewDoesNotExtendLease`, and (for the shutdown case)
  `TestCancellationCancelsJobAndWaitsForHandler`.
- **Errors compared with `==`,** so a wrapped `ErrLeaseLost` from `Renew` is
  treated as transient and the handler keeps running on a lease another worker
  may hold. Caught by `TestLostLeaseCancelsHandlerAndSuppressesComplete`,
  whose store wraps the error.
- **A fenced `Complete` (`ErrLeaseLost`) stops the loop or is reported as a
  generic error.** Caught by `TestCompleteLeaseLostIsReportedAndRunContinues`.
- **Unstopped tickers and timers** (`time.Tick`, a ticker or `AfterFunc` never
  stopped). Not caught by the tests; look for it in review.

## Open choices

- Whether a result that arrives after `ctx` was cancelled is completed or
  abandoned is the solver's choice, and so is the context `Complete` gets in
  that case. `TestCancellationCancelsJobAndWaitsForHandler` accepts either
  outcome (`nil` with one `Complete`, or `context.Canceled`).
- Which goroutine runs the handler and which runs the renewal loop is left to
  the solver.
- Whether each `Renew` call gets its own timeout is not required and not
  tested; any consistent choice is fine if the user can justify it.

## Follow-up questions

- How do you choose `renewEvery` for a 30-second lease, given store latency
  and clock skew between worker and store?
- The handler writes to a database that knows nothing about leases. How do you
  make the stale handler's writes harmless?
- Should a failed handler release the lease instead of letting it expire?
  What would `Release` need to check, and what does it buy?
- `Complete` fails with a transient error. Retry it? Until when?
- Turn this into N concurrent workers in one process. What is shared, and
  what stays per job?
- A job keeps losing its lease because it is slower than expected. What do you
  measure and alert on?
