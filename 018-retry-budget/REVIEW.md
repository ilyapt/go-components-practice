# Reviewer notes: retry-budget

> Spoilers. Read after your attempt.

## Test coverage notes

- **The balance check and the withdrawal are separate steps (two lock acquisitions, or an atomic
  load followed by an atomic add), so two callers both see one token and both retry.** Without any
  synchronisation, the race detector reports it in `TestBudgetIsSharedAndNeverOverspent`. Built
  from separate atomic operations, it passes the race detector and fails that test only when the
  scheduler interleaves badly; look for it in review.
- **A lock is held while `op` runs.** `TestBudgetIsSharedAndNeverOverspent` hangs until the 60s
  timeout, because synctest does not treat a goroutine blocked on `sync.Mutex` as blocked.
- **A lock is held during the backoff wait.** Not caught by the tests (the concurrent test uses a
  zero backoff); look for it in review.
- **Accrual runs in a refill goroutine with a ticker.** Every test fails with
  `blocked goroutines remain`.
- **Accrual is credited in whole seconds, or the accrual timestamp moves forward on a refused call
  without crediting what accrued.** Caught by `TestMinRetriesPerSecondRefillsQuietBudget`.
- **Accrual is credited as whole tokens every `1/rate` seconds, dropping fractions that a deposit
  could have topped up.** Not caught by the tests; look for it in review.
- **The clock is read outside the critical section, so two callers apply accrual out of order and
  elapsed time goes negative, subtracting tokens.** Not caught by the tests (virtual time does not
  move between the read and the update); look for it in review.
- **No cap at `Burst`: deposits or accrual pile up.** Caught by `TestBalanceNeverExceedsBurst` and
  `TestMinRetriesPerSecondRefillsQuietBudget`.
- **A token is withdrawn for a success, a permanent error, or the final allowed attempt.** Caught
  by `TestSuccessAndPermanentErrorsSpendNoTokens` and
  `TestExhaustedAttemptsReturnLastErrorWithoutWithdrawing`.
- **After the final attempt, `Do` still waits a backoff, or wraps the last error.** Caught by
  `TestExhaustedAttemptsReturnLastErrorWithoutWithdrawing` (elapsed time, exact error text).
- **`Backoff` receives the wrong attempt number.** Caught by
  `TestRetriesWithBackoffWhileBudgetLasts`.
- **The balance goes negative (decrement, then check), so a refused retry still costs a token.**
  Caught by `TestEveryFirstAttemptDepositsRatio`.
- **`Ratio` is deposited only on success, or only for calls that end up retrying.** Caught by
  `TestEveryFirstAttemptDepositsRatio`.
- **A pre-cancelled call deposits.** Caught by
  `TestPreCancelledContextCallsNothingAndDepositsNothing`.
- **A nil `op` deposits.** Not caught: `TestValidation` checks only the error, on a budget with
  `Ratio = 0`.
- **The backoff is waited before asking the budget, or a refusal waits anyway.** Caught by
  `TestEmptyBudgetRefusesRetryWithoutWaiting`.
- **A refusal returns only `ErrBudgetExhausted` or only the operation error.** Caught by
  `TestEmptyBudgetRefusesRetryWithoutWaiting` and every `expect(..., refused)` check.
- **Each `Do` gets its own budget (created inside `Do`, or a value receiver that copies the
  state).** Caught by `TestRetriesWithBackoffWhileBudgetLasts` and
  `TestBudgetIsSharedAndNeverOverspent`.
- **The backoff is an unconditional sleep.** Caught by
  `TestCancellationDuringBackoffReturnsContextError` and
  `TestDeadlineDuringBackoffReturnsDeadlineExceeded`.
- **The backoff timer is not released when the context ends first.** Not caught by the tests
  (timers are not goroutines); look for it in review.
- **When the context ends during `op`, the operation's result is returned.** Caught by
  `TestCancellationDuringOperationWins`. A withdrawal in that case is not caught: the test does not
  inspect the balance afterwards.
- **`op` receives a context other than the caller's.** Caught by
  `TestOperationReceivesCallerContext`.
- **Float drift: with `Ratio = 0.1`, ten deposits sum to slightly less than one token.** Not caught:
  the tests use binary-exact ratios and durations.

## Open choices

- Whether a token withdrawn for a retry is refunded when the context is cancelled during the
  following backoff is explicitly the solver's choice. Any consistent choice is fine if the user
  can justify it.
- The representation of the balance (floating point or fixed-point units) and how its updates are
  made atomic are left to the solver.
- How the refusal error combines `ErrBudgetExhausted` and the last operation error is open as long
  as `errors.Is` matches both.

## Follow-up questions

- Every instance of the client has its own budget, with no coordination. Does the fleet-wide bound
  still hold?
- One backend of many is down. It drains the budget and healthy backends can no longer retry.
  Per-destination budgets or one global budget? What does each cost?
- Hedged requests (014) also send extra attempts. Do they draw from the same budget?
- Retries happen in the SDK, the service, and the mesh. Where does the budget live, and what do the
  other layers do?
- How do a retry budget and a circuit breaker (019) differ, and do you need both?
- The server returns `Retry-After` or a pushback header. How does that combine with the budget?
- Which metrics show the budget working?
