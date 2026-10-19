# Reviewer notes: token-bucket

> Spoilers. Read after your attempt.

## Test coverage notes

- **A background goroutine or ticker refills the bucket.** It leaks (there is
  no `Close`) and quantizes refill. Caught by every test: synctest reports
  `blocked goroutines remain`.
- **The partial token is lost**: integer tokens with the timestamp moved to
  now on every call, so a call between two whole tokens throws the fraction
  away. Caught by `TestFractionalTokensAreNotLost`.
- **The cap is missing or applied before the refill is added**, so an idle
  bucket hands out more than `burst`. Caught by `TestTokensNeverExceedBurst`
  and `TestRefillIsProportionalToElapsedTime`.
- **`AllowN` takes what is there when it is less than `n`.** Caught by
  `TestRefillIsProportionalToElapsedTime`.
- **A waiter grabs the available tokens and waits for the rest**, so a
  cancelled waiter keeps what it grabbed. Caught by
  `TestCancelledWaiterReturnsPromptlyAndTakesNothing`.
- **A reservation-style design does not undo correctly on cancellation.** No
  restore at all is caught by
  `TestCancelledWaiterReturnsPromptlyAndTakesNothing`. A partial restore (the
  `golang.org/x/time/rate` behaviour, where a cancelled waiter can burn its
  tokens) and a full restore that leaves later waiters scheduled on top of it
  (two waiters then act at the same instant and exceed the burst) are caught
  by `TestCancelledWaiterDoesNotDelayOtherWaiters`.
- **The waiter sleeps `n/rate` regardless of the tokens already accrued**, and
  returns late. Caught by `TestFractionalTokensAreNotLost` and
  `TestCancelledWaiterDoesNotDelayOtherWaiters` (wrong return times).
- **The wait is a `time.Sleep` that cannot be interrupted by the context.**
  Caught by `TestCancelledWaiterReturnsPromptlyAndTakesNothing`.
- **A waiter sleeps until its deadline before failing, or fails although the
  deadline leaves enough time.** Caught by
  `TestWaitNFailsFastOnlyWhenDeadlineIsTooSoon`.
- **A caller with an already-done context takes available tokens.** Caught by
  `TestPreCancelledContextTakesNothing`.
- **`WaitN` with `n > burst` waits.** Caught by
  `TestRequestLargerThanBurstFailsImmediately` (synctest reports the deadlock).
- **The mutex is held while a waiter sleeps**, so every other caller blocks
  behind it. Shows up as a hang until the 60s timeout in
  `TestAllowDoesNotBlockBehindWaiters`, because synctest does not see a
  goroutine blocked on `sync.Mutex` as blocked.
- **A waiter that wakes up subtracts `n` without re-checking**, assuming
  nobody else took the tokens. Caught by
  `TestWaitersShareTokensWithoutOverAdmission` and
  `TestConcurrentCallersNeverExceedRate`.
- **Bucket state is read outside the lock as a "quick check".** Caught by the
  race detector in `TestConcurrentAllowAdmitsExactlyTheRate`.
- **Validation checks only `rate <= 0`**, so NaN and infinity pass. Caught by
  `TestValidation`.
- **A negative `n` adds tokens.** Caught by `TestNonPositiveNAsksForNothing`.
- **A computed wait rounds down to zero** (for example, a tiny float deficit
  truncated to 0 ns), and the loop spins without letting virtual time
  advance. Not caught deterministically; it may show up as a hang in
  `TestConcurrentCallersNeverExceedRate`. Look for it in review.

## Open choices

- Counting tokens as `float64` or in integer nanoseconds (or as a single
  "empty at" timestamp). The README leaves this to the solver; the tests use
  rates and instants where both are exact.
- Which waiter gets tokens first (FIFO or not), and whether `AllowN` may take
  tokens a sleeping waiter is waiting for. The README asks the user to state
  the choice; `TestAllowDoesNotBlockBehindWaiters` accepts either outcome.
- When the tokens would accrue exactly at the deadline, success or fail-fast;
  both are acceptable.
- Whether the fail-fast error is `context.DeadlineExceeded` itself or wraps
  it with more detail.
- Which result wins when several rules apply to one call (for example
  `n > burst` with a cancelled context).
- Taking tokens only at success or reserving them up front. Any design is
  fine if a cancelled waiter leaves the bucket exactly as if it had never
  called and the user can justify it.

## Follow-up questions

- One bucket per client IP or API key: how do you bound memory?
- The service runs on 20 replicas and the limit is global. What are the
  options, and what does each trade?
- A stream of `Allow()` callers starves a `WaitN(ctx, 5)`. How do you fix it,
  and what does the fix cost?
- The API must answer 429 with a `Retry-After` header. What method would you
  add?
- Change the rate at runtime. What happens to the tokens accrued so far and
  to sleeping waiters?
- Token bucket versus leaky bucket versus sliding window: which allows
  bursts, which smooths output, which is exact at window edges?
