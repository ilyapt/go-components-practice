# Reviewer notes: retry-backoff

> Spoilers. Read after your attempt.

## Test coverage notes

- **The loop waits after the final failed attempt**, so the caller sits out
  one more full backoff and then gets the error anyway. Caught by
  `TestExhaustionReturnsLastErrorWithoutFinalWait` (3h instead of 2h, and
  `Backoff` called 3 times).
- **The first error is returned on exhaustion instead of the last, or the last
  one is wrapped.** Caught by `TestExhaustionReturnsLastErrorWithoutFinalWait`,
  which compares the error text exactly, so even `%w` wrapping fails it.
- **`op` is called on a context that is already done** (no check before an
  attempt, which matters most with zero backoff). Caught by
  `TestPreCancelledContextDoesNotCallOperation` and
  `TestCancellationDuringOperationWinsAndStopsRetry` (wrong call count).
- **`op`'s success or error is returned although the context was cancelled
  while it ran.** Caught by `TestCancellationDuringOperationWinsAndStopsRetry`
  (nil or `errRetryable` instead of `context.Canceled`).
- **The backoff wait ignores cancellation** (an unconditional `time.Sleep`).
  Caught by `TestCancellationDuringBackoffStopsRetry` ("Do did not stop
  waiting") and `TestDeadlineDuringBackoffReturnsDeadlineExceeded` (returns
  after 1m instead of at the 5s deadline).
- **Off-by-one in attempt numbering** (`Backoff(0)` for the first failure, or
  `maxAttempts` treated as retries after the first call). Caught by
  `TestRetriesUntilSuccessAndNumbersAttemptsFromOne` and
  `TestWaitsExactlyBackoffBetweenAttempts`.
- **A zero or negative backoff still waits.** Caught by
  `TestZeroAndNegativeBackoffRetryImmediately` (non-zero elapsed time).
- **`Backoff` is consulted, or a wait happens, after a permanent error.** Caught
  by `TestPermanentErrorStopsImmediately` (`Backoff` called, or 1h elapsed).
- **The backoff timer is left running when the wait ends through
  cancellation.** Not caught by the tests (on Go 1.23+ an unreferenced timer is
  collected anyway); look for it in review against "release timer resources
  promptly".
- **Invalid arguments are accepted or `op` is called for them.** Caught by
  `TestInvalidArguments`. A typed-nil policy is not tested.

## Open choices

- The error returned for invalid arguments. Any consistent choice is fine if
  the user can justify it.
- How the wait is built: `time.After`, a new timer per attempt, or one timer
  reused with `Reset`, with or without the pre-1.23 drain idiom. All pass;
  ask which Go version the user's reasoning about timer cleanup assumes.
- What `Result` accompanies an error. The tests check only the error.
- Whether to use a retry library in production. Ask what such a library decides
  for the caller.

## Follow-up questions

- Add jitter. Which kind, and why does it matter when 10,000 clients fail at the
  same moment?
- The caller wants a total time budget, not an attempt count. Where does that
  live: in `Do`, or in a deadline on `ctx`?
- `op` sends a payment. It times out. Is it safe to retry? What would the
  downstream need to provide for that to be safe?
- This retry runs inside a worker pool slot. What happens to pool throughput
  during a dependency outage?
- The server returns `Retry-After`. How would the policy interface change?
- Retries happen in the client, the SDK, and the load balancer. How do you
  prevent multiplicative amplification?
- Which metrics would show that retries are hurting rather than helping?
