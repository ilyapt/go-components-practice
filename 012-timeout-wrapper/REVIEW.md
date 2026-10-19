# Reviewer notes: timeout-wrapper

> Spoilers. Read after your attempt.

## Test coverage notes

- **The client's own error is returned after a context fired**, so a client
  that wraps or replaces the context error breaks
  `errors.Is(err, context.DeadlineExceeded)`. Caught by
  `TestReturnsWrapperTimeoutIndependentlyOfClientError`,
  `TestReturnsParentCancellationIndependentlyOfClientError`, and
  `TestEarlierParentDeadlineWins` (wrong error).
- **Only the parent context is checked after the call, so the wrapper's own
  timeout goes unreported.** Caught by
  `TestReturnsWrapperTimeoutIndependentlyOfClientError` and
  `TestCallWaitsForClientThatIgnoresContext` (client error or nil instead of
  `DeadlineExceeded`).
- **The client's context is derived from `context.Background()`**, losing the
  parent's values, deadline, and cancellation. Caught by
  `TestClientReceivesChildContextWithTimeout` (value missing) and
  `TestEarlierParentDeadlineWins` (returns after 1h of virtual time instead of
  10ms).
- **The client's deadline is not exactly now plus the configured timeout.**
  Caught by `TestClientReceivesChildContextWithTimeout`.
- **A goroutine per call lets `Call` return at the deadline while the client is
  still running.** Caught by `TestCallWaitsForClientThatIgnoresContext` ("Call
  returned before the wrapped client returned").
- **The derived context is never cancelled after the call** (including
  `_ = cancel` to silence vet). Caught by
  `TestDerivedContextIsReleasedAfterCall`.
- **A successful or failed client result is altered when no context fired.**
  Caught by `TestPreservesCompletedClientResult`.
- **A separate timer or `time.After` runs alongside the context**, giving two
  deadline sources that can disagree. Not caught by the tests; look for it in
  review.
- **Invalid configuration is accepted.** Caught by
  `TestNewValidatesConfiguration`.

## Open choices

- What `Call` returns when client completion races with the deadline. The
  README accepts either outcome if it is consistent and documented; no test
  exercises the exact tie.
- Whether a context error is returned with a zero `Response` or with whatever
  the client returned. The tests check only the error.
- Whether to attach a wrapper-specific cause (for example via
  `context.WithTimeoutCause`) so callers and logs can tell "our timeout" from
  "upstream deadline". Not required.
- Whether `Call` checks an already-done parent before calling the client at
  all. Both pass `TestAlreadyCancelledParent`.
- The errors `New` returns for invalid configuration. The tests only require a
  non-nil error.

## Follow-up questions

- A request makes three sequential calls through this wrapper with a 1s
  timeout each, and the caller has a 2s budget. What happens, and how would a
  deadline budget change it?
- The client completes successfully a microsecond after the deadline. Should
  the wrapper return the success or the timeout? What does each choice cost?
- You must wrap a legacy blocking API that ignores contexts. What does a
  "hard" timeout cost there, and what does it leak?
- How would you make the timeout per-method or per-request instead of
  per-wrapper?
- Wrap this with retries. Should each attempt get the full timeout, or should
  the timeout cover all attempts? In which order do you compose the wrappers?
- Which metrics belong here?
