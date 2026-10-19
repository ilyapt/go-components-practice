# Reviewer notes: circuit-breaker

> Spoilers. Read after your attempt.

## Test coverage notes

- **The breaker mutex is held across the client call, often only on the closed
  path where it looks harmless.** Concurrent closed calls are serialized and
  rejections queue behind a slow probe. Caught by
  `TestClosedCallsDoNotWaitForEachOther`, `TestSingleProbeAndSuccessCloses`,
  `TestConcurrentCallersAtCooldownGetOneProbe` and the stale-completion tests
  (a polling helper fails with "is the breaker mutex held across the client
  call?" rather than hanging, because synctest does not see a goroutine blocked
  on sync.Mutex).
- **The breaker opens one failure early or one failure late** (confusing
  "failures so far" with "including this one"). Caught by
  `TestClassifiedFailuresOpenAtThreshold` (wrong error on the second or third
  call).
- **Non-classified errors reset or increment the failure count, or a success
  does not reset it.** Caught by
  `TestSuccessResetsFailuresAndNonClassifiedErrorsDoNotCount`.
- **The mutex is not unlocked on an early-return branch** (typically the branch
  that ignores a stale completion). The next `Call` hangs. Caught by the
  stale-completion tests, but it shows up either as a hang until the 60s
  timeout or, in `TestStaleCompletionDuringProbeIsIgnored`, as the misleading
  "mutex held across the client call" message.
- **A failure from an earlier closed period counts against a later closed
  period.** Caught by `TestStaleFailureCannotAffectNewClosedPeriod` (the first
  call of the new period returns `ErrCircuitOpen`).
- **A slow success from an earlier closed period closes or resets a breaker that
  newer failures opened.** Caught by `TestStaleSuccessCannotCloseOpenBreaker`.
- **A completion from an earlier closed period changes a half-open breaker**
  (ends the half-open state, admits a second probe, or prevents the failed probe
  from reopening). Caught by `TestStaleCompletionDuringProbeIsIgnored`.
- **A stale success from an earlier closed period resets the failure count of a
  later closed period.** Not caught: every stale-completion test uses threshold
  1, where a reset is unobservable. Look for it in review.
- **Several callers become probes when the cooldown elapses**, because the
  deadline check and the transition to half-open are not one atomic step.
  Caught by `TestConcurrentCallersAtCooldownGetOneProbe` (more than one client
  call after the cooldown, or fewer than 19 rejections).
- **The cooldown boundary is exclusive.** Caught by
  `TestCooldownBoundaryIsInclusive` (`ErrCircuitOpen` exactly at the deadline).
- **The reopen cooldown is counted from the original opening time or from probe
  start, not from probe completion.** Caught by
  `TestReopenCooldownCountsFromProbeCompletion` and
  `TestAnyProbeErrorReopensForNewCooldown`.
- **Only classified probe errors reopen**, so a non-classified probe error
  leaves the breaker half-open with no probe running, rejecting forever. Caught
  by `TestAnyProbeErrorReopensForNewCooldown`.
- **A pre-cancelled caller takes the probe slot** and leaves the breaker
  half-open with nobody probing. Caught by
  `TestPreCancelledCallDoesNotConsumeProbe`.
- **A pre-cancelled caller invokes the client in the closed state.** Caught by
  `TestValidationAndPreCancelledCall`, which also checks the three
  constructor sentinels.
- **The breaker reads `time.Now()` directly, bypassing the injected clock.** Not caught:
  the tests pass `time.Now` as the clock, and virtual time is global in the
  bubble. Look for it in review.

## Open choices

- How state, admission period, and probe ownership are represented internally
  is not prescribed; the README fixes only observable behaviour. Any consistent
  choice is fine if the user can justify it.
- The tests never send a pre-cancelled call to a breaker that is open and still
  cooling down. The README wording points at `ctx.Err()`; if the user returns
  `ErrCircuitOpen` there, ask them to justify the precedence.
- Whether `ErrCircuitOpen` is returned bare or wrapped with extra context is
  open; the tests only use `errors.Is`.
- The README does not say what happens if the injected clock moves backwards.
  Any consistent choice is fine if the user can justify it.

## Follow-up questions

- The probe's caller cancels mid-call and the probe returns a context error.
  Should that reopen the breaker for a full cooldown?
- The probe hangs for a long time. What bounds it, and who owns that bound?
- Should `context.DeadlineExceeded` from the provider count as a failure? Who
  decides, the classifier or the breaker?
- Retry and circuit breaker together: which wraps which, and why?
- With 50 replicas each running its own breaker, what does recovery look like
  for the dependency?
- When would consecutive-failure counting be the wrong signal, and what would
  you count instead?
- Which metrics would show you the breaker's behaviour in production?
