# Reviewer notes: refresh-coordinator

> Spoilers. Read after your attempt.

## Test coverage notes

- **Triggers on a queued resource are treated as dirty, or triggers on a running resource are
  coalesced as if it were queued.** Dirty triggers during a load are lost, while queued resources
  get an extra load. Caught by `TestDirtyTriggersCoalesceIntoOneFollowUp`,
  `TestTriggerDuringFollowUpSchedulesOneMore`, and the exact call list in
  `TestBoundedQueueAndQueuedCoalescing` (wrong value).
- **`queueCapacity` counts the running resource, or a repeated trigger for a queued resource
  returns `ErrFull` when the queue is full.** Caught by `TestBoundedQueueAndQueuedCoalescing`.
- **A resource rejected with `ErrFull` is loaded anyway.** Caught by
  `TestBoundedQueueAndQueuedCoalescing`.
- **A trigger that arrives in the instant between a worker deciding "not dirty" and marking the
  resource idle is coalesced into nothing: `Trigger` returns `nil`, yet no load starts after it.**
  Not caught by the tests (every dirty trigger is made while the loader is blocked); look for it in
  review.
- **The mutex is not released on one worker path** (typically the loader-error branch). Shows up
  as "call did not return promptly" from the next `Trigger`, or as a hang until the 60s timeout,
  because synctest does not treat a goroutine blocked on `sync.Mutex` as blocked.
  `TestFailedLoadDoesNotBlockLaterTriggers` exercises the error path.
- **A failed load leaves the resource marked running, so later triggers are coalesced and never
  load.** Caught by `TestFailedLoadDoesNotBlockLaterTriggers`.
- **A successful load is held back until its dirty follow-up finishes, so readers see stale data
  meanwhile and a failing follow-up discards the good result.** Caught by
  `TestSuccessfulLoadIsPublishedBeforeFollowUpFinishes` and
  `TestSuccessfulLoadRemainsPublishedWhenFollowUpFails`.
- **A lock is held while the loader runs.** Loads become serial and `Trigger` waits for the
  slowest load. Caught by `TestDifferentResourcesLoadConcurrentlyWithoutLostPublication` and
  `TestConcurrencyLimitAndNoOverlapPerResource` ("is a mutex held across the loader call?"), and
  by the prompt-return check on every `Trigger` made while a load is running.
- **More than `concurrency` loads run, or two loads of one resource overlap.** Caught by
  `TestConcurrencyLimitAndNoOverlapPerResource`.
- **The published map or snapshot is mutated in place.** Caught by the old-snapshot check in
  `TestDifferentResourcesLoadConcurrentlyWithoutLostPublication` and by the race detector.
- **Two workers publish from the same base snapshot and one update disappears.** Caught only
  sometimes, by `TestDifferentResourcesLoadConcurrentlyWithoutLostPublication` (version or length
  wrong) and `TestConcurrentTriggerAndShutdown` (version ≠ resources + 1), when the two
  publications interleave badly; look for it in review.
- **`closed` is checked under the lock but the queue send happens after unlocking.** Caught by
  `TestConcurrentTriggerAndShutdown` as a send-on-closed-channel panic or a race report, but only
  when the window is hit; look for it in review.
- **`Shutdown` closes the queue and returns without waiting for running, queued, and dirty work.**
  Caught by `TestShutdownWaitsForRunningQueuedAndDirtyWork` and by any test that inspects results
  right after `Shutdown`.
- **A pre-cancelled first `Shutdown` returns its error without initiating shutdown.** Caught by
  `TestPreCancelledShutdownInitiatesDrain` (the late `Trigger` is not `ErrClosed`).
- **A `Shutdown` waiter's cancellation cancels `workCtx` or an internal work context.** Caught by
  `TestCancelledShutdownWaiterDoesNotCancelLoad` and `TestPreCancelledShutdownInitiatesDrain`.
- **The loader receives the trigger context or a context derived from it.** Caught by
  `TestLoaderUsesWorkContextNotTriggerContext`.

## Open choices

- Which worker runs a dirty follow-up, and whether it passes back through the input queue, is not
  specified; the tests only check that exactly one follow-up runs. Any consistent choice is fine
  if the user can justify it, including what happens to fairness and to the queue bound.
- Ordering of a dirty follow-up relative to other queued resources is not specified.
- How admission state, per-resource state, and publication are synchronised (one lock, separate
  locks, or a single goroutine that owns the state) is left to the solver; the tests check
  behaviour, not structure.
- Precedence when several `Trigger` errors apply at once (empty ID, cancelled context, closed,
  full) is not specified; the tests check each condition alone.
- How loader errors are observed in production is explicitly left as a design discussion.

## Follow-up questions

- Why not cancel the running load when a new trigger arrives for the same resource?
- The queue is full because the loader is slow. Which signal tells the operator, and what should
  the event source do with `ErrFull`?
- How would you measure freshness?
- Loader errors are invisible to callers. What would you expose?
- With 100k resources, copying the map on every load is expensive. What changes?
- A loaded state is older than the published one (`SourceVersion` went backwards because a
  provider lags). Should publication reject it?
