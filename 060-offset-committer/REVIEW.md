# Reviewer notes: offset-committer

> Spoilers. Read after your attempt.

## Test coverage notes

- **The highest done offset is committed rather than the end of the
  contiguous prefix, or the last done offset rather than the next one to
  read.** Caught by `TestGapBlocksCommitUntilFilled` (wrong `Commit` values).
- **Numbers that were never tracked are treated as gaps** (walking integers
  upwards from the committed value). Caught by `TestGapBlocksCommitUntilFilled`:
  the prefix never passes 4.
- **A commit on every `Done`, or a timer per `Done`, so commits are off the
  `interval` grid.** Caught by `TestTimeTriggerCommitsOnIntervalGrid`.
- **The same value is committed again on every tick, or a tick commits while a
  gap blocks the prefix.** Caught by `TestNoCommitWithoutProgress`.
- **Rejected `Done` calls count toward `maxUncommitted`.** Caught by
  `TestDoneRejectsUnknownAndRepeatedOffsets`.
- **The count trigger is lost behind a gap:** the `Done` that fills the gap
  does not start an attempt, and progress waits for the next tick. Caught by
  `TestCountTriggerCommitsWithoutWaitingForTick` (interval is one hour).
- **The `Done` count is reset only on a successful commit,** so while `Commit`
  keeps failing every `Done` starts another attempt: a retry storm driven by
  traffic. Not caught by the tests; look for it in review.
- **A failed attempt is retried with its stale value, or retried in a loop.**
  Caught by `TestFailedCommitIsRetriedWithCurrentPrefix`; a tight loop shows up
  as an extra attempt at 10ms or as a hang, because virtual time never
  advances.
- **The window is freed or `Committed` updated before `Commit` succeeds.**
  Caught by `TestFailedCommitIsRetriedWithCurrentPrefix` (`Track(4)` succeeds
  after a failure) and `TestAtMostOneCommitInFlightAndCallersNeverWait`
  (`Committed()` non-zero while the first commit is in flight).
- **A done-but-uncommitted offset no longer occupies the window.** Caught by
  `TestWindowBoundsUncommittedOffsets`.
- **`Commit` called from `Done`, a mutex held across `Commit`, or a blocking
  kick send.** Caught by `TestAtMostOneCommitInFlightAndCallersNeverWait`. A
  mutex held across `Commit` hangs until the 60s timeout, because synctest
  does not see a goroutine blocked on `sync.Mutex`; a blocking channel send
  shows up as a synctest deadlock.
- **Two commits in flight from the commit loop,** so a lower value can land
  after a higher one. Caught by `TestAtMostOneCommitInFlightAndCallersNeverWait`
  and `TestConcurrentTrackAndDoneAreRaceFree` (max in flight, strictly
  increasing values).
- **A final commit started from the `Close` goroutine while the loop is still
  committing.** Not caught by the tests; look for it in review.
- **A rejected `Track` moves the last tracked offset or takes a slot, or the
  error precedence is wrong.** Caught by `TestTrackRejectsNonIncreasingOffsets`
  and `TestWindowBoundsUncommittedOffsets`.
- **`Done` accepted after `Close` began,** so the final value keeps moving.
  Caught by `TestCloseCommitsFinalPrefixWithWorkContext`.
- **The `Close` context passed to `Commit`.** Caught by
  `TestCloseCommitsFinalPrefixWithWorkContext` (context value seen in
  `Commit`).
- **A work context that is never cancelled** (for example `context.Background()`
  passed to `Commit`). Caught by `TestCloseDeadlineCancelsInFlightCommit`
  (synctest deadlock: the `Commit` waits on a context that never ends).
- **`Close` returns on its deadline without waiting for the in-flight
  `Commit`.** Caught by `TestCloseDeadlineCancelsInFlightCommit`.
- **A forced stop seen only by the forcing caller:** other `Close` callers hang
  or return `nil`, or a later `Close` loses the recorded error or calls
  `Commit` again. Caught by `TestCloseWithDoneContextForcesStopForEveryCaller`
  and `TestCloseRetriesFinalCommitUntilDeadline`.
- **A completed `Close` followed by a `Close` with a done context returns
  `ctx.Err()` rather than `nil`.** Caught by `TestConcurrentCloseIsSafe`.
- **An unguarded `close` of a stop channel,** which panics with close of closed
  channel on repeated `Close`. Caught by `TestConcurrentCloseIsSafe`.
- **The commit loop goroutine leaks.** Caught by every test (synctest reports
  `blocked goroutines remain`). An unstopped ticker is collected since
  Go 1.23 and is not caught; look for it in review.

## Open choices

- Whether the next attempt starts as soon as an in-flight one returns or at the
  next trigger is the solver's choice (README).
  `TestAtMostOneCommitInFlightAndCallersNeverWait` accepts either.
- The timing of `Close` retries is checked loosely:
  `TestCloseRetriesFinalCommitUntilDeadline` accepts 2 to 4 attempts in 25ms
  with a 10ms interval.
- How pending offsets are stored and how the prefix end is found is left to
  the solver, as long as offsets need not be consecutive.

## Follow-up questions

- `Commit` times out, but the broker actually stored the value. Is the next
  attempt safe?
- One message is poison and never finishes. What happens to commits, the
  window, and the partition?
- The partition is revoked during a rebalance. What do you do with in-flight
  workers before `Close`, and what stops a zombie worker from processing after
  the new owner started?
- How do you get effectively-once instead of at-least-once?
- A consumer has 200 partitions. One committer and one ticker per partition,
  or one loop that commits a map of offsets in one request? What changes in
  the failure semantics?
- Which metrics show trouble?
