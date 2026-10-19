# Reviewer notes: bulkhead

> Spoilers. Read after your attempt.

## Test coverage notes

- **One limit or one waiting room shared by all dependencies, or compartments
  keyed wrongly, so a slow dependency rejects or queues calls for a healthy
  one.** Caught by `TestSaturatedDependencyDoesNotAffectOthers`.
- **The mutex is held while `fn` runs or while a call waits.** Every
  dependency serializes behind one slow call. Shows up as a hang until the 60s
  timeout, not as a clean failure, because synctest does not see a goroutine
  blocked on `sync.Mutex` as blocked; typically the first test that runs a
  blocking `fn` next to another call hangs, for example
  `TestRunsAtMostMaxConcurrentPerDependency` or
  `TestSaturatedDependencyDoesNotAffectOthers`.
- **`Do` blocks when the waiting room is full, or the waiting room has no
  bound.** Caught by `TestWaitingRoomIsBoundedAndRejectsImmediately` and
  `TestRunsAtMostMaxConcurrentPerDependency` (synctest reports the deadlock,
  or the extra call gets a slot or waits rather than getting
  `ErrBulkheadFull`).
- **A cancelled waiter keeps its waiting place, or waiting ignores the
  context.** Caught by `TestCancelledWaiterLeavesWaitingRoom`.
- **A waiter is handed a slot at the same moment its context ends and returns
  `ctx.Err()` without giving the slot back.** The dependency loses capacity
  permanently, one race at a time. Likely, but not certainly, caught by
  `TestWaiterCancelledAtSlotHandoffDoesNotLeakCapacity`; look for it in review.
- **A done context is checked after a slot or waiting place was taken, or a
  nil `fn` takes a slot.** Caught by `TestPreCancelledCallTakesNoSlot` and
  `TestValidation`.
- **The slot is released when the caller gives up rather than when `fn`
  returns** (for example, `fn` runs in a goroutine and `Do` selects on
  `ctx.Done()`). The bulkhead then counts callers, not calls, and a dependency
  that ignores cancellation gets unbounded concurrency. Caught by
  `TestDoWaitsForFnAfterCallerCancellation`.
- **`fn` runs under `context.Background()` or a bulkhead-owned context.**
  Caught by `TestFnReceivesCallerContext`.
- **`Overrides` is read from the caller's map after `New`.** Caught by
  `TestPerDependencyOverrides`.
- **Waiters are not released by `Shutdown`, or a compartment created lazily
  after `Shutdown` admits a call.** Caught by
  `TestShutdownReleasesWaitersAndWaitsForRunningCalls` (the `never-used`
  dependency covers the second case).
- **`Shutdown` cancels running calls (a copy of the 005 design), or returns
  before they finish.** Caught by `TestShutdownDeadlineDoesNotCancelCallerWork`
  and `TestShutdownReleasesWaitersAndWaitsForRunningCalls`.
- **A `Shutdown` after everything drained returns `ctx.Err()` for an
  already-done context**, because it picks randomly between the drained signal
  and `ctx.Done()`. Caught only sometimes, by
  `TestShutdownReleasesWaitersAndWaitsForRunningCalls`; look for it in review.
- **A shutdown signal is closed twice, or repeated `Shutdown` changes state
  unguarded.** Panics with close of closed channel. Caught by
  `TestConcurrentShutdownReturnsItsOwnContextError` and
  `TestConcurrentDoAndShutdown`.
- **Running calls are tracked with a `sync.WaitGroup` whose `Add` races
  `Wait` at zero, or whose `Wait` cannot be combined with the `Shutdown`
  context.** May be caught by `TestConcurrentDoAndShutdown` under the race
  detector, or by `TestShutdownDeadlineDoesNotCancelCallerWork` as a
  `Shutdown` that ignores its deadline.
- **Free capacity is checked under the lock and reserved after unlocking**, so
  two callers take the last slot. Caught only sometimes, by
  `TestConcurrentDoAndShutdown` (per-dependency concurrency above the limit);
  look for it in review.
- **A waiter that has already been granted a slot is still counted as
  waiting**, so a newcomer briefly gets `ErrBulkheadFull` while the waiting
  room is not actually full. Not caught by the tests; look for it in review.
- **A waiter still queued at `Shutdown` is given a slot that frees afterwards
  and runs `fn`.** Not caught reliably: the shutdown test releases its waiter
  before any slot frees. Look for it in review.
- **A goroutine per call, to make `fn` abandonable.** Not caught by the tests;
  it breaks both the requirement and the point of the component.

## Open choices

- Which waiter gets a freed slot (FIFO or not). The README asks the user to
  state and justify the choice.
- When a slot frees at the same instant as the waiter's deadline, whether the
  waiter runs `fn` or returns `ctx.Err()`; the tests accept either outcome, as
  long as no capacity leaks.
- How slots and waiters are represented (counters with an explicit waiter
  list, a buffered channel per dependency, `x/sync/semaphore` with a waiting
  counter). Any consistent choice is fine if the counts stay exact and the
  user can justify it.
- One lock over all compartments or a lock per compartment. Any consistent
  choice is fine if a saturated dependency does not affect others.

## Follow-up questions

- Dependency names come from request data (a tenant ID, a host name). What
  happens to memory, and what would you change?
- How do you choose `MaxConcurrent` for a dependency?
- Should waiting have its own timeout, independent of the caller's deadline?
- How do you stack this with retries (017, 018) and a circuit breaker (019)?
  Should `ErrBulkheadFull` count as a breaker failure, and should it be
  retried?
- `fn` panics. Is the slot released, and where does the panic go?
- Critical and best-effort traffic share one dependency. How do you keep
  best-effort callers from taking every slot?
- The service runs 50 replicas. What does a per-process limit mean for the
  dependency, and when do you need a global one?
