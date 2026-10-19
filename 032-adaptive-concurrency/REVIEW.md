# Reviewer notes: adaptive-concurrency

> Spoilers. Read after your attempt.

## Test coverage notes

- **Latency is measured from the start of `Acquire`**, so queue time counts
  as dependency latency and the limit collapses under load. Caught by
  `TestQueueingTimeIsNotPartOfLatency`.
- **A freed slot is up for grabs after a release** (wake-and-retry), so a
  newcomer can take it ahead of a queued waiter and a waiter can starve under
  steady load. Caught only sometimes, by
  `TestQueuedAcquiresAreAdmittedInFIFOOrder`, when the scheduler lets the
  newcomer win; look for it in review.
- **One release admits at most one waiter**, so after a fast success from a
  full limiter one admissible waiter keeps waiting for an unrelated release.
  Caught by `TestOneReleaseAdmitsAsManyWaitersAsTheNewLimitAllows`.
- **A release admits a waiter before its outcome is applied**, letting a call
  in under the old, higher limit right after the call that proved it too high.
  Caught by `TestLoweredLimitStopsAdmissionUntilInFlightDrops`.
- **A waiter is admitted whenever a slot is released**, although the limit is
  now below `InFlight()`. Caught by
  `TestLoweredLimitStopsAdmissionUntilInFlightDrops`.
- **Wrong decrease arithmetic**: rounding to nearest where the README says floor, a missing `Min`
  clamp (the limit reaches 0 and nothing is admitted again), or two decreases
  for a slow `Dropped`. Caught by
  `TestSlowOrDroppedReleaseDecreasesLimitDownToMin`.
- **The latency target is compared with strict `<`.** Caught by
  `TestLatencyBoundaryIsInclusive`.
- **`Ignore` or an unknown `Outcome` changes the limit.** Caught by
  `TestIgnoreDoesNotChangeLimit`.
- **`Acquire` waits when there is no slot and no queue room.** Caught by
  `TestAcquireFailsFastWhenNoSlotAndNoQueueRoom` (synctest reports the
  deadlock).
- **A cancelled waiter stays in the queue**, so it still counts against
  `MaxWaiting` or later receives a slot nobody will release. Caught by
  `TestCancelledWaiterReturnsContextErrorAndLeavesQueue`.
- **A waiter is handed a slot at the same moment its context ends and returns
  `ctx.Err()` without giving the slot back**, or the slot is counted twice.
  Caught by `TestConcurrentCallsNeverExceedLimitAndLeakNoSlot` (`InFlight`
  stays above zero, or the final `Acquire` calls deadlock), but only for the
  interleavings it produces; look for it in review.
- **`Release` is not idempotent**: a second call decrements `InFlight` again,
  frees another caller's slot, or changes the limit again; a plain unguarded
  flag is a data race. Caught by `TestReleaseIsIdempotent` (wrong values or
  the race detector).
- **`Release` blocks on a send to a waiter**, for example on an unbuffered
  channel whose waiter already left, possibly while holding the lock. Caught
  by `TestConcurrentCallsNeverExceedLimitAndLeakNoSlot` (a deadlock, or a hang
  until the 60s timeout if the lock is held).
- **The context is checked only after admission**, so a pre-cancelled caller
  takes a free slot or leaves a queue entry behind. Caught by
  `TestPreCancelledAcquireAdmitsNothing`.
- **`Close` does not fail queued waiters** (they wait forever), ends admitted
  calls, or closes a channel twice under concurrent `Close` (panics). Caught
  by `TestCloseFailsQueuedAndNewAcquiresButNotAdmittedCalls` and
  `TestConcurrentAcquireAndClose`.
- **A mutex is held while waiting, or `Acquire` starts a goroutine per call to
  implement the timeout.** Not caught directly: a held mutex shows up only as
  a hang until the 60s timeout, because synctest does not see a goroutine
  blocked on `sync.Mutex` as blocked, and a helper goroutine that exits is
  invisible. Look for it in review.

## Open choices

- When a slot is handed to a waiter at the same moment its context ends,
  whether `Acquire` returns the admission or gives the slot back and returns
  `ctx.Err()`. The README allows both, and the tests accept either outcome as
  long as no slot is lost.
- Whether a `Release` after `Close` still adjusts the limit. The README
  leaves this to the solver; the tests check only `InFlight`.
- Whether a done context or `ErrClosed` wins when both apply.
- How waiters are woken (a per-waiter channel, `sync.Cond` with
  `context.AfterFunc`, or another mechanism). Any consistent choice is fine
  if FIFO order, the queue bound, and exact counts hold.

## Follow-up questions

- With only two calls in flight, a hundred fast successes push the limit to
  `Max`; then a burst arrives and the dependency is overloaded at once. How
  do you stop an under-used limiter from inflating?
- A single overload event produces twenty drops in a row and the limit falls
  to `Min`. How do you make one event count once?
- `TargetLatency` is a guess. How would you derive it from the traffic?
- Client-side or server-side limiter: what does each one measure, and which
  failures does each catch?
- Should the queue exist at all? When is fail-fast better, and when is LIFO
  better than FIFO under overload?
- Fifty client instances each run their own limiter against one backend.
  What does each one converge to, and is the result fair?
