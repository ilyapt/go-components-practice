# Reviewer notes: write-behind-cache

> Spoilers. Read after your attempt.

## Test coverage notes

- **The whole batch is marked clean after a successful `Write`, so a `Put`
  that landed during the flight is silently dropped:** the store keeps the old
  value forever while `Get` shows the new one. Caught by
  `TestPutDuringSuccessfulFlushStaysDirty`.
- **After a failed `Write`, the failed batch is written back over the dirty
  values,** so a newer `Put` is replaced by the value that failed. Caught by
  `TestPutDuringFailedFlushReplacesFailedValue` (wrong retry batch).
- **Keys leave the dirty set when the snapshot is taken and come back only on
  failure.** They are briefly clean, so `maxPending` undercounts and a failure
  can lose or reorder values. Caught by `TestFullRejectsNewKeysWithoutBlocking`
  and `TestPutDuringFailedFlushReplacesFailedValue`.
- **Tight retry loop after a failure:** the failed keys still count toward
  `maxDirty`, so the count trigger fires again at once. Caught by
  `TestFailedWriteKeepsKeysDirtyAndRetriesEveryMaxDelay` (wrong call count and
  timestamps; it also catches a retry timed from the oldest `Put`, not from
  the failure).
- **A mutex held across `Write`,** so `Put` and `Get` stall behind a slow
  store. Caught by `TestPutAndGetDoNotWaitForStore`, which hangs until the 60s
  timeout because synctest does not see a goroutine blocked on `sync.Mutex`.
- **The delay deadline is armed only after a flush returns, or only when the
  dirty set goes from empty to non-empty** (which never happens while in-flight
  keys stay dirty). A `Put` during a slow flush then waits `maxDelay` counted
  from the end of the flush, not from itself. Caught by
  `TestDeadlinePassedDuringFlushFlushesRightAfterIt` (second flush at the wrong
  time).
- **A ticker or timer started in `New`, a deadline extended by later `Put`s,
  or a deadline not reset when a count-triggered flush picks up the waiting
  `Put`s.** Caught by `TestMaxDelayCountsFromOldestUnflushedPut` and
  `TestFailedWriteKeepsKeysDirtyAndRetriesEveryMaxDelay`.
- **Edge-triggered count trigger** (a kick only when a `Put` adds the
  `maxDirty`-th key, never re-checked after a flight): keys re-dirtied during a
  flight wait for the timer. Not caught by the tests; look for it in review.
- **In-flight keys not counted toward `maxPending`, `ErrFull` returned for a
  key that is already dirty, or a `Put` that blocks when full.** Caught by
  `TestFullRejectsNewKeysWithoutBlocking` (a blocking `Put` shows up as a
  synctest deadlock).
- **A rejected `Put` (pre-cancelled context) still stores the value or makes
  the key dirty.** Caught by `TestValidationAndPreCancelledPut`.
- **Passing a `Put` or `Shutdown` caller's context to `Write`.** Caught by
  `TestStoreReceivesCacheOwnedWorkContext`, which inspects context values and
  `ctx.Err()` inside `Write`.
- **`Shutdown` waits for the delay timer before the final flush, or a later
  `Shutdown` with an already-done context returns `ctx.Err()` although the
  flusher already exited cleanly.** Caught by
  `TestShutdownFlushesAllDirtyKeysAndStopsAdmission` (virtual time advanced
  during `Shutdown`; non-nil from the late call).
- **Retrying without delay during shutdown, or giving up after the first
  failed final flush.** Caught by `TestShutdownRetriesFailedWritesEveryMaxDelay`.
- **Returning on the deadline without waiting for the flusher, starting another
  `Write` after the work context is cancelled, or returning `nil` to the other
  callers although data was lost.** Caught by
  `TestShutdownDeadlineCancelsWriteAndWaitsForFlusher`.
- **A goroutine per flush,** so concurrent `Write`s can land out of order and
  an older value overwrites a newer one in the store. Caught by the
  max-in-flight check in `r.verify` (most tests) and by the per-key ordering
  check in `TestConcurrentPutsAndShutdownPersistEveryAcceptedValue`.
- **Reusing or mutating the batch slice after `Write`** (for example
  `batch = batch[:0]`). Caught by `r.verify` in most tests.
- **A timer still armed after the flusher exits.** Since Go 1.23 this is not a
  leak; not caught by the tests, look for it in review.

## Open choices

- If shutdown begins during a retry delay, the final flush may start
  immediately or at the end of that delay; the README leaves this to the
  solver and no test pins it.
- How the cache recognises that a key changed while its value was in flight is
  left to the solver; any mechanism is fine if both race tests pass and the
  user can explain it.
- Whether a key re-put to exactly the value already in flight (`1 → 2 → 1`)
  counts as clean after that flight succeeds, or is written once more. The
  tests do not check it; any consistent choice is fine if the user can justify
  it.
- Whether `Get` takes the same lock as `Put` or reads a separately published
  snapshot. The README only requires that it never waits for `Write`.

## Follow-up questions

- The process crashes. Which `Put`s are lost, and what would make an
  acknowledged `Put` durable?
- The store rejects some entries of a batch and accepts others. How does the
  `Store` interface change, and what becomes clean?
- The store limits a batch to 500 entries. How do you split, and what does
  that do to the "one flush in flight" and deadline rules?
- The store is down for an hour. What do callers see, and what should they
  see?
- Two instances of the service run the same cache over the same keys. What
  can go wrong in the store?
- Add eviction of clean keys and read-through on a miss. Why can dirty keys
  never be evicted, and what does a miss while a key is in flight return?
- Which metrics show that the cache is unhealthy?
