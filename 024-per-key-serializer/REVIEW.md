# Reviewer notes: per-key-serializer

> Spoilers. Read after your attempt.

## Test coverage notes

- **Same-key callers are ordered by a step outside the processor's internal
  synchronization** (for example, each caller sends its item on a per-key
  channel after releasing the mutex, or from its own goroutine), so execution
  order can differ from admission order. Rarely caught: the tests admit callers
  one at a time, so `TestSameKeyRunsFIFOAndSerially` only fails when the
  scheduler interleaves badly. Look for it in review.
- **Callers wait for their turn on a per-key `sync.Mutex`.** Go mutexes are
  not FIFO, so order is not guaranteed, and key cleanup needs extra
  bookkeeping. Shows up as tests hanging until the 60s timeout: a goroutine
  blocked on sync.Mutex is not durably blocked for synctest, so the
  `synctest.Wait` inside the test helpers never returns.
- **Two executors drain one key's queue** (an executor per call, or a missing
  "already active" check). Caught by `TestSameKeyRunsFIFOAndSerially` and
  `TestManyConcurrentCallersOneKeyRunOnceEachAndNeverOverlap` (maximum
  concurrency above 1 or an operation run twice).
- **A global mutex is held while an operation runs**, serializing all keys.
  Caught by `TestDifferentKeysRunConcurrently`, as a hang until the 60s timeout
  for the reason above.
- **`Process` returns before its operation finishes, or does not return the
  operation's error.** Caught by `TestProcessWaitsForAndReturnsOperationResult`.
- **The executor blocks delivering a result to a caller that already returned
  on `ctx.Done()`**, and the key stalls for everyone. Caught by
  `TestAbandonedResultDoesNotBlockKey`,
  `TestQueuedCancellationReturnsAndSkipsOperation` and
  `TestCancellationAfterStartDoesNotOverlapNextOperation` (a later caller
  "has not returned", and synctest may also report blocked goroutines).
- **A cancelled queued operation still runs when it reaches the head**, or its
  caller does not return promptly. Caught by
  `TestQueuedCancellationReturnsAndSkipsOperation`.
- **The next operation starts as soon as the running operation's caller is
  cancelled**, before that operation returns (for example, operations run in
  detached goroutines and the queue advances when the waiter leaves). Caught by
  `TestCancellationAfterStartDoesNotOverlapNextOperation`.
- **A nil operation is admitted, a pre-cancelled operation runs, or a rejected
  call leaves the key unusable.** Caught by
  `TestRejectsNilAndPreCancelledOperation`.
- **Cleanup race: "queue is empty" is decided in one critical section and the
  key's state is deleted in another.** An item appended in between is lost and
  its caller waits forever. Not reliably caught; look for it in review.
- **Per-key state is never deleted**, so memory grows with the number of
  distinct keys ever seen. Not observable through the API and not caught; look
  for it in review.
- **The queue's backing storage is retained by a long-lived hot key** after
  items are popped. Not caught; worth a remark in review, not a defect for this
  task.

## Open choices

- Who executes a key's operations (a dedicated executor per active key, or a
  hand-off between callers) is not prescribed. Any consistent choice is fine if
  the user can justify it.
- Whether a cancelled queued item is removed from the queue eagerly or skipped
  when it reaches the head is open; the two are indistinguishable through the
  API.
- When a caller is cancelled at the same moment its operation reaches the head,
  or at the same moment its operation returns, the README does not fix the
  outcome. The tests do not create those races.
- The error for a nil operation is not a named sentinel, and the README does
  not say which error wins when the operation is nil and the context is
  already cancelled.

## Follow-up questions

- The queue is unbounded. A hot key receives 10,000 requests per second and
  each takes 1 ms. What happens, and what would you change? What error does
  the caller get?
- One goroutine per active key is unbounded too. How would you bound total
  concurrency while keeping per-key order?
- An operation for key A must also lock key B (a transfer between accounts).
  What changes?
- The process restarts. Which ordering guarantee survives, and what would you
  need to persist?
- Under mass cancellation, does it matter when a cancelled item leaves the
  queue?
- Which metrics would you expose for this component?
