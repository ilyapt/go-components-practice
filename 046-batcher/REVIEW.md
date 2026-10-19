# Reviewer notes: batcher

> Spoilers. Read after your attempt.

## Test coverage notes

- **"Full" is decided from the slice capacity, or with an off-by-one**, so
  batches are cut at the wrong size. Caught by `TestFlushesAtMaxSize` and
  `TestFlushSlicesAreNotReused` (wrong batch count or contents).
- **A `break` inside `select` leaves only the `select`**, so the loop keeps
  running after the queue is closed. Caught by the shutdown tests, but the
  symptom depends on what follows: a synctest deadlock report, a `close of
  closed channel` panic, or a busy loop that hangs until the 60s timeout.
- **`Submit` checks the closed state and sends in separate steps**, so a send
  can race with the close and panic with `send on closed channel`. Caught only
  sometimes, by `TestConcurrentSubmitAndShutdown`, when the scheduler hits the
  window; look for it in review.
- **Concurrent `Shutdown` calls close or signal something twice.** Not caught:
  the tests call `Shutdown` repeatedly but never concurrently. Sequential
  double-close shows up as a panic in `TestShutdownDrainsAndFlushesPartialBatch`.
- **`Shutdown` returns before the final partial batch is flushed**, or queued
  items are dropped during shutdown. Caught by `TestShutdownWaitsForFinalFlush`
  and `TestShutdownDrainsAndFlushesPartialBatch`.
- **The deadline is extended by later items**, so a steady trickle is never
  flushed. Caught by `TestMaxDelayCountsFromFirstItem`.
- **Flushing on a fixed ticker** (empty batches flushed, or deadlines on a grid
  rather than from the first item). Caught by `TestMaxDelayCountsFromFirstItem`
  (exact 20ms offsets, and no extra batches after an idle hour).
- **The backing array is reused after being handed to `flush`**, so an earlier
  batch is silently overwritten. Caught by `TestFlushSlicesAreNotReused`.
- **`Submit` blocks on a full queue**, or `queueCapacity` also counts items in
  the current batch. Caught by `TestFullQueueRejectsInsteadOfBlocking` (a
  blocking Submit shows up as synctest `all goroutines in bubble are blocked`;
  wrong counting rejects `queued-2`).
- **A mutex shared with `Submit` is held while `flush` runs.** Caught by
  `TestFullQueueRejectsInsteadOfBlocking` as a hang until the 60s timeout,
  because synctest does not see a goroutine blocked on sync.Mutex as durably
  blocked.
- **A mutex is held while `Shutdown` waits for completion.** Not caught unless
  the batching goroutine needs the same mutex; look for it in review.
- **A cancelled shutdown waiter stops the batcher, abandons the in-progress
  flush, or does not begin shutdown at all.** Caught by
  `TestCancelledShutdownWaiterDoesNotStopFlush` (later `Submit` is expected to
  return `ErrClosed`).
- **An item is flushed twice, or flushed although `Submit` did not return
  `nil`.** Caught by `TestConcurrentSubmitAndShutdown`.
- **A pre-cancelled `Submit` is admitted, or a validation sentinel is wrong.**
  Caught by `TestValidationAndPreCancelledSubmit`.
- **A timer is created per batch and never stopped or released.** Not caught;
  since Go 1.23 an unreferenced timer is collected, so it is a sign of unclear
  ownership more than a leak. Look for it in review.

## Open choices

- How admission is stopped during shutdown, and how the batching goroutine
  learns about it, is not prescribed. Any consistent choice is fine if the user
  can justify it.
- A `Submit` racing with `Shutdown` may return `nil`, `ErrClosed`, or `ErrFull`;
  `TestConcurrentSubmitAndShutdown` accepts all three, provided every `nil`
  item is flushed exactly once.
- The README does not say which error wins when the context is already
  cancelled and the batcher is also closed or full. The tests do not combine
  those cases.
- Whether timer stop/drain code follows pre-Go-1.23 patterns is a style point,
  not a defect; worth a short discussion if present.
- How each new batch slice is allocated is open, as long as a slice handed to
  `flush` is never touched again.

## Follow-up questions

- `flush` calls a database and sometimes takes 2 seconds. What happens to
  latency and to the queue? Would you run flushes concurrently, and what would
  that do to ordering?
- `flush` fails. Who retries, and what does `Submit` returning `nil` promise
  now?
- The batch has to be bounded by total bytes, not item count. What changes?
- Under overload, would you block `Submit`, reject, or drop the oldest items?
  Who decides?
- Which metrics would show that the batcher is unhealthy?
