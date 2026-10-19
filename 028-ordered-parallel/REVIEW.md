# Reviewer notes: ordered-parallel

> Spoilers. Read after your attempt.

## Test coverage notes

- **An item is taken from `in` and only then does the stage wait for window
  room**, so it holds `window + 1` items and the producer gets one extra send.
  Caught by `TestWindowBoundsItemsTakenButNotEmitted` and
  `TestBackpressureBlocksProducerWhenWindowIsFull`.
- **A hand-off is counted as emission**: the window slot is released when a
  result is passed to an emitter goroutine, or the returned channel is
  buffered. The memory bound silently grows by the hand-off depth. Caught by
  `TestWindowBoundsItemsTakenButNotEmitted` (exact taken count after every
  receive) and the capacity check in
  `TestEmitsInInputOrderDespiteCompletionOrder`.
- **The stage does not run ahead within the window**: it processes in batches
  of `workers` (the 001 habit) or waits for the head before dispatching more,
  so the effective window is `workers`. Caught by
  `TestSlowHeadStallsTheStageAtTheWindow` and
  `TestAtMostWorkersCallsRunConcurrently`.
- **Results are emitted in completion order, or workers send on the output
  directly.** Caught by `TestEmitsInInputOrderDespiteCompletionOrder`.
- **The first error from `fn` stops the stream** (treated as a stage failure,
  as in 001). Caught by `TestPerItemErrorIsDeliveredInPlaceAndStageContinues`.
- **The output is closed when the in-flight count reaches zero while `in` is
  still open.** Caught by `TestOutputStaysOpenUntilInputIsClosed`.
- **A plain blocking send on the output**: after cancellation with nobody
  reading, the stage blocks forever. Caught by
  `TestCancelledStageClosesOutputWithoutAReader` (shows up as the channel
  still open after cancellation).
- **The output is closed on cancellation before running `fn` calls have
  returned.** Caught by
  `TestCancellationClosesOutputOnlyAfterRunningCallsReturn`.
- **After cancellation, an item is still taken from `in` or a queued item
  still gets an `fn` call**, because a `select` with both `ctx.Done()` and
  another ready case picks at random. Caught by
  `TestPreCancelledContextReadsNothingAndCallsNothing` (it repeats `Run` 16
  times for this reason) and, depending on scheduling, by
  `TestCancellationStopsTakingInputAndStartsNoNewCalls`.
- **`fn` receives `context.Background()` or a context not derived from the
  `Run` context**, so cancellation cannot reach running calls and the stage
  never closes. Caught by `TestCancellationStopsTakingInputAndStartsNoNewCalls`
  (synctest reports `all goroutines in bubble are blocked`) and
  `TestProcessFuncReceivesContextDerivedFromRunContext`.
- **The dispatcher and the workers block on each other** (for example, the
  dispatcher blocks handing out a job while every worker blocks handing back
  a result). Caught by almost every test as a synctest deadlock.
- **Goroutines are started on a validation error, or validation happens after
  they start.** Caught by `TestValidation` (synctest reports
  `blocked goroutines remain`).
- **Emitted results after cancellation have a gap or are out of order.**
  Caught by `TestCancellationStopsTakingInputAndStartsNoNewCalls` only for the
  interleaving it happens to produce; look for it in review.
- **The goroutine count or the reorder storage grows with the length of the
  input** rather than being bounded by `workers` and `window`. Not caught by
  the tests; look for it in review.
- **A mutex is held across a send on the output or a call of `fn`** in a
  design with shared counters. Shows up as a hang until the 60s timeout, not
  as a deadlock message, because synctest does not see a goroutine blocked on
  `sync.Mutex` as blocked.

## Open choices

- A fixed pool of `workers` goroutines or one goroutine per in-flight item.
  The README makes this the user's choice and asks them to defend it.
- Where finished-but-not-yet-due results are kept (a map, a ring indexed by
  sequence, a FIFO of per-item futures) and how many goroutines own the
  ordering. Any consistent choice is fine if the window bound stays exact.
- After cancellation, how many already-finished results are still emitted to
  a reader that keeps reading, and whether they carry the context error. The
  tests accept any gapless prefix of the taken items.

## Follow-up questions

- One poisoned item takes ten minutes. What bounds the stall, and what do you
  give up?
- The stage feeds a Kafka consumer that commits offsets. Which offset is safe
  to commit, and what does that require of `fn` after a restart?
- Order is only required per key. How would you change the design, and what
  happens to the memory bound?
- Items vary from 1 KB to 50 MB. Is a count-based window still a memory
  bound?
- Which metrics tell a slow consumer apart from head-of-line blocking?
- `fn` panics. What must the stage still guarantee?
- How do you shut the stage down gracefully with a deadline?
