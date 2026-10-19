# Reviewer notes: bounded-map

> Spoilers. Read after your attempt.

## Test coverage notes

- **`Map` returns on the first error while another started call is still
  running.** Caught by `TestDoesNotReturnWhileStartedCallIsRunning` (fails with
  "Map returned ... while a started call was still running").
- **A result or error channel is closed by the receiver while senders are still
  alive.** Shows up as a panic: send on closed channel. Caught only sometimes, by
  `TestErrorCancelsInFlightAndStopsNewWork`, when a cancelled call sends after
  the close rather than before it; look for it in review.
- **Workers block forever sending to an unbuffered channel that nobody reads
  after the first error.** Caught by `TestErrorCancelsInFlightAndStopsNewWork`
  when the cancelled call reports its error (synctest reports
  `blocked goroutines remain`). An implementation that drops errors after the
  first one can hide the same leak; look for it in review.
- **Success is returned because every `fn` returned nil, although the parent
  context was cancelled.** Caught by
  `TestReturnsParentCancellationEvenIfFunctionSucceeds` (wrong value: nil
  instead of `context.Canceled`).
- **In-flight calls are not cancelled after a failure.** Caught by
  `TestErrorCancelsInFlightAndStopsNewWork` (synctest reports
  `all goroutines in bubble are blocked`: the other calls wait on their context
  forever).
- **New calls are started after a failure or after parent cancellation.** Caught
  by `TestErrorCancelsInFlightAndStopsNewWork` (more than 3 calls) and
  `TestReturnsParentCancellationEvenIfFunctionSucceeds` (more than 1 call). When
  the only guard is a `select` in which a free slot and `Done` are both ready,
  the random choice makes this caught only sometimes; look for it in review.
- **One goroutine per input is started up front and each waits for a slot
  inside.** Concurrency of `fn` stays bounded, goroutine count does not. Not
  caught by the tests; look for it in review.
- **Results are appended to a shared slice without synchronization, or in
  completion order.** Caught by `TestPreservesOrderAndBoundsConcurrency` (race
  detector, or wrong value at `outputs[i]`).
- **Fewer than `limit` calls ever run at once.** Caught by
  `TestPreservesOrderAndBoundsConcurrency`, which requires the maximum to be
  exactly the limit.
- **`fn` is called, or a nil error returned, for `limit <= 0` or nil `fn`.**
  Caught by `TestEmptyInputAndInvalidArguments`.

## Open choices

- The exact error for invalid arguments (sentinel or inline). Any consistent
  choice is fine if the user can justify it.
- Which error wins when parent cancellation races with a failing call:
  `ctx.Err()`, the call's error, or both joined. The README leaves it open.
- The shape of the concurrency: a fixed set of `limit` workers, a goroutine per
  item gated by a semaphore, or `errgroup` with `SetLimit` (which would add
  `golang.org/x/sync` to `go.mod`). Any is acceptable if the user can explain
  the trade-off and, for `errgroup`, what it does internally.
- What the output slice contains when an error is returned (nil or partial). The
  tests ignore it.
- Whether an empty input returns a nil or a non-nil empty slice; the tests only
  check the length.

## Follow-up questions

- Inputs are 10 million items. What changes?
- The caller wants all errors, not just the first. How does the API change?
- One input is consistently slow. How does that affect throughput under each
  approach, and what would you measure?
- How would you test that `limit` is respected without timing assumptions?
- Where exactly is the point after which no goroutine started by `Map` can still
  touch `outputs`?
