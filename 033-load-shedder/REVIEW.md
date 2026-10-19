# Reviewer notes: load-shedder

> Spoilers. Read after your attempt.

## Test coverage notes

- **A cancelled caller returns, but its request still occupies a queue slot**
  until a worker reaches it (typical with a buffered channel as the queue),
  so the queue reports full while holding nothing anybody wants. Caught by
  `TestCancelledQueuedRequestLeavesQueue`.
- **The shed decision uses the wrong time**: one timestamp for the whole
  queue, the time since the worker became free, or the age of the oldest
  request rather than each request's own enqueue time. Caught by
  `TestShedDecisionUsesEachRequestsOwnQueueingTime`.
- **A request that waited exactly `maxQueueDelay` is shed** (the comparison
  is `>=`). Caught by `TestShedDecisionUsesEachRequestsOwnQueueingTime`.
- **A queued request is shed by a timer or by its caller while every worker
  is busy.** Caught by `TestShedsRequestThatWaitedLongerThanMaxQueueDelay`.
- **Running requests count toward `queueCapacity`, or `Do` blocks on a full
  queue.** Caught by `TestQueueCapacityExcludesRunningRequests` (a blocking
  `Do` shows up as a synctest deadlock).
- **Requests start out of admission order.** Caught by
  `TestRequestsStartInAdmissionOrder`.
- **`fn` runs under a shedder-owned context (the 005 habit)**, so the caller's
  values and cancellation never reach it. Caught by
  `TestFnRunsUnderCallerContextAndItsErrorIsReturned` and
  `TestCallerCancellationReachesRunningFnAndDoWaitsForIt`.
- **`Do` returns `ctx.Err()` while `fn` still runs**, because the wait on
  `ctx.Done()` does not check whether the request was already taken. Caught
  by `TestCallerCancellationReachesRunningFnAndDoWaitsForIt`.
- **`Do` with an already-done context admits the request and `fn` runs.**
  Caught by `TestPreCancelledContextDoesNotRun`.
- **`fn` starts for a request whose context ended while it was queued but
  just before it was taken.** Not reliably caught by the tests; look for it
  in review.
- **`Shutdown` drains the queue, or leaves queued callers waiting** (workers
  exit while requests are still queued, and their callers block forever).
  Caught by `TestShutdownReleasesQueuedRequestsAndWaitsForRunning` (wrong
  error, or synctest reports `blocked goroutines remain`).
- **A graceful `Shutdown` cancels running `fn` calls.** Caught by
  `TestShutdownReleasesQueuedRequestsAndWaitsForRunning`.
- **An expired `Shutdown` context cannot reach a running `fn`** (its context
  derives only from the caller's), or `Shutdown` returns without waiting for
  it. Caught by `TestShutdownDeadlineCancelsRunningFnAndWaits` and
  `TestConcurrentShutdownReturnsItsOwnContextError`.
- **A `Shutdown` after completion returns `ctx.Err()` for an already-done
  context**, because it picks randomly between the completion signal and
  `ctx.Done()`. Caught only sometimes, by `TestConcurrentShutdownIsSafe`;
  look for it in review.
- **Repeated or concurrent `Shutdown` closes a channel twice.** Panics. Caught
  by `TestConcurrentShutdownIsSafe`.
- **Two parties both believe they own a request** (the cancelled caller and
  the worker, or the worker and `Shutdown`), so two results are sent to a
  one-slot channel and the second sender blocks forever, or `fn` runs for a
  request whose `Do` did not return its result. May show up in
  `TestConcurrentDoAndShutdown` as `blocked goroutines remain` or a
  mismatch between accepted requests and `fn` calls, but only on an unlucky
  interleaving; look for it in review.
- **A mutex is held while calling `fn` or while waiting for a result.** Shows
  up as a hang until the 60s timeout, because synctest does not see a
  goroutine blocked on `sync.Mutex` as blocked.
- **A goroutine per request** (`go fn(...)` in `Do`, or a watcher goroutine
  per request for the caller's context). Not caught by the tests; look for it
  in review.

## Open choices

- Whether `fn` runs on long-lived worker goroutines started by `New` or on
  the caller's goroutine after it is granted a worker slot. The README makes
  this the user's choice.
- How a cancelled request leaves the queue (removed at once, or marked and
  skipped later). Any consistent choice is fine if it stops counting toward
  `queueCapacity` before `Do` returns.
- How idle workers or waiting callers are woken (a condition variable, a
  wake channel, per-request channels). Any consistent choice is fine if no
  request is left waiting while a worker is free.

## Follow-up questions

- `maxQueueDelay` is a constant. What does CoDel change, and why?
- Under overload, why might LIFO serve callers better than FIFO?
- The caller's deadline is in 5 ms and `fn` usually takes 20 ms. Should the
  request be shed at dequeue even though it waited less than
  `maxQueueDelay`?
- Clients retry on `ErrShed`. What stops a retry storm?
- Some requests matter more. How do you shed the cheap ones first?
- Which metrics show the shedder is healthy?
