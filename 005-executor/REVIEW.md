# Reviewer notes: executor

> Spoilers. Read after your attempt.

## Test coverage notes

- **The `Submit` context reaches `Handle`, so an accepted task is cancelled as
  soon as the caller returns.** Caught by
  `TestCallerContextDoesNotBindAcceptedTask` (wrong value: `Handle` sees a done
  context).
- **`Submit` waits for queue space** (a plain send, or a send selected only
  against `ctx.Done()`). Caught by `TestQueueCapacityExcludesRunningTasks`
  (synctest reports `all goroutines in bubble are blocked`).
- **`Submit` with an already-done context can still enqueue the task.** Caught
  by `TestSubmitHonorsCallerContext`; when the send and `ctx.Done()` sit in one
  `select` and both are ready, the random choice makes it caught only
  sometimes; look for it in review.
- **The queue is closed while a `Submit` is between its stopped-check and its
  send.** Caught by `TestConcurrentSubmitAndShutdown`, usually as a race
  detector report of a send racing with close, sometimes as a panic: send on
  closed channel. The window is narrow, so a green run is not proof; look for
  it in review.
- **Graceful shutdown drops queued tasks or runs them with a cancelled work
  context.** Caught by `TestShutdownDrainsAcceptedTasksAndWaits` (wrong handled
  list, or "ran with a cancelled work context during a graceful drain").
- **Workers wait for a queue close that only happens after the workers exit.**
  Caught by `TestConcurrentShutdownIsSafe` and every other test that waits for
  `Shutdown` (synctest reports `all goroutines in bubble are blocked`).
- **Repeated or concurrent `Shutdown` closes a channel twice or unlocks an
  unlocked mutex.** Caught by `TestConcurrentShutdownIsSafe` (panic: close of
  closed channel, or fatal error: sync: unlock of unlocked mutex).
- **On deadline, `Shutdown` cancels the work context and returns while handlers
  are still running.** Caught by
  `TestShutdownDeadlineCancelsWorkAndWaitsForWorkers` ("Shutdown returned ...
  while a handler was still running").
- **`Shutdown` cannot observe its own context while waiting for workers** (for
  example a bare `WaitGroup.Wait`), so the deadline never forces cancellation.
  Caught by `TestShutdownDeadlineCancelsWorkAndWaitsForWorkers` ("work context
  was not cancelled when the shutdown deadline expired").
- **A second `Shutdown` whose context is done hangs, or returns without forcing
  cancellation.** Caught by `TestConcurrentShutdownReturnsItsOwnContextError`
  and `TestShutdownDeadlineCancelsWorkAndWaitsForWorkers`.
- **`Shutdown` after full completion returns the error of its already-cancelled
  context instead of `nil`.** Caught by `TestShutdownAfterCompletionReturnsNil`;
  when "done" and `ctx.Done()` are both ready in one `select`, it fails only
  about half the time; look for it in review.
- **A task is handled twice, or a rejected task is handled.** Caught by
  `TestConcurrentSubmitAndShutdown` (wrong value).
- **Fewer than `workers` tasks run while more are queued.** Caught by
  `TestRunsExactlyWorkersTasksConcurrently`.
- **`Submit` starts a goroutine per task, or `New` starts more goroutines than
  `workers`.** Handle concurrency may still be bounded by a semaphore. Not
  caught by the tests; look for it in review.
- **Each `Shutdown` call starts its own goroutine to wait for the workers.** Not
  caught and not a leak (they exit with the workers); worth asking what it
  costs.
- **`New` returns a non-nil executor together with an error, or the wrong
  sentinel.** Caught by `TestValidation`.

## Open choices

- The admission mechanism: a mutex, an `RWMutex` shared by submitters, a
  separate stop channel with the queue never closed, or atomic state with an
  in-flight sender count. Any is fine if the user can show that no send can
  race with a close and no accepted task is lost.
- What happens to queued tasks after a forced shutdown: dropped, or handed to
  `Handle` with a cancelled context. The README says they "may be abandoned";
  the tests accept either.
- Which error `Submit` returns when its context is done and the executor is
  already stopped. The README does not order the two checks.

## Follow-up questions

- `Handle` can fail. Where do errors go, and what changes if the caller needs
  the result?
- Should `Submit` block with a context instead of rejecting? Who absorbs
  overload in each design, and what does the upstream see?
- Tasks belong to tenants. How do you stop one tenant from filling the queue?
- A handler ignores its context. What can `Shutdown` still guarantee?
- Why does `New` take no context? What would a lifetime context passed to
  `New` add, and what would it break?
- Which metrics show that the executor is unhealthy?
- The process crashes. Which accepted tasks are lost, and what would make
  acceptance durable?
