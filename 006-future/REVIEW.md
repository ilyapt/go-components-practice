# Reviewer notes: future

> Spoilers. Read after your attempt.

## Test coverage notes

- **A nil `fn` is accepted and the background goroutine panics later, far from
  the caller.** Caught by `TestStartRejectsNilFunction` (nil error, or a panic
  during `synctest.Wait`).
- **`Start` returns a typed nil pointer inside the `Future` interface for a nil
  `fn`.** Caught by `TestStartRejectsNilFunction` ("Start returned a Future for
  a nil function").
- **A waiter giving up cancels the work for everyone** (a cancel func derived
  in `Start` and called from `Await`, or a waiter's context passed to `fn`).
  Caught by `TestCancelledWaiterDoesNotCancelSharedWork` and
  `TestAwaitDeadlineDoesNotAffectResult` (wrong value: later waiters get
  `context.Canceled` or `DeadlineExceeded` instead of the result).
- **The result is consumed by the first waiter**, for example received from a
  buffered channel, and every later waiter blocks forever. Caught by
  `TestAwaitAfterCompletionReturnsStoredResult` and
  `TestEveryWaiterReceivesTheSameError` (synctest reports
  `all goroutines in bubble are blocked`).
- **Publication blocks when nobody awaits** (an unbuffered send). Caught by
  `TestWorkCompletesWithoutWaiters` (synctest reports
  `blocked goroutines remain`).
- **`fn` runs synchronously inside `Start`.** Caught by
  `TestStartReturnsWithoutWaitingForFn` (synctest deadlock).
- **`fn` runs more than once.** Caught by
  `TestCancelledWaiterDoesNotCancelSharedWork` (wrong call count).
- **`fn` does not receive `workCtx`.** Caught by `TestWorkContextCancelsWork`
  (synctest deadlock).
- **`Await` does not return when its own context is done.** Caught by
  `TestAwaitDeadlineDoesNotAffectResult` (returns later than 10ms, or never)
  and `TestCancelledWaiterDoesNotCancelSharedWork`.
- **The result fields are written after the completion signal, or read without
  a happens-before edge from the write.** Usually caught by the race detector
  in `TestEveryWaiterReceivesTheSameError` or
  `TestCancelledWaiterDoesNotCancelSharedWork`, where waiters are already
  blocked when `fn` returns; it can also surface as an empty `Result`.
- **A mutex is held while `Await` waits.** If the publisher needs the same
  mutex, the tests hang until the 60s timeout (synctest does not see a
  goroutine blocked on `sync.Mutex`). If it does not, waiters are only
  serialized and the tests pass; look for it in review.

## Open choices

- The error returned for a nil `fn`. Any consistent choice is fine if the user
  can justify it.
- What `Await` returns when the result is already available and its context is
  already done. The README states both rules without ordering them, and no test
  covers the case; ask whether the user noticed it.
- The publication mechanism. Different designs are legitimate if every waiter
  can still honour its own context.
- Whether `Start` derives its own cancellable context from `workCtx` and
  exposes something like `Cancel()`. The README neither asks for nor forbids
  it, as long as waiters cannot trigger it.

## Follow-up questions

- The work is expensive and every waiter has left. Should it keep running? Who
  decides, and how would the API express it?
- When would cancelling the work after the last waiter leaves be harmful?
- `fn` panics. What do waiters see? Should the future turn the panic into an
  error?
- How does this become singleflight, and who owns the work context there?
- How would you add `Done() <-chan struct{}` so callers can `select` on several
  futures at once?
- Results are large and futures are retained for a long time. What is the
  memory story, and who drops the reference?
