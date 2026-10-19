# Reviewer notes: idempotent-commands

> Spoilers. Read after your attempt.

## Test coverage notes

- **A mutex is held across the handler call or across a caller's wait**, so
  commands with different IDs serialize behind one slow command. Caught by
  `TestDifferentIDsDoNotWaitForEachOther`, which runs first and fails with "is
  a mutex held across the handler call?". Later tests then hang until the 60s
  timeout, because synctest does not see a goroutine blocked on sync.Mutex as
  durably blocked.
- **Concurrent duplicates invoke the handler more than once** (the "am I
  first?" decision is not atomic with the claim). Caught by
  `TestManyConcurrentDuplicatesInvokeHandlerOnce` and
  `TestConcurrentAndLaterDuplicatesShareOneResult` (handler calls above 1).
- **The goroutine responsible for completing a command waits on that same
  command's completion** (for example, it calls `Handle` itself). Nothing ever
  completes; synctest reports `all goroutines in bubble are blocked` on the
  first test that runs a command.
- **The first caller runs the handler inline, in the calling goroutine**, so
  it cannot return when its context is cancelled. Caught by
  `TestWaiterCancellationDoesNotCancelSharedExecution` ("cancelled waiter did
  not return promptly").
- **The handler receives a caller's context, or any context other than the
  exact `workCtx` passed to `New`.** Caught by
  `TestWaiterCancellationDoesNotCancelSharedExecution`, which compares the
  handler's context to `workCtx` by identity; a context derived from `workCtx`
  also fails it.
- **A pre-cancelled caller claims the ID** (the caller context is checked after
  the record is created, or not at all), so a later command with that ID gets a
  stale outcome or a false conflict. Caught by
  `TestPreCancelledCallerDoesNotClaimID`.
- **A payload conflict is detected only after the original command finishes**,
  so the conflicting caller waits for unrelated work. Caught by
  `TestPayloadConflictDoesNotInvokeHandler` (synctest reports `all goroutines
  in bubble are blocked`).
- **A conflict is not reported after the original command has finished**, or
  the conflicting command invokes the handler. Caught by
  `TestPayloadConflictDoesNotInvokeHandler`.
- **The record is deleted or retried when the handler fails**, so a second
  call runs the handler again. Caught by `TestHandlerErrorIsCached`.
- **The handler runs although `workCtx` is already cancelled**, or the
  cancelled-work path completes the command twice (one branch records
  `workCtx.Err()` and falls through into the normal path). Caught by
  `TestValidationAndCancelledWorkContext`; the fall-through shows up as `panic:
  close of closed channel` when completion is signalled by closing a channel.
- **The stored outcome is read before it is published, or written after
  waiters were released.** Usually flagged by the race detector in
  `TestConcurrentAndLaterDuplicatesShareOneResult` and
  `TestManyConcurrentDuplicatesInvokeHandlerOnce`; otherwise shows up as a zero
  `Result`.
- **Wrong sentinel for a nil handler or an empty ID**, or the handler is
  invoked for an empty ID. Caught by `TestValidationAndCancelledWorkContext`.
- **Records grow without bound.** Accepted by the README for this task; not a
  defect, but a natural follow-up.

## Open choices

- How payload identity is checked (full bytes or a digest) is open; a digest
  carries a collision risk worth naming.
- Whether a nil payload and an empty payload are the same payload is not
  specified. The tests only repeat a nil payload against itself.
- The README defines what a pre-cancelled caller does for an unclaimed ID only.
  For an ID already claimed (running or finished), returning `ctx.Err()`, the
  stored outcome, or `ErrPayloadConflict` are all defensible; ask the user to
  justify the choice. The tests do not cover it.
- Which error wins for an empty ID with an already-cancelled context is not
  specified and not tested.
- When the stored outcome and a waiter's cancellation become ready at the same
  moment, either answer is acceptable; the tests do not create that race.

## Follow-up questions

- Records are kept forever. How would you evict them, and what does the client
  have to promise for eviction to be safe?
- The process crashes after the handler's side effect but before the outcome
  is stored. What does the client see on retry, and how would persistence
  change that?
- Two replicas behind a load balancer receive the same command. What is the
  source of truth for the claim now?
- Should a transient handler error, such as a timeout to a dependency, really
  be stored? How would you tell "definitely did not happen" from "unknown"?
- Exactly-once execution versus exactly-once observable effect: which one does
  this component give, and which one does the downstream system need?
- Which metrics would you export?
