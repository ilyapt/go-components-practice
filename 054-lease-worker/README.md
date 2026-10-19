# Lease worker

**Difficulty:** hard · **Topics:** background lease renewal, local lease deadline, job context cancellation, fencing on completion, worker loop lifecycle

The worker side of a leased job queue: claim a job, run it, keep its lease
alive in the background while the handler works, and complete it with the
generation it was claimed at. When the lease is lost, the worker stops the
work and never completes the job, because by then another worker may own it.
The same loop runs inside job runners on top of SQL or Redis queues, SQS
consumers that extend the visibility timeout, activity heartbeats in workflow
engines, and controllers that hold a lease on the resources they reconcile.

This task builds on 053-lease-fencing (the store side: `Claim`, `Renew`, and
`Complete` with generations that fence stale workers). Here you write the
worker that talks to such a store through the `Store` interface; the tests
use a fake store. A store protects itself from stale workers; this worker
makes sure it never needs to be protected from itself.

Implement `New` and `Run` in `starter.go`.

```go
type Store interface {
	Claim(ctx context.Context, workerID string, lease time.Duration) (Job, uint64, error)
	Renew(ctx context.Context, jobID, workerID string, generation uint64, lease time.Duration) error
	Complete(ctx context.Context, jobID, workerID string, generation uint64, result []byte) error
}

type Handler interface {
	Handle(ctx context.Context, job Job) ([]byte, error)
}

func New(store Store, workerID string, lease, renewEvery, pollInterval time.Duration, report func(Outcome)) (*Worker, error)
func (w *Worker) Run(ctx context.Context, handler Handler) error
```

## Requirements

### Claim loop

- `Run` calls `Store.Claim(ctx, workerID, lease)` as soon as it starts. It
  handles at most one job at a time: it does not call `Claim` again until the
  current job has ended (see "Job end" below).
- If `Claim` returns an error matching `ErrNoAvailableJob` (`errors.Is`), or
  any other error, `Run` waits `pollInterval` and calls `Claim` again. A
  `Claim` error never makes `Run` return.
- After a job ends, `Run` calls `Claim` again immediately, without waiting
  `pollInterval`.
- The handler and every `Store` call receive a context derived from `ctx`, so
  values carried by `ctx` reach them.
- A nil handler makes `Run` return `ErrNilHandler` without calling the store.
- Assume `Run` is not called concurrently on the same `Worker`.

### Renewal and the local lease deadline

- The handler runs under a job context. While it runs, exactly one renewal
  loop calls `Store.Renew(..., job.ID, workerID, generation, lease)` every
  `renewEvery`, the first time `renewEvery` after `Claim` returned, always
  with the generation returned by `Claim`. `Renew` calls never overlap.
- The worker keeps a local lease deadline: `lease` after the most recent
  successful `Claim` or `Renew` was *issued*. Measure from the moment the call
  started, not from when it returned: the store may have set the expiry at any
  moment in between.
- If `Renew` returns an error matching `ErrLeaseLost`, the lease is lost.
- Any other `Renew` error is not fatal: the next renewal tries again. If the
  local lease deadline is reached first — because `Renew` keeps failing or a
  call hangs — the lease is lost at that moment, not earlier. A lost lease
  stays lost, even if a `Renew` issued earlier succeeds afterwards.
- When the lease is lost, renewal stops, no further `Renew` is issued for that
  job, and the job context is cancelled immediately with cause `ErrLeaseLost`:
  `errors.Is(context.Cause(jobCtx), ErrLeaseLost)`. The cause is
  `ErrLeaseLost` only when the lease is lost.

### Job end, completion, and fencing

- A job ends when its handler has returned and its renewal loop, including
  any in-flight `Renew` call, has finished. An in-flight `Renew` is cancelled
  once the handler has returned.
- If the handler returns a nil error and the lease was not lost, the worker
  stops renewal, waits for it to finish, and then calls `Store.Complete`
  exactly once with the claimed generation and the handler's result. No
  `Renew` for that job starts or is still running once `Complete` is called.
  `Complete` is not retried.
- If the lease was lost, `Complete` is never called for that job, even if the
  handler later returns a result: a worker that lost its lease must not
  complete.
- If the handler returns an error, renewal stops and `Complete` is not called.
  The store has no release operation, so the lease is left to expire and
  another worker can claim the job after that.
- `Run` waits for the handler to return, however long it takes after its job
  context is done. It never claims a new job, or returns, while a handler or
  a `Renew` call is still running.

### Reporting

- If `report` is not nil, it is called exactly once per claimed job, after the
  job has ended and after `Complete` (if called) has returned, and before the
  next `Claim` or before `Run` returns. Calls never overlap.
- `Outcome.JobID` and `Outcome.Generation` identify the claim. `Outcome.Err`
  is, in order of precedence:
  - an error matching `ErrLeaseLost` if the lease was lost, by `Renew`, by the
    local deadline, or because `Complete` returned `ErrLeaseLost`;
  - otherwise the handler's error, if it returned one (match with
    `errors.Is`);
  - otherwise the error returned by `Complete`, or `nil` if it succeeded;
  - otherwise (the result was abandoned because `ctx` was cancelled; see
    below) `ctx.Err()`.

### Cancellation

- If `ctx` is already done, `Run` returns `ctx.Err()` without calling the
  store.
- When `ctx` is cancelled, waiting for the next poll and a pending `Claim`
  end promptly, and `Run` returns `ctx.Err()`.
- When `ctx` is cancelled during a job, the job context is cancelled, renewal
  stops (no new `Renew` is issued), and `Run` waits for the job to end before
  returning `ctx.Err()`. The lease then expires on its own.
- If the handler returns a result after `ctx` was cancelled, completing it is
  your choice. If you call `Complete`, use the claimed generation; if you
  abandon the result, report `ctx.Err()`. Either way `Run` returns
  `ctx.Err()`.
- When `Run` returns, every goroutine it started has exited and every timer
  and ticker it created is stopped.

## Validation

- A nil store, an empty `workerID`, a non-positive `lease`, `renewEvery`, or
  `pollInterval`, or `renewEvery >= lease` returns `ErrInvalidConfig`.
- `report` may be nil.

The renewal margin (how much smaller than `lease` `renewEvery` should be),
backoff between failed claims, and retrying `Complete` are follow-up design
questions, not requirements.

## Before coding

1. Which single point in your code decides a job's fate (complete or not),
   and how do you guarantee that no `Renew` is running after `Complete` starts
   and no `Complete` happens after the lease was lost?
2. What is the local lease deadline, why is it measured from when a call was
   issued, and what enforces it while a `Renew` call hangs?
3. Which contexts exist (`Run`'s `ctx`, the job context, the context of each
   store call), who cancels each one, and with which cause?
4. A handler ignores its context after the lease is lost and keeps writing to
   an external system. What does this worker still guarantee, and what can it
   not prevent on its own?
