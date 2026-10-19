# Bounded executor with graceful shutdown

**Difficulty:** hard · **Topics:** fixed worker pool, bounded queue, overload errors, lifecycle, graceful shutdown

Run submitted tasks on a fixed set of long-lived workers, buffer at most a
bounded number of waiting tasks, reject the rest, and shut down cleanly:
drain what was accepted, or cancel it when the shutdown deadline runs out.
Executors like this sit inside job runners, message consumers, webhook
dispatchers, and any service that must not turn a traffic spike into an
unbounded pile of goroutines.

Implement `New`, `Submit`, and `Shutdown` in `starter.go`.

## Requirements

- `New` starts exactly `workers` long-lived worker goroutines.
- At most `workers` calls to `Handler.Handle` run concurrently.
- The queue holds at most `queueCapacity` tasks, excluding tasks already
  running.
- `Submit` does not create goroutines and is safe for concurrent callers.
- `Submit` never blocks on queue capacity: it returns `nil` when the task is
  accepted and `ErrOverloaded` when the queue cannot take it immediately.
- If the `Submit` context is already done, `Submit` returns `ctx.Err()`. The
  `Submit` context only bounds admission; it does not affect the task once it
  is accepted.
- `Handler.Handle` receives an executor-owned work context. It is cancelled
  only by a shutdown whose deadline expired.
- The first `Shutdown` call atomically stops admission. Every later `Submit`
  returns `ErrStopped`.
- Tasks accepted before shutdown are drained and handled exactly once while
  the shutdown context remains active. `Shutdown` waits for queued and running
  tasks to finish, then returns `nil`.
- If the shutdown context is done first:
  - cancel the work context;
  - queued tasks may be abandoned;
  - wait for every worker to exit;
  - return `ctx.Err()`.
- `Handler.Handle` must return when its work context is cancelled.
- Repeated and concurrent `Shutdown` calls are safe and never panic. Each
  caller waits for completion or for its own context; any caller whose context
  expires forces cancellation of the work context.
- Once shutdown has fully completed, later `Shutdown` calls return `nil`.
- No send on a closed channel and no leaked worker goroutines.

## Validation

- `workers` and `queueCapacity` must be positive; otherwise return
  `ErrInvalidConfig`.
- A nil handler returns `ErrNilHandler`.

## Before coding

1. What exactly does a `nil` result from `Submit` promise, and which context
   does the accepted task run under?
2. What are the executor's lifecycle states, and what is the linearization
   point after which `Submit` can no longer succeed?
3. Who closes the queue, and why can that never race with a sender?
4. Stopping admission, draining, cancelling work, and observing that workers
   have exited: which mechanism implements each of these?
