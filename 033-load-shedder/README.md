# Load shedder by queueing time

**Difficulty:** medium · **Topics:** admission control, queueing time, load shedding, caller cancellation, synchronous execution, graceful shutdown

Run requests on a fixed number of workers, and when the service falls behind,
reject work by how long it has waited rather than by how many requests are
waiting. Queue length is a poor overload signal: a hundred queued requests
that take a millisecond each are fine, a hundred that take a second each are
not. Queueing time captures both the arrival rate and the service rate, and a
request that has already waited longer than its caller is willing to wait is
pure waste: running it burns a worker on an answer nobody will read, while
fresh requests time out behind it. RPC servers, API gateways, and database
proxies shed load this way (CoDel-style admission, "controlled delay") to keep
goodput up during overload instead of collapsing.

This task builds on 005-executor (fixed workers, bounded queue, overload
errors, shutdown) and changes two things: the caller waits for the result of
its own work, and the queue rejects work by queueing time, not only by
length.

Implement `New`, `Do`, and `Shutdown` in `starter.go`.

```go
func New(workers, queueCapacity int, maxQueueDelay time.Duration) (*Shedder, error)
func (s *Shedder) Do(ctx context.Context, fn func(context.Context) error) error
func (s *Shedder) Shutdown(ctx context.Context) error
```

A request is one `Do` call. It is admitted into the queue, waits there until a
worker is free, and is then taken from the queue: either it is started (`fn`
runs) or it is not (shed, cancelled, or stopped).

## Requirements

### Admission

- `Do` is safe for concurrent callers. The number of goroutines the `Shedder`
  creates does not grow with load: `Do` creates none per request.
- A nil `fn` returns `ErrNilFunc`.
- If `ctx` is already done, `Do` returns `ctx.Err()` and does not admit the
  request.
- After the first `Shutdown` call, every `Do` returns `ErrStopped`.
- `Do` never blocks on queue capacity: when `queueCapacity` requests are
  already waiting, it returns `ErrOverloaded` immediately. Running requests do
  not count toward `queueCapacity`.
- An admitted request records its enqueue time: the moment `Do` admitted it.

### Execution and shedding

- At most `workers` `fn` calls run at the same time. A request is taken from
  the queue as soon as a worker is free. Requests are taken in admission
  order (FIFO).
- When a request is taken from the queue, its queueing time is the time since
  its enqueue time. If the queueing time is longer than `maxQueueDelay`, the
  request is shed: `fn` is not called and `Do` returns `ErrShed`. A request
  that waited exactly `maxQueueDelay` runs.
- The shed decision is made only when a request is taken from the queue. A
  queued request is not timed out on its own: while every worker stays busy,
  its caller keeps waiting, bounded only by its own `ctx`.
- Shedding does not occupy a worker: after shedding a request, the worker
  immediately takes the next one.
- `fn` receives a context derived from the `Do` context: it carries the
  caller's values and is cancelled when the caller's context is cancelled.
- `Do` returns exactly the error `fn` returned.
- Once `fn` has started, `Do` waits for it to return and returns its error,
  even if `ctx` is cancelled meanwhile.
- Do not hold a mutex while calling `fn` or while waiting.

Whether `fn` runs on long-lived worker goroutines started by `New`, or on the
goroutine that called `Do` after it has been granted a free worker slot, is
your choice. Either way, `workers` bounds the number of running `fn` calls.

### Cancellation while queued

- If `ctx` is done while the request is still queued, `Do` returns
  `ctx.Err()` without waiting for a worker. The request leaves the queue
  before `Do` returns: it no longer counts toward `queueCapacity`, and `fn` is
  never called for it.
- A request whose `ctx` is already done when it is taken from the queue is not
  started; its `Do` returns `ctx.Err()`.

### Shutdown

- The first `Shutdown` call atomically stops admission.
- Every request still queued at that moment is removed from the queue; its
  `Do` returns `ErrStopped` without waiting for running work, and `fn` is
  never called for it. Queued work is not drained.
- Running `fn` calls are not cancelled by a graceful shutdown. `Shutdown`
  waits until no `fn` is running (and every worker goroutine, if you have
  any, has exited), then returns `nil`.
- If the shutdown context is done first:
  - cancel the contexts of the running `fn` calls;
  - still wait until no `fn` is running;
  - return `ctx.Err()`.
- Repeated and concurrent `Shutdown` calls are safe and never panic. Each
  caller waits for completion or for its own context; any caller whose
  context expires forces cancellation of the running `fn` calls.
- Once shutdown has fully completed, later `Shutdown` calls return `nil`.
- No send on a closed channel, no leaked goroutines, and no `Do` left waiting
  forever.

Assume `fn` returns soon after its context is cancelled and does not panic.

## Validation

- `workers`, `queueCapacity`, and `maxQueueDelay` must be positive; otherwise
  `New` returns `ErrInvalidConfig` and a nil `*Shedder`.

## Before coding

1. For each result `Do` can return, has `fn` run, and can the caller safely
   retry the request elsewhere?
2. A request leaves the queue in exactly one way: started, shed, cancelled by
   its caller, or stopped by `Shutdown`. Where is the single point at which
   one of these wins, and how does a cancelled request stop counting toward
   `queueCapacity`?
3. Why is queueing time a better overload signal than queue length, and why is
   it measured when the request is taken from the queue rather than when it
   is admitted?
4. Which context does `fn` run under, why is it not the shedder-owned work
   context of 005-executor, and what can cancel it?
