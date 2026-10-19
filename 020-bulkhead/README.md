# Bulkhead

**Difficulty:** medium · **Topics:** per-dependency isolation, counting semaphore, bounded waiting room, caller-scoped work, admission and drain

Give every downstream dependency its own fixed number of concurrent calls and
its own small waiting room, so that one slow dependency can tie up only its
own share of the service. Without it, a search backend that slows down from
20 ms to 2 s holds request goroutines, connections, and memory until calls to
a perfectly healthy payments service start timing out too. Bulkheads sit in
RPC clients, API gateways, and service meshes (Envoy's per-cluster
`max_requests` and `max_pending_requests` are one), usually next to a circuit
breaker.

This task builds on 005-executor (bounded concurrency, an overload error
instead of an unbounded queue, shutdown that stops admission and drains) and
019-circuit-breaker (protection per dependency): a breaker reacts to a
dependency that fails, a bulkhead contains one that is slow. Unlike in 005,
the work here belongs to the caller: `Do` runs `fn` synchronously, on the
caller's goroutine, and returns its result.

Implement `New`, `Do`, and `Shutdown` in `starter.go`.

```go
type Limits struct {
	MaxConcurrent int
	MaxWaiting    int
}

type Config struct {
	Default   Limits
	Overrides map[string]Limits
}

func New(cfg Config) (*Bulkhead, error)
func (b *Bulkhead) Do(ctx context.Context, dep string, fn func(context.Context) error) error
func (b *Bulkhead) Shutdown(ctx context.Context) error
```

## Requirements

### Admission

- Every dependency name has its own compartment with its own limits:
  `cfg.Overrides[dep]` if present, otherwise `cfg.Default`. Any string is a
  valid dependency name; its compartment is created on first use.
- At most `MaxConcurrent` calls of `fn` run at once for one dependency.
- `Do` runs `fn` as soon as a slot of its dependency is free. If every slot is
  busy and fewer than `MaxWaiting` calls are waiting for that dependency, `Do`
  waits for a slot. Otherwise it returns `ErrBulkheadFull` immediately,
  without calling `fn` and without blocking.
- Admission is exact: `ErrBulkheadFull` is returned only when every slot of
  the dependency is busy and its waiting room is full.
- `MaxWaiting` counts waiting calls only; waiting calls do not occupy slots.
  `MaxWaiting` of zero means that a call never waits.
- A freed slot never stays idle while a call is waiting for it. Which waiter
  gets it (FIFO or not) is your choice; state it and justify it.
- Calls for different dependencies never wait for, reject, or slow down each
  other. A dependency whose slots and waiting room are full has no effect on
  any other dependency.
- `Do` is safe for concurrent callers, runs `fn` on the caller's goroutine,
  and creates no goroutines.
- Do not hold a mutex while `fn` runs or while a call waits for a slot.

### Context

- `fn` receives `ctx` or a context derived from it. The work is
  caller-scoped: the bulkhead has no work context of its own.
- If `ctx` is already done when `Do` is called, `Do` returns `ctx.Err()`
  without calling `fn` and without taking a slot or a waiting place. This
  check comes before `ErrBulkheadFull` and `ErrClosed`.
- If `ctx` is done while the call waits, `Do` returns `ctx.Err()`, never calls
  `fn`, and leaves the waiting room at once, so that another call can take its
  place.
- A waiter never keeps a slot it does not use. If a slot frees up at the same
  moment as the waiter's context is done, `Do` either runs `fn` or returns
  `ctx.Err()` with the slot free again; both outcomes are allowed.
- Once `fn` has started, `Do` waits for it to return and returns exactly its
  error, even if `ctx` is cancelled meanwhile. The slot is held for exactly as
  long as `fn` runs, whatever `fn` returns.

### Shutdown

- The first `Shutdown` call atomically stops admission: every later `Do`
  returns `ErrClosed`, including for dependencies never used before.
- Calls still waiting for a slot when `Shutdown` is called return
  `ErrClosed` without calling `fn`, and from then on no slot is given to a
  waiting call, even when one frees up. A call that was given a slot before
  `Shutdown` counts as running and runs normally.
- `Shutdown` waits until every running `fn` has returned, then returns `nil`.
  It does not cancel running calls: they belong to their callers.
- If the `Shutdown` context is done first, `Shutdown` returns `ctx.Err()`.
  Admission stays closed and running calls continue undisturbed.
- Repeated and concurrent `Shutdown` calls are safe and never panic. Each
  caller waits for the running calls to finish or for its own context. Once
  every running call has finished, later `Shutdown` calls return `nil`, even
  with a done context.
- No leaked goroutines and no send on a closed channel.

Assume the set of dependency names is small and bounded: a compartment may
live as long as the `Bulkhead`.

## Validation

- `Default.MaxConcurrent` and the `MaxConcurrent` of every override must be
  positive, and every `MaxWaiting` must be zero or positive; otherwise `New`
  returns `ErrInvalidConfig`.
- `New` copies `Overrides`: changing the map after `New` has no effect.
- `Do` with a nil `fn` returns `ErrNilFunc` without taking a slot.

## Before coding

1. What state must be per dependency, and what may be shared between
   dependencies without one saturated dependency affecting another?
2. In which critical section does `Do` choose between run, wait, and reject,
   and what keeps the running and waiting counts exact when a waiter is
   cancelled or handed a slot?
3. Why does `fn` run under the caller's context here, while the handler in
   005-executor ran under an executor-owned work context? What does that
   change for `Shutdown`?
4. How does `Shutdown` stop admission, release the waiters, and observe that
   the last running call has returned, without a goroutine per call?
