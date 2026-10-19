# Adaptive concurrency limit

**Difficulty:** hard · **Topics:** AIMD, latency feedback, admission control, bounded wait queue, slot handoff, permit release

Limit how many calls run against a dependency at once, but do not pick the
number up front: discover it. While calls come back fast, allow one more
concurrent call; when latency crosses a target or the dependency starts
dropping requests, cut the limit by a ratio. This is additive increase,
multiplicative decrease (AIMD), the rule TCP uses for its congestion window.
Adaptive limiters of this kind sit in RPC clients and servers (Netflix
`concurrency-limits`, Envoy's adaptive concurrency filter), in database
connection gates, and in front of any dependency whose capacity changes with
deploys, noisy neighbours, and time of day.

This task builds on 005-executor (bounded concurrency, rejecting overload
instead of growing an unbounded queue). 020-bulkhead and 033-load-shedder
protect a dependency with thresholds chosen in advance; here the limit itself
moves at runtime, driven by the latency the limiter measures.

Implement `New`, `Acquire`, `Limit`, `InFlight`, and `Close` in `starter.go`.

```go
type Outcome int // Success, Dropped, Ignore
type Release func(Outcome)

type Config struct {
	Initial, Min, Max int
	TargetLatency     time.Duration
	BackoffRatio      float64
	MaxWaiting        int
}

func New(cfg Config) (*Limiter, error)
func (l *Limiter) Acquire(ctx context.Context) (Release, error)
func (l *Limiter) Limit() int
func (l *Limiter) InFlight() int
func (l *Limiter) Close()
```

A caller asks for permission with `Acquire`, makes its call to the
dependency, and then calls the returned `Release` with the outcome. The
limiter itself measures how long the call took.

## Requirements

### Limit

- The limit starts at `Initial` and always stays within `[Min, Max]`.
- The latency of a call is the time from its admission (the moment `Acquire`
  grants it a slot) to its `Release`. Time spent waiting in the queue is not
  part of the latency.
- `Release(Success)` with latency `<= TargetLatency` increases the limit by
  exactly 1, capped at `Max`. Each such release counts on its own; there is
  no time window.
- `Release(Success)` with latency `> TargetLatency`, and `Release(Dropped)`
  with any latency, set the limit to
  `max(Min, floor(limit * BackoffRatio))`.
- `Release(Ignore)` frees the slot and does not change the limit. Any other
  `Outcome` value is treated as `Ignore`.
- The boundary is inclusive: a `Success` whose latency equals `TargetLatency`
  increases the limit.
- `Limit` returns the current limit; `InFlight` returns the number of admitted
  calls that have not been released yet.

### Admission

- `Acquire` admits a call immediately when `InFlight() < Limit()` and no
  other `Acquire` is queued. Admission increments `InFlight`; `Acquire` then
  returns a non-nil `Release` and `nil`.
- Otherwise, if fewer than `MaxWaiting` calls are queued, `Acquire` joins the
  queue and waits. If the queue already holds `MaxWaiting` calls, `Acquire`
  returns `ErrLimitExceeded` immediately. `MaxWaiting == 0` means pure fail
  fast.
- Queued calls are admitted in FIFO order. A new `Acquire` never overtakes a
  queued one.
- Every `Release` first applies its outcome to the limit, then admits queued
  calls in FIFO order while `InFlight() < Limit()`. One release can therefore
  admit more than one waiter (a slot was freed and the limit grew).
- Lowering the limit below `InFlight()` does not affect admitted calls. It
  only stops admission until enough of them are released.
- At no moment is a call admitted while `InFlight() >= Limit()`.

### Cancellation

- If `ctx` is already done, `Acquire` returns `ctx.Err()` and admits nothing.
- A queued `Acquire` whose `ctx` ends returns `ctx.Err()` and leaves the queue,
  freeing its queue position for a later caller.
- If a slot is handed to a waiter at the same moment its `ctx` ends, `Acquire`
  may either return the slot as admitted (`Release`, `nil`) or give it back
  and return `ctx.Err()`. Either way, no slot is lost: once `Acquire` has
  returned, the slot is either held by that caller or free again (and offered
  to the next waiter).
- The `ctx` passed to `Acquire` only bounds waiting. It plays no role after
  admission.

### Release

- `Release` may be called from any goroutine. Its first call ends the call;
  every later call (including concurrent ones) is a no-op: it changes neither
  `InFlight` nor the limit.
- `Release` never blocks, whatever the waiters are doing.

### Close

- `Close` stops admission: every later `Acquire` returns `ErrClosed`.
- Every queued `Acquire` returns `ErrClosed` (unless it was admitted before
  `Close`).
- `Close` does not wait for or cancel admitted calls. Their `Release` still
  works and decrements `InFlight`. Whether a release after `Close` still
  adjusts the limit is your choice.
- Repeated and concurrent `Close` calls are safe.
- If `ctx` is already done and the limiter is closed, either `ctx.Err()` or
  `ErrClosed` is acceptable.

### Concurrency

- All methods and `Release` are safe for concurrent use.
- `Acquire` and `Release` start no goroutines. No goroutine is left blocked
  after every caller has returned.
- Do not hold a mutex while waiting.

## Validation

- `New` returns `ErrInvalidConfig` (and a nil `*Limiter`) unless
  `0 < Min <= Initial <= Max`, `0 < BackoffRatio < 1`, `TargetLatency > 0`,
  and `MaxWaiting >= 0`.

Smoothing latency over many samples, ignoring the limit growth of an idle
limiter, and fencing stale feedback from calls admitted under an older limit
are follow-up design questions, not requirements.

## Before coding

1. When exactly does a call's latency start and stop, and why must time spent
   in the limiter's own queue be excluded?
2. What state does the lock protect, and what is the linearization point at
   which a freed slot passes from a `Release` to a waiter?
3. A waiter's context ends at the same moment a `Release` hands it a slot.
   What does `Acquire` return, and how do you prove the slot is not lost?
4. The limit drops below `InFlight()`. What happens to the running calls, to
   the queued waiters, and to the next `Release`?
