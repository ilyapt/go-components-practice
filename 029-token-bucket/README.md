# Token bucket

**Difficulty:** easy · **Topics:** lazy refill from elapsed time, fractional tokens, blocking acquire with context, cancellation without side effects, deadline fail-fast

Rate limiting protects what sits behind a caller: the downstream's capacity,
fairness between clients sharing it, and the bill for metered APIs. A token
bucket is the most common limiter: tokens drip in at a fixed rate up to a
burst capacity, and every request takes some. Token buckets sit in API
gateways and reverse proxies, in client SDKs that must stay under a provider's
quota, in outbound crawlers that must be polite to each host, and in queue
consumers that must not flood a database.

Callers either ask "may I go right now?" and drop or reject the request when
the answer is no, or they wait for tokens, bounded by their own context. This
task needs both. The context-aware waiting is the same pattern as in
012-timeout-wrapper: the caller's context bounds how long the caller waits,
and an earlier deadline or cancellation wins.

Implement `New`, `Allow`, `AllowN`, `Wait`, and `WaitN` in `starter.go`.

```go
func New(rate float64, burst int) (*Bucket, error)
func (b *Bucket) Allow() bool
func (b *Bucket) AllowN(n int) bool
func (b *Bucket) Wait(ctx context.Context) error
func (b *Bucket) WaitN(ctx context.Context, n int) error
```

## Requirements

### Tokens and refill

- A new bucket is full: it holds `burst` tokens.
- Tokens refill at `rate` tokens per second, in proportion to the elapsed
  time. 100 ms at `rate` 10 adds exactly one token; 50 ms adds half a token.
- Fractional tokens are kept: a failed call must not lose the fraction that
  has accrued so far. Two 125 ms intervals at `rate` 4 add one whole token.
- The bucket never holds more than `burst` tokens, however long it was idle.
- Refill is computed lazily from the time elapsed since the last update. The
  bucket has no background goroutine and no `Close` method; nothing the
  bucket does may outlive the call that started it.
- Use real time (`time.Now`, timers). Whether you count tokens as `float64` or
  in integer nanoseconds is your choice.

### Non-blocking acquire

- `AllowN(n)` takes `n` tokens and returns `true` if at least `n` tokens are
  available right now; otherwise it returns `false` and takes nothing (all or
  nothing). It never blocks.
- `AllowN(n)` with `n > burst` returns `false`.
- `Allow()` is `AllowN(1)`.

### Blocking acquire

- `WaitN(ctx, n)` returns `nil` after it has taken `n` tokens. When the
  tokens are available, it returns immediately. Otherwise it waits until
  enough tokens have accrued; without competing callers it returns exactly
  at that moment, not later.
- `WaitN(ctx, n)` with `n > burst` can never succeed: it returns
  `ErrExceedsBurst` immediately (usable with `errors.Is`) and takes nothing.
- If `ctx` is already done, `WaitN` returns `ctx.Err()` and takes nothing,
  even if tokens are available.
- If `ctx` is cancelled while `WaitN` waits, `WaitN` returns `ctx.Err()`
  immediately and takes nothing. A cancelled waiter leaves the bucket exactly
  as if it had never called: other waiters get their tokens at the same time
  they would have without it.
- Fail fast on deadlines: if `ctx` has a deadline and the `n` tokens cannot
  have accrued by that deadline, `WaitN` returns immediately, without
  sleeping and without taking anything, with an error for which
  `errors.Is(err, context.DeadlineExceeded)` is true. Returning
  `context.DeadlineExceeded` itself or wrapping it with more detail are both
  fine. This is what `golang.org/x/time/rate` does: a caller that is going to
  time out anyway learns it now and can shed the request.
- If the tokens would accrue exactly at the deadline, either outcome is
  acceptable.
- `WaitN` never busy-waits: whenever it goes to sleep, including after
  another caller took the tokens it was waiting for, it sleeps for a positive
  duration.
- `Wait(ctx)` is `WaitN(ctx, 1)`.

### Concurrency

- All methods are safe for concurrent callers. Concurrent callers never take
  more tokens in total than the bucket had: over any time window of length
  `d`, the tokens granted add up to at most `burst + rate*d`.
- A waiting `WaitN` must not hold a lock while it sleeps: `AllowN`, other
  `WaitN` calls, and cancellations never wait behind a sleeping waiter.
- Which waiter gets tokens first when several are waiting is your choice;
  FIFO is not required. Whether `AllowN` may take tokens that a sleeping
  waiter is waiting for is also your choice. Specify what you picked.

### Edge cases

- `n <= 0` asks for nothing: `AllowN` returns `true`, `WaitN` returns `nil`,
  and the bucket does not change. A negative `n` never adds tokens.
- When more than one of these rules applies to the same call (for example
  `n > burst` with a cancelled `ctx`), which result wins is your choice.

## Validation

- `New` returns `ErrInvalidConfig` and a nil `*Bucket` when `rate` is not a
  positive finite number (`rate <= 0`, NaN, or infinite) or when
  `burst <= 0`.

## Before coding

1. What state does the bucket store, and how do you compute the number of
   tokens at an arbitrary moment from it without a background goroutine?
2. At what moment does `WaitN` actually take its tokens, and what has to be
   true about the bucket state when a waiter is cancelled?
3. A waiter has computed how long it needs to sleep, but by the time it wakes
   another caller has taken the tokens. What does it do, and how does that
   interact with its context deadline?
4. Why does `n > burst` deserve its own error instead of waiting, and why is
   it better to fail fast on a deadline that is too soon than to sleep until
   it expires?
