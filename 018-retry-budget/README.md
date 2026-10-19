# Retry budget

**Difficulty:** medium · **Topics:** retry amplification, shared token budget, atomic check-and-withdraw, lazy refill, error wrapping

Let a client retry, but only up to a fixed share of its traffic. When a
dependency goes down, every caller that retries three times turns one request
into three, exactly when the dependency can least afford it; with retries at
several layers the load multiplies. A retry budget caps the extra load: every
request earns a fraction of a retry, every retry spends a whole one, and when
the budget is empty failures are returned instead of retried. Finagle's
`RetryBudget`, gRPC retry throttling, and Envoy's retry budgets work this way
in RPC clients and service meshes.

This task builds on 017-retry-backoff (classification into retryable and
permanent errors, bounded attempts, context-aware backoff) and puts one
budget, shared by every caller of the component, in front of each retry.

Implement `New` and `Do` in `starter.go`.

```go
type Config struct {
    Ratio               float64
    MinRetriesPerSecond float64
    Burst               int
    MaxAttempts         int
    Policy              RetryPolicy
}

func New(cfg Config) (*Budget, error)
func (b *Budget) Do(ctx context.Context, op Operation) error
```

## Requirements

### Budget

- One `Budget` value holds one token balance shared by all of its `Do`
  calls, from any goroutine.
- The budget starts full: `Burst` tokens are available right after `New`.
- Each `Do` call deposits `Ratio` tokens when it starts its first attempt,
  whatever the outcome of that attempt. A call that makes no attempt deposits
  nothing.
- Tokens also accrue continuously with time at `MinRetriesPerSecond` tokens
  per second, starting at `New`: at 2 per second, one more token is available
  after 500ms.
- The balance never exceeds `Burst`. Deposits and accrual beyond it are lost.
- A retry costs exactly one token. It is allowed only when the balance is at
  least 1, and then withdraws 1. A refused retry withdraws nothing; the
  balance never goes below zero.
- Checking the balance and withdrawing the token is one atomic step:
  concurrent callers never spend more tokens than the budget holds.
- Only retries withdraw. A first attempt, a success, a permanent error, and
  a failure of the last allowed attempt never withdraw.

### Retry loop

- `MaxAttempts` includes the first call. `op` is called at most
  `MaxAttempts` times per `Do`.
- Attempts are numbered from 1; `Policy.Backoff(attempt)` receives the number
  of the attempt that just failed.
- Return `nil` immediately when `op` succeeds.
- Return the operation's error immediately when `Policy.Retryable(err)` is
  false.
- When the last allowed attempt fails, return its error unchanged, without
  waiting and without asking the budget.
- Otherwise a retry is needed, and `Do` asks the budget for a token
  *before* waiting:
  - if the budget refuses, return immediately, without waiting and without
    calling `op` again, an error for which both
    `errors.Is(err, ErrBudgetExhausted)` and `errors.Is(err, lastErr)` hold,
    where `lastErr` is the error of the attempt that just failed;
  - if the budget grants the token, wait `Policy.Backoff(attempt)`, then make
    the next attempt. A zero or negative backoff means retry immediately.

### Context

- `op` receives the caller's `ctx` (or a context derived from it).
- A caller whose context is already done gets `ctx.Err()`; `op` is not
  called and nothing is deposited.
- Cancellation before an attempt or during a backoff returns `ctx.Err()`
  without another call.
- If `op` returns after `ctx` is done, the context error takes precedence
  over whatever `op` returned, and the budget is not asked for a retry.
- Backoff waiting is context-aware: no unconditional `time.Sleep`. Release
  timer resources promptly.
- Whether a token withdrawn for a retry is refunded when the context is
  cancelled during the backoff that follows is your choice. Pick one and
  justify it.

### Concurrency

- `Do` is safe for concurrent callers.
- Do not hold a mutex while calling `op` or while waiting a backoff.
- `New` and `Do` start no goroutines that outlive them. `Budget` has no
  `Close` method.

Assume `op` returns soon after its context is cancelled, and that retries are
applied only to operations whose side effects are idempotent. Jitter is
outside this implementation.

## Validation

- `MaxAttempts < 1`, `Burst < 1`, a nil `Policy`, or a `Ratio` or
  `MinRetriesPerSecond` that is negative, NaN, or infinite returns
  `ErrInvalidConfig` and a nil `*Budget`.
- `Ratio` and `MinRetriesPerSecond` may be zero.
- `Do` with a nil `op` returns `ErrNilOperation` and deposits nothing.

## Before coding

1. Over an interval of length `T` in which `N` first attempts start, how many
   retries can the budget allow at most? Which term dominates while the
   dependency is down, and which while the service is quiet?
2. Why must one budget be shared by all callers instead of being created per
   `Do` call, and what would a per-call budget degenerate into?
3. What is the linearization point of a retry decision: which state is read
   and changed in one step, and what must never happen between the check and
   the withdrawal?
4. Why ask the budget before the backoff rather than after it, and what
   happens to a token withdrawn for a retry that is then abandoned because
   the caller's context was cancelled?
