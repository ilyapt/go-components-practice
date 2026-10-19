# Retry with context-aware backoff

**Difficulty:** easy · **Topics:** retry classification, bounded attempts, cancellable waiting, timer cleanup

Call an operation again after a transient failure, waiting between attempts,
until it succeeds, fails permanently, runs out of attempts, or the caller gives
up. Retry loops wrap almost every call to an unreliable dependency: RPC
clients, database drivers, message publishers, cloud SDKs.

Implement `Do` in `starter.go`.

```go
func Do(
    ctx context.Context,
    maxAttempts int,
    policy RetryPolicy,
    op Operation,
) (Result, error)
```

## Requirements

- `maxAttempts` includes the initial call. Invoke `op` at most `maxAttempts`
  times.
- Attempts are numbered from 1; `Backoff(attempt)` receives the number of the
  attempt that just failed.
- Return immediately on success.
- Return immediately when `policy.Retryable(err)` is false.
- After retryable failure number `attempt`, wait `policy.Backoff(attempt)`
  before the next call. A zero or negative backoff means retry immediately.
- When attempts are exhausted, return the last operation error without waiting
  again.
- Cancellation before an attempt or during backoff returns `ctx.Err()` without
  another call.
- If `op` returns after `ctx` has been cancelled, the context error takes
  precedence over whatever `op` returned.
- Backoff waiting must be context-aware: no unconditional `time.Sleep`.
- Release timer resources promptly.
- `maxAttempts <= 0`, a nil policy, or a nil operation is invalid and returns
  an error without calling `op`.

Assume retries are applied only to operations whose side effects are
idempotent or otherwise safely deduplicated. Jitter is outside this
implementation but worth discussing.

## Before coding

1. Does `maxAttempts` include the first call, and how many times is `Backoff`
   consulted when every attempt fails?
2. Which error wins when cancellation races an operation failure?
3. Where exactly in the loop is cancellation checked, and what does the wait
   look like?
4. Why can a technically correct retry loop still be unsafe in production?
