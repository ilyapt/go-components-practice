# Single-flight deduplication

**Difficulty:** medium · **Topics:** in-flight map, shared result publication, waiter vs work cancellation, cleanup

When many callers ask for the same expensive thing at the same moment, run the
work once and give every caller the same answer. Single-flight groups sit in
front of caches, configuration and metadata loaders, token refreshers, and RPC
clients to stop thundering herds on a cold or expired key.

Implement `New` and `Get` in `starter.go`.

```go
func New(workCtx context.Context, loader Loader) (*Group, error)
func (g *Group) Get(ctx context.Context, key string) (Value, error)
```

## Requirements

- Concurrent `Get` calls for the same key share exactly one `Loader.Load`
  execution.
- Calls for different keys may execute independently and concurrently.
- All waiters that observe completion receive the same `Value` and error.
- Cancelling one waiter returns that waiter's `ctx.Err()` and does not cancel
  the shared work, even if it was the caller that started it.
- `Loader.Load` receives the group-owned `workCtx` (or a context derived from
  it), never a waiter's context.
- Cancelling `workCtx` cancels all underlying loads.
- A caller whose context is already cancelled returns `ctx.Err()` and must not
  start new work.
- Remove an in-flight entry once its execution completes; a later `Get` starts
  a fresh load.
- Do not hold the group mutex while calling `Loader.Load` or while waiting.
- A nil loader is invalid: `New` returns an error.

Assume `Loader.Load` returns after `workCtx` is cancelled and does not panic.
This component deduplicates only in-flight work. It is not a result cache.

## Before coding

1. Which context is passed to `Loader.Load`, and why not the caller's?
2. What state is protected by the mutex, and what is published without it?
3. At what exact point is an entry removed from the in-flight map, and who
   removes it?
4. What happens to the load when every waiter has given up?
