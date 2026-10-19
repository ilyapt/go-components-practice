# Bounded parallel map

**Difficulty:** medium · **Topics:** bounded concurrency, ordered results, cancellation, error propagation

Apply a function to a slice of inputs concurrently, with a hard limit on how
many calls run at once. This is the smallest useful piece of every fan-out:
fetching N resources, validating N items, calling N downstream services.

Implement `Map` in `starter.go`.

```go
func Map(
    ctx context.Context,
    inputs []Input,
    limit int,
    fn func(context.Context, Input) (Output, error),
) ([]Output, error)
```

## Requirements

- Apply `fn` to every input with at most `limit` calls running concurrently.
- Return outputs in the same order as inputs, regardless of completion order.
- If any call returns an error, return an error and stop unnecessary work
  promptly: cancel calls in flight and do not start new ones.
- If `ctx` is cancelled, return `ctx.Err()` even if `fn` itself returned
  successfully after the cancellation.
- `Map` returns only after every call it started has returned.
- Do not leak goroutines.
- `limit <= 0` or a nil `fn` is invalid and returns an error without calling `fn`.
- An empty input slice returns an empty output slice and no error.

You may choose the exact error returned for invalid arguments, and which error
wins when cancellation races with a failing call. Be ready to explain your
choices.

## Before coding

1. What exactly does `limit` bound, and which mechanism enforces it?
2. How do results end up in input order when calls finish in any order?
3. When `Map` returns, what guarantees that no goroutine it started is still
   running or blocked?
4. Who cancels in-flight calls after a failure, and through which context?
