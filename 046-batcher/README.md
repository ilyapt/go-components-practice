# Count-or-time batcher

**Difficulty:** medium · **Topics:** single-owner loop, timer lifecycle, bounded admission, graceful flush

Collect items and hand them downstream in batches: when the batch is full, or
when its oldest item has waited long enough. Batchers sit in front of
databases, message brokers, and RPC endpoints wherever per-call overhead
dominates.

Implement `New`, `Submit`, and `Shutdown` in `starter.go`.

## Requirements

- One batcher-owned goroutine owns the current batch and its timer.
- Accepted items are delivered to `flush` exactly once and in admission order.
- Flush when the current batch reaches `maxSize`, or when `maxDelay` has
  elapsed since the first item entered that batch.
- The timer starts with the first item of a new batch. Adding more items does
  not extend its deadline. An empty batch is never flushed.
- `Submit` returning `nil` means the item was accepted, not necessarily flushed.
- `queueCapacity` bounds items waiting in the input queue; it does not include
  items already moved into the current batch.
- `Submit` returns `ErrFull` instead of blocking when the input queue is full.
- A caller whose context is already cancelled is not admitted and receives
  `ctx.Err()`.
- `Shutdown` stops admission, drains all accepted items, flushes the final
  partial batch, and waits for the batching goroutine to exit.
- Once shutdown begins, later submissions return `ErrClosed`.
- Repeated and concurrent calls to `Shutdown` are safe. Each caller waits for
  completion or for its own context cancellation.
- A cancelled shutdown waiter does not cancel the batcher or `flush`.
- `Submit` and `Shutdown` may be called concurrently; this must never panic.
- Do not hold a mutex while calling `flush` or while waiting for shutdown.

`flush` is synchronous: it must return before the next batch is flushed.
Assume it eventually returns and does not panic. The batcher must not reuse or
mutate a slice after passing it to `flush`.

## Validation

- `maxSize`, `maxDelay`, and `queueCapacity` must be positive; otherwise
  return `ErrInvalidConfig`.
- A nil `flush` returns `ErrNilFlush`.

## Before coding

1. Who owns the input queue, the current batch, the timer, and channel closure?
2. Exactly when is the timer started and stopped?
3. How will shutdown stop admission without racing with `Submit`?
4. How does `Shutdown` know that the final partial batch has been flushed?
