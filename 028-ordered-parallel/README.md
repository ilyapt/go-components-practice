# Ordered parallel stage

**Difficulty:** hard · **Topics:** streaming, ordered emission, bounded window, backpressure, head-of-line blocking, cancellation

Read a stream of items, process them in parallel, and emit the results in
exactly the order the items arrived, using bounded memory however long the
stream is. Stages like this sit in log and event pipelines, in Kafka
consumers that enrich records but must commit offsets in order, in ETL jobs
that call a slow API per row, and in replication appliers that may compute in
parallel but must apply in sequence.

This task builds on 001-bounded-map (bounded concurrency with ordered
results, but over a finite slice that fits in memory) and 024-per-key-serializer
(what an ordering guarantee means under parallelism). Here the input is an
unbounded channel, so the stage cannot collect everything and sort at the
end: it must decide how far ahead of the slowest item it may run.

Implement `Run` in `starter.go`.

```go
type ProcessFunc func(ctx context.Context, item Item) (Output, error)

func Run(ctx context.Context, in <-chan Item, workers, window int, fn ProcessFunc) (<-chan Result, error)
```

Three terms are used throughout:

- An item is **taken** when the stage receives it from `in`.
- An item is **emitted** when the consumer receives its `Result` from the
  returned channel.
- An item is **in flight** from the moment it is taken until it is emitted:
  waiting for a worker, being processed, or finished and waiting for its turn.

## Head-of-line blocking

Ordered output with a bounded window has an unavoidable cost. If item 0 is
slow, items 1, 2, 3, ... may finish, but their results cannot be emitted
before item 0's. They occupy the window. Once `window` items are in flight,
the stage stops taking input, even if every worker except one is idle, until
item 0 finishes and the consumer receives it. One slow item therefore stalls
the whole stream, and a producer upstream sees it as backpressure. A larger
`window` tolerates more latency variance at the cost of memory; it cannot
remove the stall, only postpone it.

## Requirements

### Order and the window

- Results are emitted in exactly the order in which their items were taken
  from `in`, regardless of the order in which `fn` calls complete. The stage
  never looks at `Item.ID`.
- `Result.Item` is the taken item; `Result.Output` and `Result.Err` are
  exactly what `fn` returned for it.
- The returned channel is unbuffered, so the moment of emission is the moment
  the consumer receives.
- At most `window` items are in flight at any moment. While the window is
  full, the stage does not read `in`; a producer sending on `in` blocks. This
  is the stage's backpressure.
- The stage does not wait needlessly: whenever ctx is not done, fewer than
  `window` items are in flight, fewer than `workers` calls of `fn` are
  running, and an item is available on `in`, the stage takes it and starts
  `fn` on it, without waiting for the consumer or for earlier items to finish.

### Concurrency

- At most `workers` calls of `fn` run concurrently.
- `fn` is called at most once per item, and exactly once for every taken item
  unless ctx is cancelled first.
- The number of goroutines the stage runs is bounded by `workers` and
  `window`, never by the length of the input. Whether you run a fixed pool of
  `workers` goroutines or one goroutine per in-flight item is your choice; be
  ready to defend it.

### Errors and completion

- An error from `fn` is a per-item outcome, not a stage failure: it is
  delivered in that item's `Result`, at that item's position, and later items
  are processed and emitted as usual.
- When `in` is closed, the stage processes and emits every item it has taken
  and then closes the returned channel. While `in` is open, the returned
  channel stays open, even if every taken item has been emitted.

### Cancellation

- `fn` receives ctx or a context derived from it, so cancelling ctx cancels
  every running call.
- Once ctx is done, the stage takes no more items from `in` and starts no new
  `fn` call. It does not drain `in`: a producer blocked on sending must watch
  ctx itself.
- After cancellation, items already taken are not necessarily emitted.
  Whatever is emitted is still in order with no gaps: the emitted results
  always correspond to a prefix of the taken items. A result emitted after
  cancellation may carry the context error.
- After cancellation, the stage closes the returned channel once every `fn`
  call it started has returned, and not before. It does this whether or not
  anyone is still reading: it never blocks forever trying to emit after ctx
  is done.
- A ctx that is already done when `Run` is called is not an error: `Run`
  returns a channel that is closed without reading `in` and without calling
  `fn`.

### Ownership and leaks

- The consumer must either read the returned channel until it is closed or
  cancel ctx. Cancellation is the only way to stop reading early; a consumer
  that walks away without cancelling leaves the stage blocked with a full
  window, and that is the caller's bug.
- No goroutine leaks, however the stage ends: by the time the returned
  channel is closed, every goroutine the stage started has exited or is about
  to exit without blocking.
- The stage owns the returned channel and is its only sender and closer. It
  never sends on it after closing it.

Assume `fn` returns soon after its context is cancelled and does not panic.

## Validation

- `workers <= 0`, or `window < workers` (some workers could never be used),
  returns `ErrInvalidConfig`.
- A nil `fn` returns `ErrNilFunc`; a nil `in` returns `ErrNilInput`.
- On a validation error `Run` returns a nil channel and starts no goroutines.

## Before coding

1. At which exact moments does an item enter and leave the window, and which
   mechanism makes it impossible for the stage to hold `window + 1` items?
2. When item 7 finishes while item 3 is still running, where does its result
   wait, and which single place decides that it is its turn to be emitted?
3. Who closes the returned channel, and what must have happened before it
   may: after `in` is closed, and after ctx is cancelled?
4. A per-item error and a cancelled ctx both end some processing. Which one
   ends the stage, what does the consumer observe in each case, and why are
   they treated differently?
