# Write-behind cache

**Difficulty:** hard · **Topics:** dirty tracking, write coalescing, flush race, retry without a tight loop, backpressure, graceful shutdown

Accept writes into memory, answer reads from memory, and persist the changes
to a slow store later, in batches. A key written a hundred times between two
flushes costs the store one write. Write-behind (write-back) caches sit in
front of databases for counters, session and presence data, rate-limit
state, user preferences, and metrics aggregation: anywhere the write rate is
much higher than what the store can absorb, and a short persistence delay is
acceptable.

This task builds on 046-batcher (flush by count or by time, one goroutine owns
the flushing and the timer, graceful final flush) and 047-immutable-snapshot
(readers never wait for the slow path; a batch is a consistent snapshot taken
in one step). The new part is that the batched state keeps changing while a
flush is in flight.

Implement `New`, `Put`, `Get`, and `Shutdown` in `starter.go`.

```go
type Entry struct {
	Key   string
	Value string
}

type Store interface {
	Write(ctx context.Context, entries []Entry) error
}

func New(store Store, maxDirty int, maxDelay time.Duration, maxPending int) (*Cache, error)
func (c *Cache) Put(ctx context.Context, key, value string) error
func (c *Cache) Get(key string) (string, bool)
func (c *Cache) Shutdown(ctx context.Context) error
```

Terms used below:

- A **flush** is one `Store.Write` call. It **starts** at the moment it takes
  its snapshot of the dirty keys and their values.
- A key is **dirty** from a successful `Put` until a flush that contains that
  `Put`'s value (or a later value of the key) succeeds. Keys in a flush that
  is in flight are still dirty.
- A `Put` is **picked up** by the first flush that starts after it.

## Requirements

### Reads and writes

- `Get` returns the value of the latest successful `Put` for the key, whether
  or not it has been persisted (read-your-writes), and `false` if the key was
  never put. The cache never reads from the store.
- `Put` returning `nil` means the value is visible to `Get` and the key is
  dirty. It does not mean the value is persisted.
- `Put` and `Get` never wait for `Store.Write`: they return while a flush is in
  flight. Do not hold a mutex while calling `Store.Write`.
- `Put` checks, in this order: a caller whose context is already done gets
  `ctx.Err()`; after shutdown has begun, `ErrClosed`; then backpressure (below).
  A rejected `Put` changes nothing.
- `Get` keeps working after `Shutdown` and returns the in-memory values,
  persisted or not.

### Flushing

- One cache-owned flusher goroutine calls `Store.Write`. At most one `Write`
  is in flight at any time.
- A flush writes every dirty key exactly once, with its latest value at the
  moment the flush starts. Several `Put`s to one key between two flushes
  produce one entry with the last value. The order of entries within a batch
  is unspecified.
- `Store.Write` is never called with an empty batch. The cache does not reuse
  or mutate a slice after passing it to `Write`.
- **Count trigger.** While no flush is in flight, a flush starts as soon as at
  least `maxDirty` keys are dirty.
- **Delay trigger.** A flush starts once `maxDelay` has elapsed since the
  oldest `Put` that has not been picked up yet. Later `Put`s do not extend this
  deadline. If the deadline passes while a flush is in flight, the next flush
  starts as soon as that flush returns (unless it failed; see below).
- An idle cache with no dirty keys never calls `Write`.

### Failures and the flush race

- A key becomes clean only if a flush containing its current value succeeds.
  A `Put` that lands while a flush is in flight is never lost:
  - if the flush succeeds, the key stays dirty with the new value, and a later
    flush writes it;
  - if the flush fails, the key stays dirty with the new value, not the value
    that failed.
- A failed `Write` persists nothing: every key in its batch stays dirty.
- After a failed `Write`, no flush starts until `maxDelay` after that `Write`
  returned, whatever the triggers say; at that moment the retry flush starts.
  Repeated failures are retried every `maxDelay`, until a flush succeeds or a
  shutdown deadline expires. There is no tight retry loop.
- `Store.Write` receives a cache-owned work context, never a context derived
  from a `Put` or `Shutdown` caller. It is cancelled only when a `Shutdown`
  context is done before the final flush completes (or after the flusher has
  exited).

### Backpressure

- `maxPending` bounds the number of dirty keys, including keys in a flush that
  is in flight or that failed.
- A `Put` that would make a new key dirty while `maxPending` keys are already
  dirty returns `ErrFull` immediately. `Put` never blocks.
- A `Put` to a key that is already dirty always succeeds (subject to the
  context and shutdown checks): it replaces the pending value and does not add
  a dirty key.

### Shutdown

- The first `Shutdown` call atomically stops admission. Every later `Put`
  returns `ErrClosed`.
- `Shutdown` starts a flush as soon as no flush is in flight, without waiting
  for the count or delay triggers. A failed flush during shutdown is retried
  every `maxDelay`, as above. If shutdown begins during a retry delay, you may
  start the flush immediately or at the end of that delay; the choice is
  yours.
- When no dirty key remains, the flusher exits and `Shutdown` returns `nil`.
  A `nil` means every `Put` that returned `nil` has been persisted.
- If the shutdown context is done first:
  - cancel the work context, so an in-flight `Write` returns;
  - start no new `Write`;
  - wait for the flusher goroutine to exit;
  - return `ctx.Err()`.

  Keys still dirty at that point are not persisted; `Get` still returns them.
- Repeated and concurrent `Shutdown` calls are safe and never panic. Each
  caller waits for the flusher to exit or for its own context; any caller
  whose context is done forces the cancellation above and returns its own
  `ctx.Err()`.
- A caller whose context is not done returns `nil` if the flusher exited with
  no dirty key left, and `ErrUnflushed` if it exited with dirty keys left
  (because another caller's context expired). Once the flusher has exited,
  every later `Shutdown` returns immediately with that same answer.
- No goroutine leaks, no timer left running after the flusher exits, and no
  send on a closed channel.

Assume `Store.Write` returns soon after its context is cancelled and does not
panic. Eviction of clean keys and reading through to the store on a miss are
out of scope: the cache keeps every key it was given.

## Validation

- `maxDirty` and `maxDelay` must be positive, and `maxPending` must be greater
  than `maxDirty`; otherwise return `ErrInvalidConfig`.
- A nil store returns `ErrNilStore`.

## Before coding

1. When exactly does a key stop being dirty? Walk through a `Put` that lands
   while a flush containing the same key is in flight, once for a flush that
   succeeds and once for one that fails.
2. Who owns the values, the dirty set, the flush deadline, and the in-flight
   batch? What does the mutex protect, and what must never happen while it is
   held?
3. How is the start of the next flush decided after a successful flush and
   after a failed one, so that no deadline is lost and no retry loop spins?
4. What does `nil` from `Shutdown` promise, and what does each `Shutdown`
   caller return when the deadline of one of them expires with dirty keys
   left?
