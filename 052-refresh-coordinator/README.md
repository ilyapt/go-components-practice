# State refresh coordinator

**Difficulty:** hard · **Topics:** bounded workers, per-resource coalescing, dirty follow-up, immutable snapshot publication, graceful shutdown

Keep a published, read-mostly view of many resources fresh while change
events keep arriving. Events say "resource X may have changed"; the
coordinator reloads X with bounded concurrency, folds repeated events for the
same resource into one load, and atomically publishes a new immutable
snapshot that readers use without locks. Components like this sit behind
caches of on-chain or market state, configuration and service-discovery
watchers, and search or pricing indexes fed by change streams.

This task combines earlier patterns into one component: fixed workers with a
bounded queue and graceful shutdown (005-executor), one load per key with
coalescing (023-singleflight), and copy-on-write snapshot publication
(047-immutable-snapshot).

Implement `New`, `Trigger`, `Current`, and `Shutdown` in `starter.go`.

## Requirements

### Refresh

- At most `concurrency` calls to the `Loader` run at once.
- At most one load for a given resource ID runs at once.
- A trigger for a resource that is neither queued nor running is admitted to
  a bounded FIFO input queue.
- `queueCapacity` counts distinct resource IDs waiting in the queue; running
  resources are not included.
- If a new resource cannot be queued, `Trigger` returns `ErrFull` and the
  resource is not admitted.
- Repeated triggers for an already queued resource are coalesced and return
  `nil`, even if the queue is full.
- A trigger for a resource whose load is running marks it dirty. After the
  current load returns, that resource is loaded once more.
- Many triggers during one running load coalesce into one follow-up load.
  A trigger during the follow-up load may schedule one further load.
- `Trigger` returning `nil` means the refresh request was admitted or
  coalesced; it does not mean that loading succeeded.
- A pre-cancelled trigger context is not admitted and returns `ctx.Err()`.
- The loader receives the coordinator-owned `workCtx` passed to `New`;
  trigger contexts never control shared work.

### Publication

- `Current` starts as a non-nil empty snapshot at version `1`.
- Each successful load publishes that resource's new state with
  copy-on-write and increments the snapshot version by one.
- Failed loads leave the published snapshot unchanged.
- `Current` is lock-free and returns an immutable snapshot that stays valid
  and unchanged after later publications.
- Successful loads of different resources must not overwrite each other's
  published updates.
- Every successful load is published as soon as it returns, independently
  of any follow-up load of the same resource.

### Shutdown

- `Shutdown` stops admission, lets accepted queued work and any dirty
  follow-up work finish, then waits for all workers to exit.
- Triggers after shutdown begins return `ErrClosed`.
- Repeated and concurrent calls to `Shutdown` are safe. Each caller waits for
  completion or for its own context cancellation.
- Cancelling a shutdown waiter does not cancel loader work.
- Even a pre-cancelled first `Shutdown` call initiates shutdown before
  returning its context error.
- `Trigger` and `Shutdown` may be called concurrently; this must never panic.

## Validation

- Non-positive `concurrency` or `queueCapacity` returns `ErrInvalidConfig`.
- A nil loader returns `ErrNilLoader`.
- An empty resource ID returns `ErrInvalidID` from `Trigger`.

Assume the `Loader` eventually returns and respects cancellation of
`workCtx`. Loader errors are not exposed through this API; how you would
observe them in production is a design discussion.

## Before coding

1. Write the per-resource state machine: what happens to `queued`,
   `running`, and `dirty` on each trigger and on each load completion?
2. How does a worker finish a load without losing a trigger that races with
   the completion?
3. Which lock or atomic protects admission, per-resource state, and snapshot
   publication? Can two workers publish at the same time?
4. What tells `Shutdown` that all accepted work, including dirty follow-ups,
   is done?
