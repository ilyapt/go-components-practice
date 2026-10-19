# Per-key serializer

**Difficulty:** medium · **Topics:** keyed queues, FIFO admission, waiter cancellation, per-key state cleanup

Run operations one at a time per key, in arrival order, while different keys
proceed in parallel. This is how systems keep updates to one account, one
workflow execution, or one partition ordered without a global lock: actor
mailboxes, per-entity event handlers, keyed Kafka consumers.

Implement `New` and `Process` in `starter.go`.

```go
func New() *Processor
func (p *Processor) Process(ctx context.Context, key string, op Operation) error
```

## Requirements

- Operations admitted for the same key execute one at a time.
- Same-key operations execute FIFO, in the order they are admitted under the
  processor's internal synchronization.
- Operations for different keys may execute concurrently.
- `Process` waits for its operation and returns the operation's error, unless
  its caller context is cancelled first.
- A caller whose context is already cancelled is not admitted and receives
  `ctx.Err()`.
- If a caller is cancelled while queued, `Process` returns `ctx.Err()`
  promptly and that operation is skipped when it reaches the head.
- An operation runs with its caller's context. If cancellation occurs after
  execution starts, the operation observes it; the next same-key operation must
  not start until the running operation returns.
- An operation executes at most once.
- A caller that stops waiting must never block the processing of its key.
- Do not hold a mutex while an operation executes or while a caller waits for
  its turn or its result.
- Remove per-key state once its queue becomes empty; inactive keys must not
  accumulate forever.
- A nil operation is invalid and returns an error without being admitted.

Assume `Operation` returns after cancellation of its context. The per-key queue
is intentionally unbounded in this task; be ready to discuss production
overload behavior.

## Before coding

1. What defines FIFO order for concurrent callers?
2. Who executes the operations of one key, and when does that executor stop?
3. What happens to a cancelled request that is still inside a key queue?
4. How will cleanup of a key's state avoid racing with a new request for the
   same key?
