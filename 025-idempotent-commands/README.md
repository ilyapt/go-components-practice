# Idempotent command processor

**Difficulty:** hard · **Topics:** idempotency keys, retained deduplication records, payload conflicts, shared execution, waiter cancellation

Execute each command at most once per command ID and give every duplicate the
same stored outcome. Clients retry on timeouts, brokers redeliver messages, and
users double-click; an idempotency layer in front of payment APIs, job
submission endpoints, and message consumers turns those duplicates into
harmless lookups instead of repeated side effects.

Implement `New` and `Handle` in `starter.go`.

```go
func New(workCtx context.Context, handler Handler) (*Processor, error)
func (p *Processor) Handle(ctx context.Context, cmd Command) (Result, error)
```

## Requirements

- The first accepted command for an ID invokes the handler exactly once.
- Concurrent and later commands with the same ID and an identical payload
  receive the same stored result or error.
- The same ID with a different payload returns `ErrPayloadConflict` and does
  not invoke the handler, whether the original command is still running or
  already finished.
- Handler errors are stored like successful results and are not retried.
- A caller's context bounds only how long that caller waits. Its cancellation
  does not cancel the shared execution.
- A caller whose context is already cancelled does not admit a new command:
  it returns `ctx.Err()` and the ID stays unclaimed.
- The handler receives the processor-owned `workCtx` passed to `New`, not a
  caller context. If `workCtx` is already cancelled, the handler is not
  invoked and the command's outcome is `workCtx.Err()`.
- Do not hold a mutex while invoking the handler or while a caller waits.
  Commands with different IDs never wait for each other.
- A nil handler returns `ErrNilHandler`; an empty command ID returns
  `ErrInvalidCommand`.
- Completed records are kept for the lifetime of the processor.

Assume callers do not mutate `Command.Payload` after calling `Handle`, and that
the number of distinct commands in flight is bounded externally. Persistence,
eviction of old records, retries, and crash recovery are discussion topics,
not implementation requirements.

## Before coding

1. At what point is a command ID considered claimed, and what happens to the
   claim when the caller's context is already cancelled?
2. What state must remain after execution completes, and why is the payload
   part of it?
3. Who owns the context the handler runs under, and what can a waiter's
   cancellation change?
4. Why does this give at-most-once execution only within one processor
   lifetime?
