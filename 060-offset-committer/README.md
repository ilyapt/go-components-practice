# Offset committer

**Difficulty:** hard · **Topics:** contiguous done prefix, at-least-once delivery, single-owner commit loop, count-or-time trigger, bounded window, final commit on close

A Kafka-style consumer reads a partition as a log. Every message has an
offset, and the only durable record of progress is one number per partition:
the committed offset, which is the next offset to read after a restart. With
one message at a time this is trivial. With messages processed in parallel it
is easy to get wrong: if message 7 finishes before message 5 and the consumer
commits 8, a crash before message 5 finishes loses message 5 for good,
because the restarted consumer starts reading at 8 and nothing reports an
error. The only safe commit is the end of the contiguous done prefix: past the
highest offset such that it and every tracked offset before it are processed.
Messages done after a gap are not committed yet and are re-delivered after a
crash. That is at-least-once delivery, and it is why handlers must be
idempotent.

The offset committer sits between the fetch loop and the workers of such a
consumer (Kafka and Pulsar consumers, Kinesis shard readers, CDC pipelines).
The fetch loop calls `Track` for every message it hands out, workers call
`Done` when a message is processed, and the committer turns the stream of
completions into a few monotonic `Commit` calls: every `interval`, or sooner
when enough messages are done. The window of tracked but uncommitted offsets
is bounded, which bounds both memory and how much is re-delivered after a
crash.

This task builds on 024-per-key-serializer (which ordering survives parallel
execution) and 046-batcher (one goroutine owns the timer and the flush,
triggered by count or by time). 028-ordered-parallel solves the neighbouring
problem of emitting results in input order; here processing stays unordered
and only the committed position must respect order.

Implement `New`, `Track`, `Done`, `Committed`, and `Close` in `starter.go`.

```go
type Committer interface {
	Commit(ctx context.Context, offset int64) error
}

func New(committer Committer, interval time.Duration, maxUncommitted, maxInFlight int) (*OffsetCommitter, error)
func (c *OffsetCommitter) Track(offset int64) error
func (c *OffsetCommitter) Done(offset int64) error
func (c *OffsetCommitter) Committed() int64
func (c *OffsetCommitter) Close(ctx context.Context) error
```

## Requirements

### Tracking

- `Track(offset)` registers an offset as in flight. Offsets must be strictly
  increasing across `Track` calls: an offset that is not greater than every
  previously tracked offset returns `ErrOutOfOrder`. Before the first `Track`
  the last tracked offset counts as -1, so a negative offset also returns
  `ErrOutOfOrder`.
- Offsets need not be consecutive. Numbers that were never tracked
  (compacted messages, transaction markers) are not gaps: the done prefix is
  defined over tracked offsets only.
- A rejected `Track` changes nothing: the offset is not tracked, does not
  occupy the window, and may be tracked by a later call.
- `Done(offset)` marks a tracked offset as processed. Each tracked offset is
  marked done at most once. `Done` for an offset that is not tracked and
  pending returns `ErrUnknownOffset`: an offset that was never tracked, one
  already marked done (a repeated `Done` is an error, not a no-op), and one
  already committed.
- A rejected `Done` changes nothing and does not count toward
  `maxUncommitted`.
- `Track`, `Done`, `Committed`, and `Close` are safe for concurrent use. The
  order of concurrent `Track` calls is the order in which they are admitted
  under the committer's internal synchronization.
- When several errors apply, `ErrClosed` wins, then `ErrOutOfOrder`, then
  `ErrWindowFull`.

### Window

- An offset occupies the window from its successful `Track` until a
  successful `Commit` covers it (the committed value is greater than the
  offset). An offset that is done but not yet committed still occupies it.
- At most `maxInFlight` offsets occupy the window. A `Track` that would
  exceed it returns `ErrWindowFull` immediately. `Track` never blocks.
- `maxInFlight` counts tracked offsets, not the numeric distance between
  them.

### Commit value

- The done prefix is the longest run of uncommitted tracked offsets, starting
  at the oldest one, that are all done. The commit value is one past the
  highest offset of the done prefix (Kafka's "next offset to read"): with
  offsets 1, 2, 3, 4 tracked and 1, 2, 4 done, the value is 3; once 3 is
  done, it is 5.
- There is something to commit when the commit value is greater than the
  last successfully committed value. `Commit` is called only then: a
  successful commit is never repeated with the same or a lower value.
- Each attempt passes the commit value computed when that attempt starts.
- `Committed` returns the last successfully committed value, or 0 before the
  first successful commit.

### Triggers and commit calls

- One committer-owned goroutine calls `Committer.Commit`. `Track`, `Done`,
  and `Committed` never call `Commit` and never wait for it.
- Time trigger: on a fixed grid of `interval` after `New` (at `interval`,
  `2*interval`, ...), an attempt starts if there is something to commit. A
  tick with nothing to commit does nothing.
- Count trigger: once at least `maxUncommitted` successful `Done` calls have
  happened since the last attempt started (whatever triggered it), an attempt
  starts promptly, without waiting for the next tick, if there is something
  to commit. If a gap blocks the prefix, no attempt starts, and the first
  `Done` that moves the commit value starts one.
- At most one `Commit` call is in flight at any time. A trigger that fires
  while an attempt is in flight does not start a second one. Whether the next
  attempt starts as soon as the in-flight one returns or at the next trigger
  is your choice; either way it passes the then-current commit value.
- A failed `Commit` leaves `Committed` and the window unchanged. It is retried
  by the next trigger with the commit value current at that point, not the
  stale value of the failed attempt. Never retry in a loop between triggers.
- `Commit` receives a committer-owned work context. It is cancelled only when
  a `Close` context is done before the final commit succeeds (or after the
  committer goroutine has exited). It never carries a caller's values.
- Do not hold a mutex while calling `Commit`.

### Close

- The first `Close` call atomically stops admission: from then on `Track` and
  `Done` return `ErrClosed`. The commit value at that moment is final. Offsets
  not done by then are not committed; they will be re-delivered after a
  restart.
- If there is something to commit, `Close` starts the final attempt promptly
  (after the in-flight attempt, if any, returns) and retries it every
  `interval` until it succeeds or a `Close` context is done.
- `Close` waits for the committer goroutine to exit. It returns `nil` if the
  final value was committed or there was nothing to commit, even if its
  context is done by then.
- If a `Close` context is done before the final commit succeeds: cancel the
  work context, wait for the committer goroutine to exit (an in-flight
  `Commit` must return first), record `ctx.Err()` as the outcome, and return
  it.
- Repeated and concurrent `Close` calls are safe and never panic. Each caller
  waits for completion or for its own context, and any caller whose context is
  done forces the stop as above. A caller whose own context is not done
  returns the outcome: `nil`, or the recorded error.
- Once `Close` has completed, later `Close` calls never call `Commit`.
- No goroutine leaks, no send on a closed channel.

Assume `Commit` returns soon after its context is cancelled and does not
panic. One committer serves one partition.

## Validation

- A nil committer returns `ErrNilCommitter`.
- `interval`, `maxUncommitted`, and `maxInFlight` must be positive, and
  `maxUncommitted` must not exceed `maxInFlight` (otherwise the count trigger
  could never fire); otherwise return `ErrInvalidConfig`.

## Before coding

1. What does a successful `Commit(n)` promise to a consumer that restarts
   after a crash, and which messages may it see twice? Why must `n` never
   cover an offset that is not done, even when every later offset is done?
2. Who owns the ticker and calls `Commit`, and how does the `Done` call that
   reaches `maxUncommitted` get that owner to act without blocking and
   without starting a second concurrent `Commit`?
3. At which moment does an offset stop occupying the window, and why not
   earlier? After a failed `Commit`, which value does the next attempt pass?
4. What is the linearization point of `Close`, and what exactly does `nil`
   from `Close` promise about the restart position?
