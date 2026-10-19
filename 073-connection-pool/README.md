# Connection pool

**Difficulty:** hard · **Topics:** bounded resources, slot accounting, FIFO waiters, handoff vs cancellation race, lazy idle eviction, shutdown

Keep a bounded set of expensive connections and lend them to callers: reuse
an idle connection when one is healthy, dial a new one while the pool is below
its limit, and make everyone else wait in line until a connection or a slot
comes back. This is the shape of `database/sql`'s pool, Redis and memcached
client pools, and pools of gRPC channels or HTTP/2 connections: the limit
protects the server (which has its own `max_connections`), and the idle set
saves the cost of a handshake on every request.

This task builds on 005-executor (a bounded resource, explicit overload
behaviour, shutdown semantics) and 006-future (waiters with independent
cancellation). The new part is that the thing being waited for is handed to
one specific waiter, and that handoff can race with that waiter giving up.

Implement `New`, `Get`, `Put`, `Close`, and `Stats` in `starter.go`.

```go
type Conn interface {
	Healthy() bool
	Close() error
}

type Dialer interface {
	Dial(ctx context.Context) (Conn, error)
}

type Stats struct {
	Open    int
	Idle    int
	Waiting int
}

func New(dialer Dialer, maxOpen, maxIdle int, idleTimeout time.Duration) (*Pool, error)
func (p *Pool) Get(ctx context.Context) (Conn, error)
func (p *Pool) Put(conn Conn, err error)
func (p *Pool) Close() error
func (p *Pool) Stats() Stats
```

## Requirements

### Slots and limits

- A *slot* is held by every dial in progress and by every connection that has
  been dialed and not yet closed, whether it is idle, checked out, or being
  closed. At most `maxOpen` slots are held at any moment.
- A slot is taken before `Dialer.Dial` is called. It is released only after a
  failed `Dial` has returned, or after `Conn.Close` for its connection has
  returned. The server therefore never sees more than `maxOpen` connections,
  not even for the moment it takes to close one.
- At most `maxIdle` connections are in the idle set.
- Do not hold the pool mutex while calling `Dialer.Dial` or `Conn.Close`.
  `Conn.Healthy` is cheap and may be called under the mutex.
- The pool starts no goroutines: all work happens in the goroutines that call
  `Get`, `Put`, and `Close`. `New` does not dial.
- All methods are safe for concurrent use. No goroutine leaks and no send on a
  closed channel.

### Get

- If `ctx` is already done, `Get` returns `ctx.Err()` without taking an idle
  connection, a slot, or a place in the queue.
- After `Close`, `Get` returns `ErrClosed`.
- Otherwise `Get` takes a connection from the idle set. An idle connection
  that has been idle for `idleTimeout` or longer (measured from the `Put` that
  made it idle), or whose `Healthy` reports false, is closed instead, and
  `Get` moves on. After closing it, `Get` may keep that connection's slot for
  its own dial or release it. Which idle connection is tried first is your
  choice.
- If no usable idle connection exists and fewer than `maxOpen` slots are held,
  `Get` takes a slot and calls `Dialer.Dial` with its own `ctx` (or a context
  derived from it): the caller's context bounds the dial, the connection
  outlives the call.
- If `Dial` fails, `Get` releases the slot and returns the dial error
  (`errors.Is` must match). If `Dial` returns a connection, `Get` returns it,
  even if `ctx` is done by then.
- Otherwise `Get` waits.

### Waiting

- Waiters are served in FIFO order.
- When `Put` returns a usable connection while callers wait, it hands the
  connection directly to the waiter that has waited longest. The connection
  does not pass through the idle set, and a `Get` that arrives later cannot
  take it.
- When a slot is released while callers wait, it passes to the waiter that
  has waited longest, which then dials with its own `ctx` as above.
- A waiter whose `ctx` is done returns `ctx.Err()`. It no longer counts in
  `Stats().Waiting` once `Get` has returned, and no later `Put` hands it
  anything.
- If a connection or a slot is assigned to a waiter at the same moment its
  `ctx` is done, `Get` either uses it (returns the connection with a nil
  error, or dials with the slot), or returns `ctx.Err()` after giving it back
  to the pool exactly as `Put(conn, nil)` or a slot release would. The give
  back is complete before `Get` returns. A connection or a slot is never lost.

### Put

- `Put(conn, err)` returns a connection obtained from `Get`. Call it exactly
  once per connection; anything else is a caller bug with undefined behaviour.
- If `err` is not nil or `conn.Healthy()` reports false, the connection is
  broken: `Put` closes it and then releases its slot.
- Otherwise the connection goes to the longest waiting `Get`; if nobody waits,
  to the idle set if it holds fewer than `maxIdle` connections; otherwise
  `Put` closes it and releases its slot.
- After `Close`, `Put` closes the connection and releases its slot.
- `Put` blocks only inside `Conn.Close`.

### Close

- The first `Close` makes every later `Get` return `ErrClosed`, wakes every
  waiter with `ErrClosed`, and closes every idle connection, releasing their
  slots before it returns. It returns the `errors.Join` of the errors from
  closing idle connections (nil if there were none).
- `Close` does not wait for checked-out connections or dials in progress.
  Checked-out connections are closed by their `Put`. A dial in progress when
  `Close` is called may complete: `Get` then either returns the connection
  (its later `Put` closes it) or closes it, releases its slot, and returns
  `ErrClosed`. Both are accepted.
- Repeated and concurrent `Close` calls are safe and never panic. Calls after
  the first return nil and need not wait for the first to finish.

### Stats

- `Open` is the number of slots held, as defined above.
- `Idle` is the number of connections in the idle set, including expired ones
  that no `Get` has evicted yet: eviction is lazy.
- `Waiting` is the number of `Get` calls currently waiting.
- `Stats` never blocks on `Dial` or `Close`.

Assume `Dial` returns either a connection or an error, and returns soon after
its context is done. Bounding the number of waiters, a maximum connection
lifetime, and background eviction are follow-up design questions, not
requirements.

## Validation

- `maxOpen` must be positive, `maxIdle` must be between 0 and `maxOpen`
  inclusive, and `idleTimeout` must be positive; otherwise return
  `ErrInvalidConfig`.
- A nil dialer returns `ErrNilDialer`.
- On error `New` returns a nil `*Pool`.

## Before coding

1. What exactly does a slot represent, at which moments is it taken and
   released, and why must a dial in progress and a connection being closed
   still hold one?
2. When `Put` returns a healthy connection and callers are waiting, where does
   the connection go, and what goes wrong if it goes to the idle set with a
   "wake up" signal instead?
3. A waiter's context is cancelled at the same moment a connection is
   assigned to it. What is the linearization point that decides who owns the
   connection, and how does each side learn the outcome?
4. After `Close`, what happens to idle connections, waiters, dials in
   progress, and checked-out connections, and who closes each of them?
