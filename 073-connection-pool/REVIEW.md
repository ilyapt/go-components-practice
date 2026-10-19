# Reviewer notes: connection-pool

> Spoilers. Read after your attempt.

## Test coverage notes

- **A connection is counted only after `Dial` returns,** so several callers see
  free capacity, all dial, and the server sees more than `maxOpen` connections
  exactly when it is slow. Caught by the `Stats` checks during a dial in
  `TestDialFailureReleasesSlot` and `TestDialUsesCallerContextOutsideTheMutex`;
  caught only sometimes by the `maxLive` check in
  `TestConcurrentUseRespectsLimitsAndCloses`.
- **The pool mutex is held across `Dial` or `Conn.Close`,** so one slow
  handshake blocks every `Put`, `Get`, and `Stats`. Caught by
  `TestDialUsesCallerContextOutsideTheMutex` and
  `TestSlotIsReleasedOnlyAfterCloseReturns`, which hang until the 60s timeout
  because synctest does not see a goroutine blocked on `sync.Mutex`.
- **A failed dial does not release its slot, or releases it without passing it
  to a waiter:** the pool shrinks permanently, or a waiter sleeps while
  capacity exists. Caught by `TestDialFailureReleasesSlot`.
- **A broken or unhealthy connection is dropped without passing its slot on,**
  so a waiter blocks forever below `maxOpen`. Caught by
  `TestBrokenConnectionIsClosedAndSlotFreed`.
- **The slot is released before `Conn.Close` returns.** Caught deterministically
  by `TestSlotIsReleasedOnlyAfterCloseReturns` (a second dial while the first
  connection is closing); caught only sometimes by the `maxLive` check in
  `TestConcurrentUseRespectsLimitsAndCloses`.
- **The grant is lost in the cancellation race:** the waiter sees `ctx.Done()`,
  returns `ctx.Err()`, and never looks at what `Put` already handed it, so the
  connection vanishes (`Open` 1, nothing idle, an extra dial or a hang later).
  Caught by `TestCancellationRacingHandoffNeverLosesConnection`, which repeats
  the race 50 times, so a lost grant shows up with near certainty.
- **A cancelled waiter is left in the queue,** so a later `Put` hands a
  connection to a goroutine that is gone and `Waiting` stays non-zero. Caught
  by `TestWaiterCancellationLeavesNoState`.
- **A returned connection goes to the idle set with a wake-up signal,** so a
  `Get` that arrives later overtakes the queued waiters. Caught by
  `TestWaitersAreServedInFIFOOrder`.
- **FIFO holds per path but not across them:** a waiter that queued later is
  served first when a released slot and a returned connection reach waiters
  through different paths. Not caught by the tests; look for it in review.
- **Expired or unhealthy idle connections are dropped without `Close`,** so the
  socket leaks and the slot is never released; or an idle connection's age is
  measured from somewhere other than its `Put`. Caught by
  `TestIdleConnectionExpiresAfterIdleTimeout` and
  `TestBrokenConnectionIsClosedAndSlotFreed`.
- **A connection over `maxIdle` is kept or not closed.** Caught by
  `TestIdleSetIsBoundedByMaxIdle`.
- **A pre-cancelled `Get` still takes an idle connection, dials, or enqueues.**
  Caught by `TestPreCancelledGetTakesNothing` (an enqueued pre-cancelled `Get`
  that does not watch `ctx` shows up as a synctest deadlock).
- **`Dial` gets `context.Background()` or a pool context,** so a hung dial can
  no longer be abandoned by its caller. Caught by
  `TestDialUsesCallerContextOutsideTheMutex` (context value check, then
  `Get has not returned` after the caller cancels).
- **`Close` forgets waiters, closes their channels while `Put` may still send,
  or waits for checked-out connections or dials in progress.** Shows up as a
  synctest deadlock or a panic with send on closed channel. Caught by
  `TestCloseWakesWaitersClosesIdleAndRejectsGet` and
  `TestConcurrentUseRespectsLimitsAndCloses`.
- **A connection dialed after `Close` that nobody closes.** Caught by the
  dial-in-progress part of `TestCloseWakesWaitersClosesIdleAndRejectsGet`
  (`assertAllClosedOnce`).
- **A background reaper goroutine.** Forbidden by the requirements; synctest
  reports `blocked goroutines remain` in every test that does not call
  `Close`.

## Open choices

- Which idle connection `Get` tries first (most or least recently returned) is
  the solver's choice (README); the tests pass either way.
- After evicting an idle connection, `Get` may keep its slot for its own dial
  or release it (README).
- When a grant and cancellation race, the waiter may use the grant or give it
  back; `TestCancellationRacingHandoffNeverLosesConnection` accepts either
  outcome per round.
- A dial that completes after `Close` may be returned to the caller or closed
  by `Get` with `ErrClosed`; `TestCloseWakesWaitersClosesIdleAndRejectsGet`
  accepts both.

## Follow-up questions

- Add `MaxLifetime`: connections older than N are recycled even when busy.
  Where is it checked, and why is it different from `idleTimeout`?
- The database restarts. Every idle connection is broken but reports
  `Healthy`. What happens on the next 100 `Get` calls?
- Should the wait queue be bounded? What would `Get` return when it is full,
  and how is that different from a short `ctx` deadline?
- A caller's `Dial` is cancelled 1 ms before the handshake would have
  finished, while ten others wait. What is wasted, and how would a pool-owned
  dial change it?
- `database/sql` releases the open count before closing the driver connection.
  What does the stricter rule here cost, and when is the looser one acceptable?
- Which metrics tell you the pool is too small or too large?
- Should the pool keep a minimum number of idle connections warm? Who dials
  them, and with which context?
