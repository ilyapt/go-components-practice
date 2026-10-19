# Reviewer notes: singleflight

> Spoilers. Read after your attempt.

## Test coverage notes

- **Same-key callers run more than one `Load`.** Caught by
  `TestConcurrentSameKeySharesOneLoad` and `TestAllWaitersReceiveSameError`
  (Load call count above 1).
- **The group mutex is held while `Load` runs**, so all keys wait behind one
  slow load. Caught by `TestDifferentKeysRunIndependently` and most concurrent
  tests, but it shows up as a hang until the 60s timeout, not as a clean
  failure: synctest does not see a goroutine blocked on sync.Mutex as durably
  blocked, so `synctest.Wait` never returns.
- **A pre-cancelled caller creates an in-flight entry and returns without
  starting a load**, so every later caller for that key waits forever; or it
  starts a load anyway. Caught by `TestPreCancelledCallerStartsNoWork` (the
  next caller "has not returned", or Load is called for the rejected caller).
- **The first caller's context is passed to `Load`**, so cancelling the creator
  fails every other waiter. Caught by
  `TestCreatorCancellationDoesNotCancelSharedLoad` (the load's context is
  cancelled) and `TestLoaderReceivesWorkContextNotWaiterContext` (the caller's
  context value is visible inside Load).
- **`Load` runs inline in the creator's goroutine**, so the creator cannot stop
  waiting when its context is cancelled. Caught by
  `TestCreatorCancellationDoesNotCancelSharedLoad` and
  `TestLoadCompletesAfterEveryWaiterLeaves` (the cancelled creator "has not
  returned").
- **The load stops, or its entry disappears, when the last waiter leaves.** A
  caller arriving while the abandoned load still runs starts a second load.
  Caught by `TestLoadCompletesAfterEveryWaiterLeaves` (the joiner returns
  `call-2` immediately rather than blocking for `call-1`).
- **The entry is removed from a waiter's path while the load is still running**
  (for example the creator deletes it when it stops waiting), so two loads for
  one key overlap. Caught by `TestLoadCompletesAfterEveryWaiterLeaves` (a
  joiner arriving after the creator left starts a fresh load).
- **The entry is never removed, or removed only when a waiter is present**,
  turning the group into a permanent cache. Caught by
  `TestCompletedEntryIsNotCached` and `TestLoadCompletesAfterEveryWaiterLeaves`.
- **The result is written after waiters are released, or read without waiting
  for completion.** Flagged by the race detector in any test with concurrent
  waiters, such as `TestConcurrentSameKeySharesOneLoad`.
- **Cancelling `workCtx` does not reach the running loads**, or waiters keep
  waiting after the loads return. Caught by `TestWorkContextCancellationCancelsLoads`.
- **`New` accepts a nil loader.** Caught by `TestRejectsNilLoader`.

## Open choices

- When the shared result and a waiter's own cancellation are ready at the same
  moment, the README does not say which one that waiter returns, and the tests
  never create that race. Either is defensible; ask which the user chose and
  why.
- `Load` may receive `workCtx` itself or a context derived from it; the README
  allows both.
- The README does not say what `Get` does when `workCtx` is already cancelled
  before the call. Starting a load that returns immediately and short-circuiting
  are both consistent with the requirements; the tests do not cover it.
- The error for a nil loader is not a named sentinel; the test only checks that
  it is non-nil.

## Follow-up questions

- Turn this into a cache with a TTL. Where does the cached value live, and what
  happens to load at the moment an entry expires?
- Loads are expensive and callers often time out. Would you cancel a load that
  has zero waiters? What would that cost in complexity and races?
- `Load` panics. What happens to the waiters and to the map entry?
- `Load` sometimes hangs forever. How do you bound it without using a caller's
  context?
- Should a failed load's error be shared with a caller that arrives 50 ms after
  it completed?
- Which metrics would show that the group is doing its job?
