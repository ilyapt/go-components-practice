# Reviewer notes: stale-while-revalidate

> Spoilers. Read after your attempt.

## Test coverage notes

- **Two stale readers both see no refresh in flight and both start one**
  (the check and the start happen in separate critical sections). Caught only
  sometimes, by `TestStaleReadReturnsOldValueAndStartsOneRefresh`, when the
  window is hit; look for it in review.
- **Background refreshes are tracked separately from synchronous loads**, so
  an expired read starts a second load beside the running refresh and the two
  race to publish. Caught by `TestExpiredReadJoinsRunningRefresh`.
- **Concurrent callers for a missing or expired key start more than one load,
  or loads of different keys serialize.** Caught by
  `TestMissingOrExpiredKeyLoadsOnceForConcurrentCallers`.
- **A stale reader waits for the refresh** (the refresh runs inline, or the
  stale path falls through to the waiting path). Caught by
  `TestStaleReadReturnsOldValueAndStartsOneRefresh`.
- **A fresh hit calls the loader.** Caught by `TestFreshHitDoesNotLoad`.
- **A load runs under the caller's context**, so a refresh dies as soon as
  the request that triggered it returns. Caught by
  `TestLoaderReceivesWorkContextNotCallerContext`.
- **A failed refresh evicts, overwrites, or re-timestamps the entry**, so
  stale readers start getting errors, or the timestamp bump hides the failure
  for another `freshFor`. Caught by `TestFailedRefreshKeepsStaleValue` and
  `TestFreshnessBoundaries`.
- **The error of a synchronous load is cached, or the expired value is
  returned when the load fails.** Caught by
  `TestFailedLoadReturnsErrorAndCachesNothing`.
- **The timestamp is taken when the load started rather than when it
  returned.** Caught by `TestStaleReadReturnsOldValueAndStartsOneRefresh`.
- **A tier boundary is off by one** (an age exactly at a
  boundary is classified into the earlier tier). Caught by
  `TestFreshnessBoundaries`.
- **The entry is published and the in-flight load is cleared in separate
  critical sections**, so a `Get` in between sees an expired entry with
  nothing in flight (a duplicate load), or a new value while the old load
  still looks in flight. Not caught by the tests; look for it in review.
- **A published entry is mutated in place** (its value or timestamp updated
  through a pointer that readers use without the lock). A data race, flagged
  by the race detector only if a test happens to hit it; look for it in
  review.
- **A waiter's cancellation cancels the shared load, or a load whose waiters
  have all left is not published.** Caught by
  `TestWaiterCancellationDoesNotCancelSharedLoad`.
- **A `Get` with an already-done context starts a load or a refresh.** Caught
  by `TestPreCancelledGetStartsNoLoad`.
- **The closed check and the registration of a new load happen in separate
  critical sections**, so a load starts after `Close` returned, or a
  `sync.WaitGroup` reports misuse. Caught only sometimes, by
  `TestConcurrentGetAndClose`, when the window is hit; look for it in review.
- **`Close` holds the mutex while waiting for loads**, so every `Get` and
  every finishing load blocks on it. Shows up as a hang until the 60s timeout
  in `TestCloseCancelsWorkAndWaitsForLoads`, not as a deadlock message,
  because synctest does not see a goroutine blocked on `sync.Mutex` as
  blocked.
- **`Close` returns before loads have returned, does not cancel them, or
  closes a channel twice on repeated or concurrent calls** (panics). Caught
  by `TestCloseCancelsWorkAndWaitsForLoads` and
  `TestRepeatedAndConcurrentCloseIsSafe`.

## Open choices

- How entries and in-flight loads are stored and guarded (one mutex, a
  read-write lock with fresh hits under the read lock, a copy-on-write
  snapshot of entries). Any consistent choice is fine if the requirements
  hold and the user can justify it.
- What a `Get` racing `Close` returns: a value, `ErrClosed`, or the load's
  context error. `TestConcurrentGetAndClose` accepts any of these.

## Follow-up questions

- The backend is down. Every stale read starts a refresh that fails at once.
  What does the backend see, and how do you stop it?
- Should an expired value be served when the synchronous load fails?
- A deploy warms 100k keys at once; they all go stale in the same second.
  What happens, and what would you change?
- Does the caller need to know it got a stale value?
- A load hangs forever. What breaks?
- Entries are never evicted. How do you add a size bound without evicting a
  key whose load is in flight?
