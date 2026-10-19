# Reviewer notes: lease-fencing

> Spoilers. Read after your attempt.

## Test coverage notes

- **The ordered list and the lookup by ID hold different copies of a job's record, so `Claim`
  updates one while `Renew` and `Complete` read the other (and see generation 0 or no owner).**
  Caught by `TestRenewExtendsLeaseFromCurrentTime` and `TestWrongWorkerGenerationOrJobIsRejected`.
- **The completed check is inverted or missing: completed jobs are claimable, or only completed
  jobs are.** Caught by `TestCompleteBoundaryAndCompletedJobIsFinal` and
  `TestExpiredLeaseIsReclaimedAndOldGenerationIsFenced`.
- **Expiry is computed or compared with `time.Now` somewhere.** Caught by
  `TestUsesOnlyInjectedClock` (virtual time moves an hour while the injected clock stands still)
  and by every expiry test, because the fake clock starts in the year 2100.
- **State flips on expiry from a timer callback or a background goroutine.** A timer callback is
  caught by `TestUsesOnlyInjectedClock`: it runs on runtime time, which moves while the injected
  clock does not. A goroutine still waiting for an expiry when a test returns makes synctest report
  `blocked goroutines remain`.
- **Fencing by worker ID only: a stale attempt of the same worker renews or completes after the
  job was reclaimed.** Caught by `TestExpiredLeaseIsReclaimedAndOldGenerationIsFenced`.
- **Fencing by generation only: another worker presenting the current generation is accepted.**
  Caught by `TestWrongWorkerGenerationOrJobIsRejected` (the "other worker" case).
- **Off-by-one expiry boundary: a lease is still valid at exactly `now() == expiry`.** Caught by
  `TestExpiredLeaseIsReclaimedAndOldGenerationIsFenced` and
  `TestCompleteBoundaryAndCompletedJobIsFinal`.
- **`Renew` does not extend the lease.** Caught by `TestRenewExtendsLeaseFromCurrentTime`.
- **`Renew` adds the lease to the old expiry rather than to the current `now()`.** Caught by
  `TestUsesOnlyInjectedClock` (the final `Renew` still succeeds);
  `TestRenewExtendsLeaseFromCurrentTime` does not distinguish the two.
- **A reclaim does not increment the generation.** Caught by `TestClaimPrefersEarlierExpiredJob`
  and `TestExpiredLeaseIsReclaimedAndOldGenerationIsFenced` (wrong value).
- **`Claim` skips an expired earlier job in favour of a never-claimed later one.** Caught by
  `TestClaimPrefersEarlierExpiredJob`.
- **Availability is checked in one critical section and the claim written in another, so two
  workers both take the same job.** Caught by `TestConcurrentClaimHasOneWinnerPerJob`
  probabilistically (reliable in practice); `TestConcurrentCompleteHasOneWinner` covers the same
  race for `Complete`. Unsynchronised record access also shows up there as a race report.
- **A cancelled call changes state before returning `ctx.Err()` (for example, renews and then
  reports cancellation).** Caught by `TestCancelledCallsDoNotMutateState`.
- **An invalid argument consumes a generation or otherwise changes state.** Caught by
  `TestInvalidArguments` (generation after invalid claims is not 1).
- **`Complete` succeeds but does not store the result, or stores a slice the store later
  mutates.** Not caught by the tests: the API has no way to read the result back; look for it in
  review.

## Open choices

- Precedence when several errors apply at once (cancelled context together with an empty worker
  ID or a non-positive lease, or an invalid lease on a call whose lease is also lost) is not
  specified; the tests check each condition alone. Any consistent choice is fine if the user can
  justify it.
- Whether the returned `Job.Payload` and the stored result are copied is open: the README assumes
  callers do not mutate them.
- The internal representation of a job's state (separate fields or an explicit state enum with
  expiry evaluated lazily) is left to the solver.

## Follow-up questions

- The store is now a SQL table. Write the claim as one statement. Whose clock is `now` then?
- Clocks on workers and the store differ by 2 seconds. Which clock decides expiry, and how does
  the worker choose its renewal interval?
- The job sends a payment through an external API that knows nothing about generations. How do you
  prevent a stale worker's duplicate payment?
- A worker's job runs for 10 minutes with a 30-second lease. Design the renewal loop: interval,
  what to do on `ErrLeaseLost`, how to stop the work.
- A job keeps failing and being reclaimed forever. What would you add?
- What metrics show trouble?
