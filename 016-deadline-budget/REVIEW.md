# Reviewer notes: deadline-budget

> Spoilers. Read after your attempt.

## Test coverage notes

- **The budget is a counter: a step's `limit` (or its measured duration) is deducted from a stored
  total, so time between steps is free, early-finishing steps are over-charged, and overlapping
  steps are charged twice.** Caught by `TestRemainingFollowsWallClockNotAllocations` and
  `TestConcurrentStepsDoNotDeductFromEachOther`.
- **Only one of the two bounds is used: `Total` when the parent's deadline is earlier, or only the
  parent's deadline.** Caught by `TestBudgetDeadlineIsEarlierOfParentDeadlineAndTotal`.
- **The reserve is subtracted in `Remaining()` but not in the step deadline, or the other way
  round.** Caught by `TestStepDeadlineIsCappedByLimitAndExcludesReserve`,
  `TestSequentialStepsShareOneBudget`, and `TestBudgetDeadlineIsEarlierOfParentDeadlineAndTotal`.
- **`limit` is ignored or treated as a floor, or `limit <= 0` yields an already-expired context.**
  Caught by `TestStepDeadlineIsCappedByLimitAndExcludesReserve`.
- **`MinStep` is compared with the granted duration or with `limit`, so small deliberate steps are
  refused.** Caught by `TestStepDeadlineIsCappedByLimitAndExcludesReserve` (the 10ms case).
- **Off-by-one at the boundary: refusing when `Remaining() == MinStep`, or granting a zero-length
  step when `MinStep == 0`.** Caught by `TestStepRefusedBelowMinStep` and
  `TestExhaustedBudgetRefusesStepsWithoutMinStep`.
- **`ErrBudgetExhausted` is returned to a caller whose context is already done.** Caught by
  `TestDoneCallerContextWinsOverExhaustedBudget`.
- **The step context is derived from `context.Background()`, from the parent, or from a context
  created in `New`, rather than from the `ctx` argument, so parent cancellation or per-step values
  are lost.** Caught by `TestStepContextIsDerivedFromCallerContext` and
  `TestParentCancellationReachesStepsAndBudget`.
- **`Remaining()` reports time left after the parent is done.** Caught by
  `TestParentCancellationReachesStepsAndBudget` and `TestNewAcceptsDoneParent`.
- **A refused step returns a context or a `CancelFunc`, typically because the derived context was
  created before the admission checks.** Caught by every test that uses `assertRefused`.
- **A step's `CancelFunc` affects the parent, the budget, or another step, or is unsafe to call
  twice.** Caught by `TestCancelReleasesOnlyItsStep`.
- **Validation rejects a valid configuration (a parent-deadline-only budget whose `Reserve` and
  `MinStep` are not checked, `Reserve + MinStep == Total`, or an already-done parent), or accepts
  an invalid one.** Caught by `TestValidation` and `TestNewAcceptsDoneParent`.
- **`New` starts a goroutine that waits on the parent's `Done`.** With a `context.Background()`
  parent it never exits; synctest reports `blocked goroutines remain` in every test with such a
  parent.
- **`New` creates a context with a timer that nothing ever cancels; the timer lives until the
  budget deadline.** Not caught by the tests (timers are not goroutines); look for it in review.
- **Budget fields are written after `New` without synchronisation.** Caught by the race detector
  in `TestConcurrentStepsDoNotDeductFromEachOther`.
- **`Step` reads the clock twice, once for admission and once for the deadline, so with a real
  clock a step admitted with exactly `MinStep` is granted slightly less.** Not caught by the tests
  (virtual time does not move inside a call); look for it in review.

## Open choices

- What happens when `Step` receives a context unrelated to the parent is explicitly unspecified.
  Any consistent choice is fine if the user can justify it.
- Whether a step context records why it ended (step `limit` versus budget deadline), for example
  through a context cause, is left to the solver; `Err()` is all the tests inspect.
- Whether the errors carry extra detail (remaining time, step name) is open as long as
  `errors.Is` works.

## Follow-up questions

- The budget has to cross a process boundary. How does the downstream service learn it?
- How would you size `Reserve`, and would it be a fixed duration or a fraction of the budget?
- A step is refused with `ErrBudgetExhausted`. What should the handler do?
- Combine this with 017-retry-backoff. Is a retry admitted by the same budget, and is the backoff
  sleep checked against it?
- The number of steps is known up front. Would you split the remaining budget between them, and
  what would that cost?
- Which metrics belong here?
