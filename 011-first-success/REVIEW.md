# Reviewer notes: first-success

> Spoilers. Read after your attempt.

## Test coverage notes

- **The receiver closes the result channel on return while losers can still
  send.** Caught by `TestLateSuccessesDoNotBlock` when a late success reaches
  the channel after the close (panic: send on closed channel). If the buffer
  lets every sender finish before the close, the test passes, so it is caught
  only sometimes; look for it in review.
- **A closed channel is read as a success when every provider failed**
  (`(Response{}, nil)`). Caught by `TestAllProvidersFail` (nil error).
- **Losers block forever publishing a result nobody receives.** Caught by
  `TestLateSuccessesDoNotBlock` (synctest reports
  `blocked goroutines remain`).
- **Losers are not cancelled after a success** (the parent `ctx` passed
  straight to providers, or a derived context whose cancel is never called).
  Caught by `TestReturnsFirstSuccessAndCancelsLoser` ("losing provider was not
  cancelled").
- **`FirstSuccess` waits for losers to return.** Caught by
  `TestReturnsFirstSuccessAndCancelsLoser` ("waited for the losing provider to
  return").
- **The first result ends the race even when it is a failure.** Caught by
  `TestFailureDoesNotEndTheRace`.
- **"All failed" is declared before the last provider has answered.** Caught by
  `TestAllProvidersFail` (returns before 20ms).
- **Results are taken in provider slice order instead of completion order.**
  Caught by `TestReturnsInCompletionOrder` (wrong source or wrong elapsed time).
- **Parent cancellation does not reach every in-flight call, or
  `FirstSuccess` does not return on it.** Caught by
  `TestParentCancellationCancelsAllCalls`.
- **An already-cancelled parent loses to a provider that answers instantly and
  ignores its context.** Caught by `TestAlreadyCancelledParentWins`; when the
  success and `ctx.Done()` are both ready in one `select`, the random choice
  makes it caught only sometimes; look for it in review.
- **Some calls start before a nil provider later in the slice is detected.**
  Caught by `TestRejectsInvalidProviders` (provider called more than 0 times).

## Open choices

- How provider errors are aggregated when all calls fail, and whether callers
  can inspect individual errors. The README leaves it to the solver.
- The error for a nil provider. Any consistent choice is fine if the user can
  justify it.
- Whether the result channel carries only successes or every outcome, and
  whether anything closes it. Any shape is fine if no sender can block or panic
  and "all failed" is detected correctly.
- Which result wins when a success and parent cancellation become ready at the
  same moment. Only the already-cancelled case is tested.

## Follow-up questions

- Providers cost money per call. How would you avoid calling all of them every
  time?
- One provider is fast but often wrong. What changes if you need two agreeing
  answers instead of the first one?
- Losers ignore cancellation and keep running for seconds. What does that do
  to the process under load, and how would you bound it?
- How would callers distinguish "all providers returned not-found" from "all
  providers were down"?
- Would `errgroup` fit here? Why or why not?
- Which metrics matter for this component?
