# Reviewer notes: dag-executor

> Spoilers. Read after your attempt.

## Test coverage notes

- **Cycles are detected by running the graph:** the runnable part starts, and
  a cycle is reported when progress stops, after nodes outside the cycle have
  already run. Caught by `TestValidationRejectsInvalidGraphBeforeRunningAnything`
  (the "cycle behind runnable nodes" case counts `Run` calls).
- **The input is assumed to be topologically sorted,** or "listed earlier" is
  treated as "already returned". Caught by
  `TestDependencyFinishesBeforeDependentStarts`, which lists dependents first
  and makes dependencies slower.
- **`DependsOn` entries are counted rather than distinct dependencies,** so
  `b: [a, a]` never becomes ready, or `b` runs twice. Caught by
  `TestRepeatedDependencyIsCountedOnce` (a hang shows up as `Execute has not
  returned`).
- **The diamond's join starts twice** when two dependencies finish at once and
  both see "all my siblings are done". Caught by
  `TestDiamondDependencyRunsEachNodeOnce`.
- **Level-by-level scheduling, or any barrier that waits for unrelated
  nodes.** Caught by `TestReadyNodeDoesNotWaitForUnrelatedSlowNode`.
- **The limit counts goroutines or nodes "in progress" rather than `Run`
  calls,** so nodes waiting for dependencies consume slots and the graph
  deadlocks or under-uses the limit. Caught by
  `TestRunsAtMostLimitNodesConcurrently` and
  `TestReadyNodeDoesNotWaitForUnrelatedSlowNode`.
- **Fail-fast by accident:** `errgroup.WithContext`, whose context is cancelled
  on the first error, or returning from `Execute` on the first failure. Caught
  by `TestIndependentBranchContinuesAfterFailure` (the slow node sees a done
  context, or its dependent never runs).
- **Only direct dependents are skipped,** so a grandchild waits forever for a
  dependency that is never decided. Caught by
  `TestFailureSkipsTransitiveDependents` (synctest reports `all goroutines in
  bubble are blocked` when the skip does not propagate).
- **`Execute` waits only for nodes that ran:** it counts `Run` calls that
  skipped nodes never make and hangs, or returns when the failed node returns
  while another branch still runs. Caught by
  `TestIndependentBranchContinuesAfterFailure`.
- **A node starts after cancellation.** A coordinator that enqueues newly
  ready nodes without looking at `ctx` is caught deterministically by
  `TestCancellationStopsStartingNodesAndWaitsForRunning` (the roots succeed
  after the cancellation, so their children become ready while `ctx` is done).
  A `select` where a free slot and `ctx.Done()` are both ready picks at random,
  so that variant is caught only sometimes, when the scheduler interleaves
  badly; look for it in review.
- **`Execute` returns on cancellation without waiting for running nodes.**
  Caught by `TestCancellationStopsStartingNodesAndWaitsForRunning`.
- **`context.Canceled` is hard-coded rather than reporting `ctx.Err()`.**
  Caught by `TestPreCancelledContextRunsNothing`, which uses an expired
  deadline.
- **Nodes run under `context.Background()` or a fresh context,** losing
  cancellation and request values. Caught by
  `TestNodeContextCarriesCallerValues` and
  `TestCancellationStopsStartingNodesAndWaitsForRunning` (roots wait on
  `ctx.Done()`, so a detached context hangs).
- **A node's `deps` map contains extra entries, or values other than the exact
  outputs.** Caught by `TestOutputsArePassedToDependentsUnchanged` and
  `TestDependencyFinishesBeforeDependentStarts`.
- **One `deps` map is shared between nodes, or the internal outputs map itself
  is handed out,** so a `Run` that modifies its map races with the executor.
  Not caught by the tests; look for it in review.
- **The scheduler mutex is held while calling `Run`,** which serializes the
  graph. Caught by `TestIndependentNodesRunInParallel` and
  `TestRunsAtMostLimitNodesConcurrently`, which hang until the 60s timeout
  because synctest does not see a goroutine blocked on `sync.Mutex`.
- **The overall error loses information:** only the first error, or
  `ErrGraphFailed` without the node errors, so `errors.Is(err, myErr)` fails.
  Caught by `TestFailureSkipsTransitiveDependents` and
  `TestIndependentBranchContinuesAfterFailure`.

## Open choices

- Which problem is reported when a graph has several is the solver's choice
  (README), as long as the message names an offending node.
- The order in which ready nodes start, and the goroutine structure (a
  goroutine per node or a fixed set of workers), are the solver's choice
  (README).
- A skipped node's error may name its direct dependency or the node that
  failed originally; `TestFailureSkipsTransitiveDependents` accepts either.
- A never-started node whose dependency also did not run after cancellation
  may report `ctx.Err()` or `ErrDependencyFailed`;
  `TestCancellationStopsStartingNodesAndWaitsForRunning` and
  `TestPreCancelledContextRunsNothing` accept both.
- How cycles are found is left to the solver; reporting one node on the cycle
  is enough, the full cycle path is optional.

## Follow-up questions

- Switch to fail-fast for a deploy orchestrator. What changes in the
  scheduler, and what do running nodes see?
- Nodes are retried up to three times. Where does the retry live: inside the
  node, or in the executor? What does a retried node's dependent see?
- The graph has 100,000 nodes. What breaks first in your design?
- Two ready nodes compete for the last slot. Which one goes first, and why?
- The process crashes halfway through a pipeline. How do you resume without
  rerunning finished nodes?
- Some nodes call a rate-limited API. How do you limit them separately from
  the global limit?
