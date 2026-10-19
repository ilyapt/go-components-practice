# DAG executor

**Difficulty:** hard · **Topics:** dependency graphs, topological scheduling, bounded concurrency, failure propagation, cancellation

Run a set of tasks where some tasks need the results of others: a task starts
only when everything it depends on has succeeded, unrelated tasks run in
parallel, and a failure skips exactly the tasks downstream of it. This is the
core of build systems (Bazel, `make -j`), data pipelines (Airflow, dbt),
workflow engines, CI runners, and deploy orchestrators that roll out services
in dependency order.

This task builds on 005-executor (a fixed bound on how much work runs at once)
and 006-future (a result published once and observed by any number of
waiters): each node's completion is a published result that all of its
dependents wait on.

Implement `Execute` in `starter.go`.

```go
type Output any

type Node struct {
	ID        string
	DependsOn []string
	Run       func(ctx context.Context, deps map[string]Output) (Output, error)
}

type Result struct {
	Output Output
	Err    error
}

func Execute(ctx context.Context, nodes []Node, limit int) (map[string]Result, error)
```

## Requirements

### Validation

- `Execute` validates the whole graph before calling any `Run`. If the graph
  is invalid, no `Run` is called and `Execute` returns a nil map and an error
  that satisfies `errors.Is` with the matching sentinel:
  - `limit < 1`: `ErrInvalidLimit`;
  - a node with a nil `Run`: `ErrNilRun`;
  - two nodes with the same `ID`: `ErrDuplicateNode`;
  - a `DependsOn` entry that is not the `ID` of any node:
    `ErrUnknownDependency`;
  - a node listing its own `ID` in `DependsOn`: `ErrSelfDependency`;
  - any other cycle (`a → b → a`, `a → b → c → a`, ...): `ErrCycle`.
- The validation error message names the offending node: the node with a nil
  `Run`, the duplicated `ID`, the missing dependency `ID`, the self-dependent
  node, or at least one node on the cycle.
- When a graph has several problems, which one is reported is your choice.
- The same `ID` listed more than once in one node's `DependsOn` is valid and
  counts as one dependency.
- `nodes` may list nodes in any order; a dependency may appear after its
  dependents.
- An empty graph is valid: `Execute` returns an empty map and `nil`.

### Scheduling

- `Run` of a node is called at most once, and only after the `Run` of every
  one of its dependencies has returned with a nil error.
- `deps` contains exactly one entry per distinct dependency `ID`, mapped to
  the exact `Output` value that dependency's `Run` returned. A node without
  dependencies receives an empty or nil map.
- At most `limit` `Run` calls execute at the same time.
- Scheduling is work-conserving: whenever fewer than `limit` `Run` calls are
  executing and some node is ready (all of its dependencies have succeeded),
  a ready node starts. A ready node never waits for nodes it does not depend
  on, for example for the rest of a "level" of the graph.
- The order in which ready nodes start is your choice.
- Do not hold a mutex while calling `Run`.
- The goroutine structure is your choice: one goroutine per node gated by a
  semaphore, or a coordinator feeding a pool of at most `limit` workers, are
  both acceptable. The bound that matters is concurrent `Run` calls.

### Failure

- A node whose `Run` returns a non-nil error has failed. Its `Result` holds
  exactly the `Output` and error `Run` returned.
- Every node that depends on a failed node, directly or transitively, is
  skipped: its `Run` is never called, its `Result.Output` is nil, and its
  `Result.Err` satisfies `errors.Is(err, ErrDependencyFailed)`. The message
  names a dependency that did not succeed (the direct dependency or the node
  that failed originally; your choice).
- A failure does not stop independent branches: nodes that do not depend on
  the failed node keep running and start as usual, and a failure never
  cancels another node's context. (This is the "run everything that can still
  run" policy, not fail-fast.)

### Context and cancellation

- Each `Run` receives a context derived from `ctx`: it carries `ctx`'s values
  and is cancelled when `ctx` is cancelled.
- Once `ctx` is done, no further `Run` is started, including for nodes that
  became ready after the cancellation or were waiting for a free slot.
- A node that never started because `ctx` was done has a `Result.Err` that
  satisfies `errors.Is(err, ctx.Err())`. If one of its dependencies also did
  not succeed, `ErrDependencyFailed` is acceptable instead.
- With an already-done `ctx` and a valid graph, no `Run` is called and every
  node gets a `Result` as described above.
- `Execute` returns only after every `Run` it started has returned, whether
  `ctx` was cancelled or not. Assume `Run` returns soon after its context is
  cancelled and does not panic.

### Results

- After validation, `Execute` returns a map with exactly one `Result` per
  node, keyed by `ID`.
- The returned error is `nil` if and only if every node succeeded. Otherwise
  it satisfies `errors.Is(err, ErrGraphFailed)`, `errors.Is(err, e)` for every
  non-nil error `e` returned by a `Run`, and `errors.Is(err, ctx.Err())` if
  cancellation kept any node from starting.
- No goroutine started by `Execute` is still running when it returns.

## Before coding

1. What is the exact event that makes a node ready, and which goroutine
   observes it? How do you guarantee that a node with two paths to the same
   dependency (a diamond) is started exactly once?
2. Where is the concurrency limit enforced, and what does a ready node do
   while every slot is busy? What must happen at that waiting point when
   `ctx` is cancelled?
3. When a node fails, how do its transitive dependents learn that they will
   never run, and how does `Execute` know that every node has reached a final
   state so it can return?
4. Which context does each `Run` receive, who cancels it, and why must a
   failure in one branch not cancel it?
