# go-components-practice

Practice tasks for experienced Go engineers: build the concurrent components
that backend and infrastructure systems are made of, and understand them well
enough to defend every design decision.

Worker pools, batchers, retries, deduplication, leases, circuit breakers,
snapshot publication — each task is one real component with a precise
set of requirements, a starter file, and tests that run under the race detector with
virtual time.

This is not an algorithms site and not interview simulation. Tasks take from
half an hour to a few hours.

## Requirements

- Go 1.26+
- Optional: an AI coding agent (Claude Code, Codex, Cursor, ...) to act as a
  tutor and reviewer. Instructions for it are in [AGENTS.md](AGENTS.md).

## How to use

Fork the repository and solve tasks in place: your fork keeps your solutions
and your progress.

Pick a task from the [list below](#tasks), read `<task>/README.md`, answer the
"Before coding" questions for yourself, then implement `<task>/starter.go`. It
is the only file you need to change. Run the tests with:

```bash
go test -race -count=1 -timeout=60s ./001-bounded-map/
```

To start a task over:

```bash
git checkout -- 001-bounded-map/starter.go
```

### With an AI tutor

Open the repository in your agent and say something like "start bounded-map".
The agent will walk you through clarification, invariants, implementation,
tests, and a review against the task's reviewer notes. In Claude Code you can
also use `/start <task>`, `/hint`, `/review`, and `/next`.

### Without an AI tutor

Everything works without an agent. `<task>/REVIEW.md` contains the
reviewer notes: which failures the tests catch and which they do not, the
choices the statement leaves open, and follow-up questions. It is a spoiler —
read it after your attempt.

## Tasks

Each task is a numbered directory in the repository root. Linked tasks are
available; the rest are planned and will appear under the same number.

Numbers group tasks by topic and order them within a topic, from simpler to
harder. You do not have to go through all 100 in sequence:

1. **Start with "Concurrency and lifecycle".** At least 001, 005, and 006:
   bounded concurrency, shutdown, and the difference between waiting for work
   and owning it. Almost everything else builds on these.
2. **Then take topics in any order** — by interest, by what your work needs,
   or by what you find hardest. Within a topic, follow the numbers.
3. **Leave the capstones for last.** Each combines patterns from several
   topics.

A few tasks build on specific earlier ones; their statements say so (for
example, 052 builds on 005, 023, and 047).

### Concurrency and lifecycle

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 001 | [bounded-map](001-bounded-map/README.md) | Parallel map with a concurrency limit, ordered results, cancellation | medium |
| 002 | fan-in | Merge N channels into one; close when all inputs are done | easy |
| 003 | pipeline | Multi-stage pipeline with backpressure and error propagation | medium |
| 004 | semaphore | Weighted semaphore with context-aware, FIFO-fair acquire | medium |
| 005 | [executor](005-executor/README.md) | Fixed workers, bounded queue, overload errors, graceful shutdown | hard |
| 006 | [future](006-future/README.md) | Shared result with independent waiter and work cancellation | medium |
| 007 | once-with-retry | Lazy initialization that retries after failure | easy |
| 008 | periodic-runner | Interval jobs without overlap, skip vs catch-up, clean stop | medium |
| 009 | pubsub | In-process topics with a slow-subscriber policy | medium |
| 010 | lifecycle-manager | Start and stop components in dependency order with timeouts | medium |

### External calls

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 011 | [first-success](011-first-success/README.md) | Fan-out to providers, return the first success, cancel losers | easy |
| 012 | [timeout-wrapper](012-timeout-wrapper/README.md) | Per-call deadline that respects parent cancellation | easy |
| 013 | quorum-read | Wait for N of M agreeing responses with early exit | medium |
| 014 | hedged-request | Send a backup request after a delay, keep the first answer | medium |
| 015 | scatter-gather | Collect partial results until a deadline | medium |
| 016 | deadline-budget | Split the remaining time budget across sequential calls | medium |

### Failure handling

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 017 | [retry-backoff](017-retry-backoff/README.md) | Retries with classification and context-aware backoff | easy |
| 018 | retry-budget | Limit retries to a share of traffic to prevent retry storms | medium |
| 019 | [circuit-breaker](019-circuit-breaker/README.md) | Closed / open / half-open breaker with a single probe | hard |
| 020 | bulkhead | Per-dependency concurrency isolation | medium |
| 021 | fallback-chain | Primary, secondary, and stale-cache fallbacks | medium |
| 022 | health-checker | Active probes with hysteresis against flapping | medium |

### Deduplication and ordering

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 023 | [singleflight](023-singleflight/README.md) | One shared load per key, waiters cancel independently | medium |
| 024 | [per-key-serializer](024-per-key-serializer/README.md) | Same key runs in order, different keys run in parallel | medium |
| 025 | [idempotent-commands](025-idempotent-commands/README.md) | Command IDs with stored outcomes and payload conflicts | hard |
| 026 | dedup-window | Bounded seen-set of message IDs with expiry | medium |
| 027 | reorder-buffer | Deliver out-of-order sequence numbers in order, with gap timeout | medium |
| 028 | ordered-parallel | Process in parallel, emit in input order, bounded window | hard |

### Rate limiting and flow control

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 029 | token-bucket | Token bucket with blocking and non-blocking acquire | easy |
| 030 | sliding-window-limiter | Request limit over a sliding time window | medium |
| 031 | fair-queue | Per-tenant fair scheduling (deficit round robin) | hard |
| 032 | adaptive-concurrency | Concurrency limit that adapts to latency (AIMD) | hard |
| 033 | load-shedder | Reject work by queueing time before it overloads the service | medium |
| 034 | priority-admission | Reserve capacity for high-priority requests | medium |

### Caching

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 035 | lru-cache | Concurrent LRU cache | easy |
| 036 | ttl-cache | Cache with lazy and active expiry | medium |
| 037 | loading-cache | Cache that loads misses once and caches errors briefly | medium |
| 038 | stale-while-revalidate | Serve stale values while refreshing in the background | medium |
| 039 | sharded-cache | Lock striping for a read-heavy cache | medium |
| 040 | write-behind-cache | Buffer writes and persist them asynchronously | hard |

### Scheduling and time

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 041 | delay-queue | Items that become available at a given time | medium |
| 042 | timer-wheel | Hashed timer wheel for many cheap timeouts | hard |
| 043 | job-scheduler | Recurring jobs with misfire policy and no overlap | medium |
| 044 | debounce-throttle | Debounce and throttle for bursts of events | easy |
| 045 | priority-queue-aging | Priority scheduling without starvation | medium |
| 046 | [batcher](046-batcher/README.md) | Flush by count or by time, graceful final flush | medium |

### State publication

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 047 | [immutable-snapshot](047-immutable-snapshot/README.md) | Copy-on-write state with atomic publication | medium |
| 048 | config-reload | Validate, swap, and notify subscribers of new config | medium |
| 049 | versioned-state | Read old and new state formats during a rolling deploy | medium |
| 050 | event-sourced-aggregate | Rebuild state from events, with snapshots | medium |
| 051 | checkpointer | Periodic checkpoints and replay from the last one | medium |
| 052 | [refresh-coordinator](052-refresh-coordinator/README.md) | Coalesced refreshes, bounded workers, immutable publication | hard |

### Coordination and ownership

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 053 | [lease-fencing](053-lease-fencing/README.md) | Job leases with generations that fence stale workers | medium |
| 054 | lease-worker | Background lease renewal, stop work when the lease is lost | hard |
| 055 | leader-election | Leader election over a compare-and-set store with TTL | hard |
| 056 | lock-client | Distributed lock client with renewal and fencing tokens | hard |
| 057 | failure-detector | Heartbeats with suspicion timeouts | medium |
| 058 | consistent-hash | Hash ring with virtual nodes and moved-key tracking | medium |
| 059 | shard-assigner | Assign partitions to workers with minimal movement | hard |

### Messaging and streaming

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 060 | offset-committer | Parallel processing, commit only the contiguous done prefix | hard |
| 061 | redelivery-tracker | Visibility timeouts, ack and nack | medium |
| 062 | dead-letter | Attempt counting and poison-message handling | medium |
| 063 | outbox-relay | Publish stored events at least once, in order | medium |
| 064 | consumer-rebalance | Hand off partitions between consumers with draining | hard |
| 065 | window-aggregator | Tumbling windows with watermarks and late events | hard |
| 066 | credit-flow-control | Credit-based flow control between producer and consumer | medium |

### Storage internals

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 067 | wal-writer | Write-ahead log with group commit on a fake disk | hard |
| 068 | wal-recovery | Recover a log with checksums and a torn tail | medium |
| 069 | memtable-flush | Rotate and flush memtables while reads see both | hard |
| 070 | file-refcount | Keep files alive for readers during compaction | medium |
| 071 | mvcc-store | Snapshot reads and garbage collection of old versions | hard |
| 072 | occ-transactions | Optimistic transactions with conflict detection | hard |

### Clients and networking

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 073 | connection-pool | Pool with limits, health checks, idle eviction, cancellable wait | hard |
| 074 | request-multiplexer | Many concurrent requests over one connection | hard |
| 075 | p2c-balancer | Power-of-two-choices load balancing | medium |
| 076 | resolver-cache | Background refresh that keeps the last good answer | medium |
| 077 | [multi-provider-rpc](077-multi-provider-rpc/README.md) | Bounded fan-out with freshness policy and per-call timeout | hard |
| 078 | provider-scoring | Latency and error scoring with failover | medium |

### Execution engines

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 079 | dag-executor | Run tasks after their dependencies, cancel downstream on failure | hard |
| 080 | step-executor | Workflow steps with futures, dedup, retries, and shutdown | hard |
| 081 | saga | Multi-step operation with compensations | hard |
| 082 | durable-workflow | Persist step results and resume after a crash | hard |
| 083 | trigger-dispatcher | Turn events into workflow runs with dedup and rate limits | medium |
| 084 | job-queue | Claim, acknowledge, and retry jobs on a fake store | hard |

### Observability

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 085 | latency-histogram | Concurrent histogram with cheap recording | medium |
| 086 | rate-counter | Event rate over the last N seconds | easy |
| 087 | burn-rate-alert | SLO error-budget burn-rate tracking | medium |
| 088 | span-buffer | Bounded trace buffer with sampling and drop accounting | medium |

### Performance

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 089 | sharded-counter | Counter without contention or false sharing | medium |
| 090 | object-pool | Reuse expensive objects, compare with `sync.Pool` | medium |
| 091 | zero-alloc-parser | Parse a hot-path format with zero allocations | medium |
| 092 | spsc-ring | Lock-free single-producer single-consumer ring buffer | hard |
| 093 | buffered-writer | Flush by size or time with sticky errors | medium |
| 094 | read-heavy-map | Copy-on-write vs `RWMutex` vs `sync.Map`, with benchmarks | medium |

### Capstones

| # | Task | What you build | Difficulty |
|---|---|---|---|
| 095 | state-update-engine | Event ingestion, coalescing, bounded refresh, publication | hard |
| 096 | rpc-gateway | Limits, retries, circuit breaking, and hedging in one client | hard |
| 097 | crawler | Per-host limits, dedup, politeness, cancellation | hard |
| 098 | durable-job-worker | Claims, lease renewal, fencing, retries, shutdown | hard |
| 099 | workflow-engine-lite | DAG execution with durable resume and retries | hard |
| 100 | capability-executor | Bounded, deduplicated, retrying calls to external capabilities | hard |

## How the tests work

- Every test runs with `-race`.
- Concurrency tests run inside [`testing/synctest`](https://pkg.go.dev/testing/synctest)
  bubbles: time is virtual, so timeouts and delays are tested instantly and
  deterministically.
- A test fails if your component leaves blocked goroutines behind
  (`deadlock: main bubble goroutine has exited but blocked goroutines remain`).
- Tests check the requirements and the failures that are hard to see by reading
  code. Passing tests do not mean the design is good — that is what the review
  is for.

## License

[MIT](LICENSE)
