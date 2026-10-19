# Lease ownership and fencing

**Difficulty:** medium · **Topics:** expiring leases, fencing tokens, atomic state transitions, injected clock

A job store hands out jobs to workers under time-limited leases. A worker that
stops renewing loses its job to another worker, and a generation number (a
fencing token) keeps the old worker from renewing or completing a job it no
longer owns. Durable job queues, distributed schedulers, leader election, and
lock services are built on this pattern.

Implement `New`, `Claim`, `Renew`, and `Complete` in `starter.go`.

```go
func New(jobs []Job, now func() time.Time) *Store
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (Job, uint64, error)
func (s *Store) Renew(ctx context.Context, jobID, workerID string, generation uint64, lease time.Duration) error
func (s *Store) Complete(ctx context.Context, jobID, workerID string, generation uint64, result []byte) error
```

## Requirements

- `Claim` immediately takes the first available job in the order passed to
  `New`, or returns `ErrNoAvailableJob` if there is none.
- A job is available if it has never been claimed or its lease has expired.
  Completed jobs are never available.
- A successful claim records the worker, sets the expiry to `now() + lease`,
  and increments that job's generation. The first generation is `1`.
- `Renew` succeeds only for the current worker and generation while the lease
  is still valid. It sets the expiry to `now() + lease`.
- `Complete` succeeds only for the current worker and generation while the
  lease is still valid. It stores the result and permanently completes the job.
- Once another claim has taken an expired job, an older generation can never
  renew or complete it, even if the worker ID is the same.
- Otherwise `Renew` and `Complete` return `ErrLeaseLost`, including for an
  unknown job ID or an already completed job.
- A lease is expired when `now()` is equal to or later than its expiry.
- Use only the injected `now` for time; never `time.Now`.
- A method whose context is already cancelled returns `ctx.Err()` without
  changing any state.
- An empty worker ID returns `ErrInvalidWorker`; a non-positive lease duration
  returns `ErrInvalidLease`.
- All state transitions are safe under concurrent calls.
- Do not start timer goroutines: expiry is evaluated lazily when a method is
  called.

Assume job IDs are unique and non-empty, `now` is non-nil, and callers do not
mutate the payload or result byte slices after passing them in.

## Before coding

1. Why is the worker ID alone not enough to reject a stale worker?
2. Which checks and writes must happen in one critical section?
3. What exactly does the generation protect, and what can it not protect on
   its own?
4. Which clock decides expiry, and what goes wrong if two clocks are mixed?
