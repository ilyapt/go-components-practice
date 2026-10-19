# Deadline budget across sequential calls

**Difficulty:** medium · **Topics:** deadline propagation, derived contexts, reserved time, refusing doomed work, wall-clock budgets

A request arrives with a deadline and has to make several downstream calls in
sequence: an auth lookup, then a database query, then a notification. A fixed
per-call timeout ignores what the earlier calls already spent, so the last
call starts with a timeout that outlives the request, or with 3 ms left and
no chance of finishing. A deadline budget gives each call a context bounded by
what is actually left, keeps some time back for writing the response, and
refuses to start a call that cannot do anything useful in the time remaining.
API gateways, gRPC servers that propagate deadlines, and request handlers that
fan through several dependencies all need this.

This task builds on 012-timeout-wrapper (a per-call deadline derived from the
caller's context, released as soon as the call returns) and adds a shared
deadline for the whole request, a reserve, and admission of each step against
what is left.

Implement `New`, `Step`, and `Remaining` in `starter.go`.

```go
type Config struct {
	Total   time.Duration
	Reserve time.Duration
	MinStep time.Duration
}

func New(ctx context.Context, cfg Config) (*Budget, error)
func (b *Budget) Step(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc, error)
func (b *Budget) Remaining() time.Duration
```

Below, the *parent* is the context passed to `New`, and the *budget deadline*
is the deadline `New` fixes for the whole request.

## Requirements

### Budget

- `New` fixes the budget deadline once: the earlier of the parent's deadline
  (if it has one) and `time.Now()` + `Total` at the time of `New` (if `Total`
  is positive). It never changes afterwards.
- `Remaining()` returns the budget deadline minus `Reserve` minus the current
  time, or zero if that is negative. It excludes the reserve: it is the time
  steps may still use.
- If the parent is done, `Remaining()` returns zero.
- `Remaining()` never increases, and never exceeds the time left until the
  parent's deadline minus `Reserve`.
- The budget is wall-clock time, not a pool of allocations. Time spent between
  steps counts. A step that finishes early gives nothing back, and a step's
  `limit` is not deducted in advance. Concurrent steps overlap in time and do
  not deduct from each other.
- The budget owns no goroutines and nothing that needs releasing, so it has no
  `Close`. `New` must not start goroutines or timers that outlive it.

### Steps

- If `ctx` is done, `Step` returns `ctx.Err()`. This check comes first, even
  when the budget is also exhausted.
- Otherwise, if `Remaining()` is zero or less than `MinStep`, `Step` returns
  `ErrBudgetExhausted` instead of starting a call that is doomed to time out.
- `MinStep` is compared with `Remaining()`, not with `limit`: a step that asks
  for less than `MinStep` is the caller's choice and is granted.
- Otherwise `Step` returns a child of `ctx` whose deadline is the earlier of
  now + `limit` and the budget deadline minus `Reserve`. A step never reaches
  into the reserve. `limit <= 0` means the step has no cap of its own and may
  use all of `Remaining()`. An earlier deadline already on `ctx` still wins.
- `ctx` is the parent or a context derived from it (for example one carrying
  a per-step trace span), so the parent's values and cancellation reach the
  step context. Passing a context unrelated to the parent is a caller error;
  its behaviour is not specified.
- On error, `Step` returns a nil context and a nil `CancelFunc`, and leaves
  nothing behind that needs releasing.
- The caller must call the returned `CancelFunc` when the call returns. It
  cancels the step context and releases its timer immediately. Calling it
  more than once is safe. It does not affect the parent, the budget, or any
  other step.
- `Step` and `Remaining` are safe for concurrent use and never block. The
  typical caller is sequential, but steps may be issued from different
  goroutines.
- Errors are usable with `errors.Is`.

## Validation

`New` returns `ErrInvalidConfig` when:

- `Total`, `Reserve`, or `MinStep` is negative;
- `Total` is zero and the parent has no deadline (there is no budget at all);
- `Total` is positive and `Reserve >= Total`, or `Reserve + MinStep > Total`
  (no step could ever be granted).

When `Total` is zero, the reserve and `MinStep` are not checked against the
parent's deadline: that depends on when `New` is called. `New` does not fail
because the parent is already done; such a budget simply has nothing left.

## Before coding

1. What exactly is the budget deadline, when is it fixed, and why is
   `Remaining()` a function of the clock rather than a counter that steps
   decrement?
2. Which context is each step's context derived from, and which of its
   possible deadlines (the caller's, the budget's, the step's `limit`) wins
   in each case?
3. In what order does `Step` decide between `ctx.Err()`, `ErrBudgetExhausted`,
   and success, and why is refusing a short step better than granting it?
4. What does the budget own that needs releasing, and what does each step own?
   Who releases each of them, and when?
