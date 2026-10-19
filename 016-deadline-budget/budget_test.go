package deadlinebudget

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func mustNew(t *testing.T, ctx context.Context, cfg Config) *Budget {
	t.Helper()
	b, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New(%+v) returned error: %v", cfg, err)
	}
	return b
}

func mustStep(t *testing.T, b *Budget, ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	stepCtx, cancel, err := b.Step(ctx, limit)
	if err != nil {
		t.Fatalf("Step(limit=%v) returned error: %v", limit, err)
	}
	if stepCtx == nil || cancel == nil {
		t.Fatalf("Step(limit=%v) returned a nil context or CancelFunc without an error", limit)
	}
	return stepCtx, cancel
}

// assertRefused checks that Step fails with want and returns nothing to release.
func assertRefused(t *testing.T, b *Budget, ctx context.Context, limit time.Duration, want error, what string) {
	t.Helper()
	stepCtx, cancel, err := b.Step(ctx, limit)
	if cancel != nil {
		cancel()
	}
	if !errors.Is(err, want) {
		t.Fatalf("%s: Step error = %v, want %v", what, err, want)
	}
	if stepCtx != nil || cancel != nil {
		t.Fatalf("%s: refused Step returned a non-nil context or CancelFunc", what)
	}
}

func assertDeadline(t *testing.T, ctx context.Context, start time.Time, want time.Duration, what string) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("%s: step context has no deadline, want start+%v", what, want)
	}
	if !deadline.Equal(start.Add(want)) {
		t.Fatalf("%s: step deadline = start+%v, want start+%v", what, deadline.Sub(start), want)
	}
}

func assertRemaining(t *testing.T, b *Budget, want time.Duration, what string) {
	t.Helper()
	if got := b.Remaining(); got != want {
		t.Fatalf("%s: Remaining() = %v, want %v", what, got, want)
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		withDeadline, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		for _, tc := range []struct {
			name    string
			ctx     context.Context
			cfg     Config
			wantErr error
		}{
			{name: "negative total", ctx: withDeadline, cfg: Config{Total: -time.Second}, wantErr: ErrInvalidConfig},
			{name: "negative reserve", ctx: context.Background(), cfg: Config{Total: time.Second, Reserve: -1}, wantErr: ErrInvalidConfig},
			{name: "negative min step", ctx: context.Background(), cfg: Config{Total: time.Second, MinStep: -1}, wantErr: ErrInvalidConfig},
			{name: "no total and no parent deadline", ctx: context.Background(), cfg: Config{}, wantErr: ErrInvalidConfig},
			{name: "reserve equals total", ctx: context.Background(), cfg: Config{Total: time.Second, Reserve: time.Second}, wantErr: ErrInvalidConfig},
			{name: "reserve exceeds total", ctx: context.Background(), cfg: Config{Total: time.Second, Reserve: 2 * time.Second}, wantErr: ErrInvalidConfig},
			{name: "reserve plus min step exceeds total", ctx: context.Background(), cfg: Config{Total: time.Second, Reserve: 600 * time.Millisecond, MinStep: 500 * time.Millisecond}, wantErr: ErrInvalidConfig},
			{name: "reserve plus min step equals total", ctx: context.Background(), cfg: Config{Total: time.Second, Reserve: 500 * time.Millisecond, MinStep: 500 * time.Millisecond}},
			{name: "parent deadline only", ctx: withDeadline, cfg: Config{}},
			{name: "parent deadline only, reserve not checked against it", ctx: withDeadline, cfg: Config{Reserve: time.Second, MinStep: time.Second}},
		} {
			b, err := New(tc.ctx, tc.cfg)
			if tc.wantErr == nil {
				if err != nil || b == nil {
					t.Errorf("%s: New = %v, %v; want a budget and nil error", tc.name, b, err)
				}
				continue
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if b != nil {
				t.Errorf("%s: New returned a non-nil budget with an error", tc.name)
			}
		}
	})
}

func TestBudgetDeadlineIsEarlierOfParentDeadlineAndTotal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const reserve = 100 * time.Millisecond
		for _, tc := range []struct {
			name           string
			parentTimeout  time.Duration // 0: the parent has no deadline
			total          time.Duration
			wantUntilStart time.Duration // budget deadline minus reserve, relative to New
		}{
			{name: "parent deadline earlier than total", parentTimeout: 500 * time.Millisecond, total: time.Second, wantUntilStart: 400 * time.Millisecond},
			{name: "total earlier than parent deadline", parentTimeout: 2 * time.Second, total: time.Second, wantUntilStart: 900 * time.Millisecond},
			{name: "parent deadline only", parentTimeout: 800 * time.Millisecond, total: 0, wantUntilStart: 700 * time.Millisecond},
			{name: "total only", parentTimeout: 0, total: time.Second, wantUntilStart: 900 * time.Millisecond},
		} {
			parent, cancel := context.Background(), context.CancelFunc(func() {})
			if tc.parentTimeout > 0 {
				parent, cancel = context.WithTimeout(context.Background(), tc.parentTimeout)
			}
			start := time.Now()
			b := mustNew(t, parent, Config{Total: tc.total, Reserve: reserve})
			assertRemaining(t, b, tc.wantUntilStart, tc.name)

			stepCtx, stepCancel := mustStep(t, b, parent, 0)
			assertDeadline(t, stepCtx, start, tc.wantUntilStart, tc.name)
			stepCancel()
			cancel()
		}
	})
}

func TestRemainingFollowsWallClockNotAllocations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, context.Background(), Config{Total: time.Second})

		time.Sleep(300 * time.Millisecond) // local work before the first call
		assertRemaining(t, b, 700*time.Millisecond, "after 300ms outside any step")

		_, cancel := mustStep(t, b, context.Background(), 500*time.Millisecond)
		assertRemaining(t, b, 700*time.Millisecond, "right after granting a 500ms step (limit is not deducted in advance)")

		time.Sleep(100 * time.Millisecond) // the call finishes early
		cancel()
		assertRemaining(t, b, 600*time.Millisecond, "after a step that took 100ms (nothing is given back or overcharged)")

		time.Sleep(50 * time.Millisecond)
		assertRemaining(t, b, 550*time.Millisecond, "after 50ms more between steps")
	})
}

func TestStepDeadlineIsCappedByLimitAndExcludesReserve(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent := context.Background()
		start := time.Now()
		b := mustNew(t, parent, Config{Total: time.Second, Reserve: 200 * time.Millisecond, MinStep: 50 * time.Millisecond})

		for _, tc := range []struct {
			name  string
			limit time.Duration
			want  time.Duration
		}{
			{name: "limit below remaining", limit: 300 * time.Millisecond, want: 300 * time.Millisecond},
			{name: "limit above remaining stops before the reserve", limit: 2 * time.Second, want: 800 * time.Millisecond},
			{name: "zero limit uses all of Remaining", limit: 0, want: 800 * time.Millisecond},
			{name: "negative limit uses all of Remaining", limit: -time.Second, want: 800 * time.Millisecond},
			{name: "limit below MinStep is the caller's choice", limit: 10 * time.Millisecond, want: 10 * time.Millisecond},
		} {
			stepCtx, cancel := mustStep(t, b, parent, tc.limit)
			assertDeadline(t, stepCtx, start, tc.want, tc.name)
			cancel()
		}

		// An earlier deadline already on the caller's context still wins.
		short, cancelShort := context.WithTimeout(parent, 50*time.Millisecond)
		defer cancelShort()
		stepCtx, cancel := mustStep(t, b, short, 0)
		assertDeadline(t, stepCtx, start, 50*time.Millisecond, "caller context with an earlier deadline")
		cancel()
	})
}

func TestSequentialStepsShareOneBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
		defer cancelParent()
		start := time.Now()
		b := mustNew(t, parent, Config{Reserve: 100 * time.Millisecond, MinStep: 50 * time.Millisecond})

		// Auth lookup: capped at 300ms, answers after 200ms.
		auth, cancelAuth := mustStep(t, b, parent, 300*time.Millisecond)
		assertDeadline(t, auth, start, 300*time.Millisecond, "auth step")
		time.Sleep(200 * time.Millisecond)
		cancelAuth()
		assertRemaining(t, b, 700*time.Millisecond, "after the auth step")

		// Database query: uncapped, hangs until its deadline.
		db, cancelDB := mustStep(t, b, parent, 0)
		assertDeadline(t, db, start, 900*time.Millisecond, "db step")
		<-db.Done()
		if got := time.Since(start); got != 900*time.Millisecond {
			t.Fatalf("db step ended %v after New, want 900ms (budget deadline minus reserve)", got)
		}
		if err := db.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("db step error = %v, want context.DeadlineExceeded", err)
		}
		cancelDB()

		// Notification: nothing left for it, but the reserve is still there.
		assertRemaining(t, b, 0, "after the db step used the rest")
		assertRefused(t, b, parent, 0, ErrBudgetExhausted, "notification step")
		if err := parent.Err(); err != nil {
			t.Fatalf("parent context is done (%v) with the reserve unused: steps must not consume the reserve", err)
		}
	})
}

func TestStepRefusedBelowMinStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, context.Background(), Config{Total: time.Second, MinStep: 100 * time.Millisecond})

		time.Sleep(900 * time.Millisecond)
		stepCtx, cancel := mustStep(t, b, context.Background(), 0)
		assertDeadline(t, stepCtx, start, time.Second, "step with exactly MinStep remaining")
		cancel()

		time.Sleep(time.Nanosecond)
		assertRefused(t, b, context.Background(), 0, ErrBudgetExhausted, "step with less than MinStep remaining")
		assertRefused(t, b, context.Background(), 10*time.Millisecond, ErrBudgetExhausted, "small step with less than MinStep remaining")
	})
}

func TestExhaustedBudgetRefusesStepsWithoutMinStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, context.Background(), Config{Total: time.Second})

		time.Sleep(time.Second)
		assertRemaining(t, b, 0, "at the budget deadline")
		assertRefused(t, b, context.Background(), 0, ErrBudgetExhausted, "step at the budget deadline")

		time.Sleep(time.Second)
		assertRemaining(t, b, 0, "long after the budget deadline")
		assertRefused(t, b, context.Background(), time.Second, ErrBudgetExhausted, "step after the budget deadline")
	})
}

func TestStepContextIsDerivedFromCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		parent := context.WithValue(context.Background(), ctxKey("request"), "req-1")
		b := mustNew(t, parent, Config{Total: time.Second})

		span := context.WithValue(parent, ctxKey("span"), "db")
		stepCtx, cancel := mustStep(t, b, span, 0)
		defer cancel()
		if got := stepCtx.Value(ctxKey("request")); got != "req-1" {
			t.Fatalf("step context request value = %v, want req-1: it must be derived from the parent", got)
		}
		if got := stepCtx.Value(ctxKey("span")); got != "db" {
			t.Fatalf("step context span value = %v, want db: it must be derived from the ctx passed to Step", got)
		}
	})
}

func TestParentCancellationReachesStepsAndBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		b := mustNew(t, parent, Config{Total: time.Second})

		stepCtx, cancel := mustStep(t, b, parent, 0)
		defer cancel()
		cancelParent()
		synctest.Wait()

		if err := stepCtx.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("step context error after parent cancellation = %v, want context.Canceled", err)
		}
		assertRemaining(t, b, 0, "after parent cancellation")
		assertRefused(t, b, parent, 0, context.Canceled, "step after parent cancellation")
	})
}

func TestDoneCallerContextWinsOverExhaustedBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent := context.Background()
		b := mustNew(t, parent, Config{Total: time.Second})

		done, cancel := context.WithCancel(parent)
		cancel()
		assertRefused(t, b, done, 0, context.Canceled, "pre-cancelled step with budget left")
		assertRemaining(t, b, time.Second, "after a refused step")

		time.Sleep(time.Second)
		assertRefused(t, b, done, 0, context.Canceled, "pre-cancelled step with the budget exhausted")
	})
}

func TestCancelReleasesOnlyItsStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		b := mustNew(t, parent, Config{Total: time.Second})

		a, cancelA := mustStep(t, b, parent, 0)
		other, cancelOther := mustStep(t, b, parent, 0)
		defer cancelOther()

		cancelA()
		if err := a.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("step context error after its CancelFunc = %v, want context.Canceled", err)
		}
		cancelA() // a second call must be safe

		if err := other.Err(); err != nil {
			t.Fatalf("another step's context is done (%v) after cancelling a different step", err)
		}
		if err := parent.Err(); err != nil {
			t.Fatalf("parent context is done (%v) after cancelling a step", err)
		}
		assertRemaining(t, b, time.Second, "after cancelling a step")
		_, cancelNext := mustStep(t, b, parent, 0)
		cancelNext()
	})
}

func TestConcurrentStepsDoNotDeductFromEachOther(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 32
		start := time.Now()
		b := mustNew(t, context.Background(), Config{Total: time.Second, Reserve: 100 * time.Millisecond})

		var mu sync.Mutex
		var deadlines []time.Time
		var remaining []time.Duration
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				stepCtx, cancel, err := b.Step(context.Background(), 0)
				if err != nil {
					t.Errorf("concurrent Step returned error: %v", err)
					return
				}
				deadline, _ := stepCtx.Deadline()
				mu.Lock()
				deadlines = append(deadlines, deadline)
				remaining = append(remaining, b.Remaining())
				mu.Unlock()
				time.Sleep(100 * time.Millisecond) // the calls overlap in time
				cancel()
			})
		}
		wg.Wait()

		if len(deadlines) != callers {
			t.Fatalf("%d of %d concurrent steps were granted", len(deadlines), callers)
		}
		for i, d := range deadlines {
			if !d.Equal(start.Add(900 * time.Millisecond)) {
				t.Fatalf("concurrent step %d deadline = start+%v, want start+900ms: overlapping steps must not deduct from each other", i, d.Sub(start))
			}
			if remaining[i] != 900*time.Millisecond {
				t.Fatalf("Remaining() during concurrent steps = %v, want 900ms", remaining[i])
			}
		}
		assertRemaining(t, b, 800*time.Millisecond, "after 32 overlapping 100ms steps")
	})
}

func TestNewAcceptsDoneParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		b := mustNew(t, cancelled, Config{Total: time.Second})
		assertRemaining(t, b, 0, "budget with a cancelled parent")
		assertRefused(t, b, cancelled, 0, context.Canceled, "step with a cancelled parent")

		expired, cancelExpired := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancelExpired()
		time.Sleep(100 * time.Millisecond)
		synctest.Wait() // let the parent's timer fire
		b = mustNew(t, expired, Config{})
		assertRemaining(t, b, 0, "budget with an expired parent")
		assertRefused(t, b, expired, 0, context.DeadlineExceeded, "step with an expired parent")
	})
}
