package retrybudget

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type policyFunc struct {
	retryable func(error) bool
	backoff   func(int) time.Duration
}

func (p policyFunc) Retryable(err error) bool          { return p.retryable(err) }
func (p policyFunc) Backoff(attempt int) time.Duration { return p.backoff(attempt) }

var (
	errRetryable = errors.New("retryable")
	errPermanent = errors.New("permanent")
)

// retryOn retries errors matching errRetryable and waits backoff after each
// failed attempt.
func retryOn(backoff time.Duration) policyFunc {
	return policyFunc{
		retryable: func(err error) bool { return errors.Is(err, errRetryable) },
		backoff:   func(int) time.Duration { return backoff },
	}
}

func config(ratio, perSecond float64, burst, maxAttempts int) Config {
	return Config{
		Ratio:               ratio,
		MinRetriesPerSecond: perSecond,
		Burst:               burst,
		MaxAttempts:         maxAttempts,
		Policy:              retryOn(0),
	}
}

func mustNew(t *testing.T, cfg Config) *Budget {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v) returned error: %v", cfg, err)
	}
	return b
}

// always makes an operation fail on every attempt.
const always = math.MaxInt

type outcome int

const (
	succeeds  outcome = iota // Do returns nil
	refused                  // ErrBudgetExhausted wrapping the last operation error
	exhausted                // the last operation error alone: attempts ran out
)

// expect runs one Do whose operation fails with errRetryable the first
// failures times and then succeeds, and checks the number of operation calls
// and the outcome.
func expect(t *testing.T, b *Budget, what string, failures, wantCalls int, want outcome) {
	t.Helper()
	calls := 0
	err := b.Do(context.Background(), func(context.Context) error {
		calls++
		if calls <= failures {
			return errRetryable
		}
		return nil
	})
	switch want {
	case succeeds:
		if err != nil {
			t.Fatalf("%s: Do = %v, want nil", what, err)
		}
	case refused:
		if !errors.Is(err, ErrBudgetExhausted) || !errors.Is(err, errRetryable) {
			t.Fatalf("%s: Do = %v, want an error matching both ErrBudgetExhausted and the operation error", what, err)
		}
	case exhausted:
		if !errors.Is(err, errRetryable) || errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("%s: Do = %v, want the last operation error without ErrBudgetExhausted", what, err)
		}
	}
	if calls != wantCalls {
		t.Fatalf("%s: operation called %d times, want %d", what, calls, wantCalls)
	}
}

// goDo runs Do in a new goroutine and returns a channel with its error.
func goDo(b *Budget, ctx context.Context, op Operation) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- b.Do(ctx, op) }()
	return ch
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		return err
	default:
		t.Fatalf("%s: Do has not returned", what)
		return nil
	}
}

func assertBlocked(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		t.Fatalf("%s: Do returned %v; want it still waiting", what, err)
	default:
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			modify func(*Config)
		}{
			{"zero max attempts", func(c *Config) { c.MaxAttempts = 0 }},
			{"negative max attempts", func(c *Config) { c.MaxAttempts = -1 }},
			{"zero burst", func(c *Config) { c.Burst = 0 }},
			{"negative burst", func(c *Config) { c.Burst = -1 }},
			{"nil policy", func(c *Config) { c.Policy = nil }},
			{"negative ratio", func(c *Config) { c.Ratio = -0.1 }},
			{"NaN ratio", func(c *Config) { c.Ratio = math.NaN() }},
			{"infinite ratio", func(c *Config) { c.Ratio = math.Inf(1) }},
			{"negative rate", func(c *Config) { c.MinRetriesPerSecond = -1 }},
			{"NaN rate", func(c *Config) { c.MinRetriesPerSecond = math.NaN() }},
			{"infinite rate", func(c *Config) { c.MinRetriesPerSecond = math.Inf(1) }},
		} {
			cfg := config(0.1, 1, 10, 3)
			tc.modify(&cfg)
			b, err := New(cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if b != nil {
				t.Errorf("%s: New returned a non-nil budget with an error", tc.name)
			}
		}

		// Zero ratio and zero rate are valid: the budget holds only its burst.
		b := mustNew(t, config(0, 0, 1, 1))
		if err := b.Do(context.Background(), nil); !errors.Is(err, ErrNilOperation) {
			t.Fatalf("Do(nil op) = %v, want ErrNilOperation", err)
		}
	})
}

func TestRetriesWithBackoffWhileBudgetLasts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var backoffAttempts []int
		cfg := config(0, 0, 3, 4)
		cfg.Policy = policyFunc{
			retryable: func(err error) bool { return errors.Is(err, errRetryable) },
			backoff: func(attempt int) time.Duration {
				backoffAttempts = append(backoffAttempts, attempt)
				return time.Duration(attempt) * 10 * time.Millisecond
			},
		}
		b := mustNew(t, cfg)

		start := time.Now()
		var offsets []time.Duration
		err := b.Do(context.Background(), func(context.Context) error {
			offsets = append(offsets, time.Since(start))
			if len(offsets) < 4 {
				return errRetryable
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Do = %v, want nil: three retries fit in a burst of 3", err)
		}
		// Waits of 10ms, 20ms and 30ms after attempts 1, 2 and 3.
		if want := []time.Duration{0, 10 * time.Millisecond, 30 * time.Millisecond, 60 * time.Millisecond}; !reflect.DeepEqual(offsets, want) {
			t.Fatalf("attempt start times = %v, want %v", offsets, want)
		}
		if want := []int{1, 2, 3}; !reflect.DeepEqual(backoffAttempts, want) {
			t.Fatalf("Backoff attempts = %v, want %v", backoffAttempts, want)
		}

		// Each retry withdrew one token: nothing is left.
		expect(t, b, "after spending the burst", 1, 1, refused)
	})
}

func TestSuccessAndPermanentErrorsSpendNoTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var backoffs atomic.Int32
		cfg := config(0, 0, 1, 3)
		cfg.Policy = policyFunc{
			retryable: func(err error) bool { return errors.Is(err, errRetryable) },
			backoff: func(int) time.Duration {
				backoffs.Add(1)
				return time.Hour
			},
		}
		b := mustNew(t, cfg)

		expect(t, b, "first-attempt success", 0, 1, succeeds)

		start := time.Now()
		calls := 0
		err := b.Do(context.Background(), func(context.Context) error {
			calls++
			return errPermanent
		})
		if !errors.Is(err, errPermanent) || errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Do = %v, want the permanent error without ErrBudgetExhausted", err)
		}
		if calls != 1 || backoffs.Load() != 0 {
			t.Fatalf("calls = %d, backoffs = %d; want 1 and 0", calls, backoffs.Load())
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("Do took %v after a permanent error, want no waiting", elapsed)
		}

		// The single token is still there.
		expect(t, b, "retry after success and permanent error", 1, 2, succeeds)
		expect(t, b, "retry with the token spent", 1, 1, refused)
	})
}

func TestExhaustedAttemptsReturnLastErrorWithoutWithdrawing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, Config{Burst: 3, MaxAttempts: 3, Policy: retryOn(time.Hour)})

		start := time.Now()
		calls := 0
		err := b.Do(context.Background(), func(context.Context) error {
			calls++
			return fmt.Errorf("attempt %d: %w", calls, errRetryable)
		})
		if err == nil || err.Error() != "attempt 3: retryable" {
			t.Fatalf("Do = %v, want the last operation error unchanged (attempt 3)", err)
		}
		if calls != 3 {
			t.Fatalf("operation called %d times, want 3", calls)
		}
		if elapsed := time.Since(start); elapsed != 2*time.Hour {
			t.Fatalf("Do took %v, want 2h: no wait after the final attempt", elapsed)
		}

		// Two retries spent two of three tokens; the final failure spent none.
		expect(t, b, "retry with one token left", 1, 2, succeeds)
		expect(t, b, "retry with no tokens left", 1, 1, refused)
	})
}

func TestEmptyBudgetRefusesRetryWithoutWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, Config{Burst: 1, MaxAttempts: 5, Policy: retryOn(time.Hour)})

		var errs []error
		failEveryAttempt := func(context.Context) error {
			errs = append(errs, fmt.Errorf("attempt %d: %w", len(errs)+1, errRetryable))
			return errs[len(errs)-1]
		}

		start := time.Now()
		err := b.Do(context.Background(), failEveryAttempt)
		if len(errs) != 2 {
			t.Fatalf("operation called %d times, want 2: one retry, then the budget is empty", len(errs))
		}
		if !errors.Is(err, ErrBudgetExhausted) || !errors.Is(err, errs[1]) {
			t.Fatalf("Do = %v, want an error matching ErrBudgetExhausted and the attempt 2 error", err)
		}
		if elapsed := time.Since(start); elapsed != time.Hour {
			t.Fatalf("Do took %v, want 1h: one backoff, none after the refusal", elapsed)
		}

		errs = nil
		start = time.Now()
		err = b.Do(context.Background(), failEveryAttempt)
		if len(errs) != 1 {
			t.Fatalf("operation called %d times with an empty budget, want 1", len(errs))
		}
		if !errors.Is(err, ErrBudgetExhausted) || !errors.Is(err, errs[0]) {
			t.Fatalf("Do = %v, want an error matching ErrBudgetExhausted and the operation error", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("refused Do took %v, want an immediate return without waiting the backoff", elapsed)
		}
	})
}

func TestEveryFirstAttemptDepositsRatio(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, config(0.25, 0, 1, 2))

		expect(t, b, "A: spends the initial token", always, 2, exhausted) // 1 -> 0
		expect(t, b, "B: own deposit is not enough", 1, 1, refused)       // 0.25
		expect(t, b, "C: success deposits", 0, 1, succeeds)               // 0.50
		expect(t, b, "D: success deposits", 0, 1, succeeds)               // 0.75
		expect(t, b, "E: fourth deposit funds a retry", 1, 2, succeeds)   // 1.00 -> 0
		expect(t, b, "F: own deposit is not enough again", 1, 1, refused) // 0.25
	})
}

func TestBalanceNeverExceedsBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, config(0.5, 0, 2, 10))
		for i := range 10 {
			expect(t, b, fmt.Sprintf("success %d", i), 0, 1, succeeds)
		}
		// Ten deposits of 0.5 on a full budget are lost: only 2 retries remain.
		expect(t, b, "after deposits on a full budget", always, 3, refused)
	})
}

func TestMinRetriesPerSecondRefillsQuietBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, config(0, 2, 2, 3))

		expect(t, b, "spend the burst", always, 3, exhausted)
		expect(t, b, "empty at 0ms", 1, 1, refused)

		time.Sleep(250 * time.Millisecond)
		expect(t, b, "half a token at 250ms", 1, 1, refused)

		time.Sleep(250 * time.Millisecond)
		expect(t, b, "one token at 500ms", 1, 2, succeeds)
		expect(t, b, "empty again at 500ms", 1, 1, refused)

		// A long quiet period refills only up to Burst.
		time.Sleep(time.Minute)
		expect(t, b, "first of two tokens", 1, 2, succeeds)
		expect(t, b, "second of two tokens", 1, 2, succeeds)
		expect(t, b, "accrual is capped at Burst", 1, 1, refused)
	})
}

func TestBudgetIsSharedAndNeverOverspent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers, burst = 20, 5
		b := mustNew(t, config(0, 0, burst, 2))

		release := make(chan struct{})
		var started, calls, succeeded, refusedCalls atomic.Int32
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				first := true
				err := b.Do(context.Background(), func(context.Context) error {
					calls.Add(1)
					if first {
						first = false
						started.Add(1)
						<-release // every first attempt fails at the same moment
						return errRetryable
					}
					return nil
				})
				switch {
				case err == nil:
					succeeded.Add(1)
				case errors.Is(err, ErrBudgetExhausted) && errors.Is(err, errRetryable):
					refusedCalls.Add(1)
				default:
					t.Errorf("Do = %v, want nil or ErrBudgetExhausted wrapping the operation error", err)
				}
			})
		}
		// A Do that held a lock across op would leave the others stuck here.
		synctest.Wait()
		if got := started.Load(); got != callers {
			t.Errorf("%d of %d first attempts running at once, want all: Do must not serialize callers", got, callers)
		}
		close(release)
		wg.Wait()

		if got := succeeded.Load(); got != burst {
			t.Errorf("%d callers got a retry from a budget of %d tokens, want exactly %d", got, burst, burst)
		}
		if got := refusedCalls.Load(); got != callers-burst {
			t.Errorf("%d callers were refused, want %d", got, callers-burst)
		}
		if got := calls.Load(); got != callers+burst {
			t.Errorf("operation called %d times, want %d", got, callers+burst)
		}
	})
}

func TestCancellationDuringBackoffReturnsContextError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, Config{Burst: 2, MaxAttempts: 3, Policy: retryOn(time.Hour)})
		var calls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		done := goDo(b, ctx, func(context.Context) error {
			calls.Add(1)
			return errRetryable
		})

		assertBlocked(t, done, "during the first backoff")
		start := time.Now()
		cancel()
		if err := receive(t, done, "after cancellation"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("Do returned %v after cancellation, want immediately", elapsed)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("operation called %d times, want 1", got)
		}
	})
}

func TestDeadlineDuringBackoffReturnsDeadlineExceeded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, Config{Burst: 2, MaxAttempts: 3, Policy: retryOn(time.Minute)})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		calls := 0
		err := b.Do(ctx, func(context.Context) error {
			calls++
			return errRetryable
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Fatalf("Do returned after %v, want at the 5s deadline", elapsed)
		}
		if calls != 1 {
			t.Fatalf("operation called %d times, want 1", calls)
		}
	})
}

func TestCancellationDuringOperationWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, config(0, 0, 5, 3))
		for _, opErr := range []error{errRetryable, nil} {
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			err := b.Do(ctx, func(context.Context) error {
				calls++
				cancel()
				return opErr
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("op error %v: Do = %v, want context.Canceled", opErr, err)
			}
			if calls != 1 {
				t.Fatalf("op error %v: operation called %d times after cancellation, want 1", opErr, calls)
			}
		}
	})
}

func TestPreCancelledContextCallsNothingAndDepositsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, config(0.5, 0, 1, 2))
		expect(t, b, "spend the initial token", always, 2, exhausted)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		for range 2 {
			err := b.Do(ctx, func(context.Context) error {
				calls++
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-cancelled Do = %v, want context.Canceled", err)
			}
		}
		if calls != 0 {
			t.Fatalf("operation called %d times for pre-cancelled callers, want 0", calls)
		}

		// Only this call's own 0.5 is in the budget; two deposits from the
		// cancelled callers would have made it 1.5.
		expect(t, b, "after pre-cancelled callers", 1, 1, refused)
	})
}

func TestOperationReceivesCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey struct{}
		b := mustNew(t, config(0, 0, 1, 2))
		ctx := context.WithValue(context.Background(), ctxKey{}, "caller")

		var seen []any
		err := b.Do(ctx, func(ctx context.Context) error {
			seen = append(seen, ctx.Value(ctxKey{}))
			if len(seen) == 1 {
				return errRetryable
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Do = %v, want nil", err)
		}
		if want := []any{"caller", "caller"}; !reflect.DeepEqual(seen, want) {
			t.Fatalf("operation saw context values %v, want %v: every attempt runs under the caller's context", seen, want)
		}
	})
}
