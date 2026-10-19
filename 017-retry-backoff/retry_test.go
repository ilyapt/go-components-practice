package retrybackoff

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

func alwaysRetry(backoff time.Duration) policyFunc {
	return policyFunc{
		retryable: func(error) bool { return true },
		backoff:   func(int) time.Duration { return backoff },
	}
}

var errRetryable = errors.New("retryable")

func TestRetriesUntilSuccessAndNumbersAttemptsFromOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		var backoffAttempts []int
		policy := policyFunc{
			retryable: func(error) bool { return true },
			backoff: func(attempt int) time.Duration {
				backoffAttempts = append(backoffAttempts, attempt)
				return 0
			},
		}

		result, err := Do(context.Background(), 3, policy, func(context.Context) (Result, error) {
			if calls.Add(1) < 3 {
				return Result{}, errRetryable
			}
			return Result{Value: "success"}, nil
		})
		if err != nil {
			t.Fatalf("Do returned error: %v", err)
		}
		if result.Value != "success" {
			t.Fatalf("result = %q, want success", result.Value)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("operation called %d times, want 3", got)
		}
		if want := []int{1, 2}; !reflect.DeepEqual(backoffAttempts, want) {
			t.Fatalf("Backoff attempts = %v, want %v", backoffAttempts, want)
		}
	})
}

func TestWaitsExactlyBackoffBetweenAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := policyFunc{
			retryable: func(error) bool { return true },
			backoff:   func(attempt int) time.Duration { return time.Duration(attempt) * 10 * time.Millisecond },
		}
		start := time.Now()
		var offsets []time.Duration
		result, err := Do(context.Background(), 4, policy, func(context.Context) (Result, error) {
			offsets = append(offsets, time.Since(start))
			if len(offsets) < 4 {
				return Result{}, errRetryable
			}
			return Result{Value: "ok"}, nil
		})
		if err != nil || result.Value != "ok" {
			t.Fatalf("Do = %v, %v; want ok, nil", result, err)
		}
		// Waits of 10ms, 20ms and 30ms after attempts 1, 2 and 3.
		want := []time.Duration{0, 10 * time.Millisecond, 30 * time.Millisecond, 60 * time.Millisecond}
		if !reflect.DeepEqual(offsets, want) {
			t.Fatalf("attempt start times = %v, want %v", offsets, want)
		}
	})
}

func TestZeroAndNegativeBackoffRetryImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, backoff := range []time.Duration{0, -time.Second} {
			start := time.Now()
			var calls atomic.Int32
			_, err := Do(context.Background(), 3, alwaysRetry(backoff), func(context.Context) (Result, error) {
				calls.Add(1)
				return Result{}, errRetryable
			})
			if !errors.Is(err, errRetryable) {
				t.Fatalf("backoff %v: Do error = %v, want %v", backoff, err, errRetryable)
			}
			if got := calls.Load(); got != 3 {
				t.Fatalf("backoff %v: operation called %d times, want 3", backoff, got)
			}
			if elapsed := time.Since(start); elapsed != 0 {
				t.Fatalf("backoff %v: Do took %v, want no waiting", backoff, elapsed)
			}
		}
	})
}

func TestPermanentErrorStopsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errPermanent := errors.New("permanent")
		var calls, backoffs atomic.Int32
		policy := policyFunc{
			retryable: func(error) bool { return false },
			backoff: func(int) time.Duration {
				backoffs.Add(1)
				return time.Hour
			},
		}

		start := time.Now()
		_, err := Do(context.Background(), 5, policy, func(context.Context) (Result, error) {
			calls.Add(1)
			return Result{}, errPermanent
		})
		if !errors.Is(err, errPermanent) {
			t.Fatalf("Do error = %v, want %v", err, errPermanent)
		}
		if calls.Load() != 1 || backoffs.Load() != 0 {
			t.Fatalf("calls = %d, backoffs = %d; want 1 and 0", calls.Load(), backoffs.Load())
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("Do took %v after a permanent error, want no waiting", elapsed)
		}
	})
}

func TestExhaustionReturnsLastErrorWithoutFinalWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, backoffs atomic.Int32
		policy := policyFunc{
			retryable: func(error) bool { return true },
			backoff: func(int) time.Duration {
				backoffs.Add(1)
				return time.Hour
			},
		}

		start := time.Now()
		_, err := Do(context.Background(), 3, policy, func(context.Context) (Result, error) {
			return Result{}, fmt.Errorf("attempt %d: %w", calls.Add(1), errRetryable)
		})
		if err == nil || err.Error() != "attempt 3: retryable" {
			t.Fatalf("Do error = %v, want the last operation error (attempt 3)", err)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("operation called %d times, want 3", got)
		}
		if got := backoffs.Load(); got != 2 {
			t.Fatalf("Backoff called %d times, want 2 (no backoff after the last attempt)", got)
		}
		if elapsed := time.Since(start); elapsed != 2*time.Hour {
			t.Fatalf("Do took %v, want 2h: it must not wait after the final attempt", elapsed)
		}
	})
}

func TestCancellationDuringBackoffStopsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := Do(ctx, 3, alwaysRetry(time.Hour), func(context.Context) (Result, error) {
				calls.Add(1)
				return Result{}, errRetryable
			})
			done <- err
		}()

		synctest.Wait() // Do is now waiting out the first backoff
		start := time.Now()
		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Do error = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("Do did not stop waiting when ctx was cancelled during backoff")
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var calls atomic.Int32
		start := time.Now()
		_, err := Do(ctx, 3, alwaysRetry(time.Minute), func(context.Context) (Result, error) {
			calls.Add(1)
			return Result{}, errRetryable
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do error = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Fatalf("Do returned after %v, want at the 5s deadline", elapsed)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("operation called %d times, want 1", got)
		}
	})
}

func TestCancellationDuringOperationWinsAndStopsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, opErr := range []error{errRetryable, nil} {
			ctx, cancel := context.WithCancel(context.Background())
			var calls atomic.Int32
			// Zero backoff: there is no wait in which cancellation could be noticed,
			// so Do must check the context itself after the operation returns.
			_, err := Do(ctx, 3, alwaysRetry(0), func(context.Context) (Result, error) {
				calls.Add(1)
				cancel()
				return Result{Value: "late"}, opErr
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("op error %v: Do error = %v, want context.Canceled", opErr, err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("op error %v: operation called %d times after cancellation, want 1", opErr, got)
			}
		}
	})
}

func TestPreCancelledContextDoesNotCallOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls atomic.Int32
		_, err := Do(ctx, 3, alwaysRetry(0), func(context.Context) (Result, error) {
			calls.Add(1)
			return Result{Value: "ok"}, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Do error = %v, want context.Canceled", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("operation called %d times, want 0", got)
		}
	})
}

func TestInvalidArguments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		op := func(context.Context) (Result, error) {
			calls.Add(1)
			return Result{}, nil
		}
		for _, n := range []int{0, -1} {
			if _, err := Do(context.Background(), n, alwaysRetry(0), op); err == nil {
				t.Fatalf("Do(maxAttempts=%d) returned nil error", n)
			}
		}
		if _, err := Do(context.Background(), 1, nil, op); err == nil {
			t.Fatal("Do(nil policy) returned nil error")
		}
		if _, err := Do(context.Background(), 1, alwaysRetry(0), nil); err == nil {
			t.Fatal("Do(nil op) returned nil error")
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("operation called %d times, want 0 for invalid arguments", got)
		}
	})
}
