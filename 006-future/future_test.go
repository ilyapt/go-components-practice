package future

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func mustStart(t *testing.T, workCtx context.Context, fn func(context.Context) (Result, error)) Future {
	t.Helper()
	f, err := Start(workCtx, fn)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if f == nil {
		t.Fatal("Start returned a nil Future")
	}
	return f
}

func TestCancelledWaiterDoesNotCancelSharedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		f := mustStart(t, context.Background(), func(ctx context.Context) (Result, error) {
			calls.Add(1)
			select {
			case <-release:
				return Result{Value: "shared"}, nil
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		})
		synctest.Wait()

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := f.Await(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("Await with cancelled ctx = %v, want context.Canceled", err)
		}

		// A waiter that gives up mid-wait must not cancel the work either.
		waitCtx, cancelWait := context.WithCancel(context.Background())
		gaveUp := make(chan error, 1)
		go func() {
			_, err := f.Await(waitCtx)
			gaveUp <- err
		}()
		synctest.Wait()
		cancelWait()
		if err := <-gaveUp; !errors.Is(err, context.Canceled) {
			t.Fatalf("Await cancelled mid-wait = %v, want context.Canceled", err)
		}

		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				result, err := f.Await(context.Background())
				if err != nil || result.Value != "shared" {
					t.Errorf("Await = (%q, %v), want (shared, nil)", result.Value, err)
				}
			})
		}
		synctest.Wait()
		close(release)
		wg.Wait()

		if got := calls.Load(); got != 1 {
			t.Fatalf("fn called %d times, want 1", got)
		}
	})
}

func TestStartReturnsWithoutWaitingForFn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		started := make(chan struct{})
		// If Start ran fn synchronously, this call would never return and
		// synctest would report a deadlock.
		f := mustStart(t, context.Background(), func(context.Context) (Result, error) {
			close(started)
			<-release
			return Result{Value: "done"}, nil
		})
		<-started

		close(release)
		if result, err := f.Await(context.Background()); err != nil || result.Value != "done" {
			t.Fatalf("Await = (%q, %v), want (done, nil)", result.Value, err)
		}
	})
}

func TestAwaitAfterCompletionReturnsStoredResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := mustStart(t, context.Background(), func(context.Context) (Result, error) {
			return Result{Value: "complete"}, nil
		})
		synctest.Wait()

		for i := range 3 {
			result, err := f.Await(context.Background())
			if err != nil || result.Value != "complete" {
				t.Fatalf("Await #%d = (%q, %v), want (complete, nil)", i, result.Value, err)
			}
		}
	})
}

func TestEveryWaiterReceivesTheSameError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workErr := errors.New("work failed")
		release := make(chan struct{})
		f := mustStart(t, context.Background(), func(context.Context) (Result, error) {
			<-release
			return Result{Value: "partial"}, workErr
		})

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				result, err := f.Await(context.Background())
				if !errors.Is(err, workErr) || result.Value != "partial" {
					t.Errorf("Await = (%q, %v), want (partial, %v)", result.Value, err, workErr)
				}
			})
		}
		synctest.Wait()
		close(release)
		wg.Wait()

		// Waiters that arrive after completion see the same stored outcome.
		if result, err := f.Await(context.Background()); !errors.Is(err, workErr) || result.Value != "partial" {
			t.Fatalf("late Await = (%q, %v), want (partial, %v)", result.Value, err, workErr)
		}
	})
}

func TestWorkContextCancelsWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.Background())
		f := mustStart(t, workCtx, func(ctx context.Context) (Result, error) {
			<-ctx.Done()
			return Result{}, ctx.Err()
		})
		synctest.Wait()

		cancelWork()
		if _, err := f.Await(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("Await after work cancellation = %v, want context.Canceled from fn", err)
		}
	})
}

func TestAwaitDeadlineDoesNotAffectResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := mustStart(t, context.Background(), func(ctx context.Context) (Result, error) {
			select {
			case <-time.After(time.Hour):
				return Result{Value: "slow"}, nil
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		})

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if _, err := f.Await(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Await with 10ms deadline = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 10*time.Millisecond {
			t.Fatalf("Await returned after %v, want 10ms", got)
		}

		if result, err := f.Await(context.Background()); err != nil || result.Value != "slow" {
			t.Fatalf("Await = (%q, %v), want (slow, nil)", result.Value, err)
		}
	})
}

func TestWorkCompletesWithoutWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finished := make(chan struct{})
		mustStart(t, context.Background(), func(context.Context) (Result, error) {
			defer close(finished)
			return Result{Value: "unobserved"}, nil
		})
		<-finished
		// Nobody ever calls Await. Publishing the result must not block on a
		// missing reader: synctest fails the test if the goroutine that ran fn
		// is still blocked when the test returns.
	})
}

func TestStartRejectsNilFunction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, err := Start(context.Background(), nil)
		if err == nil {
			t.Fatal("Start returned nil error for a nil function")
		}
		if f != nil {
			t.Fatal("Start returned a Future for a nil function")
		}
		synctest.Wait() // a goroutine calling a nil fn would panic here
	})
}
