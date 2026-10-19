package perkeyserializer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// goProcess runs Process in a new goroutine and waits until it has been
// admitted (or has returned), so callers are admitted in program order.
func goProcess(p *Processor, ctx context.Context, key string, op Operation) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.Process(ctx, key, op) }()
	synctest.Wait()
	return done
}

func receive(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-done:
		return err
	default:
		t.Fatalf("%s: Process has not returned", what)
		return nil
	}
}

func assertBlocked(t *testing.T, done <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-done:
		t.Fatalf("%s: Process returned %v, want it still waiting", what, err)
	default:
	}
}

type orderLog struct {
	mu    sync.Mutex
	order []int
}

func (l *orderLog) op(n int, wait <-chan struct{}) Operation {
	return func(context.Context) error {
		l.mu.Lock()
		l.order = append(l.order, n)
		l.mu.Unlock()
		if wait != nil {
			<-wait
		}
		return nil
	}
}

func (l *orderLog) get() []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int(nil), l.order...)
}

func TestSameKeyRunsFIFOAndSerially(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		var log orderLog
		releaseFirst := make(chan struct{})

		first := goProcess(p, context.Background(), "key", log.op(1, releaseFirst))
		second := goProcess(p, context.Background(), "key", log.op(2, nil))
		third := goProcess(p, context.Background(), "key", log.op(3, nil))

		if got := log.get(); !reflect.DeepEqual(got, []int{1}) {
			t.Fatalf("started operations = %v while the first runs, want [1]", got)
		}
		assertBlocked(t, second, "second")
		close(releaseFirst)

		for i, done := range []<-chan error{first, second, third} {
			if err := receive(t, done, fmt.Sprintf("operation %d", i+1)); err != nil {
				t.Fatalf("operation %d returned error: %v", i+1, err)
			}
		}
		if got := log.get(); !reflect.DeepEqual(got, []int{1, 2, 3}) {
			t.Fatalf("execution order = %v, want [1 2 3]", got)
		}
	})
}

func TestManyConcurrentCallersOneKeyRunOnceEachAndNeverOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		const callers = 50
		var active, maximum atomic.Int32
		var runs [callers]atomic.Int32

		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				err := p.Process(context.Background(), "key", func(context.Context) error {
					current := active.Add(1)
					for {
						previous := maximum.Load()
						if current <= previous || maximum.CompareAndSwap(previous, current) {
							break
						}
					}
					runs[i].Add(1)
					time.Sleep(time.Millisecond)
					active.Add(-1)
					return nil
				})
				if err != nil {
					t.Errorf("Process %d returned error: %v", i, err)
				}
			})
		}
		wg.Wait()

		if got := maximum.Load(); got != 1 {
			t.Fatalf("maximum concurrent same-key operations = %d, want 1", got)
		}
		for i := range runs {
			if got := runs[i].Load(); got != 1 {
				t.Fatalf("operation %d executed %d times, want exactly once", i, got)
			}
		}
	})
}

func TestDifferentKeysRunConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		release := make(chan struct{})
		var started atomic.Int32
		op := func(context.Context) error {
			started.Add(1)
			<-release
			return nil
		}

		a := goProcess(p, context.Background(), "a", op)
		b := goProcess(p, context.Background(), "b", op)
		if got := started.Load(); got != 2 {
			t.Fatalf("started operations = %d, want 2: different keys must not wait for each other", got)
		}
		close(release)
		for key, done := range map[string]<-chan error{"a": a, "b": b} {
			if err := receive(t, done, key); err != nil {
				t.Fatalf("Process(%q) returned error: %v", key, err)
			}
		}
	})
}

func TestProcessWaitsForAndReturnsOperationResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		errOp := errors.New("operation failed")
		release := make(chan struct{})
		done := goProcess(p, context.Background(), "key", func(context.Context) error {
			<-release
			return errOp
		})
		assertBlocked(t, done, "running operation")
		close(release)
		if err := receive(t, done, "finished operation"); !errors.Is(err, errOp) {
			t.Fatalf("Process error = %v, want %v", err, errOp)
		}
	})
}

func TestQueuedCancellationReturnsAndSkipsOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		var log orderLog
		releaseFirst := make(chan struct{})
		first := goProcess(p, context.Background(), "key", log.op(1, releaseFirst))

		ctx, cancel := context.WithCancel(context.Background())
		cancelled := goProcess(p, ctx, "key", log.op(2, nil))
		third := goProcess(p, context.Background(), "key", log.op(3, nil))
		cancel()

		if err := receive(t, cancelled, "cancelled queued caller"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Process error = %v, want context.Canceled", err)
		}
		assertBlocked(t, first, "first")

		close(releaseFirst)
		if err := receive(t, first, "first"); err != nil {
			t.Fatalf("first Process returned error: %v", err)
		}
		if err := receive(t, third, "third"); err != nil {
			t.Fatalf("third Process returned error: %v", err)
		}
		if got := log.get(); !reflect.DeepEqual(got, []int{1, 3}) {
			t.Fatalf("executed operations = %v, want [1 3]: the cancelled queued operation must be skipped", got)
		}
	})
}

func TestCancellationAfterStartDoesNotOverlapNextOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		ctx, cancel := context.WithCancel(context.Background())
		allowFirstReturn := make(chan struct{})
		first := goProcess(p, ctx, "key", func(ctx context.Context) error {
			<-ctx.Done() // the operation runs with its caller's context
			<-allowFirstReturn
			return ctx.Err()
		})

		var secondStarted atomic.Bool
		second := goProcess(p, context.Background(), "key", func(context.Context) error {
			secondStarted.Store(true)
			return nil
		})
		cancel()

		if err := receive(t, first, "cancelled running caller"); !errors.Is(err, context.Canceled) {
			t.Fatalf("first Process error = %v, want context.Canceled", err)
		}
		if secondStarted.Load() {
			t.Fatal("second operation started before the running operation returned")
		}

		close(allowFirstReturn)
		if err := receive(t, second, "second"); err != nil {
			t.Fatalf("second Process returned error: %v", err)
		}
		if !secondStarted.Load() {
			t.Fatal("second operation did not run")
		}
	})
}

func TestAbandonedResultDoesNotBlockKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		ctx, cancel := context.WithCancel(context.Background())
		release := make(chan struct{})
		// This operation ignores cancellation for a while and finishes successfully
		// after its caller has already left.
		abandoned := goProcess(p, ctx, "key", func(context.Context) error {
			<-release
			return nil
		})
		cancel()
		if err := receive(t, abandoned, "abandoned caller"); !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned Process error = %v, want context.Canceled", err)
		}
		close(release)
		synctest.Wait()

		// Nobody receives the abandoned result; the key must still make progress.
		for i := range 3 {
			done := goProcess(p, context.Background(), "key", func(context.Context) error { return nil })
			if err := receive(t, done, fmt.Sprintf("later operation %d", i)); err != nil {
				t.Fatalf("later operation %d returned error: %v", i, err)
			}
		}
	})
}

func TestRejectsNilAndPreCancelledOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New()
		if err := p.Process(context.Background(), "key", nil); err == nil {
			t.Fatal("nil operation was accepted")
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var called atomic.Bool
		err := p.Process(ctx, "key", func(context.Context) error {
			called.Store(true)
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Process error = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if called.Load() {
			t.Fatal("pre-cancelled operation was executed")
		}

		// Rejected calls leave the key usable.
		if err := p.Process(context.Background(), "key", func(context.Context) error { return nil }); err != nil {
			t.Fatalf("Process after rejected calls returned error: %v", err)
		}
	})
}
