package loadshedder

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// longDelay is a maxQueueDelay that no test reaches, for tests about
// something other than shedding.
const longDelay = time.Hour

var errFn = errors.New("fn failed")

func mustNew(t *testing.T, workers, queueCapacity int, maxQueueDelay time.Duration) *Shedder {
	t.Helper()
	s, err := New(workers, queueCapacity, maxQueueDelay)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return s
}

func mustShutdown(t *testing.T, s *Shedder) {
	t.Helper()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

// goDo runs Do in a new goroutine and returns a channel with its result.
func goDo(s *Shedder, ctx context.Context, fn func(context.Context) error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.Do(ctx, fn) }()
	return ch
}

// goShutdown runs Shutdown in a new goroutine and returns a channel with its result.
func goShutdown(s *Shedder, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.Shutdown(ctx) }()
	return ch
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		return err
	default:
		t.Fatalf("%s has not returned", what)
		return nil
	}
}

func assertBlocked(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		t.Fatalf("%s returned %v; want it still waiting", what, err)
	default:
	}
}

// blockUntil returns an fn that runs until release is closed.
func blockUntil(release <-chan struct{}) func(context.Context) error {
	return func(context.Context) error {
		<-release
		return nil
	}
}

// counted returns an fn that counts its calls and returns nil.
func counted(calls *atomic.Int32) func(context.Context) error {
	return func(context.Context) error {
		calls.Add(1)
		return nil
	}
}

// recorder remembers the IDs of requests whose fn ran, in start order.
type recorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *recorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name          string
			workers       int
			queueCapacity int
			maxQueueDelay time.Duration
		}{
			{name: "zero workers", workers: 0, queueCapacity: 1, maxQueueDelay: time.Second},
			{name: "negative workers", workers: -1, queueCapacity: 1, maxQueueDelay: time.Second},
			{name: "zero queue capacity", workers: 1, queueCapacity: 0, maxQueueDelay: time.Second},
			{name: "negative queue capacity", workers: 1, queueCapacity: -1, maxQueueDelay: time.Second},
			{name: "zero max queue delay", workers: 1, queueCapacity: 1, maxQueueDelay: 0},
			{name: "negative max queue delay", workers: 1, queueCapacity: 1, maxQueueDelay: -time.Second},
		} {
			s, err := New(tc.workers, tc.queueCapacity, tc.maxQueueDelay)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if s != nil {
				t.Errorf("%s: New returned a non-nil Shedder with an error", tc.name)
			}
		}

		s := mustNew(t, 1, 1, time.Second)
		if err := s.Do(context.Background(), nil); !errors.Is(err, ErrNilFunc) {
			t.Fatalf("Do(nil fn) = %v, want ErrNilFunc", err)
		}
		mustShutdown(t, s)
	})
}

func TestFnRunsUnderCallerContextAndItsErrorIsReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		s := mustNew(t, 1, 1, longDelay)

		ctx := context.WithValue(context.Background(), ctxKey("caller"), "me")
		var seen any
		err := s.Do(ctx, func(ctx context.Context) error {
			seen = ctx.Value(ctxKey("caller"))
			return errFn
		})
		if !errors.Is(err, errFn) {
			t.Fatalf("Do = %v, want the error fn returned (%v)", err, errFn)
		}
		if seen != "me" {
			t.Fatal("fn did not receive a context derived from the Do context")
		}
		if err := s.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("Do with a successful fn = %v, want nil", err)
		}
		mustShutdown(t, s)
	})
}

func TestCallerCancellationReachesRunningFnAndDoWaitsForIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustNew(t, 1, 1, longDelay)
		finish := make(chan struct{})
		var fnCtxErr error

		ctx, cancel := context.WithCancel(context.Background())
		res := goDo(s, ctx, func(ctx context.Context) error {
			<-ctx.Done()
			fnCtxErr = ctx.Err()
			<-finish // fn takes a while to clean up
			return errFn
		})
		synctest.Wait() // fn is running

		cancel()
		assertBlocked(t, res, "Do whose fn is still running")
		close(finish)
		if err := receive(t, res, "Do"); !errors.Is(err, errFn) {
			t.Fatalf("Do = %v, want fn's error %v: once fn has started, Do returns its result", err, errFn)
		}
		if !errors.Is(fnCtxErr, context.Canceled) {
			t.Fatalf("fn's context error = %v, want context.Canceled from the caller", fnCtxErr)
		}

		// The worker is free again.
		if err := s.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("Do after the cancelled request = %v, want nil", err)
		}
		mustShutdown(t, s)
	})
}

func TestRunsAtMostWorkersConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		release := make(chan struct{})
		var mu sync.Mutex
		active, maxActive := 0, 0
		s := mustNew(t, workers, 10, longDelay)
		fn := func(context.Context) error {
			mu.Lock()
			active++
			maxActive = max(maxActive, active)
			mu.Unlock()
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		}

		results := make([]<-chan error, 8)
		for i := range results {
			results[i] = goDo(s, context.Background(), fn)
		}
		synctest.Wait()
		mu.Lock()
		if active != workers {
			t.Errorf("%d fn calls running with 8 requests admitted, want exactly %d (one per worker)", active, workers)
		}
		mu.Unlock()

		close(release)
		for i, ch := range results {
			if err := receive(t, ch, fmt.Sprintf("request %d", i)); err != nil {
				t.Fatalf("request %d: Do = %v, want nil", i, err)
			}
		}
		mustShutdown(t, s)
		if maxActive > workers {
			t.Fatalf("max concurrent fn calls = %d, want <= %d", maxActive, workers)
		}
	})
}

func TestQueueCapacityExcludesRunningRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		s := mustNew(t, 2, 2, longDelay)

		var results []<-chan error
		for range 2 {
			results = append(results, goDo(s, context.Background(), blockUntil(release)))
		}
		synctest.Wait() // both workers are busy
		for range 2 {
			results = append(results, goDo(s, context.Background(), blockUntil(release)))
		}
		synctest.Wait() // two requests are queued

		// A Do that blocked on capacity would deadlock here, which synctest
		// reports as a failure.
		var calls atomic.Int32
		if err := s.Do(context.Background(), counted(&calls)); !errors.Is(err, ErrOverloaded) {
			t.Fatalf("Do on a full queue = %v, want ErrOverloaded", err)
		}

		close(release)
		for i, ch := range results {
			if err := receive(t, ch, fmt.Sprintf("request %d", i)); err != nil {
				t.Fatalf("request %d: Do = %v, want nil", i, err)
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn of the rejected request ran %d times, want 0", got)
		}
		mustShutdown(t, s)
	})
}

func TestShedsRequestThatWaitedLongerThanMaxQueueDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Millisecond
		release := make(chan struct{})
		s := mustNew(t, 1, 1, delay)

		blocker := goDo(s, context.Background(), blockUntil(release))
		synctest.Wait()
		var calls atomic.Int32
		stale := goDo(s, context.Background(), counted(&calls))
		synctest.Wait()

		time.Sleep(delay + time.Millisecond)
		// The shed decision is made when the request is taken from the queue,
		// not by a timer while it waits.
		assertBlocked(t, stale, "request queued behind a busy worker")

		close(release)
		if err := receive(t, blocker, "running request"); err != nil {
			t.Fatalf("running request: Do = %v, want nil", err)
		}
		if err := receive(t, stale, "stale request"); !errors.Is(err, ErrShed) {
			t.Fatalf("Do after waiting %v with maxQueueDelay %v = %v, want ErrShed", delay+time.Millisecond, delay, err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn of the shed request ran %d times, want 0", got)
		}

		// Shedding did not take the worker: a fresh request runs.
		if err := s.Do(context.Background(), counted(&calls)); err != nil {
			t.Fatalf("Do after shedding = %v, want nil", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("fn calls = %d, want 1", got)
		}
		mustShutdown(t, s)
	})
}

func TestShedDecisionUsesEachRequestsOwnQueueingTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Millisecond
		release := make(chan struct{})
		s := mustNew(t, 1, 2, delay)

		blocker := goDo(s, context.Background(), blockUntil(release))
		synctest.Wait()

		var oldCalls, boundaryCalls atomic.Int32
		old := goDo(s, context.Background(), counted(&oldCalls)) // enqueued at t=0
		synctest.Wait()
		time.Sleep(5 * time.Millisecond)
		boundary := goDo(s, context.Background(), counted(&boundaryCalls)) // enqueued at t=5ms
		synctest.Wait()
		time.Sleep(delay) // t=15ms: old waited 15ms, boundary exactly 10ms

		close(release)
		if err := receive(t, blocker, "running request"); err != nil {
			t.Fatalf("running request: Do = %v, want nil", err)
		}
		if err := receive(t, old, "request that waited 15ms"); !errors.Is(err, ErrShed) {
			t.Fatalf("request that waited 15ms with maxQueueDelay 10ms: Do = %v, want ErrShed", err)
		}
		if got := oldCalls.Load(); got != 0 {
			t.Fatalf("fn of the shed request ran %d times, want 0", got)
		}
		if err := receive(t, boundary, "request that waited exactly 10ms"); err != nil {
			t.Fatalf("request that waited exactly maxQueueDelay: Do = %v, want nil (only longer waits are shed)", err)
		}
		if got := boundaryCalls.Load(); got != 1 {
			t.Fatalf("fn of the request that waited exactly maxQueueDelay ran %d times, want 1", got)
		}
		mustShutdown(t, s)
	})
}

func TestRequestsStartInAdmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		s := mustNew(t, 1, 3, longDelay)
		blocker := goDo(s, context.Background(), blockUntil(release))
		synctest.Wait()

		var r recorder
		var results []<-chan error
		for _, id := range []string{"a", "b", "c"} {
			results = append(results, goDo(s, context.Background(), func(context.Context) error {
				r.add(id)
				return nil
			}))
			synctest.Wait() // admit one at a time so the admission order is defined
		}

		close(release)
		if err := receive(t, blocker, "running request"); err != nil {
			t.Fatalf("running request: Do = %v, want nil", err)
		}
		for i, ch := range results {
			if err := receive(t, ch, fmt.Sprintf("request %d", i)); err != nil {
				t.Fatalf("request %d: Do = %v, want nil", i, err)
			}
		}
		if got, want := r.snapshot(), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("start order = %v, want admission order %v", got, want)
		}
		mustShutdown(t, s)
	})
}

func TestCancelledQueuedRequestLeavesQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		s := mustNew(t, 1, 1, longDelay)
		blocker := goDo(s, context.Background(), blockUntil(release))
		synctest.Wait()

		var calls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		queued := goDo(s, ctx, counted(&calls))
		synctest.Wait()
		if err := s.Do(context.Background(), counted(&calls)); !errors.Is(err, ErrOverloaded) {
			t.Fatalf("Do with a full queue = %v, want ErrOverloaded", err)
		}

		cancel()
		// The worker is still busy: the cancelled caller must not wait for it.
		if err := receive(t, queued, "cancelled queued request"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled queued request: Do = %v, want context.Canceled", err)
		}

		next := goDo(s, context.Background(), counted(&calls))
		assertBlocked(t, next, "request admitted into the slot the cancelled request left")

		close(release)
		if err := receive(t, blocker, "running request"); err != nil {
			t.Fatalf("running request: Do = %v, want nil", err)
		}
		if err := receive(t, next, "next request"); err != nil {
			t.Fatalf("next request: Do = %v, want nil", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("fn calls = %d, want 1: fn must never run for the cancelled request", got)
		}
		mustShutdown(t, s)
	})
}

func TestPreCancelledContextDoesNotRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustNew(t, 1, 1, longDelay) // the worker is idle

		var calls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.Do(ctx, counted(&calls)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do with a cancelled ctx = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn ran %d times for a pre-cancelled caller, want 0", got)
		}
		mustShutdown(t, s)
	})
}

func TestShutdownReleasesQueuedRequestsAndWaitsForRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		s := mustNew(t, 1, 2, longDelay)

		var runningCtxErr error
		running := goDo(s, context.Background(), func(ctx context.Context) error {
			<-release
			runningCtxErr = ctx.Err()
			return nil
		})
		synctest.Wait()
		var calls atomic.Int32
		queued := []<-chan error{
			goDo(s, context.Background(), counted(&calls)),
			goDo(s, context.Background(), counted(&calls)),
		}
		synctest.Wait()

		done := goShutdown(s, context.Background())
		synctest.Wait()
		for i, ch := range queued {
			if err := receive(t, ch, fmt.Sprintf("queued request %d", i)); !errors.Is(err, ErrStopped) {
				t.Fatalf("queued request %d: Do = %v, want ErrStopped without waiting for running work", i, err)
			}
		}
		if err := s.Do(context.Background(), counted(&calls)); !errors.Is(err, ErrStopped) {
			t.Fatalf("Do during Shutdown = %v, want ErrStopped", err)
		}
		assertBlocked(t, done, "Shutdown with fn still running")
		assertBlocked(t, running, "running request during a graceful shutdown")

		close(release)
		if err := receive(t, done, "Shutdown"); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if err := receive(t, running, "running request"); err != nil {
			t.Fatalf("running request: Do = %v, want nil", err)
		}
		if runningCtxErr != nil {
			t.Fatalf("running fn saw a cancelled context (%v) during a graceful shutdown", runningCtxErr)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn ran %d times for requests released by Shutdown, want 0", got)
		}
		if err := s.Do(context.Background(), counted(&calls)); !errors.Is(err, ErrStopped) {
			t.Fatalf("Do after Shutdown = %v, want ErrStopped", err)
		}
	})
}

func TestShutdownDeadlineCancelsRunningFnAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var mu sync.Mutex
		var cancelledAt time.Time
		s := mustNew(t, 1, 1, longDelay)

		// The caller's context is never cancelled: only the shutdown deadline
		// can cancel fn here.
		running := goDo(s, context.Background(), func(ctx context.Context) error {
			<-ctx.Done()
			mu.Lock()
			cancelledAt = time.Now()
			mu.Unlock()
			<-release // fn takes a while to clean up
			return errFn
		})
		synctest.Wait()

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		done := goShutdown(s, ctx)

		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		mu.Lock()
		at := cancelledAt
		mu.Unlock()
		if at.IsZero() {
			t.Fatal("running fn's context was not cancelled when the shutdown deadline expired")
		}
		if got := at.Sub(start); got != 10*time.Millisecond {
			t.Fatalf("fn cancelled %v after Shutdown started, want 10ms", got)
		}
		assertBlocked(t, done, "Shutdown with fn still running")

		close(release)
		if err := receive(t, done, "Shutdown"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
		}
		if err := receive(t, running, "running request"); !errors.Is(err, errFn) {
			t.Fatalf("running request: Do = %v, want fn's error %v", err, errFn)
		}
		if err := s.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrStopped) {
			t.Fatalf("Do after forced shutdown = %v, want ErrStopped", err)
		}
	})
}

func TestConcurrentShutdownIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustNew(t, 4, 1, longDelay)

		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				if err := s.Shutdown(context.Background()); err != nil {
					t.Errorf("Shutdown returned error: %v", err)
				}
			})
		}
		wg.Wait()

		if err := s.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrStopped) {
			t.Fatalf("Do after Shutdown = %v, want ErrStopped", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown after completion = %v, want nil", err)
		}
	})
}

func TestConcurrentShutdownReturnsItsOwnContextError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustNew(t, 1, 1, longDelay)
		running := goDo(s, context.Background(), func(ctx context.Context) error {
			<-ctx.Done()
			return errFn
		})
		synctest.Wait()

		first := goShutdown(s, context.Background())
		assertBlocked(t, first, "first Shutdown with fn running")
		if err := s.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrStopped) {
			t.Fatalf("Do during Shutdown = %v, want ErrStopped", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("second Shutdown = %v, want context.Canceled", err)
		}
		if err := receive(t, first, "first Shutdown"); err != nil {
			t.Fatalf("first Shutdown = %v, want nil: its own context never expired", err)
		}
		if err := receive(t, running, "running request"); !errors.Is(err, errFn) {
			t.Fatalf("running request: Do = %v, want fn's error %v", err, errFn)
		}
	})
}

func TestConcurrentDoAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustNew(t, 2, 8, longDelay)
		var r recorder

		var accepted sync.Map
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				id := strconv.Itoa(i)
				switch err := s.Do(context.Background(), func(context.Context) error {
					r.add(id)
					return nil
				}); {
				case err == nil:
					accepted.Store(id, true)
				case errors.Is(err, ErrOverloaded), errors.Is(err, ErrStopped):
				default:
					t.Errorf("Do(%q) = %v, want nil, ErrOverloaded, or ErrStopped", id, err)
				}
			})
		}
		wg.Go(func() {
			if err := s.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown returned error: %v", err)
			}
		})
		wg.Wait()
		mustShutdown(t, s)

		seen := map[string]int{}
		for _, id := range r.snapshot() {
			seen[id]++
		}
		accepted.Range(func(k, _ any) bool {
			id := k.(string)
			if seen[id] != 1 {
				t.Errorf("request %q returned nil but its fn ran %d times, want exactly once", id, seen[id])
			}
			delete(seen, id)
			return true
		})
		for id := range seen {
			t.Errorf("fn of request %q ran but its Do did not return nil", id)
		}
	})
}
