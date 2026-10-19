package executor

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type handlerFunc func(context.Context, Task)

func (f handlerFunc) Handle(ctx context.Context, task Task) {
	f(ctx, task)
}

func mustNew(t *testing.T, workers, queueCapacity int, h handlerFunc) *Executor {
	t.Helper()
	e, err := New(workers, queueCapacity, h)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return e
}

func mustSubmit(t *testing.T, e *Executor, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := e.Submit(context.Background(), Task{ID: id}); err != nil {
			t.Fatalf("Submit(%q) returned error: %v", id, err)
		}
	}
}

func mustShutdown(t *testing.T, e *Executor) {
	t.Helper()
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

// recorder remembers the IDs of handled tasks in completion order.
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
		valid := handlerFunc(func(context.Context, Task) {})
		for _, tc := range []struct {
			name          string
			workers       int
			queueCapacity int
			handler       Handler
			wantErr       error
		}{
			{name: "zero workers", workers: 0, queueCapacity: 1, handler: valid, wantErr: ErrInvalidConfig},
			{name: "negative workers", workers: -1, queueCapacity: 1, handler: valid, wantErr: ErrInvalidConfig},
			{name: "zero capacity", workers: 1, queueCapacity: 0, handler: valid, wantErr: ErrInvalidConfig},
			{name: "nil handler", workers: 1, queueCapacity: 1, handler: nil, wantErr: ErrNilHandler},
		} {
			e, err := New(tc.workers, tc.queueCapacity, tc.handler)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if e != nil {
				t.Errorf("%s: New returned a non-nil executor with an error", tc.name)
			}
		}
	})
}

func TestRunsExactlyWorkersTasksConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		release := make(chan struct{})
		var mu sync.Mutex
		active, maxActive := 0, 0
		var r recorder

		e := mustNew(t, workers, 10, func(_ context.Context, task Task) {
			mu.Lock()
			active++
			maxActive = max(maxActive, active)
			mu.Unlock()
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			r.add(task.ID)
		})

		for i := range 8 {
			mustSubmit(t, e, strconv.Itoa(i))
		}
		synctest.Wait()
		mu.Lock()
		if active != workers {
			t.Errorf("%d tasks running with %d queued, want exactly %d (one per worker)", active, 8-active, workers)
		}
		mu.Unlock()

		close(release)
		mustShutdown(t, e)
		if maxActive > workers {
			t.Fatalf("max concurrent Handle calls = %d, want <= %d", maxActive, workers)
		}
		if got := len(r.snapshot()); got != 8 {
			t.Fatalf("handled %d tasks, want 8", got)
		}
	})
}

func TestQueueCapacityExcludesRunningTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		e := mustNew(t, 2, 2, func(context.Context, Task) { <-release })

		mustSubmit(t, e, "running-1", "running-2")
		synctest.Wait() // both workers are now inside Handle
		mustSubmit(t, e, "queued-1", "queued-2")

		// A Submit that blocked on capacity would deadlock here, which synctest
		// reports as a failure.
		if err := e.Submit(context.Background(), Task{ID: "overload"}); !errors.Is(err, ErrOverloaded) {
			t.Fatalf("Submit on a full queue = %v, want ErrOverloaded", err)
		}

		close(release)
		mustShutdown(t, e)
	})
}

func TestSubmitHonorsCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		e := mustNew(t, 1, 1, func(_ context.Context, task Task) { r.add(task.ID) })

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.Submit(ctx, Task{ID: "cancelled"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit with cancelled ctx = %v, want context.Canceled", err)
		}

		mustShutdown(t, e)
		if got := r.snapshot(); len(got) != 0 {
			t.Fatalf("handled %v, want no tasks: a rejected task must not run", got)
		}
	})
}

func TestCallerContextDoesNotBindAcceptedTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var r recorder
		var workErr error
		e := mustNew(t, 1, 1, func(ctx context.Context, task Task) {
			if task.ID == "blocker" {
				<-release
				return
			}
			workErr = ctx.Err()
			r.add(task.ID)
		})

		mustSubmit(t, e, "blocker")
		synctest.Wait()

		ctx, cancel := context.WithCancel(context.Background())
		if err := e.Submit(ctx, Task{ID: "accepted"}); err != nil {
			t.Fatalf("Submit returned error: %v", err)
		}
		cancel() // the caller stops caring after admission

		close(release)
		mustShutdown(t, e)
		if got := r.snapshot(); !reflect.DeepEqual(got, []string{"accepted"}) {
			t.Fatalf("handled %v, want [accepted]", got)
		}
		if workErr != nil {
			t.Fatalf("Handle saw a done context (%v): the Submit context must not become the work context", workErr)
		}
	})
}

func TestShutdownDrainsAcceptedTasksAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var r recorder
		var cancelledDuringDrain []string
		e := mustNew(t, 1, 2, func(ctx context.Context, task Task) {
			if task.ID == "running" {
				<-release
			}
			if ctx.Err() != nil {
				cancelledDuringDrain = append(cancelledDuringDrain, task.ID)
			}
			r.add(task.ID)
		})

		mustSubmit(t, e, "running")
		synctest.Wait()
		mustSubmit(t, e, "queued-1", "queued-2")

		done := make(chan error, 1)
		go func() { done <- e.Shutdown(context.Background()) }()
		synctest.Wait()

		if err := e.Submit(context.Background(), Task{ID: "late"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("Submit during Shutdown = %v, want ErrStopped", err)
		}
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v before accepted tasks finished", err)
		default:
		}

		close(release)
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Shutdown returned error: %v", err)
			}
		default:
			t.Fatal("Shutdown did not return after every accepted task finished")
		}

		if got, want := r.snapshot(), []string{"running", "queued-1", "queued-2"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("handled %v, want %v", got, want)
		}
		if len(cancelledDuringDrain) != 0 {
			t.Fatalf("tasks %v ran with a cancelled work context during a graceful drain", cancelledDuringDrain)
		}
		if err := e.Submit(context.Background(), Task{ID: "after"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("Submit after Shutdown = %v, want ErrStopped", err)
		}
	})
}

func TestShutdownDeadlineCancelsWorkAndWaitsForWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var mu sync.Mutex
		var cancelledAt time.Time
		e := mustNew(t, 1, 1, func(ctx context.Context, task Task) {
			<-ctx.Done()
			if task.ID == "running" {
				mu.Lock()
				cancelledAt = time.Now()
				mu.Unlock()
				<-release // the handler takes a while to clean up
			}
		})

		mustSubmit(t, e, "running")
		synctest.Wait()
		mustSubmit(t, e, "queued") // may be abandoned

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- e.Shutdown(ctx) }()

		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		if cancelledAt.IsZero() {
			t.Fatal("work context was not cancelled when the shutdown deadline expired")
		}
		if got := cancelledAt.Sub(start); got != 10*time.Millisecond {
			t.Fatalf("work cancelled %v after Shutdown started, want 10ms", got)
		}
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v while a handler was still running", err)
		default:
		}

		close(release)
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown error = %v, want context.DeadlineExceeded", err)
			}
		default:
			t.Fatal("Shutdown did not return after the handler exited")
		}
		if err := e.Submit(context.Background(), Task{ID: "late"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("Submit after forced shutdown = %v, want ErrStopped", err)
		}
	})
}

func TestConcurrentShutdownIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := mustNew(t, 4, 1, func(context.Context, Task) {})

		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				if err := e.Shutdown(context.Background()); err != nil {
					t.Errorf("Shutdown returned error: %v", err)
				}
			})
		}
		wg.Wait()

		if err := e.Submit(context.Background(), Task{ID: "late"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("Submit after Shutdown = %v, want ErrStopped", err)
		}
	})
}

func TestShutdownAfterCompletionReturnsNil(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := mustNew(t, 1, 1, func(context.Context, Task) {})
		mustShutdown(t, e)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown after completion = %v, want nil", err)
		}
	})
}

func TestConcurrentShutdownReturnsItsOwnContextError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := mustNew(t, 1, 1, func(ctx context.Context, _ Task) { <-ctx.Done() })
		mustSubmit(t, e, "running")
		synctest.Wait()

		first := make(chan error, 1)
		go func() { first <- e.Shutdown(context.Background()) }()
		synctest.Wait()
		if err := e.Submit(context.Background(), Task{ID: "late"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("Submit during Shutdown = %v, want ErrStopped", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("second Shutdown = %v, want context.Canceled", err)
		}
		if err := <-first; err != nil {
			t.Fatalf("first Shutdown = %v, want nil", err)
		}
	})
}

func TestConcurrentSubmitAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		e := mustNew(t, 2, 8, func(_ context.Context, task Task) { r.add(task.ID) })

		var accepted sync.Map
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				id := strconv.Itoa(i)
				switch err := e.Submit(context.Background(), Task{ID: id}); {
				case err == nil:
					accepted.Store(id, true)
				case errors.Is(err, ErrStopped), errors.Is(err, ErrOverloaded):
				default:
					t.Errorf("Submit(%q) = %v, want nil, ErrStopped, or ErrOverloaded", id, err)
				}
			})
		}
		wg.Go(func() {
			if err := e.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown returned error: %v", err)
			}
		})
		wg.Wait()
		mustShutdown(t, e)

		seen := map[string]int{}
		for _, id := range r.snapshot() {
			seen[id]++
		}
		accepted.Range(func(k, _ any) bool {
			id := k.(string)
			if seen[id] != 1 {
				t.Errorf("accepted task %q handled %d times, want exactly once", id, seen[id])
			}
			delete(seen, id)
			return true
		})
		for id := range seen {
			t.Errorf("task %q was handled but Submit did not accept it", id)
		}
	})
}
