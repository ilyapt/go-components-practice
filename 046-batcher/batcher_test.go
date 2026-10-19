package batcher

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

// recorder is a FlushFunc that remembers every batch and when it was flushed.
type recorder struct {
	mu      sync.Mutex
	batches [][]Item
	times   []time.Time
}

func (r *recorder) flush(items []Item) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, items)
	r.times = append(r.times, time.Now())
}

func (r *recorder) snapshot() ([][]Item, []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]Item(nil), r.batches...), append([]time.Time(nil), r.times...)
}

func mustNew(t *testing.T, maxSize int, maxDelay time.Duration, queueCapacity int, flush FlushFunc) *Batcher {
	t.Helper()
	b, err := New(maxSize, maxDelay, queueCapacity, flush)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return b
}

func mustSubmit(t *testing.T, b *Batcher, values ...string) {
	t.Helper()
	for _, v := range values {
		if err := b.Submit(context.Background(), Item{Value: v}); err != nil {
			t.Fatalf("Submit(%q) returned error: %v", v, err)
		}
	}
}

func mustShutdown(t *testing.T, b *Batcher) {
	t.Helper()
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

func TestFlushesAtMaxSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := mustNew(t, 3, time.Hour, 3, r.flush)

		mustSubmit(t, b, "a", "b", "c")
		synctest.Wait()

		batches, _ := r.snapshot()
		if len(batches) != 1 {
			t.Fatalf("batch count = %d, want 1 flush as soon as maxSize is reached", len(batches))
		}
		assertValues(t, batches[0], "a", "b", "c")
		mustShutdown(t, b)
	})
}

func TestMaxDelayCountsFromFirstItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := mustNew(t, 10, 20*time.Millisecond, 10, r.flush)
		start := time.Now()

		mustSubmit(t, b, "a")
		sleep(15 * time.Millisecond)
		mustSubmit(t, b, "b") // must not extend the deadline of the batch
		sleep(4 * time.Millisecond)
		if batches, _ := r.snapshot(); len(batches) != 0 {
			t.Fatalf("flushed %d batches before maxDelay elapsed", len(batches))
		}

		sleep(1 * time.Millisecond)
		batches, times := r.snapshot()
		if len(batches) != 1 {
			t.Fatalf("batch count = %d at maxDelay after the first item, want 1", len(batches))
		}
		assertValues(t, batches[0], "a", "b")
		if got := times[0].Sub(start); got != 20*time.Millisecond {
			t.Fatalf("flushed %v after the first item, want 20ms", got)
		}

		// The next batch gets its own deadline, counted from its own first item.
		sleep(time.Hour)
		mustSubmit(t, b, "c")
		next := time.Now()
		sleep(20 * time.Millisecond)
		batches, times = r.snapshot()
		if len(batches) != 2 {
			t.Fatalf("batch count = %d, want 2 (an idle batcher must not flush empty batches)", len(batches))
		}
		assertValues(t, batches[1], "c")
		if got := times[1].Sub(next); got != 20*time.Millisecond {
			t.Fatalf("second batch flushed %v after its first item, want 20ms", got)
		}
		mustShutdown(t, b)
	})
}

func TestFullQueueRejectsInsteadOfBlocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var flushCount int
		b := mustNew(t, 1, time.Hour, 2, func([]Item) {
			flushCount++
			if flushCount == 1 {
				<-release
			}
		})

		mustSubmit(t, b, "running")
		synctest.Wait() // the first flush is now blocked, so the queue cannot drain
		mustSubmit(t, b, "queued-1", "queued-2")

		// A blocking Submit would deadlock here, which synctest reports as a failure.
		if err := b.Submit(context.Background(), Item{Value: "rejected"}); !errors.Is(err, ErrFull) {
			t.Fatalf("Submit on a full queue = %v, want ErrFull", err)
		}

		close(release)
		mustShutdown(t, b)
		if flushCount != 3 {
			t.Fatalf("flush count = %d, want 3", flushCount)
		}
	})
}

func TestShutdownDrainsAndFlushesPartialBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := mustNew(t, 3, time.Hour, 5, r.flush)

		mustSubmit(t, b, "a", "b", "c", "d", "e")
		mustShutdown(t, b)
		mustShutdown(t, b) // repeated shutdown is safe

		if err := b.Submit(context.Background(), Item{Value: "late"}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Submit after Shutdown = %v, want ErrClosed", err)
		}
		batches, _ := r.snapshot()
		if len(batches) != 2 {
			t.Fatalf("batch count = %d, want 2", len(batches))
		}
		assertValues(t, batches[0], "a", "b", "c")
		assertValues(t, batches[1], "d", "e")
	})
}

func TestCancelledShutdownWaiterDoesNotStopFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var r recorder
		b := mustNew(t, 1, time.Hour, 1, func(items []Item) {
			<-release
			r.flush(items)
		})
		mustSubmit(t, b, "a")
		synctest.Wait()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := b.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown with cancelled ctx = %v, want context.Canceled", err)
		}
		if err := b.Submit(context.Background(), Item{Value: "late"}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Submit after cancelled Shutdown = %v, want ErrClosed: shutdown must begin anyway", err)
		}

		close(release)
		mustShutdown(t, b)
		batches, _ := r.snapshot()
		if len(batches) != 1 {
			t.Fatalf("batch count = %d, want 1: the in-progress flush must complete", len(batches))
		}
	})
}

func TestShutdownWaitsForFinalFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		b := mustNew(t, 10, time.Hour, 10, func([]Item) { <-release })
		mustSubmit(t, b, "a")

		done := make(chan error, 1)
		go func() { done <- b.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v before the final flush finished", err)
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
			t.Fatal("Shutdown did not return after the final flush")
		}
	})
}

func TestConcurrentSubmitAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := mustNew(t, 4, time.Millisecond, 64, r.flush)

		var accepted sync.Map
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Go(func() {
				v := strconv.Itoa(i)
				switch err := b.Submit(context.Background(), Item{Value: v}); {
				case err == nil:
					accepted.Store(v, true)
				case errors.Is(err, ErrClosed), errors.Is(err, ErrFull):
				default:
					t.Errorf("Submit(%q) = %v, want nil, ErrClosed, or ErrFull", v, err)
				}
			})
		}
		wg.Go(func() {
			if err := b.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown returned error: %v", err)
			}
		})
		wg.Wait()

		seen := map[string]int{}
		batches, _ := r.snapshot()
		for _, batch := range batches {
			for _, item := range batch {
				seen[item.Value]++
			}
		}
		accepted.Range(func(k, _ any) bool {
			if seen[k.(string)] != 1 {
				t.Errorf("accepted item %q flushed %d times, want exactly once", k, seen[k.(string)])
			}
			delete(seen, k.(string))
			return true
		})
		for v := range seen {
			t.Errorf("item %q was flushed but Submit did not accept it", v)
		}
	})
}

func TestFlushSlicesAreNotReused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := mustNew(t, 2, time.Hour, 4, r.flush)
		mustSubmit(t, b, "a", "b", "c", "d")
		mustShutdown(t, b)

		batches, _ := r.snapshot()
		if len(batches) != 2 {
			t.Fatalf("batch count = %d, want 2", len(batches))
		}
		assertValues(t, batches[0], "a", "b")
		assertValues(t, batches[1], "c", "d")
	})
}

func TestValidationAndPreCancelledSubmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		validFlush := func([]Item) {}
		for _, tc := range []struct {
			name          string
			maxSize       int
			maxDelay      time.Duration
			queueCapacity int
			flush         FlushFunc
			wantErr       error
		}{
			{name: "max size", maxDelay: time.Second, queueCapacity: 1, flush: validFlush, wantErr: ErrInvalidConfig},
			{name: "max delay", maxSize: 1, queueCapacity: 1, flush: validFlush, wantErr: ErrInvalidConfig},
			{name: "capacity", maxSize: 1, maxDelay: time.Second, flush: validFlush, wantErr: ErrInvalidConfig},
			{name: "nil flush", maxSize: 1, maxDelay: time.Second, queueCapacity: 1, wantErr: ErrNilFlush},
		} {
			if _, err := New(tc.maxSize, tc.maxDelay, tc.queueCapacity, tc.flush); !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
		}

		var r recorder
		b := mustNew(t, 1, time.Hour, 1, r.flush)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := b.Submit(ctx, Item{Value: "a"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Submit = %v, want context.Canceled", err)
		}
		mustShutdown(t, b)
		if batches, _ := r.snapshot(); len(batches) != 0 {
			t.Fatal("a pre-cancelled item was flushed")
		}
	})
}

func assertValues(t *testing.T, items []Item, want ...string) {
	t.Helper()
	got := make([]string, len(items))
	for i := range items {
		got[i] = items[i].Value
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch = %v, want %v", got, want)
	}
}

// sleep advances virtual time by d and lets the batcher settle.
func sleep(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}
