package writebehind

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var errStore = errors.New("store unavailable")

type writeCall struct {
	at      time.Time
	entries []Entry // copy taken when Write was called
	batch   []Entry // the slice the cache passed in
}

// recorder is a Store that remembers every Write call and what was persisted.
// hook, if set, runs inside Write after the call is recorded; n is the
// 1-based number of the call, and a non-nil error fails the Write.
type recorder struct {
	hook func(ctx context.Context, n int) error

	mu                    sync.Mutex
	calls                 []writeCall
	persisted             map[string]string
	inFlight, maxInFlight int
}

func newRecorder(hook func(ctx context.Context, n int) error) *recorder {
	return &recorder{hook: hook, persisted: map[string]string{}}
}

func (r *recorder) Write(ctx context.Context, batch []Entry) error {
	r.mu.Lock()
	r.inFlight++
	r.maxInFlight = max(r.maxInFlight, r.inFlight)
	r.calls = append(r.calls, writeCall{at: time.Now(), entries: slices.Clone(batch), batch: batch})
	n := len(r.calls)
	r.mu.Unlock()

	var err error
	if r.hook != nil {
		err = r.hook(ctx, n)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	if err == nil {
		for _, e := range r.calls[n-1].entries {
			r.persisted[e.Key] = e.Value
		}
	}
	return err
}

func (r *recorder) snapshot() []writeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recorder) persistedCopy() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.persisted)
}

// verify checks the batch rules over every Write call. Call it only after
// Shutdown has returned, when the flusher can no longer touch a batch.
func (r *recorder) verify(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxInFlight > 1 {
		t.Errorf("%d Write calls were in flight at once, want at most 1", r.maxInFlight)
	}
	for i, call := range r.calls {
		if len(call.entries) == 0 {
			t.Errorf("Write call %d received an empty batch", i+1)
		}
		seen := map[string]bool{}
		for _, e := range call.entries {
			if seen[e.Key] {
				t.Errorf("Write call %d contains key %q more than once", i+1, e.Key)
			}
			seen[e.Key] = true
		}
		if !slices.Equal(call.batch, call.entries) {
			t.Errorf("batch of Write call %d changed after it was passed to Write: %v, was %v", i+1, call.batch, call.entries)
		}
	}
}

func mustNew(t *testing.T, store Store, maxDirty int, maxDelay time.Duration, maxPending int) *Cache {
	t.Helper()
	c, err := New(store, maxDirty, maxDelay, maxPending)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return c
}

func mustPut(t *testing.T, c *Cache, key, value string) {
	t.Helper()
	if err := c.Put(context.Background(), key, value); err != nil {
		t.Fatalf("Put(%q, %q) returned error: %v", key, value, err)
	}
}

func mustShutdown(t *testing.T, c *Cache) {
	t.Helper()
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

// goShutdown runs Shutdown in a new goroutine and returns a channel with its result.
func goShutdown(c *Cache, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- c.Shutdown(ctx) }()
	return ch
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		return err
	default:
		t.Fatalf("%s: Shutdown has not returned", what)
		return nil
	}
}

func assertBlocked(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		t.Fatalf("%s: Shutdown returned %v; want it still waiting", what, err)
	default:
	}
}

func assertGet(t *testing.T, c *Cache, key, want string) {
	t.Helper()
	if got, ok := c.Get(key); !ok || got != want {
		t.Fatalf("Get(%q) = %q, %v; want %q, true", key, got, ok, want)
	}
}

func assertMissing(t *testing.T, c *Cache, key string) {
	t.Helper()
	if got, ok := c.Get(key); ok {
		t.Fatalf("Get(%q) = %q, true; want the key to be absent", key, got)
	}
}

func assertCalls(t *testing.T, r *recorder, want int, why string) []writeCall {
	t.Helper()
	calls := r.snapshot()
	if len(calls) != want {
		t.Fatalf("Write called %d times, want %d: %s", len(calls), want, why)
	}
	return calls
}

func assertEntries(t *testing.T, got []Entry, want map[string]string, what string) {
	t.Helper()
	m := map[string]string{}
	for _, e := range got {
		m[e.Key] = e.Value
	}
	if len(got) != len(want) || !maps.Equal(m, want) {
		t.Fatalf("%s = %v, want exactly %v (one entry per key)", what, got, want)
	}
}

func assertPersisted(t *testing.T, r *recorder, want map[string]string) {
	t.Helper()
	if got := r.persistedCopy(); !maps.Equal(got, want) {
		t.Fatalf("store holds %v, want %v", got, want)
	}
}

// sleep advances virtual time by d and lets the cache settle.
func sleep(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func TestValidationAndPreCancelledPut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valid := newRecorder(nil)
		for _, tc := range []struct {
			name       string
			store      Store
			maxDirty   int
			maxDelay   time.Duration
			maxPending int
			wantErr    error
		}{
			{name: "zero maxDirty", store: valid, maxDirty: 0, maxDelay: time.Second, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "negative maxDirty", store: valid, maxDirty: -1, maxDelay: time.Second, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "zero maxDelay", store: valid, maxDirty: 1, maxDelay: 0, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "negative maxDelay", store: valid, maxDirty: 1, maxDelay: -time.Second, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "maxPending equal to maxDirty", store: valid, maxDirty: 2, maxDelay: time.Second, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "maxPending below maxDirty", store: valid, maxDirty: 3, maxDelay: time.Second, maxPending: 2, wantErr: ErrInvalidConfig},
			{name: "nil store", store: nil, maxDirty: 1, maxDelay: time.Second, maxPending: 2, wantErr: ErrNilStore},
		} {
			c, err := New(tc.store, tc.maxDirty, tc.maxDelay, tc.maxPending)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if c != nil {
				t.Errorf("%s: New returned a non-nil cache with an error", tc.name)
			}
		}

		r := newRecorder(nil)
		c := mustNew(t, r, 1, time.Hour, 2)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Put(ctx, "a", "1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Put = %v, want context.Canceled", err)
		}
		assertMissing(t, c, "a")
		synctest.Wait()
		mustShutdown(t, c)
		assertCalls(t, r, 0, "a rejected Put must not make a key dirty")
	})
}

func TestCoalescesPutsToOneKeyIntoOneEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(nil)
		c := mustNew(t, r, 10, 10*time.Millisecond, 20)

		mustPut(t, c, "a", "1")
		mustPut(t, c, "a", "2")
		mustPut(t, c, "b", "1")
		mustPut(t, c, "a", "3")
		assertGet(t, c, "a", "3")

		sleep(10 * time.Millisecond)
		calls := assertCalls(t, r, 1, "one flush at maxDelay")
		assertEntries(t, calls[0].entries, map[string]string{"a": "3", "b": "1"}, "batch")

		mustShutdown(t, c)
		assertCalls(t, r, 1, "Shutdown with no dirty keys must not call Write")
		assertPersisted(t, r, map[string]string{"a": "3", "b": "1"})
		r.verify(t)
	})
}

func TestFlushesAsSoonAsMaxDirtyKeysAreDirty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(nil)
		c := mustNew(t, r, 3, time.Hour, 10)
		start := time.Now()

		mustPut(t, c, "a", "1")
		mustPut(t, c, "b", "1")
		mustPut(t, c, "a", "2") // same key: still two dirty keys
		synctest.Wait()
		assertCalls(t, r, 0, "two dirty keys are below maxDirty")

		mustPut(t, c, "c", "1")
		synctest.Wait()
		calls := assertCalls(t, r, 1, "the third dirty key reaches maxDirty")
		assertEntries(t, calls[0].entries, map[string]string{"a": "2", "b": "1", "c": "1"}, "batch")
		if got := calls[0].at.Sub(start); got != 0 {
			t.Fatalf("count-triggered flush started %v after the Put, want immediately", got)
		}

		mustPut(t, c, "d", "1")
		synctest.Wait()
		assertCalls(t, r, 1, "one dirty key is below maxDirty")

		mustShutdown(t, c)
		calls = assertCalls(t, r, 2, "Shutdown flushes the remaining dirty key")
		assertEntries(t, calls[1].entries, map[string]string{"d": "1"}, "final batch")
		r.verify(t)
	})
}

func TestMaxDelayCountsFromOldestUnflushedPut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(nil)
		c := mustNew(t, r, 10, 20*time.Millisecond, 20)
		sleep(3 * time.Millisecond) // the deadline is not tied to New
		start := time.Now()

		mustPut(t, c, "a", "1")
		sleep(15 * time.Millisecond)
		mustPut(t, c, "b", "1") // must not extend the deadline
		sleep(4 * time.Millisecond)
		assertCalls(t, r, 0, "maxDelay has not elapsed since the oldest Put")

		sleep(1 * time.Millisecond)
		calls := assertCalls(t, r, 1, "maxDelay elapsed since the oldest Put")
		assertEntries(t, calls[0].entries, map[string]string{"a": "1", "b": "1"}, "batch")
		if got := calls[0].at.Sub(start); got != 20*time.Millisecond {
			t.Fatalf("flushed %v after the oldest Put, want 20ms", got)
		}

		// The next deadline counts from the next Put, not from a fixed tick.
		sleep(time.Hour + 5*time.Millisecond)
		assertCalls(t, r, 1, "an idle cache must not call Write")
		mustPut(t, c, "c", "1")
		next := time.Now()
		sleep(20 * time.Millisecond)
		calls = assertCalls(t, r, 2, "maxDelay elapsed since the next Put")
		assertEntries(t, calls[1].entries, map[string]string{"c": "1"}, "second batch")
		if got := calls[1].at.Sub(next); got != 20*time.Millisecond {
			t.Fatalf("second batch flushed %v after its Put, want 20ms", got)
		}

		mustShutdown(t, c)
		r.verify(t)
	})
}

// A Put that lands during a slow flush keeps its own deadline: when that
// deadline passes before the flush returns, the next flush follows at once.
func TestDeadlinePassedDuringFlushFlushesRightAfterIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(func(_ context.Context, n int) error {
			if n == 1 {
				time.Sleep(30 * time.Millisecond) // a slow store
			}
			return nil
		})
		c := mustNew(t, r, 10, 10*time.Millisecond, 20)
		start := time.Now()

		mustPut(t, c, "a", "1")
		sleep(10 * time.Millisecond) // the first flush is now in flight until start+40ms
		assertCalls(t, r, 1, "maxDelay elapsed since the Put of a")
		mustPut(t, c, "b", "1") // deadline start+20ms, while the flush is in flight

		sleep(29 * time.Millisecond)
		assertCalls(t, r, 1, "at most one Write may be in flight")

		sleep(1 * time.Millisecond)
		calls := assertCalls(t, r, 2, "the deadline of b passed during the first flush")
		assertEntries(t, calls[1].entries, map[string]string{"b": "1"}, "second batch")
		if got := calls[1].at.Sub(start); got != 40*time.Millisecond {
			t.Fatalf("second flush started at +%v, want +40ms: right after the first flush returned", got)
		}

		mustShutdown(t, c)
		assertPersisted(t, r, map[string]string{"a": "1", "b": "1"})
		r.verify(t)
	})
}

func TestPutAndGetDoNotWaitForStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newRecorder(func(ctx context.Context, n int) error {
			if n == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		c := mustNew(t, r, 1, time.Hour, 10)

		assertMissing(t, c, "a")
		mustPut(t, c, "a", "1")
		assertGet(t, c, "a", "1")
		synctest.Wait()
		assertCalls(t, r, 1, "one dirty key reaches maxDirty")

		// The flush of a=1 is blocked in the store. A Put or Get that waits for
		// it deadlocks here or hangs on a mutex held across Write.
		mustPut(t, c, "a", "2")
		assertGet(t, c, "a", "2")
		mustPut(t, c, "b", "1")
		assertGet(t, c, "b", "1")
		synctest.Wait()
		assertCalls(t, r, 1, "at most one Write may be in flight")

		close(release)
		mustShutdown(t, c)
		assertPersisted(t, r, map[string]string{"a": "2", "b": "1"})
		assertGet(t, c, "a", "2") // Get keeps working after Shutdown
		r.verify(t)
	})
}

func TestFailedWriteKeepsKeysDirtyAndRetriesEveryMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(func(_ context.Context, n int) error {
			if n <= 2 {
				return errStore
			}
			return nil
		})
		c := mustNew(t, r, 2, 10*time.Millisecond, 10)
		start := time.Now()

		mustPut(t, c, "a", "1")
		sleep(5 * time.Millisecond)
		mustPut(t, c, "b", "1") // count trigger: first Write fails at +5ms
		synctest.Wait()
		calls := assertCalls(t, r, 1, "two dirty keys reach maxDirty")
		if got := calls[0].at.Sub(start); got != 5*time.Millisecond {
			t.Fatalf("first flush at +%v, want +5ms", got)
		}

		// Four dirty keys exceed maxDirty, but a failed Write is retried only
		// maxDelay after the failure: no tight loop, and not counted from the
		// oldest Put (which would be +10ms).
		mustPut(t, c, "c", "1")
		mustPut(t, c, "d", "1")
		sleep(9 * time.Millisecond)
		assertCalls(t, r, 1, "the retry must wait maxDelay after the failure")

		sleep(1 * time.Millisecond)
		calls = assertCalls(t, r, 2, "first retry at failure+maxDelay")
		if got := calls[1].at.Sub(start); got != 15*time.Millisecond {
			t.Fatalf("first retry at +%v, want +15ms", got)
		}
		assertEntries(t, calls[1].entries, map[string]string{"a": "1", "b": "1", "c": "1", "d": "1"}, "retry batch")

		sleep(9 * time.Millisecond)
		assertCalls(t, r, 2, "the second retry must wait maxDelay after the second failure")
		sleep(1 * time.Millisecond)
		calls = assertCalls(t, r, 3, "second retry at failure+maxDelay")
		if got := calls[2].at.Sub(start); got != 25*time.Millisecond {
			t.Fatalf("second retry at +%v, want +25ms", got)
		}
		assertPersisted(t, r, map[string]string{"a": "1", "b": "1", "c": "1", "d": "1"})

		mustShutdown(t, c)
		assertCalls(t, r, 3, "nothing is dirty after the successful retry")
		r.verify(t)
	})
}

// The central race: a key written while its older value is being persisted
// must stay dirty even though that Write succeeds.
func TestPutDuringSuccessfulFlushStaysDirty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newRecorder(func(ctx context.Context, n int) error {
			if n == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		c := mustNew(t, r, 10, 10*time.Millisecond, 20)

		mustPut(t, c, "a", "1")
		sleep(10 * time.Millisecond)
		assertCalls(t, r, 1, "maxDelay elapsed since the Put")

		mustPut(t, c, "a", "2") // lands while a=1 is being written
		close(release)
		synctest.Wait()
		assertPersisted(t, r, map[string]string{"a": "1"})

		sleep(10 * time.Millisecond)
		calls := r.snapshot()
		if len(calls) < 2 {
			t.Fatalf("Write called %d times, want a second flush: a=2 was put during a successful flush of a=1 and must stay dirty", len(calls))
		}
		assertEntries(t, calls[1].entries, map[string]string{"a": "2"}, "second batch")
		assertPersisted(t, r, map[string]string{"a": "2"})

		mustShutdown(t, c)
		assertCalls(t, r, 2, "nothing is dirty after the second flush")
		r.verify(t)
	})
}

// The other half of the race: a failed Write must not bring back the value it
// failed to write over a newer Put.
func TestPutDuringFailedFlushReplacesFailedValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newRecorder(func(ctx context.Context, n int) error {
			if n == 1 {
				select {
				case <-release:
					return errStore
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		c := mustNew(t, r, 1, 10*time.Millisecond, 10)
		start := time.Now()

		mustPut(t, c, "a", "1")
		synctest.Wait()
		assertCalls(t, r, 1, "one dirty key reaches maxDirty")

		mustPut(t, c, "a", "2") // lands while a=1 is being written
		mustPut(t, c, "b", "1")
		synctest.Wait()
		assertCalls(t, r, 1, "at most one Write may be in flight")

		close(release) // the flush of a=1 fails at +0
		sleep(10 * time.Millisecond)
		calls := assertCalls(t, r, 2, "retry at failure+maxDelay")
		if got := calls[1].at.Sub(start); got != 10*time.Millisecond {
			t.Fatalf("retry at +%v, want +10ms", got)
		}
		assertEntries(t, calls[1].entries, map[string]string{"a": "2", "b": "1"}, "retry batch")

		mustShutdown(t, c)
		assertPersisted(t, r, map[string]string{"a": "2", "b": "1"})
		assertGet(t, c, "a", "2")
		r.verify(t)
	})
}

func TestFullRejectsNewKeysWithoutBlocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newRecorder(func(ctx context.Context, n int) error {
			if n == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		c := mustNew(t, r, 2, time.Hour, 3)

		mustPut(t, c, "a", "1")
		mustPut(t, c, "b", "1")
		synctest.Wait() // a and b are in flight and still dirty
		assertCalls(t, r, 1, "two dirty keys reach maxDirty")
		mustPut(t, c, "c", "1")

		// A blocking Put would deadlock here, which synctest reports as a failure.
		if err := c.Put(context.Background(), "d", "1"); !errors.Is(err, ErrFull) {
			t.Fatalf("Put of a new key with maxPending keys dirty (two of them in flight) = %v, want ErrFull", err)
		}
		assertMissing(t, c, "d")

		// Puts to keys that are already dirty add no dirty key.
		mustPut(t, c, "a", "2")
		mustPut(t, c, "c", "2")
		assertGet(t, c, "a", "2")

		close(release)
		synctest.Wait()
		mustPut(t, c, "d", "1") // the flush made room

		mustShutdown(t, c)
		assertPersisted(t, r, map[string]string{"a": "2", "b": "1", "c": "2", "d": "1"})
		r.verify(t)
	})
}

func TestShutdownFlushesAllDirtyKeysAndStopsAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(nil)
		c := mustNew(t, r, 10, time.Hour, 20)
		mustPut(t, c, "a", "1")
		mustPut(t, c, "b", "1")
		mustPut(t, c, "c", "1")
		start := time.Now()

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if err := c.Shutdown(context.Background()); err != nil {
					t.Errorf("concurrent Shutdown returned error: %v", err)
				}
			})
		}
		wg.Wait()

		if got := time.Since(start); got != 0 {
			t.Fatalf("Shutdown took %v, want it to flush at once instead of waiting for maxDelay", got)
		}
		calls := assertCalls(t, r, 1, "one final flush")
		assertEntries(t, calls[0].entries, map[string]string{"a": "1", "b": "1", "c": "1"}, "final batch")

		if err := c.Put(context.Background(), "a", "2"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Put after Shutdown = %v, want ErrClosed", err)
		}
		assertGet(t, c, "a", "1")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown after completion = %v, want nil", err)
		}
		r.verify(t)
	})
}

func TestShutdownRetriesFailedWritesEveryMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(func(_ context.Context, n int) error {
			if n <= 2 {
				return errStore
			}
			return nil
		})
		c := mustNew(t, r, 10, 10*time.Millisecond, 20)
		mustPut(t, c, "a", "1")
		start := time.Now()

		done := goShutdown(c, context.Background())
		assertBlocked(t, done, "first flush failed")
		calls := assertCalls(t, r, 1, "Shutdown flushes at once")
		if err := c.Put(context.Background(), "b", "1"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Put during Shutdown = %v, want ErrClosed", err)
		}

		sleep(10 * time.Millisecond)
		assertBlocked(t, done, "second flush failed")
		calls = assertCalls(t, r, 2, "retry at failure+maxDelay")

		sleep(10 * time.Millisecond)
		if err := receive(t, done, "third flush succeeded"); err != nil {
			t.Fatalf("Shutdown returned %v, want nil after a successful retry", err)
		}
		calls = assertCalls(t, r, 3, "two failures and one success")
		for i, want := range []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond} {
			if got := calls[i].at.Sub(start); got != want {
				t.Fatalf("Write call %d at +%v, want +%v", i+1, got, want)
			}
		}
		assertPersisted(t, r, map[string]string{"a": "1"})
		r.verify(t)
	})
}

func TestShutdownDeadlineCancelsWriteAndWaitsForFlusher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var mu sync.Mutex
		var cancelledAt time.Time
		r := newRecorder(func(ctx context.Context, _ int) error {
			<-ctx.Done()
			mu.Lock()
			cancelledAt = time.Now()
			mu.Unlock()
			<-release // the store takes a while to clean up
			return ctx.Err()
		})
		c := mustNew(t, r, 10, time.Hour, 20)
		mustPut(t, c, "a", "1")
		start := time.Now()

		patient := goShutdown(c, context.Background())
		assertBlocked(t, patient, "patient caller")
		assertCalls(t, r, 1, "Shutdown flushes at once")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		forced := goShutdown(c, ctx)

		sleep(10 * time.Millisecond)
		mu.Lock()
		at := cancelledAt
		mu.Unlock()
		if at.IsZero() {
			t.Fatal("the in-flight Write's context was not cancelled when the shutdown deadline expired")
		}
		if got := at.Sub(start); got != 10*time.Millisecond {
			t.Fatalf("Write cancelled at +%v, want +10ms", got)
		}
		assertBlocked(t, forced, "caller whose deadline expired, while Write still runs")
		assertBlocked(t, patient, "patient caller, while Write still runs")

		close(release)
		if err := receive(t, forced, "caller whose deadline expired"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown whose deadline expired = %v, want context.DeadlineExceeded", err)
		}
		if err := receive(t, patient, "patient caller"); !errors.Is(err, ErrUnflushed) {
			t.Fatalf("patient Shutdown = %v, want ErrUnflushed: key a was never persisted", err)
		}
		assertCalls(t, r, 1, "no Write may start after the shutdown deadline")
		assertPersisted(t, r, map[string]string{})

		if err := c.Shutdown(context.Background()); !errors.Is(err, ErrUnflushed) {
			t.Fatalf("Shutdown after a forced stop = %v, want ErrUnflushed", err)
		}
		if err := c.Put(context.Background(), "b", "1"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Put after Shutdown = %v, want ErrClosed", err)
		}
		assertGet(t, c, "a", "1")
		r.verify(t)
	})
}

func TestStoreReceivesCacheOwnedWorkContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		type seenCtx struct {
			put, shutdown any
			err           error
		}
		var mu sync.Mutex
		var seen []seenCtx
		r := newRecorder(func(ctx context.Context, _ int) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, seenCtx{
				put:      ctx.Value(ctxKey("put")),
				shutdown: ctx.Value(ctxKey("shutdown")),
				err:      ctx.Err(),
			})
			return nil
		})
		c := mustNew(t, r, 10, 10*time.Millisecond, 20)

		putCtx, cancelPut := context.WithCancel(context.WithValue(context.Background(), ctxKey("put"), "caller"))
		if err := c.Put(putCtx, "a", "1"); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		cancelPut() // the caller stops caring after Put returns
		sleep(10 * time.Millisecond)

		if err := c.Put(context.WithValue(context.Background(), ctxKey("put"), "caller"), "b", "1"); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		shutdownCtx := context.WithValue(context.Background(), ctxKey("shutdown"), "caller")
		if err := c.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(seen) != 2 {
			t.Fatalf("Write called %d times, want 2 (one timed flush, one final flush)", len(seen))
		}
		for i, s := range seen {
			if s.put != nil {
				t.Errorf("Write call %d received a context derived from a Put caller's context", i+1)
			}
			if s.shutdown != nil {
				t.Errorf("Write call %d received a context derived from the Shutdown caller's context", i+1)
			}
			if s.err != nil {
				t.Errorf("Write call %d received a done context (%v), want the live work context", i+1, s.err)
			}
		}
	})
}

func TestConcurrentPutsAndShutdownPersistEveryAcceptedValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(func(context.Context, int) error {
			time.Sleep(time.Millisecond)
			return nil
		})
		c := mustNew(t, r, 4, 2*time.Millisecond, 8)

		var mu sync.Mutex
		accepted := map[string]int{} // key -> last value whose Put returned nil
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Go(func() {
				last := map[string]int{}
				for i := range 30 {
					key := fmt.Sprintf("%d-%d", g, i%3)
					switch err := c.Put(context.Background(), key, strconv.Itoa(i)); {
					case err == nil:
						last[key] = i
					case errors.Is(err, ErrFull), errors.Is(err, ErrClosed):
					default:
						t.Errorf("Put(%q) = %v, want nil, ErrFull, or ErrClosed", key, err)
					}
					if i%5 == 4 {
						time.Sleep(time.Millisecond)
					}
				}
				mu.Lock()
				maps.Copy(accepted, last)
				mu.Unlock()
			})
		}
		wg.Go(func() {
			time.Sleep(3 * time.Millisecond)
			if err := c.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown returned error: %v", err)
			}
		})
		wg.Wait()
		mustShutdown(t, c)

		persisted := r.persistedCopy()
		for key, i := range accepted {
			want := strconv.Itoa(i)
			if persisted[key] != want {
				t.Errorf("store holds %q for key %q, want the last accepted value %q", persisted[key], key, want)
			}
			if got, _ := c.Get(key); got != want {
				t.Errorf("Get(%q) = %q, want the last accepted value %q", key, got, want)
			}
		}
		for key := range persisted {
			if _, ok := accepted[key]; !ok {
				t.Errorf("store holds key %q, but no Put for it was accepted", key)
			}
		}

		// Values of one key reach the store in Put order.
		lastWritten := map[string]int{}
		for i, call := range r.snapshot() {
			for _, e := range call.entries {
				v, _ := strconv.Atoi(e.Value)
				if prev, ok := lastWritten[e.Key]; ok && v < prev {
					t.Errorf("Write call %d wrote %s=%d after %d had been written", i+1, e.Key, v, prev)
				}
				lastWritten[e.Key] = v
			}
		}
		r.verify(t)
	})
}
