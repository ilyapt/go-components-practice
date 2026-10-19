package offsetcommitter

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

var errCommit = errors.New("commit failed")

type commitFunc func(context.Context, int64) error

func (f commitFunc) Commit(ctx context.Context, offset int64) error { return f(ctx, offset) }

// attempt is one recorded Commit call.
type attempt struct {
	offset int64
	at     time.Duration // since the recorder was created
	failed bool
}

// recorder is a Committer that remembers every Commit call. The first
// failFirst calls fail with errCommit.
type recorder struct {
	start     time.Time
	failFirst int

	mu       sync.Mutex
	attempts []attempt
}

func newRecorder(failFirst int) *recorder {
	return &recorder{start: time.Now(), failFirst: failFirst}
}

func (r *recorder) Commit(_ context.Context, offset int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	failed := len(r.attempts) < r.failFirst
	r.attempts = append(r.attempts, attempt{offset: offset, at: time.Since(r.start), failed: failed})
	if failed {
		return errCommit
	}
	return nil
}

func (r *recorder) snapshot() []attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]attempt(nil), r.attempts...)
}

func (r *recorder) offsets() []int64 {
	var out []int64
	for _, a := range r.snapshot() {
		out = append(out, a.offset)
	}
	return out
}

func assertAttempts(t *testing.T, r *recorder, what string, want ...attempt) {
	t.Helper()
	if got := r.snapshot(); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
		t.Fatalf("%s: Commit calls = %+v, want %+v", what, got, want)
	}
}

func assertOffsets(t *testing.T, r *recorder, what string, want ...int64) {
	t.Helper()
	if got := r.offsets(); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
		t.Fatalf("%s: Commit offsets = %v, want %v", what, got, want)
	}
}

func mustNew(t *testing.T, committer Committer, interval time.Duration, maxUncommitted, maxInFlight int) *OffsetCommitter {
	t.Helper()
	c, err := New(committer, interval, maxUncommitted, maxInFlight)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return c
}

func mustTrack(t *testing.T, c *OffsetCommitter, offsets ...int64) {
	t.Helper()
	for _, off := range offsets {
		if err := c.Track(off); err != nil {
			t.Fatalf("Track(%d) returned error: %v", off, err)
		}
	}
}

func mustDone(t *testing.T, c *OffsetCommitter, offsets ...int64) {
	t.Helper()
	for _, off := range offsets {
		if err := c.Done(off); err != nil {
			t.Fatalf("Done(%d) returned error: %v", off, err)
		}
	}
}

func mustClose(t *testing.T, c *OffsetCommitter) {
	t.Helper()
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func assertCommitted(t *testing.T, c *OffsetCommitter, what string, want int64) {
	t.Helper()
	if got := c.Committed(); got != want {
		t.Fatalf("%s: Committed() = %d, want %d", what, got, want)
	}
}

// goCall runs fn in a new goroutine and returns a channel with its result.
func goCall(fn func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return ch
}

func goClose(c *OffsetCommitter, ctx context.Context) <-chan error {
	return goCall(func() error { return c.Close(ctx) })
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
		t.Fatalf("%s returned %v, want it still waiting", what, err)
	default:
	}
}

// maxTracker counts concurrent Commit calls.
type maxTracker struct{ cur, max atomic.Int32 }

func (m *maxTracker) enter() {
	n := m.cur.Add(1)
	for {
		old := m.max.Load()
		if n <= old || m.max.CompareAndSwap(old, n) {
			return
		}
	}
}

func (m *maxTracker) exit() { m.cur.Add(-1) }

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valid := newRecorder(0)
		for _, tc := range []struct {
			name           string
			committer      Committer
			interval       time.Duration
			maxUncommitted int
			maxInFlight    int
			wantErr        error
		}{
			{"nil committer", nil, time.Second, 1, 1, ErrNilCommitter},
			{"zero interval", valid, 0, 1, 1, ErrInvalidConfig},
			{"negative interval", valid, -time.Second, 1, 1, ErrInvalidConfig},
			{"zero maxUncommitted", valid, time.Second, 0, 1, ErrInvalidConfig},
			{"zero maxInFlight", valid, time.Second, 1, 0, ErrInvalidConfig},
			{"maxUncommitted above maxInFlight", valid, time.Second, 5, 4, ErrInvalidConfig},
		} {
			c, err := New(tc.committer, tc.interval, tc.maxUncommitted, tc.maxInFlight)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if c != nil {
				t.Errorf("%s: New returned a non-nil committer with an error", tc.name)
			}
		}
	})
}

// A gap blocks the commit; filling it commits the whole prefix in one call.
// Numbers that were never tracked are not gaps.
func TestGapBlocksCommitUntilFilled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, 10*time.Millisecond, 100, 100)

		mustTrack(t, c, 1, 2, 3, 5, 8) // 4, 6, 7 are never tracked
		mustDone(t, c, 1, 2, 5)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "1, 2, 5 done, 3 pending", 3)
		assertCommitted(t, c, "after the first tick", 3)

		mustDone(t, c, 3)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "gap at 3 filled: 3 and 5 are covered by one Commit, 4 was never tracked", 3, 6)
		assertCommitted(t, c, "after the gap was filled", 6)

		mustDone(t, c, 8)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "8 done, 6 and 7 were never tracked", 3, 6, 9)

		mustClose(t, c)
		assertOffsets(t, r, "Close with nothing new to commit", 3, 6, 9)
	})
}

func TestTimeTriggerCommitsOnIntervalGrid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, 10*time.Millisecond, 100, 100)

		mustTrack(t, c, 0, 1, 2, 3)
		mustDone(t, c, 0)
		time.Sleep(10*time.Millisecond - time.Nanosecond)
		synctest.Wait()
		assertAttempts(t, r, "before the first tick, below maxUncommitted")

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertAttempts(t, r, "at the first tick", attempt{offset: 1, at: 10 * time.Millisecond})

		time.Sleep(5 * time.Millisecond)
		mustDone(t, c, 1, 2)
		synctest.Wait()
		assertAttempts(t, r, "between ticks, below maxUncommitted", attempt{offset: 1, at: 10 * time.Millisecond})

		time.Sleep(5 * time.Millisecond)
		synctest.Wait()
		assertAttempts(t, r, "at the second tick",
			attempt{offset: 1, at: 10 * time.Millisecond},
			attempt{offset: 3, at: 20 * time.Millisecond})

		mustClose(t, c)
	})
}

func TestCountTriggerCommitsWithoutWaitingForTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, time.Hour, 3, 10)

		mustTrack(t, c, 0, 1, 2, 3, 4, 5, 6, 7)
		mustDone(t, c, 0, 1)
		synctest.Wait()
		assertAttempts(t, r, "two Done calls with maxUncommitted 3")

		mustDone(t, c, 2)
		synctest.Wait()
		assertAttempts(t, r, "third Done call", attempt{offset: 3, at: 0})

		mustDone(t, c, 4, 5, 6) // enough Done calls, but 3 blocks the prefix
		synctest.Wait()
		assertAttempts(t, r, "count reached behind a gap", attempt{offset: 3, at: 0})

		mustDone(t, c, 3) // moves the commit value: the pending count fires now
		synctest.Wait()
		assertAttempts(t, r, "gap filled after the count was reached",
			attempt{offset: 3, at: 0}, attempt{offset: 7, at: 0})
		assertCommitted(t, c, "after the count trigger", 7)

		mustClose(t, c)
	})
}

// Ticks with nothing new to commit, and Close with nothing new, do not call
// Commit; values only grow.
func TestNoCommitWithoutProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, 10*time.Millisecond, 100, 100)

		mustTrack(t, c, 0, 1, 2)
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "ticks with nothing done")

		mustDone(t, c, 0)
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "several ticks after one Done", 1)

		mustDone(t, c, 2) // behind a gap at 1: the commit value does not move
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "ticks while a gap blocks the prefix", 1)

		mustDone(t, c, 1)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "gap filled", 1, 3)

		mustClose(t, c)
		assertOffsets(t, r, "Close after everything was committed", 1, 3)
	})
}

func TestFailedCommitIsRetriedWithCurrentPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(1)
		c := mustNew(t, r, 10*time.Millisecond, 4, 4)

		mustTrack(t, c, 0, 1, 2, 3)
		mustDone(t, c, 0)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertAttempts(t, r, "first tick", attempt{offset: 1, at: 10 * time.Millisecond, failed: true})
		assertCommitted(t, c, "after a failed Commit", 0)
		if err := c.Track(4); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("Track(4) after a failed Commit = %v, want ErrWindowFull: a failed Commit frees nothing", err)
		}

		time.Sleep(5 * time.Millisecond)
		mustDone(t, c, 1, 2)
		synctest.Wait()
		assertAttempts(t, r, "no retry between triggers",
			attempt{offset: 1, at: 10 * time.Millisecond, failed: true})

		time.Sleep(5 * time.Millisecond)
		synctest.Wait()
		assertAttempts(t, r, "retry at the next tick carries the current value, not the failed one",
			attempt{offset: 1, at: 10 * time.Millisecond, failed: true},
			attempt{offset: 3, at: 20 * time.Millisecond})
		assertCommitted(t, c, "after the retry", 3)
		mustTrack(t, c, 4) // the successful commit freed offsets 0, 1, 2

		mustClose(t, c)
	})
}

func TestAtMostOneCommitInFlightAndCallersNeverWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		var inFlight maxTracker
		var mu sync.Mutex
		var values []int64
		c := mustNew(t, commitFunc(func(_ context.Context, offset int64) error {
			inFlight.enter()
			defer inFlight.exit()
			mu.Lock()
			values = append(values, offset)
			mu.Unlock()
			<-gate
			return nil
		}), 10*time.Millisecond, 2, 100)
		snapshot := func() []int64 {
			mu.Lock()
			defer mu.Unlock()
			return append([]int64(nil), values...)
		}

		mustTrack(t, c, 0, 1, 2, 3, 4, 5)
		mustDone(t, c, 0, 1) // count trigger: Commit(2) starts and blocks
		synctest.Wait()
		if got := snapshot(); !reflect.DeepEqual(got, []int64{2}) {
			t.Fatalf("Commit calls = %v, want [2]", got)
		}

		// Count triggers and ticks fire while Commit(2) is in flight; callers
		// keep working without waiting for it.
		for _, off := range []int64{2, 3} {
			ch := goCall(func() error { return c.Done(off) })
			if err := receive(t, ch, fmt.Sprintf("Done(%d) during an in-flight Commit", off)); err != nil {
				t.Fatalf("Done(%d) returned error: %v", off, err)
			}
		}
		if err := receive(t, goCall(func() error { return c.Track(6) }), "Track during an in-flight Commit"); err != nil {
			t.Fatalf("Track(6) returned error: %v", err)
		}
		assertCommitted(t, c, "while the first Commit is in flight", 0)
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		if got := snapshot(); !reflect.DeepEqual(got, []int64{2}) {
			t.Fatalf("Commit calls while one is in flight = %v, want only [2]", got)
		}

		close(gate)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		if got := snapshot(); !reflect.DeepEqual(got, []int64{2, 4}) {
			t.Fatalf("Commit calls after the first returned = %v, want [2 4]", got)
		}
		assertCommitted(t, c, "after both commits", 4)
		if got := inFlight.max.Load(); got != 1 {
			t.Fatalf("max concurrent Commit calls = %d, want 1", got)
		}

		mustClose(t, c)
	})
}

func TestWindowBoundsUncommittedOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, 10*time.Millisecond, 3, 3)

		mustTrack(t, c, 0, 1, 2)
		if err := c.Track(3); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("Track beyond maxInFlight = %v, want ErrWindowFull", err)
		}
		mustDone(t, c, 0)
		if err := c.Track(3); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("Track with a done but uncommitted offset = %v, want ErrWindowFull", err)
		}

		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assertOffsets(t, r, "first tick", 1)
		mustTrack(t, c, 3) // the rejected offset can be tracked once Commit(1) freed a slot
		if err := c.Track(4); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("Track(4) with 1, 2, 3 uncommitted = %v, want ErrWindowFull", err)
		}

		mustClose(t, c)
		assertOffsets(t, r, "Close with nothing new done", 1)
	})
}

func TestTrackRejectsNonIncreasingOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, time.Hour, 2, 2)

		if err := c.Track(-1); !errors.Is(err, ErrOutOfOrder) {
			t.Fatalf("Track(-1) = %v, want ErrOutOfOrder", err)
		}
		mustTrack(t, c, 5)
		for _, off := range []int64{5, 3} {
			if err := c.Track(off); !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("Track(%d) after Track(5) = %v, want ErrOutOfOrder", off, err)
			}
		}
		// Rejected calls changed nothing: 6 is accepted and fits in the window.
		mustTrack(t, c, 6)

		if err := c.Track(6); !errors.Is(err, ErrOutOfOrder) {
			t.Fatalf("Track(6) with a full window = %v, want ErrOutOfOrder (it takes precedence)", err)
		}
		if err := c.Track(7); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("Track(7) with a full window = %v, want ErrWindowFull", err)
		}

		mustClose(t, c)
		assertOffsets(t, r, "Close with nothing done")
	})
}

func TestDoneRejectsUnknownAndRepeatedOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, time.Hour, 2, 10)

		mustTrack(t, c, 1, 2, 4)
		for _, off := range []int64{0, 3, 5} {
			if err := c.Done(off); !errors.Is(err, ErrUnknownOffset) {
				t.Fatalf("Done(%d) for an offset never tracked = %v, want ErrUnknownOffset", off, err)
			}
		}
		mustDone(t, c, 1)
		if err := c.Done(1); !errors.Is(err, ErrUnknownOffset) {
			t.Fatalf("repeated Done(1) = %v, want ErrUnknownOffset", err)
		}
		synctest.Wait()
		assertOffsets(t, r, "one successful Done with maxUncommitted 2: rejected Done calls must not count")

		mustDone(t, c, 2) // second successful Done: count trigger
		synctest.Wait()
		assertOffsets(t, r, "second successful Done", 3)
		for _, off := range []int64{1, 2} {
			if err := c.Done(off); !errors.Is(err, ErrUnknownOffset) {
				t.Fatalf("Done(%d) for a committed offset = %v, want ErrUnknownOffset", off, err)
			}
		}

		mustDone(t, c, 4)
		mustClose(t, c)
		assertOffsets(t, r, "final commit", 3, 5)
	})
}

func TestCloseCommitsFinalPrefixWithWorkContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		gate := make(chan struct{})
		var mu sync.Mutex
		var values []int64
		var closerValue any
		var commitCtxErr error
		c := mustNew(t, commitFunc(func(ctx context.Context, offset int64) error {
			mu.Lock()
			values = append(values, offset)
			closerValue = ctx.Value(ctxKey("closer"))
			commitCtxErr = ctx.Err()
			mu.Unlock()
			<-gate
			return nil
		}), time.Hour, 100, 100)

		mustTrack(t, c, 0, 1, 2, 3, 4)
		mustDone(t, c, 0, 1, 3)

		closeCtx := context.WithValue(context.Background(), ctxKey("closer"), "close")
		done := goClose(c, closeCtx)
		assertBlocked(t, done, "Close while the final Commit is in flight")
		mu.Lock()
		if !reflect.DeepEqual(values, []int64{2}) {
			t.Fatalf("Commit calls after Close = %v, want a prompt final Commit(2)", values)
		}
		mu.Unlock()

		// Admission stopped when Close began: the final value is fixed.
		if err := c.Done(2); !errors.Is(err, ErrClosed) {
			t.Fatalf("Done(2) during Close = %v, want ErrClosed", err)
		}
		if err := c.Track(5); !errors.Is(err, ErrClosed) {
			t.Fatalf("Track(5) during Close = %v, want ErrClosed", err)
		}

		close(gate)
		if err := receive(t, done, "Close after the final Commit succeeded"); err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !reflect.DeepEqual(values, []int64{2}) {
			t.Fatalf("Commit calls = %v, want only [2]", values)
		}
		if closerValue != nil {
			t.Fatal("Commit received a context derived from the Close context, want the work context")
		}
		if commitCtxErr != nil {
			t.Fatalf("Commit received a done context (%v) during a graceful Close", commitCtxErr)
		}
		assertCommitted(t, c, "after Close", 2)
		if err := c.Done(99); !errors.Is(err, ErrClosed) {
			t.Fatalf("Done(99) after Close = %v, want ErrClosed (it takes precedence)", err)
		}
	})
}

func TestCloseRetriesFinalCommitUntilDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(1 << 30) // every Commit fails
		c := mustNew(t, r, 10*time.Millisecond, 100, 100)

		mustTrack(t, c, 0)
		mustDone(t, c, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if err := c.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close with a failing Committer = %v, want context.DeadlineExceeded", err)
		}

		attempts := r.snapshot()
		if n := len(attempts); n < 2 || n > 4 {
			t.Fatalf("Close made %d Commit calls in 25ms with interval 10ms, want a prompt attempt plus retries every interval", n)
		}
		for _, a := range attempts {
			if a.offset != 1 {
				t.Fatalf("final Commit offset = %d, want 1", a.offset)
			}
		}
		assertCommitted(t, c, "after a failed Close", 0)

		// The committer stopped for good: no more Commit calls, the outcome is recorded.
		time.Sleep(time.Second)
		if err := c.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close after a failed Close = %v, want the recorded context.DeadlineExceeded", err)
		}
		if got := len(r.snapshot()); got != len(attempts) {
			t.Fatalf("Commit calls after Close returned: %d, want %d", got, len(attempts))
		}
	})
}

func TestCloseDeadlineCancelsInFlightCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var mu sync.Mutex
		var cancelledAt time.Time
		c := mustNew(t, commitFunc(func(ctx context.Context, _ int64) error {
			<-ctx.Done()
			mu.Lock()
			cancelledAt = time.Now()
			mu.Unlock()
			<-release // the Commit takes a while to return
			return ctx.Err()
		}), time.Hour, 100, 100)

		mustTrack(t, c, 0)
		mustDone(t, c, 0)
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		done := goClose(c, ctx)

		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		mu.Lock()
		if cancelledAt.IsZero() {
			mu.Unlock()
			t.Fatal("the work context was not cancelled when the Close deadline expired")
		}
		if got := cancelledAt.Sub(start); got != 10*time.Millisecond {
			mu.Unlock()
			t.Fatalf("work context cancelled %v after Close started, want 10ms", got)
		}
		mu.Unlock()
		assertBlocked(t, done, "Close while Commit has not returned")

		close(release)
		if err := receive(t, done, "Close after Commit returned"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close = %v, want context.DeadlineExceeded", err)
		}
		assertCommitted(t, c, "after a cancelled final Commit", 0)
	})
}

func TestConcurrentCloseIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRecorder(0)
		c := mustNew(t, r, time.Hour, 100, 100)
		mustTrack(t, c, 0, 1, 2)
		mustDone(t, c, 0, 1)

		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				if err := c.Close(context.Background()); err != nil {
					t.Errorf("Close returned error: %v", err)
				}
			})
		}
		wg.Wait()
		assertOffsets(t, r, "16 concurrent Close calls", 2)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Close(ctx); err != nil {
			t.Fatalf("Close after completion with a cancelled context = %v, want nil", err)
		}
		assertOffsets(t, r, "Close after completion", 2)
		if err := c.Track(3); !errors.Is(err, ErrClosed) {
			t.Fatalf("Track after Close = %v, want ErrClosed", err)
		}
		if err := c.Done(2); !errors.Is(err, ErrClosed) {
			t.Fatalf("Done after Close = %v, want ErrClosed", err)
		}
	})
}

func TestCloseWithDoneContextForcesStopForEveryCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, commitFunc(func(ctx context.Context, _ int64) error {
			<-ctx.Done()
			return ctx.Err()
		}), time.Hour, 100, 100)
		mustTrack(t, c, 0)
		mustDone(t, c, 0)

		first := goClose(c, context.Background())
		assertBlocked(t, first, "Close while the final Commit hangs")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("second Close with a cancelled context = %v, want context.Canceled", err)
		}
		if err := receive(t, first, "first Close after the forced stop"); !errors.Is(err, context.Canceled) {
			t.Fatalf("first Close = %v, want the recorded context.Canceled", err)
		}
		assertCommitted(t, c, "after a forced stop", 0)
	})
}

// A fetch loop tracks offsets and backs off on ErrWindowFull while many
// workers call Done concurrently and commits run in the background.
func TestConcurrentTrackAndDoneAreRaceFree(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 1000
		var doneCalled [n]atomic.Bool
		var inFlight maxTracker
		var mu sync.Mutex
		var values []int64
		c := mustNew(t, commitFunc(func(_ context.Context, offset int64) error {
			inFlight.enter()
			defer inFlight.exit()
			for o := range offset {
				if !doneCalled[o].Load() {
					t.Errorf("Commit(%d) covers offset %d before Done(%d) was called", offset, o, o)
					break
				}
			}
			mu.Lock()
			values = append(values, offset)
			mu.Unlock()
			time.Sleep(time.Millisecond) // triggers pile up while a commit runs
			return nil
		}), 10*time.Millisecond, 16, 64)

		var wg sync.WaitGroup
		for off := int64(0); off < n; {
			switch err := c.Track(off); {
			case err == nil:
				o := off
				wg.Go(func() {
					doneCalled[o].Store(true)
					if err := c.Done(o); err != nil {
						t.Errorf("Done(%d) returned error: %v", o, err)
					}
				})
				off++
			case errors.Is(err, ErrWindowFull):
				time.Sleep(time.Millisecond) // wait for a commit to free the window
			default:
				t.Fatalf("Track(%d) = %v, want nil or ErrWindowFull", off, err)
			}
		}
		wg.Wait()
		mustClose(t, c)

		assertCommitted(t, c, "after every offset was done and Close returned", n)
		mu.Lock()
		defer mu.Unlock()
		for i := 1; i < len(values); i++ {
			if values[i] <= values[i-1] {
				t.Fatalf("Commit values %d then %d: commits must strictly increase", values[i-1], values[i])
			}
		}
		if got := inFlight.max.Load(); got != 1 {
			t.Fatalf("max concurrent Commit calls = %d, want 1", got)
		}
	})
}
