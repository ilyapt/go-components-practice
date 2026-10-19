package tokenbucket

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// The tests use rates and instants (rate 10/s at multiples of 100ms, rate 4/s
// at multiples of 125ms, rate 0.5/s at whole seconds) for which both float64
// and integer-nanosecond token arithmetic are exact, so they can compare token
// counts and return times without tolerance.

const ms = time.Millisecond

type result struct {
	err error
	at  time.Time
}

func mustNew(t *testing.T, rate float64, burst int) *Bucket {
	t.Helper()
	b, err := New(rate, burst)
	if err != nil {
		t.Fatalf("New(%v, %d) returned error: %v", rate, burst, err)
	}
	return b
}

// goWaitN runs WaitN in a new goroutine and returns a channel with its result
// and the moment it returned.
func goWaitN(b *Bucket, ctx context.Context, n int) <-chan result {
	ch := make(chan result, 1)
	go func() {
		err := b.WaitN(ctx, n)
		ch <- result{err: err, at: time.Now()}
	}()
	return ch
}

func receive(t *testing.T, ch <-chan result, what string) result {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		return r
	default:
		t.Fatalf("%s: WaitN has not returned", what)
		return result{}
	}
}

func assertBlocked(t *testing.T, ch <-chan result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: WaitN returned %v; want it still waiting", what, r.err)
	default:
	}
}

func sleepUntil(start time.Time, d time.Duration) {
	time.Sleep(time.Until(start.Add(d)))
}

func checkAllowN(t *testing.T, b *Bucket, n int, want bool, when string) {
	t.Helper()
	if got := b.AllowN(n); got != want {
		t.Fatalf("AllowN(%d) %s = %v, want %v", n, when, got, want)
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			rate  float64
			burst int
		}{
			{name: "zero rate", rate: 0, burst: 1},
			{name: "negative rate", rate: -1, burst: 1},
			{name: "NaN rate", rate: math.NaN(), burst: 1},
			{name: "infinite rate", rate: math.Inf(1), burst: 1},
			{name: "zero burst", rate: 10, burst: 0},
			{name: "negative burst", rate: 10, burst: -1},
		} {
			b, err := New(tc.rate, tc.burst)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if b != nil {
				t.Errorf("%s: New returned a non-nil bucket with an error", tc.name)
			}
		}
		mustNew(t, 0.5, 1)
		mustNew(t, 1000, 100)
	})
}

func TestRefillIsProportionalToElapsedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		for i := range 5 {
			if !b.Allow() {
				t.Fatalf("Allow #%d on a new bucket = false, want true: a new bucket holds burst tokens", i+1)
			}
		}
		checkAllowN(t, b, 1, false, "after 5 Allow calls on a bucket with burst 5")

		sleepUntil(start, 100*ms)
		checkAllowN(t, b, 1, true, "at 100ms (rate 10/s refills one token)")
		checkAllowN(t, b, 1, false, "at 100ms after taking the refilled token")

		sleepUntil(start, 400*ms)
		checkAllowN(t, b, 4, false, "at 400ms with 3 tokens (all or nothing)")
		checkAllowN(t, b, 3, true, "at 400ms with 3 tokens: the failed AllowN(4) must not take any")
		checkAllowN(t, b, 1, false, "at 400ms after taking all 3 tokens")

		sleepUntil(start, 1400*ms)
		checkAllowN(t, b, 5, true, "after 1s of refill")
		checkAllowN(t, b, 1, false, "after 1s of refill: 10 tokens must be capped at burst 5")
	})
}

func TestTokensNeverExceedBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, 10, 5)
		time.Sleep(time.Hour)
		checkAllowN(t, b, 5, true, "after 1h idle")
		checkAllowN(t, b, 1, false, "after 1h idle: the bucket holds at most burst tokens")

		time.Sleep(10 * time.Second)
		t1 := time.Now()
		if err := b.WaitN(context.Background(), 5); err != nil {
			t.Fatalf("WaitN(5) after 10s idle returned error: %v", err)
		}
		if got := time.Since(t1); got != 0 {
			t.Fatalf("WaitN(5) after 10s idle waited %v, want an immediate return", got)
		}
		if err := b.WaitN(context.Background(), 5); err != nil {
			t.Fatalf("second WaitN(5) returned error: %v", err)
		}
		if got := time.Since(t1); got != 500*ms {
			t.Fatalf("second WaitN(5) after 10s idle returned after %v, want 500ms: tokens must be capped at burst", got)
		}
	})
}

func TestFractionalTokensAreNotLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 4, 2)
		checkAllowN(t, b, 2, true, "on a new bucket")
		sleepUntil(start, 125*ms)
		checkAllowN(t, b, 1, false, "at 125ms with half a token")
		if err := b.Wait(context.Background()); err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
		if got := time.Since(start); got != 250*ms {
			t.Fatalf("Wait with half a token accrued returned at %v, want 250ms: the failed Allow must not lose the half token", got)
		}
		checkAllowN(t, b, 1, false, "at 250ms after Wait took the token")

		// A rate below one token per second.
		slowStart := time.Now()
		slow := mustNew(t, 0.5, 1)
		checkAllowN(t, slow, 1, true, "on a new bucket with rate 0.5")
		sleepUntil(slowStart, time.Second)
		checkAllowN(t, slow, 1, false, "after 1s at rate 0.5 (half a token)")
		sleepUntil(slowStart, 2*time.Second)
		checkAllowN(t, slow, 1, true, "after 2s at rate 0.5")
		if err := slow.Wait(context.Background()); err != nil {
			t.Fatalf("Wait at rate 0.5 returned error: %v", err)
		}
		if got := time.Since(slowStart); got != 4*time.Second {
			t.Fatalf("Wait at rate 0.5 returned at %v, want 4s", got)
		}
	})
}

func TestNonPositiveNAsksForNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		checkAllowN(t, b, 0, true, "on an empty bucket")
		checkAllowN(t, b, -3, true, "on an empty bucket")
		for _, n := range []int{0, -3} {
			if err := b.WaitN(context.Background(), n); err != nil {
				t.Fatalf("WaitN(%d) returned error: %v", n, err)
			}
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("WaitN with n <= 0 waited %v, want an immediate return", got)
		}
		checkAllowN(t, b, 1, false, "after calls with n <= 0: they must not add tokens")

		sleepUntil(start, 100*ms)
		checkAllowN(t, b, 1, true, "at 100ms")
		checkAllowN(t, b, 1, false, "at 100ms: calls with n <= 0 must not add tokens")
	})
}

func TestRequestLargerThanBurstFailsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 6, false, "on a full bucket with burst 5")

		start := time.Now()
		// A WaitN that waited instead would block forever, which synctest
		// reports as a deadlock.
		if err := b.WaitN(context.Background(), 6); !errors.Is(err, ErrExceedsBurst) {
			t.Fatalf("WaitN(6) with burst 5 = %v, want ErrExceedsBurst", err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("WaitN(6) waited %v before failing, want an immediate ErrExceedsBurst", got)
		}
		checkAllowN(t, b, 5, true, "after the rejected calls: they must take nothing")
	})
}

func TestWaitNReturnsWhenTokensAccrue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		if err := b.WaitN(context.Background(), 3); err != nil {
			t.Fatalf("WaitN(3) on a full bucket returned error: %v", err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("WaitN(3) on a full bucket waited %v, want an immediate return", got)
		}
		checkAllowN(t, b, 3, false, "after WaitN took 3 of 5 tokens")
		checkAllowN(t, b, 2, true, "after WaitN took 3 of 5 tokens")

		ch := goWaitN(b, context.Background(), 3)
		sleepUntil(start, 200*ms)
		assertBlocked(t, ch, "WaitN(3) at 200ms with 2 tokens")
		sleepUntil(start, 300*ms)
		r := receive(t, ch, "WaitN(3) at 300ms with 3 tokens")
		if r.err != nil {
			t.Fatalf("WaitN(3) returned error: %v", r.err)
		}
		if got := r.at.Sub(start); got != 300*ms {
			t.Fatalf("WaitN(3) returned at %v, want 300ms", got)
		}
		checkAllowN(t, b, 1, false, "at 300ms: WaitN must have taken its 3 tokens")

		if err := b.Wait(context.Background()); err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
		if got := time.Since(start); got != 400*ms {
			t.Fatalf("Wait on an empty bucket returned at %v, want 400ms", got)
		}
	})
}

func TestWaitersShareTokensWithoutOverAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		chans := make([]<-chan result, 5)
		for i := range chans {
			chans[i] = goWaitN(b, context.Background(), 1)
		}
		sleepUntil(start, 500*ms)
		var got []time.Duration
		for _, ch := range chans {
			r := receive(t, ch, "waiter at 500ms")
			if r.err != nil {
				t.Fatalf("waiter returned error: %v", r.err)
			}
			got = append(got, r.at.Sub(start))
		}
		slices.Sort(got)
		want := []time.Duration{100 * ms, 200 * ms, 300 * ms, 400 * ms, 500 * ms}
		if !slices.Equal(got, want) {
			t.Fatalf("waiters returned at %v, want %v: one token every 100ms, one waiter per token", got, want)
		}
		checkAllowN(t, b, 1, false, "at 500ms after five waiters took five tokens")
	})
}

func TestCancelledWaiterReturnsPromptlyAndTakesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		ctx, cancel := context.WithCancel(context.Background())
		ch := goWaitN(b, ctx, 5)
		sleepUntil(start, 300*ms)
		assertBlocked(t, ch, "WaitN(5) at 300ms with 3 tokens")

		cancel()
		r := receive(t, ch, "cancelled waiter")
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled WaitN = %v, want context.Canceled", r.err)
		}
		if got := r.at.Sub(start); got != 300*ms {
			t.Fatalf("cancelled WaitN returned at %v, want 300ms (when it was cancelled)", got)
		}
		checkAllowN(t, b, 4, false, "at 300ms with 3 tokens")
		checkAllowN(t, b, 3, true, "at 300ms: the cancelled waiter must not have taken the 3 accrued tokens")
		checkAllowN(t, b, 1, false, "at 300ms after taking the 3 tokens")
	})
}

func TestCancelledWaiterDoesNotDelayOtherWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		ctxA, cancelA := context.WithCancel(context.Background())
		a := goWaitN(b, ctxA, 5)
		synctest.Wait() // a starts waiting before the others
		w1 := goWaitN(b, context.Background(), 5)

		sleepUntil(start, 100*ms)
		cancelA()
		if r := receive(t, a, "cancelled waiter"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled WaitN = %v, want context.Canceled", r.err)
		}
		w2 := goWaitN(b, context.Background(), 5)

		sleepUntil(start, time.Second)
		var got []time.Duration
		for _, ch := range []<-chan result{w1, w2} {
			r := receive(t, ch, "remaining waiter at 1s")
			if r.err != nil {
				t.Fatalf("remaining waiter returned error: %v", r.err)
			}
			got = append(got, r.at.Sub(start))
		}
		slices.Sort(got)
		if want := []time.Duration{500 * ms, time.Second}; !slices.Equal(got, want) {
			t.Fatalf("remaining waiters returned at %v, want %v: the cancelled waiter must leave the bucket as if it had never waited", got, want)
		}
		checkAllowN(t, b, 1, false, "at 1s after both waiters took 5 tokens each")
	})
}

func TestPreCancelledContextTakesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, 10, 5)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := b.WaitN(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitN with a cancelled context = %v, want context.Canceled", err)
		}
		if err := b.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait with a cancelled context = %v, want context.Canceled", err)
		}

		expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancelExpired()
		if err := b.WaitN(expired, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitN with an expired deadline = %v, want context.DeadlineExceeded", err)
		}

		checkAllowN(t, b, 5, true, "after calls with done contexts: they must take nothing")
	})
}

func TestWaitNFailsFastOnlyWhenDeadlineIsTooSoon(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		short, cancelShort := context.WithTimeout(context.Background(), 100*ms)
		defer cancelShort()
		if err := b.WaitN(short, 2); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitN(2) needing 200ms with a 100ms deadline = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("WaitN(2) slept %v before failing; want an immediate failure: 2 tokens need 200ms and the deadline is in 100ms", got)
		}

		long, cancelLong := context.WithTimeout(context.Background(), 250*ms)
		defer cancelLong()
		ch := goWaitN(b, long, 2)
		sleepUntil(start, 200*ms)
		r := receive(t, ch, "WaitN(2) with a 250ms deadline at 200ms")
		if r.err != nil {
			t.Fatalf("WaitN(2) needing 200ms with a 250ms deadline = %v, want nil", r.err)
		}
		if got := r.at.Sub(start); got != 200*ms {
			t.Fatalf("WaitN(2) returned at %v, want 200ms: the rejected waiter must not have taken tokens", got)
		}
		checkAllowN(t, b, 1, false, "at 200ms after WaitN took both tokens")
	})
}

func TestAllowDoesNotBlockBehindWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := mustNew(t, 10, 5)
		checkAllowN(t, b, 5, true, "on a new bucket")

		waiter := goWaitN(b, context.Background(), 5)
		sleepUntil(start, 100*ms)
		synctest.Wait()

		// If the sleeping waiter held the bucket's lock, these calls would hang
		// and the test would time out. Either result is acceptable here: the
		// README leaves it to you whether Allow may take a waiter's tokens.
		b.Allow()
		b.AllowN(1)
		ctx, cancel := context.WithCancel(context.Background())
		other := goWaitN(b, ctx, 1)
		synctest.Wait()
		cancel()
		if r := receive(t, other, "second waiter"); r.err != nil && !errors.Is(r.err, context.Canceled) {
			t.Fatalf("second WaitN = %v, want nil or context.Canceled", r.err)
		}

		sleepUntil(start, 2*time.Second)
		if r := receive(t, waiter, "first waiter at 2s"); r.err != nil {
			t.Fatalf("first WaitN returned error: %v", r.err)
		}
	})
}

func TestConcurrentAllowAdmitsExactlyTheRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, 10, 5)
		var admitted atomic.Int32
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				for range 11 { // at 0ms, 100ms, ..., 1000ms
					if b.Allow() {
						admitted.Add(1)
					}
					time.Sleep(100 * ms)
				}
			})
		}
		wg.Wait()
		// 5 at the start, then one per 100ms for 1s.
		if got := admitted.Load(); got != 15 {
			t.Fatalf("20 concurrent callers were admitted %d times over 1s, want exactly 15 (burst 5 + 10/s)", got)
		}
	})
}

func TestConcurrentCallersNeverExceedRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			rate  = 10.0
			burst = 5
		)
		start := time.Now()
		b := mustNew(t, rate, burst)

		type grant struct {
			at time.Duration
			n  int
		}
		var mu sync.Mutex
		var grants []grant
		record := func(n int) {
			at := time.Since(start)
			mu.Lock()
			grants = append(grants, grant{at, n})
			mu.Unlock()
		}
		checkWaitErr := func(err error, n int) {
			switch {
			case err == nil:
				record(n)
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			default:
				t.Errorf("WaitN(%d) = %v, want nil, context.DeadlineExceeded, or context.Canceled", n, err)
			}
		}

		var wg sync.WaitGroup
		for g := range 12 {
			wg.Go(func() {
				for i := range 12 {
					switch (g + i) % 4 {
					case 0:
						if b.Allow() {
							record(1)
						}
						time.Sleep(time.Duration(7+g) * ms)
					case 1:
						n := 2 + g%3
						if b.AllowN(n) {
							record(n)
						}
						time.Sleep(time.Duration(11+g) * ms)
					case 2:
						n := 1 + g%3
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(40+15*g)*ms)
						checkWaitErr(b.WaitN(ctx, n), n)
						cancel()
					case 3:
						ctx, cancel := context.WithCancel(context.Background())
						stop := time.AfterFunc(time.Duration(5+3*g)*ms, cancel)
						checkWaitErr(b.WaitN(ctx, 2), 2)
						stop.Stop()
						cancel()
					}
				}
			})
		}
		wg.Wait()

		if len(grants) == 0 {
			t.Fatal("no call was granted any tokens")
		}
		slices.SortFunc(grants, func(x, y grant) int { return cmp.Compare(x.at, y.at) })
		// Over any window [a, b], the tokens granted are at most burst + rate*(b-a).
		for i := range grants {
			sum := 0
			for j := i; j < len(grants); j++ {
				sum += grants[j].n
				window := grants[j].at - grants[i].at
				if limit := burst + rate*window.Seconds(); float64(sum) > limit+1e-9 {
					t.Fatalf("%d tokens granted between %v and %v, want at most %.2f (burst %d + %v/s over %v)",
						sum, grants[i].at, grants[j].at, limit, burst, rate, window)
				}
			}
		}
	})
}
