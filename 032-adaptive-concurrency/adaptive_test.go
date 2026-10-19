package adaptiveconcurrency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

const target = 100 * time.Millisecond

type result struct {
	release Release
	err     error
}

func mustNew(t *testing.T, cfg Config) *Limiter {
	t.Helper()
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v) returned error: %v", cfg, err)
	}
	return l
}

// fixed returns a configuration whose limit cannot move (Min == Initial == Max).
func fixed(limit, maxWaiting int) Config {
	return Config{
		Initial:       limit,
		Min:           limit,
		Max:           limit,
		TargetLatency: target,
		BackoffRatio:  0.5,
		MaxWaiting:    maxWaiting,
	}
}

// mustAcquire calls Acquire in the test goroutine. An Acquire that waits here
// leaves every goroutine blocked, which synctest reports as a deadlock.
func mustAcquire(t *testing.T, l *Limiter) Release {
	t.Helper()
	rel, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire with a free slot returned error: %v", err)
	}
	if rel == nil {
		t.Fatal("Acquire returned a nil Release with a nil error")
	}
	return rel
}

// call runs one admitted call that takes latency and ends with outcome.
func call(t *testing.T, l *Limiter, latency time.Duration, outcome Outcome) {
	t.Helper()
	rel := mustAcquire(t, l)
	time.Sleep(latency)
	rel(outcome)
}

// goAcquire runs Acquire in a new goroutine and returns a channel with its result.
func goAcquire(l *Limiter, ctx context.Context) <-chan result {
	ch := make(chan result, 1)
	go func() {
		rel, err := l.Acquire(ctx)
		ch <- result{rel, err}
	}()
	return ch
}

// goWaiter starts an Acquire that must queue, and returns once it is waiting.
// Waiters started one after another therefore join the queue in that order.
func goWaiter(t *testing.T, l *Limiter, ctx context.Context, what string) <-chan result {
	t.Helper()
	ch := goAcquire(l, ctx)
	assertBlocked(t, ch, what)
	return ch
}

func receive(t *testing.T, ch <-chan result, what string) result {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		return r
	default:
		t.Fatalf("%s: Acquire has not returned", what)
		return result{}
	}
}

// admitted receives a result that must be an admission.
func admitted(t *testing.T, ch <-chan result, what string) Release {
	t.Helper()
	r := receive(t, ch, what)
	if r.err != nil {
		t.Fatalf("%s: Acquire returned %v, want admission", what, r.err)
	}
	if r.release == nil {
		t.Fatalf("%s: Acquire returned a nil Release with a nil error", what)
	}
	return r.release
}

func assertBlocked(t *testing.T, ch <-chan result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: Acquire returned (err = %v); want it still waiting", what, r.err)
	default:
	}
}

func assertLimit(t *testing.T, l *Limiter, want int, what string) {
	t.Helper()
	if got := l.Limit(); got != want {
		t.Fatalf("%s: Limit() = %d, want %d", what, got, want)
	}
}

func assertInFlight(t *testing.T, l *Limiter, want int, what string) {
	t.Helper()
	if got := l.InFlight(); got != want {
		t.Fatalf("%s: InFlight() = %d, want %d", what, got, want)
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valid := Config{Initial: 4, Min: 2, Max: 8, TargetLatency: target, BackoffRatio: 0.5, MaxWaiting: 2}
		for _, tc := range []struct {
			name   string
			mutate func(*Config)
		}{
			{"zero min", func(c *Config) { c.Min = 0 }},
			{"negative min", func(c *Config) { c.Min = -1 }},
			{"initial below min", func(c *Config) { c.Initial = 1 }},
			{"initial above max", func(c *Config) { c.Initial = 9 }},
			{"max below min", func(c *Config) { c.Initial, c.Max = 2, 1 }},
			{"zero backoff ratio", func(c *Config) { c.BackoffRatio = 0 }},
			{"negative backoff ratio", func(c *Config) { c.BackoffRatio = -0.5 }},
			{"backoff ratio of one", func(c *Config) { c.BackoffRatio = 1 }},
			{"backoff ratio above one", func(c *Config) { c.BackoffRatio = 1.5 }},
			{"zero target latency", func(c *Config) { c.TargetLatency = 0 }},
			{"negative target latency", func(c *Config) { c.TargetLatency = -time.Millisecond }},
			{"negative max waiting", func(c *Config) { c.MaxWaiting = -1 }},
		} {
			cfg := valid
			tc.mutate(&cfg)
			l, err := New(cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if l != nil {
				t.Errorf("%s: New returned a non-nil limiter with an error", tc.name)
			}
		}

		tight := Config{Initial: 1, Min: 1, Max: 1, TargetLatency: time.Nanosecond, BackoffRatio: 0.99, MaxWaiting: 0}
		for _, cfg := range []Config{valid, tight} {
			l := mustNew(t, cfg)
			assertLimit(t, l, cfg.Initial, fmt.Sprintf("new limiter %+v", cfg))
			assertInFlight(t, l, 0, fmt.Sprintf("new limiter %+v", cfg))
		}
	})
}

func TestFastSuccessIncreasesLimitByOneUpToMax(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 2, Min: 1, Max: 6, TargetLatency: target, BackoffRatio: 0.5})
		for want := 3; want <= 6; want++ {
			call(t, l, 10*time.Millisecond, Success)
			assertLimit(t, l, want, fmt.Sprintf("after %d fast successes from 2", want-2))
		}
		for range 3 {
			call(t, l, 0, Success)
		}
		assertLimit(t, l, 6, "fast successes at Max")
		assertInFlight(t, l, 0, "every call released")

		// No time window: two fast successes that overlap count twice.
		l = mustNew(t, Config{Initial: 2, Min: 1, Max: 10, TargetLatency: target, BackoffRatio: 0.5})
		a, b := mustAcquire(t, l), mustAcquire(t, l)
		time.Sleep(10 * time.Millisecond)
		a(Success)
		b(Success)
		assertLimit(t, l, 4, "two overlapping fast successes from 2")
	})
}

func TestSlowOrDroppedReleaseDecreasesLimitDownToMin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := target + time.Millisecond

		half := mustNew(t, Config{Initial: 8, Min: 1, Max: 8, TargetLatency: target, BackoffRatio: 0.5})
		for _, want := range []int{4, 2, 1, 1} {
			call(t, half, slow, Success)
			assertLimit(t, half, want, "slow success with ratio 0.5")
		}

		// floor(7*0.75)=5, floor(5*0.75)=3, floor(3*0.75)=2, floor(2*0.75)=1 -> Min 2.
		threeQuarters := mustNew(t, Config{Initial: 7, Min: 2, Max: 8, TargetLatency: target, BackoffRatio: 0.75})
		for _, want := range []int{5, 3, 2, 2} {
			call(t, threeQuarters, slow, Success)
			assertLimit(t, threeQuarters, want, "slow success with ratio 0.75")
		}

		dropped := mustNew(t, Config{Initial: 8, Min: 1, Max: 8, TargetLatency: target, BackoffRatio: 0.5})
		call(t, dropped, 0, Dropped)
		assertLimit(t, dropped, 4, "fast Dropped from 8")
		call(t, dropped, 10*target, Dropped)
		assertLimit(t, dropped, 2, "slow Dropped from 4 (one decrease, not two)")
		assertInFlight(t, dropped, 0, "every call released")
	})
}

func TestLatencyBoundaryIsInclusive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 4, Min: 1, Max: 8, TargetLatency: target, BackoffRatio: 0.5})
		call(t, l, target, Success)
		assertLimit(t, l, 5, "success with latency == TargetLatency")
		call(t, l, target+time.Nanosecond, Success)
		assertLimit(t, l, 2, "success with latency == TargetLatency+1ns")
	})
}

func TestIgnoreDoesNotChangeLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 4, Min: 1, Max: 8, TargetLatency: target, BackoffRatio: 0.5})
		for _, tc := range []struct {
			latency time.Duration
			outcome Outcome
			what    string
		}{
			{0, Ignore, "fast Ignore"},
			{10 * target, Ignore, "slow Ignore"},
			{0, Outcome(0), "fast zero Outcome"},
			{10 * target, Outcome(42), "slow unknown Outcome"},
		} {
			call(t, l, tc.latency, tc.outcome)
			assertLimit(t, l, 4, tc.what)
			assertInFlight(t, l, 0, tc.what)
		}
	})
}

func TestAcquireFailsFastWhenNoSlotAndNoQueueRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, fixed(2, 0))
		a, b := mustAcquire(t, l), mustAcquire(t, l)

		// An Acquire that waited here would deadlock, which synctest reports.
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Acquire at the limit with MaxWaiting 0 = %v, want ErrLimitExceeded", err)
		}
		assertInFlight(t, l, 2, "after a rejected Acquire")

		a(Ignore)
		c := mustAcquire(t, l)
		assertInFlight(t, l, 2, "after a release and a new admission")
		b(Ignore)
		c(Ignore)
		assertInFlight(t, l, 0, "every call released")
	})
}

func TestQueuedAcquiresAreAdmittedInFIFOOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, fixed(1, 3))
		holder := mustAcquire(t, l)
		w1 := goWaiter(t, l, context.Background(), "first waiter")
		w2 := goWaiter(t, l, context.Background(), "second waiter")

		holder(Ignore)
		// A caller arriving right after the release must not take the freed slot
		// ahead of the waiters; it queues behind them.
		late := goAcquire(l, context.Background())
		r1 := admitted(t, w1, "first waiter after a release")
		assertBlocked(t, w2, "second waiter while the first holds the slot")
		assertBlocked(t, late, "late caller while the first waiter holds the slot")
		assertInFlight(t, l, 1, "one waiter admitted")

		r1(Ignore)
		r2 := admitted(t, w2, "second waiter after the first released")
		assertBlocked(t, late, "late caller while the second waiter holds the slot")
		r2(Ignore)
		r3 := admitted(t, late, "late caller after both waiters released")
		r3(Ignore)
		assertInFlight(t, l, 0, "every call released")
	})
}

func TestOneReleaseAdmitsAsManyWaitersAsTheNewLimitAllows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 1, Min: 1, Max: 5, TargetLatency: target, BackoffRatio: 0.5, MaxWaiting: 3})
		holder := mustAcquire(t, l)
		w1 := goWaiter(t, l, context.Background(), "waiter 1")
		w2 := goWaiter(t, l, context.Background(), "waiter 2")
		w3 := goWaiter(t, l, context.Background(), "waiter 3")

		holder(Success) // frees one slot and raises the limit to 2
		assertLimit(t, l, 2, "after a fast success from 1")
		r1 := admitted(t, w1, "waiter 1 after the release")
		r2 := admitted(t, w2, "waiter 2 after the release")
		assertBlocked(t, w3, "waiter 3 with 2 in flight and limit 2")
		assertInFlight(t, l, 2, "two waiters admitted by one release")

		r1(Ignore)
		r3 := admitted(t, w3, "waiter 3 after a slot freed")
		r2(Ignore)
		r3(Ignore)
		assertInFlight(t, l, 0, "every call released")
		assertLimit(t, l, 2, "Ignore releases")
	})
}

func TestLoweredLimitStopsAdmissionUntilInFlightDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 4, Min: 1, Max: 8, TargetLatency: target, BackoffRatio: 0.5, MaxWaiting: 1})
		rels := make([]Release, 4)
		for i := range rels {
			rels[i] = mustAcquire(t, l)
		}
		w := goWaiter(t, l, context.Background(), "waiter at the limit")

		rels[0](Dropped)
		assertLimit(t, l, 2, "after a drop from 4")
		assertInFlight(t, l, 3, "admitted calls survive a lower limit")
		assertBlocked(t, w, "waiter with 3 in flight and limit 2")

		rels[1](Ignore)
		assertInFlight(t, l, 2, "after a second release")
		assertBlocked(t, w, "waiter with 2 in flight and limit 2")

		rels[2](Ignore)
		r := admitted(t, w, "waiter with 1 in flight and limit 2")
		assertInFlight(t, l, 2, "after the waiter was admitted")

		rels[3](Ignore)
		r(Ignore)
		assertInFlight(t, l, 0, "every call released")
		assertLimit(t, l, 2, "Ignore releases")
	})
}

func TestQueueingTimeIsNotPartOfLatency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 1, Min: 1, Max: 4, TargetLatency: target, BackoffRatio: 0.5, MaxWaiting: 1})
		holder := mustAcquire(t, l)
		w := goWaiter(t, l, context.Background(), "waiter")

		time.Sleep(10 * target) // the waiter queues far longer than the target
		holder(Ignore)
		r := admitted(t, w, "waiter after the release")
		assertLimit(t, l, 1, "after an Ignore release")

		r(Success) // released at the moment of admission: latency 0
		assertLimit(t, l, 2, "waiter that queued 1s and ran 0s released with Success (queue time counted as latency?)")
	})
}

func TestCancelledWaiterReturnsContextErrorAndLeavesQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, fixed(1, 1))
		holder := mustAcquire(t, l)

		ctx, cancel := context.WithCancel(context.Background())
		w := goWaiter(t, l, ctx, "waiter to cancel")
		cancel()
		if r := receive(t, w, "cancelled waiter"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled waiter error = %v, want context.Canceled", r.err)
		}
		assertInFlight(t, l, 1, "after a waiter was cancelled")

		// The cancelled waiter freed its queue position (MaxWaiting is 1).
		next := goWaiter(t, l, context.Background(), "waiter after the cancellation")
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Acquire with a full queue = %v, want ErrLimitExceeded", err)
		}
		holder(Ignore)
		r := admitted(t, next, "waiter after the holder released")
		assertInFlight(t, l, 1, "the cancelled waiter must not hold a slot")

		dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer dcancel()
		start := time.Now()
		if _, err := l.Acquire(dctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter with a deadline = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 50*time.Millisecond {
			t.Fatalf("waiter with a 50ms deadline returned after %v", got)
		}

		r(Ignore)
		assertInFlight(t, l, 0, "every call released")
		mustAcquire(t, l)(Ignore) // no slot was lost to the cancelled waiters
	})
}

func TestPreCancelledAcquireAdmitsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, fixed(1, 1))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := l.Acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Acquire with a free slot = %v, want context.Canceled", err)
		}
		assertInFlight(t, l, 0, "after a pre-cancelled Acquire")

		holder := mustAcquire(t, l)
		if _, err := l.Acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Acquire at the limit = %v, want context.Canceled", err)
		}
		// The rejected caller left no queue entry behind (MaxWaiting is 1).
		w := goWaiter(t, l, context.Background(), "waiter after a pre-cancelled Acquire")
		holder(Ignore)
		admitted(t, w, "waiter after the holder released")(Ignore)
		assertInFlight(t, l, 0, "every call released")
	})
}

func TestReleaseIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 2, Min: 1, Max: 10, TargetLatency: target, BackoffRatio: 0.5})
		rel := mustAcquire(t, l)
		rel(Success)
		assertLimit(t, l, 3, "first release")
		rel(Success)
		rel(Dropped)
		assertLimit(t, l, 3, "repeated releases of one call")
		assertInFlight(t, l, 0, "repeated releases of one call")

		rel = mustAcquire(t, l)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { rel(Success) })
		}
		wg.Wait()
		assertLimit(t, l, 4, "eight concurrent releases of one call")
		assertInFlight(t, l, 0, "eight concurrent releases of one call")

		// A repeated release must not free a slot that belongs to another call.
		l = mustNew(t, fixed(1, 0))
		a := mustAcquire(t, l)
		a(Ignore)
		b := mustAcquire(t, l)
		a(Ignore)
		assertInFlight(t, l, 1, "stale release while another call holds the slot")
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Acquire after a stale release = %v, want ErrLimitExceeded: the slot still belongs to another call", err)
		}
		b(Ignore)
		assertInFlight(t, l, 0, "every call released")
	})
}

func TestCloseFailsQueuedAndNewAcquiresButNotAdmittedCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, fixed(1, 2))
		holder := mustAcquire(t, l)
		w1 := goWaiter(t, l, context.Background(), "waiter 1")
		w2 := goWaiter(t, l, context.Background(), "waiter 2")

		l.Close()
		for i, ch := range []<-chan result{w1, w2} {
			if r := receive(t, ch, fmt.Sprintf("waiter %d after Close", i+1)); !errors.Is(r.err, ErrClosed) {
				t.Fatalf("waiter %d after Close = %v, want ErrClosed", i+1, r.err)
			}
		}
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
		}
		assertInFlight(t, l, 1, "Close must not end admitted calls")

		holder(Success)
		assertInFlight(t, l, 0, "release after Close")
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Acquire after Close and a release = %v, want ErrClosed", err)
		}
		l.Close()
	})
}

func TestConcurrentAcquireAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Initial: 2, Min: 1, Max: 4, TargetLatency: target, BackoffRatio: 0.5, MaxWaiting: 64})
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Go(func() {
				rel, err := l.Acquire(context.Background())
				switch {
				case err == nil:
					time.Sleep(time.Millisecond)
					rel(Success)
				case errors.Is(err, ErrClosed), errors.Is(err, ErrLimitExceeded):
				default:
					t.Errorf("Acquire %d = %v, want nil, ErrClosed, or ErrLimitExceeded", i, err)
				}
			})
		}
		for range 2 {
			wg.Go(l.Close)
		}
		wg.Wait()

		assertInFlight(t, l, 0, "after every caller returned")
		if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
		}
	})
}

func TestConcurrentCallsNeverExceedLimitAndLeakNoSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 3
		l := mustNew(t, fixed(limit, 4))
		outcomes := []Outcome{Success, Dropped, Ignore}
		var active, maxActive atomic.Int32
		var wg sync.WaitGroup
		for i := range 40 {
			wg.Go(func() {
				// Deadlines and hold times land on the same virtual instants, so
				// waiter timeouts race with releases that hand them a slot.
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%4)*5*time.Millisecond)
				defer cancel()
				rel, err := l.Acquire(ctx)
				switch {
				case err == nil:
				case errors.Is(err, ErrLimitExceeded), errors.Is(err, context.DeadlineExceeded):
					return
				default:
					t.Errorf("Acquire %d = %v, want nil, ErrLimitExceeded, or context.DeadlineExceeded", i, err)
					return
				}
				n := active.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				time.Sleep(time.Duration(i%3+1) * 5 * time.Millisecond)
				active.Add(-1)
				rel(outcomes[i%3])
			})
		}
		wg.Wait()

		if got := maxActive.Load(); got > limit {
			t.Fatalf("max concurrently admitted calls = %d, want <= %d", got, limit)
		}
		assertInFlight(t, l, 0, "after every caller returned")
		assertLimit(t, l, limit, "fixed limit")
		for range limit {
			mustAcquire(t, l) // every slot is still available
		}
	})
}
