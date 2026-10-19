package connectionpool

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

var (
	errBroken = errors.New("connection broken")
	errDial   = errors.New("dial failed")
)

type dialerFunc func(context.Context) (Conn, error)

func (f dialerFunc) Dial(ctx context.Context) (Conn, error) { return f(ctx) }

// tracker sees connections the way a server does: it counts dials in progress
// plus connections that were dialed and whose Close has not returned yet.
type tracker struct {
	mu      sync.Mutex
	conns   []*fakeConn
	dials   atomic.Int32
	live    atomic.Int32
	maxLive atomic.Int32
}

// start records that a dial has begun.
func (tr *tracker) start() {
	tr.dials.Add(1)
	n := tr.live.Add(1)
	for {
		m := tr.maxLive.Load()
		if n <= m || tr.maxLive.CompareAndSwap(m, n) {
			return
		}
	}
}

// fail records that a started dial failed.
func (tr *tracker) fail() { tr.live.Add(-1) }

// succeed returns the connection produced by a started dial.
func (tr *tracker) succeed() *fakeConn {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	c := &fakeConn{id: len(tr.conns) + 1, tr: tr}
	tr.conns = append(tr.conns, c)
	return c
}

// dialer returns a Dialer whose dials succeed immediately.
func (tr *tracker) dialer() Dialer {
	return dialerFunc(func(context.Context) (Conn, error) {
		tr.start()
		return tr.succeed(), nil
	})
}

func (tr *tracker) assertAllClosedOnce(t *testing.T) {
	t.Helper()
	tr.mu.Lock()
	conns := append([]*fakeConn(nil), tr.conns...)
	tr.mu.Unlock()
	for _, c := range conns {
		if n := c.closes.Load(); n != 1 {
			t.Errorf("%v was closed %d times, want exactly once", c, n)
		}
	}
	if n := tr.live.Load(); n != 0 {
		t.Errorf("%d connections or dials are still open, want 0", n)
	}
}

type fakeConn struct {
	id        int
	tr        *tracker
	broken    atomic.Bool
	closes    atomic.Int32
	closeGate chan struct{} // if non-nil, Close blocks until it is closed
}

func (c *fakeConn) Healthy() bool { return !c.broken.Load() }

func (c *fakeConn) Close() error {
	n := c.closes.Add(1)
	if c.closeGate != nil {
		<-c.closeGate
	}
	if n == 1 {
		c.tr.live.Add(-1) // the server forgets the connection when Close returns
	}
	return nil
}

func (c *fakeConn) String() string { return fmt.Sprintf("conn-%d", c.id) }

type result struct {
	conn Conn
	err  error
}

func mustNew(t *testing.T, d Dialer, maxOpen, maxIdle int, idleTimeout time.Duration) *Pool {
	t.Helper()
	p, err := New(d, maxOpen, maxIdle, idleTimeout)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return p
}

func mustGet(t *testing.T, p *Pool) *fakeConn {
	t.Helper()
	conn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	c, ok := conn.(*fakeConn)
	if !ok {
		t.Fatalf("Get returned %T, want a connection from the dialer", conn)
	}
	return c
}

// goGet runs Get in a new goroutine and returns a channel with its result.
func goGet(p *Pool, ctx context.Context) <-chan result {
	ch := make(chan result, 1)
	go func() {
		conn, err := p.Get(ctx)
		ch <- result{conn, err}
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
		t.Fatalf("%s: Get has not returned", what)
		return result{}
	}
}

func assertBlocked(t *testing.T, ch <-chan result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: Get returned %v, %v; want it still waiting", what, r.conn, r.err)
	default:
	}
}

func assertStats(t *testing.T, p *Pool, want Stats, what string) {
	t.Helper()
	if got := p.Stats(); got != want {
		t.Fatalf("%s: Stats = %+v, want %+v", what, got, want)
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := (&tracker{}).dialer()
		for _, tc := range []struct {
			name        string
			dialer      Dialer
			maxOpen     int
			maxIdle     int
			idleTimeout time.Duration
			wantErr     error
		}{
			{"zero maxOpen", d, 0, 0, time.Second, ErrInvalidConfig},
			{"negative maxOpen", d, -1, 0, time.Second, ErrInvalidConfig},
			{"negative maxIdle", d, 2, -1, time.Second, ErrInvalidConfig},
			{"maxIdle above maxOpen", d, 2, 3, time.Second, ErrInvalidConfig},
			{"zero idleTimeout", d, 2, 1, 0, ErrInvalidConfig},
			{"negative idleTimeout", d, 2, 1, -time.Second, ErrInvalidConfig},
			{"nil dialer", nil, 2, 1, time.Second, ErrNilDialer},
		} {
			p, err := New(tc.dialer, tc.maxOpen, tc.maxIdle, tc.idleTimeout)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if p != nil {
				t.Errorf("%s: New returned a non-nil pool with an error", tc.name)
			}
		}
		for _, maxIdle := range []int{0, 2} {
			if _, err := New(d, 2, maxIdle, time.Second); err != nil {
				t.Errorf("New(maxOpen=2, maxIdle=%d) = %v, want nil", maxIdle, err)
			}
		}
	})
}

func TestGetReusesIdleConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 2, 2, time.Minute)
		if n := tr.dials.Load(); n != 0 {
			t.Fatalf("New dialed %d times, want 0: connections are opened lazily", n)
		}
		assertStats(t, p, Stats{}, "new pool")

		c := mustGet(t, p)
		assertStats(t, p, Stats{Open: 1}, "one connection checked out")
		p.Put(c, nil)
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "after Put")

		if again := mustGet(t, p); again != c {
			t.Fatalf("Get returned %v, want the idle %v", again, c)
		}
		if n := tr.dials.Load(); n != 1 {
			t.Fatalf("dials = %d, want 1: an idle healthy connection must be reused", n)
		}
		if n := c.closes.Load(); n != 0 {
			t.Fatalf("reused connection was closed %d times", n)
		}
	})
}

func TestGetDialsUpToMaxOpenThenWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 2, 2, time.Minute)

		c1 := mustGet(t, p)
		c2 := mustGet(t, p)
		w := goGet(p, context.Background())
		assertBlocked(t, w, "third Get at maxOpen=2")
		assertStats(t, p, Stats{Open: 2, Waiting: 1}, "at capacity with one waiter")

		p.Put(c1, nil)
		if r := receive(t, w, "waiter"); r.err != nil || r.conn != c1 {
			t.Fatalf("waiter got %v, %v; want %v handed over by Put", r.conn, r.err, c1)
		}
		assertStats(t, p, Stats{Open: 2}, "after handoff")
		if n := tr.dials.Load(); n != 2 {
			t.Fatalf("dials = %d, want 2", n)
		}

		p.Put(c1, nil)
		p.Put(c2, nil)
		assertStats(t, p, Stats{Open: 2, Idle: 2}, "both returned")
	})
}

func TestWaitersAreServedInFIFOOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)
		c := mustGet(t, p)

		w1 := goGet(p, context.Background())
		synctest.Wait()
		w2 := goGet(p, context.Background())
		synctest.Wait()
		w3 := goGet(p, context.Background())
		synctest.Wait()
		assertStats(t, p, Stats{Open: 1, Waiting: 3}, "three waiters queued")

		p.Put(c, nil)
		if r := receive(t, w1, "first waiter"); r.err != nil || r.conn != c {
			t.Fatalf("first waiter got %v, %v; want %v", r.conn, r.err, c)
		}
		assertBlocked(t, w2, "second waiter")
		assertBlocked(t, w3, "third waiter")

		// Put hands the connection over itself, so a Get that arrives right
		// after Put cannot overtake the queued waiters.
		p.Put(c, nil)
		late := goGet(p, context.Background())
		if r := receive(t, w2, "second waiter"); r.err != nil || r.conn != c {
			t.Fatalf("second waiter got %v, %v; want %v", r.conn, r.err, c)
		}
		assertBlocked(t, w3, "third waiter")
		assertBlocked(t, late, "late Get")

		p.Put(c, nil)
		if r := receive(t, w3, "third waiter"); r.err != nil || r.conn != c {
			t.Fatalf("third waiter got %v, %v; want %v", r.conn, r.err, c)
		}
		assertBlocked(t, late, "late Get")

		p.Put(c, nil)
		if r := receive(t, late, "late Get"); r.err != nil || r.conn != c {
			t.Fatalf("late Get got %v, %v; want %v", r.conn, r.err, c)
		}
		if n := tr.dials.Load(); n != 1 {
			t.Fatalf("dials = %d, want 1", n)
		}
	})
}

func TestWaiterCancellationLeavesNoState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)
		c := mustGet(t, p)

		ctx, cancel := context.WithCancel(context.Background())
		w := goGet(p, ctx)
		assertBlocked(t, w, "waiter")
		assertStats(t, p, Stats{Open: 1, Waiting: 1}, "one waiter")
		cancel()
		if r := receive(t, w, "cancelled waiter"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled waiter got %v, %v; want context.Canceled", r.conn, r.err)
		}
		assertStats(t, p, Stats{Open: 1}, "after the waiter left")

		// Nobody waits any more: the connection must reach the idle set, not
		// the waiter that already left.
		p.Put(c, nil)
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "Put after the waiter left")

		if again := mustGet(t, p); again != c {
			t.Fatalf("Get returned %v, want the idle %v", again, c)
		}
		dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer dcancel()
		w = goGet(p, dctx)
		time.Sleep(49 * time.Millisecond)
		assertBlocked(t, w, "waiter before its deadline")
		time.Sleep(time.Millisecond)
		if r := receive(t, w, "waiter at its deadline"); !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("waiter got %v, %v; want context.DeadlineExceeded", r.conn, r.err)
		}
		assertStats(t, p, Stats{Open: 1}, "after the deadline")
		if n := tr.dials.Load(); n != 1 {
			t.Fatalf("dials = %d, want 1", n)
		}
	})
}

func TestPreCancelledGetTakesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Free capacity: no dial.
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)
		if _, err := p.Get(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get = %v, want context.Canceled", err)
		}
		if n := tr.dials.Load(); n != 0 {
			t.Fatalf("pre-cancelled Get dialed %d times, want 0", n)
		}
		assertStats(t, p, Stats{}, "after pre-cancelled Get on an empty pool")

		// An idle connection is available: it stays idle.
		c := mustGet(t, p)
		p.Put(c, nil)
		if conn, err := p.Get(ctx); !errors.Is(err, context.Canceled) || conn != nil {
			t.Fatalf("pre-cancelled Get = %v, %v; want nil, context.Canceled", conn, err)
		}
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "after pre-cancelled Get with an idle connection")

		// At capacity: no waiting (a Get that waited without watching ctx would
		// deadlock here).
		c = mustGet(t, p)
		if _, err := p.Get(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get at capacity = %v, want context.Canceled", err)
		}
		assertStats(t, p, Stats{Open: 1}, "after pre-cancelled Get at capacity")
		p.Put(c, nil)
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "Put after pre-cancelled Get")
	})
}

// A connection handed to a waiter whose context is done at the same moment
// must end up either with the waiter or back in the pool, never lost.
func TestCancellationRacingHandoffNeverLosesConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)
		held := mustGet(t, p)

		for i := range 50 {
			ctx, cancel := context.WithCancel(context.Background())
			w := goGet(p, ctx)
			synctest.Wait()
			cancel()
			p.Put(held, nil) // races with the waiter noticing its cancellation
			r := receive(t, w, fmt.Sprintf("round %d: waiter", i))
			switch {
			case r.err == nil:
				if r.conn != held {
					t.Fatalf("round %d: waiter got %v, want %v", i, r.conn, held)
				}
				assertStats(t, p, Stats{Open: 1}, fmt.Sprintf("round %d: waiter kept the connection", i))
			case errors.Is(r.err, context.Canceled):
				assertStats(t, p, Stats{Open: 1, Idle: 1}, fmt.Sprintf("round %d: waiter gave up", i))
				if again := mustGet(t, p); again != held {
					t.Fatalf("round %d: Get returned %v, want %v back from the pool", i, again, held)
				}
			default:
				t.Fatalf("round %d: waiter got %v, %v; want the connection or context.Canceled", i, r.conn, r.err)
			}
		}
		if n := tr.dials.Load(); n != 1 {
			t.Fatalf("dials = %d, want 1: the connection was lost and replaced", n)
		}
		if n := held.closes.Load(); n != 0 {
			t.Fatalf("the healthy connection was closed %d times", n)
		}
	})
}

func TestBrokenConnectionIsClosedAndSlotFreed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)

		// Put with an error, while a caller waits: the slot passes to it.
		c1 := mustGet(t, p)
		w := goGet(p, context.Background())
		assertBlocked(t, w, "waiter")
		p.Put(c1, errBroken)
		if n := c1.closes.Load(); n != 1 {
			t.Fatalf("broken connection closed %d times, want 1", n)
		}
		r := receive(t, w, "waiter after a broken Put")
		if r.err != nil || r.conn == nil || r.conn == c1 {
			t.Fatalf("waiter got %v, %v; want a newly dialed connection", r.conn, r.err)
		}
		c2 := r.conn.(*fakeConn)
		assertStats(t, p, Stats{Open: 1}, "waiter holds the new connection")

		// Put without an error, but the connection reports itself unhealthy.
		c2.broken.Store(true)
		p.Put(c2, nil)
		if n := c2.closes.Load(); n != 1 {
			t.Fatalf("unhealthy connection closed %d times, want 1", n)
		}
		assertStats(t, p, Stats{}, "after the unhealthy connection was closed")

		// A connection that breaks while idle is skipped by Get.
		c3 := mustGet(t, p)
		p.Put(c3, nil)
		c3.broken.Store(true)
		c4 := mustGet(t, p)
		if c4 == c3 {
			t.Fatalf("Get returned the unhealthy idle %v", c3)
		}
		if n := c3.closes.Load(); n != 1 {
			t.Fatalf("unhealthy idle connection closed %d times, want 1", n)
		}
		assertStats(t, p, Stats{Open: 1}, "after skipping the unhealthy idle connection")
		if n := tr.dials.Load(); n != 4 {
			t.Fatalf("dials = %d, want 4", n)
		}
		if m := tr.maxLive.Load(); m > 1 {
			t.Fatalf("server saw %d connections at once, want at most maxOpen=1", m)
		}
	})
}

func TestIdleConnectionExpiresAfterIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 2, 2, time.Second)

		c1 := mustGet(t, p)
		p.Put(c1, nil)
		time.Sleep(500 * time.Millisecond)
		if got := mustGet(t, p); got != c1 {
			t.Fatalf("Get after 500ms idle = %v, want %v: idleTimeout is 1s", got, c1)
		}
		p.Put(c1, nil)

		time.Sleep(1500 * time.Millisecond)
		c2 := mustGet(t, p)
		if c2 == c1 {
			t.Fatalf("Get returned %v after 1.5s idle, want it evicted (idleTimeout 1s)", c1)
		}
		if n := c1.closes.Load(); n != 1 {
			t.Fatalf("expired connection closed %d times, want 1", n)
		}
		assertStats(t, p, Stats{Open: 1}, "after eviction")

		// c2 is put back first and expires; c3 is still fresh when Get runs.
		c3 := mustGet(t, p)
		p.Put(c2, nil)
		time.Sleep(750 * time.Millisecond)
		p.Put(c3, nil)
		time.Sleep(500 * time.Millisecond)
		if got := mustGet(t, p); got != c3 {
			t.Fatalf("Get = %v, want the fresh idle %v", got, c3)
		}
		if n := c3.closes.Load(); n != 0 {
			t.Fatalf("fresh idle connection closed %d times", n)
		}
		if n := tr.dials.Load(); n != 3 {
			t.Fatalf("dials = %d, want 3", n)
		}
	})
}

func TestIdleSetIsBoundedByMaxIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 3, 1, time.Minute)

		a, b, c := mustGet(t, p), mustGet(t, p), mustGet(t, p)
		p.Put(a, nil)
		p.Put(b, nil)
		p.Put(c, nil)
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "three returned with maxIdle=1")
		for _, tc := range []struct {
			conn *fakeConn
			want int32
		}{{a, 0}, {b, 1}, {c, 1}} {
			if n := tc.conn.closes.Load(); n != tc.want {
				t.Errorf("%v closed %d times, want %d", tc.conn, n, tc.want)
			}
		}
		if got := mustGet(t, p); got != a {
			t.Fatalf("Get = %v, want the idle %v", got, a)
		}
		if n := tr.dials.Load(); n != 3 {
			t.Fatalf("dials = %d, want 3", n)
		}
	})
}

func TestDialFailureReleasesSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		var calls atomic.Int32
		p := mustNew(t, dialerFunc(func(context.Context) (Conn, error) {
			tr.start()
			if calls.Add(1) == 1 {
				tr.fail()
				return nil, errDial
			}
			return tr.succeed(), nil
		}), 1, 1, time.Minute)
		if _, err := p.Get(context.Background()); !errors.Is(err, errDial) {
			t.Fatalf("Get with a failing dial = %v, want %v", err, errDial)
		}
		assertStats(t, p, Stats{}, "after a failed dial")
		mustGet(t, p)

		// A dial fails while another caller waits: the slot passes to it.
		tr2 := &tracker{}
		gate := make(chan struct{})
		var calls2 atomic.Int32
		p2 := mustNew(t, dialerFunc(func(context.Context) (Conn, error) {
			tr2.start()
			if calls2.Add(1) == 1 {
				<-gate
				tr2.fail()
				return nil, errDial
			}
			return tr2.succeed(), nil
		}), 1, 1, time.Minute)
		first := goGet(p2, context.Background())
		synctest.Wait()
		assertStats(t, p2, Stats{Open: 1}, "dial in progress")
		w := goGet(p2, context.Background())
		assertBlocked(t, w, "waiter behind a dial in progress")
		assertStats(t, p2, Stats{Open: 1, Waiting: 1}, "waiter behind a dial in progress")

		close(gate)
		if r := receive(t, first, "failed dial"); !errors.Is(r.err, errDial) {
			t.Fatalf("first Get = %v, %v; want %v", r.conn, r.err, errDial)
		}
		if r := receive(t, w, "waiter"); r.err != nil || r.conn == nil {
			t.Fatalf("waiter got %v, %v; want a connection dialed with the freed slot", r.conn, r.err)
		}
		assertStats(t, p2, Stats{Open: 1}, "waiter holds the new connection")
		if n := tr2.dials.Load(); n != 2 {
			t.Fatalf("dials = %d, want 2", n)
		}
	})
}

func TestDialUsesCallerContextOutsideTheMutex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey struct{}
		tr := &tracker{}
		var calls atomic.Int32
		seen := make(chan any, 1)
		p := mustNew(t, dialerFunc(func(ctx context.Context) (Conn, error) {
			tr.start()
			if calls.Add(1) == 1 {
				return tr.succeed(), nil
			}
			seen <- ctx.Value(ctxKey{})
			<-ctx.Done()
			tr.fail()
			return nil, ctx.Err()
		}), 2, 2, time.Minute)
		c1 := mustGet(t, p)

		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "caller"))
		g := goGet(p, ctx)
		synctest.Wait()

		// The second dial is in progress; the pool must keep working.
		assertStats(t, p, Stats{Open: 2}, "one connection out, one dial in progress")
		p.Put(c1, nil)
		assertStats(t, p, Stats{Open: 2, Idle: 1}, "Put during a dial")
		if got := mustGet(t, p); got != c1 {
			t.Fatalf("Get during a dial = %v, want the idle %v", got, c1)
		}
		p.Put(c1, nil)
		assertBlocked(t, g, "dialing Get")
		if v := <-seen; v != "caller" {
			t.Fatal("Dial did not receive the caller's context (or a context derived from it)")
		}

		cancel()
		if r := receive(t, g, "dialing Get"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Get whose dial was cancelled = %v, %v; want context.Canceled", r.conn, r.err)
		}
		assertStats(t, p, Stats{Open: 1, Idle: 1}, "after the cancelled dial")
	})
}

func TestSlotIsReleasedOnlyAfterCloseReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 1, 1, time.Minute)
		c1 := mustGet(t, p)
		gate := make(chan struct{})
		c1.closeGate = gate

		w := goGet(p, context.Background())
		assertBlocked(t, w, "waiter")
		putDone := make(chan struct{})
		go func() {
			p.Put(c1, errBroken)
			close(putDone)
		}()
		synctest.Wait()

		// c1.Close has not returned: the server still sees c1.
		assertStats(t, p, Stats{Open: 1, Waiting: 1}, "while the broken connection is closing")
		assertBlocked(t, w, "waiter while the broken connection is closing")
		if n := tr.dials.Load(); n != 1 {
			t.Fatalf("dials = %d while c1 is still closing, want 1", n)
		}

		close(gate)
		r := receive(t, w, "waiter after Close returned")
		if r.err != nil || r.conn == nil || r.conn == c1 {
			t.Fatalf("waiter got %v, %v; want a newly dialed connection", r.conn, r.err)
		}
		select {
		case <-putDone:
		default:
			t.Fatal("Put has not returned after Close returned")
		}
		assertStats(t, p, Stats{Open: 1}, "waiter holds the new connection")
		if m := tr.maxLive.Load(); m > 1 {
			t.Fatalf("server saw %d connections at once, want at most maxOpen=1", m)
		}
	})
}

func TestCloseWakesWaitersClosesIdleAndRejectsGet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Close closes idle connections; Put closes checked-out ones later.
		tr := &tracker{}
		p := mustNew(t, tr.dialer(), 3, 2, time.Minute)
		idle1, idle2, out := mustGet(t, p), mustGet(t, p), mustGet(t, p)
		p.Put(idle1, nil)
		p.Put(idle2, nil)
		assertStats(t, p, Stats{Open: 3, Idle: 2}, "two idle, one checked out")

		if err := p.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		for _, c := range []*fakeConn{idle1, idle2} {
			if n := c.closes.Load(); n != 1 {
				t.Errorf("idle %v closed %d times by Close, want 1", c, n)
			}
		}
		if n := out.closes.Load(); n != 0 {
			t.Fatalf("checked-out connection closed %d times by Close, want 0", n)
		}
		assertStats(t, p, Stats{Open: 1}, "only the checked-out connection holds a slot")
		if _, err := p.Get(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Get after Close = %v, want ErrClosed", err)
		}
		p.Put(out, nil)
		assertStats(t, p, Stats{}, "checked-out connection returned after Close")
		if err := p.Close(); err != nil {
			t.Fatalf("second Close returned error: %v", err)
		}
		tr.assertAllClosedOnce(t)

		// Waiters are woken with ErrClosed.
		tr2 := &tracker{}
		p2 := mustNew(t, tr2.dialer(), 1, 1, time.Minute)
		held := mustGet(t, p2)
		w1 := goGet(p2, context.Background())
		w2 := goGet(p2, context.Background())
		assertBlocked(t, w1, "first waiter")
		assertBlocked(t, w2, "second waiter")
		if err := p2.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		for i, w := range []<-chan result{w1, w2} {
			if r := receive(t, w, fmt.Sprintf("waiter %d", i+1)); !errors.Is(r.err, ErrClosed) {
				t.Fatalf("waiter %d got %v, %v; want ErrClosed", i+1, r.conn, r.err)
			}
		}
		assertStats(t, p2, Stats{Open: 1}, "waiters gone, one connection checked out")
		p2.Put(held, nil)
		assertStats(t, p2, Stats{}, "after the last Put")
		tr2.assertAllClosedOnce(t)

		// Close does not wait for a dial in progress (if it did, this test
		// would deadlock). The finished dial either reaches the caller, whose
		// Put closes it, or is closed by Get.
		tr3 := &tracker{}
		gate := make(chan struct{})
		p3 := mustNew(t, dialerFunc(func(context.Context) (Conn, error) {
			tr3.start()
			<-gate
			return tr3.succeed(), nil
		}), 1, 1, time.Minute)
		g := goGet(p3, context.Background())
		synctest.Wait()
		if err := p3.Close(); err != nil {
			t.Fatalf("Close during a dial returned error: %v", err)
		}
		close(gate)
		switch r := receive(t, g, "Get whose dial finished after Close"); {
		case r.err == nil:
			p3.Put(r.conn, nil)
		case errors.Is(r.err, ErrClosed):
		default:
			t.Fatalf("Get got %v, %v; want the connection or ErrClosed", r.conn, r.err)
		}
		assertStats(t, p3, Stats{}, "after the late dial was cleaned up")
		tr3.assertAllClosedOnce(t)
	})
}

// Many callers Get and Put concurrently with slow dials, failing dials,
// broken connections, timeouts, and idle expiry; then several goroutines
// Close the pool while it is in use.
func TestConcurrentUseRespectsLimitsAndCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxOpen, maxIdle = 3, 2
		tr := &tracker{}
		var calls atomic.Int32
		p := mustNew(t, dialerFunc(func(context.Context) (Conn, error) {
			tr.start()
			time.Sleep(time.Millisecond)
			if calls.Add(1)%4 == 0 {
				tr.fail()
				return nil, errDial
			}
			return tr.succeed(), nil
		}), maxOpen, maxIdle, 5*time.Millisecond)

		stop := make(chan struct{})
		var sampler sync.WaitGroup
		sampler.Go(func() {
			for {
				if s := p.Stats(); s.Open > maxOpen || s.Idle > maxIdle || s.Idle > s.Open || s.Waiting < 0 {
					t.Errorf("Stats = %+v, want Open <= %d, Idle <= %d, Idle <= Open", s, maxOpen, maxIdle)
					return
				}
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Microsecond):
				}
			}
		})

		var workers sync.WaitGroup
		for i := range 12 {
			workers.Go(func() {
				for j := 0; ; j++ {
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1+(i+j)%4)*time.Millisecond)
					conn, err := p.Get(ctx)
					cancel()
					switch {
					case errors.Is(err, ErrClosed):
						return
					case errors.Is(err, errDial), errors.Is(err, context.DeadlineExceeded):
						continue
					case err != nil:
						t.Errorf("Get = %v, want a connection, ErrClosed, %v, or context.DeadlineExceeded", err, errDial)
						return
					}
					time.Sleep(time.Duration(1+(i*j)%3) * time.Millisecond)
					var useErr error
					if (i+j)%5 == 0 {
						useErr = errBroken
					}
					p.Put(conn, useErr)
				}
			})
		}

		time.Sleep(50 * time.Millisecond)
		var closers sync.WaitGroup
		for range 3 {
			closers.Go(func() {
				if err := p.Close(); err != nil {
					t.Errorf("Close returned error: %v", err)
				}
			})
		}
		closers.Wait()
		workers.Wait()
		close(stop)
		sampler.Wait()

		if m := tr.maxLive.Load(); m > maxOpen {
			t.Errorf("server saw %d connections and dials at once, want at most maxOpen=%d", m, maxOpen)
		}
		if n := tr.dials.Load(); n <= maxOpen {
			t.Errorf("only %d dials happened; the test did not exercise the pool", n)
		}
		assertStats(t, p, Stats{}, "after Close and the last Put")
		tr.assertAllClosedOnce(t)
	})
}
