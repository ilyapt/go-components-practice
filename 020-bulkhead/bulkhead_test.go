package bulkhead

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

func mustNew(t *testing.T, cfg Config) *Bulkhead {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return b
}

// uniform returns a Config with the same limits for every dependency.
func uniform(maxConcurrent, maxWaiting int) Config {
	return Config{Default: Limits{MaxConcurrent: maxConcurrent, MaxWaiting: maxWaiting}}
}

// goDo runs Do in a new goroutine and returns a channel with its result.
func goDo(b *Bulkhead, ctx context.Context, dep string, fn func(context.Context) error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- b.Do(ctx, dep, fn) }()
	return ch
}

// goShutdown runs Shutdown in a new goroutine and returns a channel with its result.
func goShutdown(b *Bulkhead, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- b.Shutdown(ctx) }()
	return ch
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		return err
	default:
		t.Fatalf("%s: has not returned", what)
		return nil
	}
}

func assertBlocked(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		t.Fatalf("%s: returned %v; want it still waiting", what, err)
	default:
	}
}

// gate is a fn that counts its calls and blocks until the gate is opened.
// It ignores its context on purpose: a bulkhead must not rely on fn giving up.
type gate struct {
	release chan struct{}
	calls   atomic.Int32
}

func newGate() *gate { return &gate{release: make(chan struct{})} }

func (g *gate) fn(context.Context) error {
	g.calls.Add(1)
	<-g.release
	return nil
}

func (g *gate) open() { close(g.release) }

func ok(context.Context) error { return nil }

// mustNotRun returns a fn that fails the test if it is ever called.
func mustNotRun(t *testing.T, what string) func(context.Context) error {
	return func(context.Context) error {
		t.Errorf("%s: fn was called, want it never called", what)
		return nil
	}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name string
			cfg  Config
		}{
			{name: "zero MaxConcurrent", cfg: uniform(0, 1)},
			{name: "negative MaxConcurrent", cfg: uniform(-1, 1)},
			{name: "negative MaxWaiting", cfg: uniform(1, -1)},
			{name: "override with zero MaxConcurrent", cfg: Config{
				Default:   Limits{MaxConcurrent: 1, MaxWaiting: 1},
				Overrides: map[string]Limits{"db": {MaxConcurrent: 0, MaxWaiting: 1}},
			}},
			{name: "override with negative MaxWaiting", cfg: Config{
				Default:   Limits{MaxConcurrent: 1, MaxWaiting: 1},
				Overrides: map[string]Limits{"db": {MaxConcurrent: 1, MaxWaiting: -1}},
			}},
		} {
			b, err := New(tc.cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if b != nil {
				t.Errorf("%s: New returned a non-nil bulkhead with an error", tc.name)
			}
		}

		b := mustNew(t, uniform(1, 0)) // a zero waiting room is valid
		if err := b.Do(context.Background(), "db", nil); !errors.Is(err, ErrNilFunc) {
			t.Fatalf("Do with nil fn = %v, want ErrNilFunc", err)
		}
		if err := b.Do(context.Background(), "db", ok); err != nil {
			t.Fatalf("Do after a nil fn = %v, want nil: a nil fn must not take the slot", err)
		}
	})
}

func TestRunsAtMostMaxConcurrentPerDependency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxConcurrent, callers = 3, 20
		release := make(chan struct{})
		var mu sync.Mutex
		active, maxActive := 0, 0
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
		b := mustNew(t, uniform(maxConcurrent, callers-maxConcurrent))

		results := make([]<-chan error, callers)
		for i := range results {
			results[i] = goDo(b, context.Background(), "db", fn)
		}
		synctest.Wait()
		mu.Lock()
		running := active
		mu.Unlock()
		if running != maxConcurrent {
			t.Fatalf("%d calls running with %d callers, want exactly %d (one per slot)", running, callers, maxConcurrent)
		}
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call beyond the waiting room")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do with every slot and waiting place taken = %v, want ErrBulkheadFull", err)
		}

		close(release)
		for i, ch := range results {
			if err := receive(t, ch, fmt.Sprintf("caller %d", i)); err != nil {
				t.Fatalf("caller %d: Do = %v, want nil", i, err)
			}
		}
		if maxActive > maxConcurrent {
			t.Fatalf("max concurrent fn calls = %d, want <= %d", maxActive, maxConcurrent)
		}
	})
}

func TestWaitingRoomIsBoundedAndRejectsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGate()
		b := mustNew(t, uniform(1, 2))

		running := goDo(b, context.Background(), "db", g.fn)
		synctest.Wait()
		waiters := []<-chan error{
			goDo(b, context.Background(), "db", g.fn),
			goDo(b, context.Background(), "db", g.fn),
		}
		for i, ch := range waiters {
			assertBlocked(t, ch, fmt.Sprintf("waiter %d", i))
		}
		if got := g.calls.Load(); got != 1 {
			t.Fatalf("fn running %d times with one slot, want 1: waiters must not run", got)
		}

		// A Do that blocked on a full waiting room would deadlock here, which
		// synctest reports as a failure.
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call over a full waiting room")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do with a full waiting room = %v, want ErrBulkheadFull", err)
		}

		g.open()
		if err := receive(t, running, "running call"); err != nil {
			t.Fatalf("running call: Do = %v, want nil", err)
		}
		for i, ch := range waiters {
			if err := receive(t, ch, fmt.Sprintf("waiter %d", i)); err != nil {
				t.Fatalf("waiter %d: Do = %v, want nil once a slot frees up", i, err)
			}
		}
		if got := g.calls.Load(); got != 3 {
			t.Fatalf("fn called %d times, want 3", got)
		}
	})
}

func TestCancelledWaiterLeavesWaitingRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGate()
		b := mustNew(t, uniform(1, 1))

		running := goDo(b, context.Background(), "db", g.fn)
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		waiter := goDo(b, ctx, "db", mustNotRun(t, "cancelled waiter"))
		assertBlocked(t, waiter, "waiter")
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call over a full waiting room")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do with a full waiting room = %v, want ErrBulkheadFull", err)
		}

		cancel()
		if err := receive(t, waiter, "cancelled waiter"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter: Do = %v, want context.Canceled", err)
		}

		// The cancelled waiter's place is free again: the next call waits
		// instead of being rejected.
		next := goDo(b, context.Background(), "db", g.fn)
		assertBlocked(t, next, "call after the cancelled waiter left (ErrBulkheadFull means its place was not freed)")

		g.open()
		for what, ch := range map[string]<-chan error{"running call": running, "next waiter": next} {
			if err := receive(t, ch, what); err != nil {
				t.Fatalf("%s: Do = %v, want nil", what, err)
			}
		}
		if got := g.calls.Load(); got != 2 {
			t.Fatalf("fn called %d times, want 2", got)
		}
	})
}

func TestPreCancelledCallTakesNoSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, uniform(1, 0))
		ctx := cancelledContext()

		if err := b.Do(ctx, "db", mustNotRun(t, "pre-cancelled call")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do with a cancelled ctx = %v, want context.Canceled", err)
		}
		if err := b.Do(context.Background(), "db", ok); err != nil {
			t.Fatalf("Do after a pre-cancelled call = %v, want nil: the pre-cancelled call must not keep the slot", err)
		}

		// A done context is checked before capacity.
		g := newGate()
		running := goDo(b, context.Background(), "db", g.fn)
		synctest.Wait()
		if err := b.Do(ctx, "db", mustNotRun(t, "pre-cancelled call on a full bulkhead")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do with a cancelled ctx on a full dependency = %v, want context.Canceled", err)
		}
		g.open()
		if err := receive(t, running, "running call"); err != nil {
			t.Fatalf("running call: Do = %v, want nil", err)
		}
	})
}

func TestFnReceivesCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		b := mustNew(t, uniform(1, 0))
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey("request"), "req-42"))
		defer cancel()

		var seen any
		res := goDo(b, ctx, "db", func(ctx context.Context) error {
			seen = ctx.Value(ctxKey("request"))
			<-ctx.Done()
			return ctx.Err()
		})
		assertBlocked(t, res, "call before its caller is cancelled")

		cancel()
		if err := receive(t, res, "call after its caller is cancelled"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do = %v, want context.Canceled from fn: fn's context must end with the caller's", err)
		}
		if seen != "req-42" {
			t.Fatal("fn did not receive the caller's context (or a context derived from it)")
		}
	})
}

func TestDoWaitsForFnAfterCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errSlow := errors.New("slow call failed")
		release := make(chan struct{})
		b := mustNew(t, uniform(1, 0))

		ctx, cancel := context.WithCancel(context.Background())
		res := goDo(b, ctx, "db", func(ctx context.Context) error {
			<-ctx.Done()
			<-release // fn takes a while to give up
			return errSlow
		})
		synctest.Wait()

		cancel()
		assertBlocked(t, res, "Do whose caller was cancelled while fn still runs")
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call while fn still runs")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do while fn still runs = %v, want ErrBulkheadFull: the slot is held until fn returns", err)
		}

		close(release)
		if err := receive(t, res, "Do after fn returned"); !errors.Is(err, errSlow) {
			t.Fatalf("Do = %v, want fn's own error %v", err, errSlow)
		}
		if err := b.Do(context.Background(), "db", ok); err != nil {
			t.Fatalf("Do after a failed call = %v, want nil: the slot is released whatever fn returns", err)
		}
	})
}

func TestSaturatedDependencyDoesNotAffectOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow, fast := newGate(), newGate()
		b := mustNew(t, uniform(1, 1))

		// Saturate search: its slot and its waiting place are taken.
		slowRunning := goDo(b, context.Background(), "search", slow.fn)
		synctest.Wait()
		slowWaiting := goDo(b, context.Background(), "search", slow.fn)
		assertBlocked(t, slowWaiting, "search waiter")
		if err := b.Do(context.Background(), "search", mustNotRun(t, "call to saturated search")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do for saturated search = %v, want ErrBulkheadFull", err)
		}

		// payments has its own slot...
		if err := b.Do(context.Background(), "payments", ok); err != nil {
			t.Fatalf("Do for payments while search is saturated = %v, want nil", err)
		}
		// ...and its own waiting room.
		fastRunning := goDo(b, context.Background(), "payments", fast.fn)
		synctest.Wait()
		fastWaiting := goDo(b, context.Background(), "payments", fast.fn)
		assertBlocked(t, fastWaiting, "payments waiter (ErrBulkheadFull means the waiting room is shared)")
		if err := b.Do(context.Background(), "payments", mustNotRun(t, "call to saturated payments")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("Do for saturated payments = %v, want ErrBulkheadFull", err)
		}

		fast.open()
		for what, ch := range map[string]<-chan error{"payments call": fastRunning, "payments waiter": fastWaiting} {
			if err := receive(t, ch, what); err != nil {
				t.Fatalf("%s: Do = %v, want nil", what, err)
			}
		}
		assertBlocked(t, slowRunning, "search call while payments completes")

		slow.open()
		for what, ch := range map[string]<-chan error{"search call": slowRunning, "search waiter": slowWaiting} {
			if err := receive(t, ch, what); err != nil {
				t.Fatalf("%s: Do = %v, want nil", what, err)
			}
		}
	})
}

func TestPerDependencyOverrides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		overrides := map[string]Limits{"search": {MaxConcurrent: 3, MaxWaiting: 0}}
		b := mustNew(t, Config{Default: Limits{MaxConcurrent: 1, MaxWaiting: 0}, Overrides: overrides})

		// Changing the caller's map after New must not change the bulkhead.
		overrides["search"] = Limits{MaxConcurrent: 1, MaxWaiting: 0}
		overrides["db"] = Limits{MaxConcurrent: 5, MaxWaiting: 0}

		g := newGate()
		var results []<-chan error
		for range 3 {
			results = append(results, goDo(b, context.Background(), "search", g.fn))
		}
		synctest.Wait()
		if got := g.calls.Load(); got != 3 {
			t.Fatalf("search runs %d calls at once, want 3 from its override as given to New", got)
		}
		if err := b.Do(context.Background(), "search", mustNotRun(t, "fourth search call")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("fourth search call = %v, want ErrBulkheadFull", err)
		}

		results = append(results, goDo(b, context.Background(), "db", g.fn))
		synctest.Wait()
		if err := b.Do(context.Background(), "db", mustNotRun(t, "second db call")); !errors.Is(err, ErrBulkheadFull) {
			t.Fatalf("second db call = %v, want ErrBulkheadFull: db has no override in New and uses Default", err)
		}

		g.open()
		for i, ch := range results {
			if err := receive(t, ch, fmt.Sprintf("call %d", i)); err != nil {
				t.Fatalf("call %d: Do = %v, want nil", i, err)
			}
		}
	})
}

func TestShutdownReleasesWaitersAndWaitsForRunningCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGate()
		b := mustNew(t, uniform(1, 1))

		running := goDo(b, context.Background(), "db", g.fn)
		synctest.Wait()
		waiter := goDo(b, context.Background(), "db", mustNotRun(t, "waiter released by Shutdown"))
		assertBlocked(t, waiter, "waiter")

		done := goShutdown(b, context.Background())
		if err := receive(t, waiter, "waiter during Shutdown"); !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter during Shutdown: Do = %v, want ErrClosed", err)
		}
		assertBlocked(t, done, "Shutdown while a call is running")
		for _, dep := range []string{"db", "never-used"} {
			if err := b.Do(context.Background(), dep, mustNotRun(t, "call during Shutdown")); !errors.Is(err, ErrClosed) {
				t.Fatalf("Do(%q) during Shutdown = %v, want ErrClosed", dep, err)
			}
		}

		g.open()
		if err := receive(t, running, "running call"); err != nil {
			t.Fatalf("running call: Do = %v, want nil", err)
		}
		if err := receive(t, done, "Shutdown after the running call returned"); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if err := b.Shutdown(cancelledContext()); err != nil {
			t.Fatalf("Shutdown after completion = %v, want nil", err)
		}
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call after Shutdown")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Do after Shutdown = %v, want ErrClosed", err)
		}
	})
}

func TestShutdownDeadlineDoesNotCancelCallerWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		b := mustNew(t, uniform(1, 0))
		running := goDo(b, context.Background(), "db", func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		done := goShutdown(b, ctx)
		assertBlocked(t, done, "Shutdown before its deadline")

		time.Sleep(10 * time.Millisecond)
		if err := receive(t, done, "Shutdown after its deadline"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
		}
		assertBlocked(t, running, "running call after the shutdown deadline (the bulkhead must not cancel caller work)")
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call after an expired Shutdown")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Do after an expired Shutdown = %v, want ErrClosed", err)
		}

		close(release)
		if err := receive(t, running, "running call"); err != nil {
			t.Fatalf("running call: Do = %v, want nil", err)
		}
		if err := b.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown after the running call returned = %v, want nil", err)
		}
	})
}

func TestConcurrentShutdownReturnsItsOwnContextError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGate()
		b := mustNew(t, uniform(1, 0))
		running := goDo(b, context.Background(), "db", g.fn)
		synctest.Wait()

		shutdowns := make([]<-chan error, 8)
		for i := range shutdowns {
			shutdowns[i] = goShutdown(b, context.Background())
		}
		for i, ch := range shutdowns {
			assertBlocked(t, ch, fmt.Sprintf("Shutdown %d while a call is running", i))
		}
		if err := b.Shutdown(cancelledContext()); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown with a cancelled ctx while a call is running = %v, want context.Canceled", err)
		}
		if err := b.Do(context.Background(), "db", mustNotRun(t, "call during Shutdown")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Do during Shutdown = %v, want ErrClosed", err)
		}

		g.open()
		if err := receive(t, running, "running call"); err != nil {
			t.Fatalf("running call: Do = %v, want nil", err)
		}
		for i, ch := range shutdowns {
			if err := receive(t, ch, fmt.Sprintf("Shutdown %d", i)); err != nil {
				t.Fatalf("Shutdown %d = %v, want nil", i, err)
			}
		}
	})
}

// A slot frees up at the same instant as the waiter's deadline expires. The
// waiter may run fn or give up, but the slot and the waiting place must both
// be free afterwards.
func TestWaiterCancelledAtSlotHandoffDoesNotLeakCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := mustNew(t, uniform(1, 1))
		for i := range 20 {
			dep := fmt.Sprintf("dep-%d", i)
			running := goDo(b, context.Background(), dep, func(context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			})
			synctest.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			waiter := goDo(b, ctx, dep, ok)
			assertBlocked(t, waiter, dep+" waiter")

			time.Sleep(10 * time.Millisecond)
			if err := receive(t, running, dep+" running call"); err != nil {
				t.Fatalf("%s running call: Do = %v, want nil", dep, err)
			}
			if err := receive(t, waiter, dep+" waiter"); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s waiter: Do = %v, want nil or context.DeadlineExceeded", dep, err)
			}
			cancel()

			g := newGate()
			holder := goDo(b, context.Background(), dep, g.fn)
			synctest.Wait()
			if got := g.calls.Load(); got != 1 {
				t.Fatalf("%s: a new call did not get the free slot: the slot leaked at the handoff", dep)
			}
			next := goDo(b, context.Background(), dep, g.fn)
			assertBlocked(t, next, dep+" next waiter (ErrBulkheadFull means the waiting place leaked)")
			g.open()
			for what, ch := range map[string]<-chan error{"holder": holder, "next waiter": next} {
				if err := receive(t, ch, dep+" "+what); err != nil {
					t.Fatalf("%s %s: Do = %v, want nil", dep, what, err)
				}
			}
		}
	})
}

func TestConcurrentDoAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const perDep = 2
		deps := []string{"a", "b", "c", "d"}
		b := mustNew(t, uniform(perDep, 2))

		var active [4]atomic.Int32
		var total, calls, admitted atomic.Int32
		var drained atomic.Bool
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				d := i % len(deps)
				err := b.Do(context.Background(), deps[d], func(context.Context) error {
					if drained.Load() {
						t.Errorf("fn for %s started after Shutdown returned", deps[d])
					}
					calls.Add(1)
					total.Add(1)
					if n := active[d].Add(1); n > perDep {
						t.Errorf("%d calls of %s running at once, want <= %d", n, deps[d], perDep)
					}
					time.Sleep(time.Millisecond)
					active[d].Add(-1)
					total.Add(-1)
					return nil
				})
				switch {
				case err == nil:
					admitted.Add(1)
				case errors.Is(err, ErrBulkheadFull), errors.Is(err, ErrClosed):
				default:
					t.Errorf("Do(%s) = %v, want nil, ErrBulkheadFull, or ErrClosed", deps[d], err)
				}
			})
		}
		wg.Go(func() {
			time.Sleep(time.Millisecond)
			if err := b.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown = %v, want nil", err)
			}
			if n := total.Load(); n != 0 {
				t.Errorf("Shutdown returned while %d fn calls were still running", n)
			}
			drained.Store(true)
		})
		wg.Wait()

		if c, a := calls.Load(), admitted.Load(); c != a {
			t.Fatalf("fn called %d times, but Do returned nil %d times", c, a)
		}
	})
}
