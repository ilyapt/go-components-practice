package refreshcoordinator

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func TestBoundedQueueAndQueuedCoalescing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		releaseFirst := newGate(t)
		var calls callLog
		c := mustNew(t, context.Background(), 1, 2, func(_ context.Context, id string) (State, error) {
			calls.add(id)
			if id == "running" {
				close(firstStarted)
				releaseFirst.wait()
			}
			return State{Value: id}, nil
		})

		mustTrigger(t, c, "running")
		<-firstStarted
		mustTrigger(t, c, "queued-1", "queued-2", "queued-1")
		if err := c.Trigger(context.Background(), "overflow"); !errors.Is(err, ErrFull) {
			t.Fatalf("overflow Trigger error = %v, want ErrFull", err)
		}
		mustTrigger(t, c, "queued-2") // already queued: coalesced even though the queue is full

		releaseFirst.open()
		mustShutdown(t, c)
		if got, want := calls.get(), []string{"running", "queued-1", "queued-2"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("loader calls = %v, want %v", got, want)
		}
		if _, ok := c.Current().Get("overflow"); ok {
			t.Fatal("a rejected resource was loaded")
		}
	})
}

func TestDirtyTriggersCoalesceIntoOneFollowUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		releaseFirst := newGate(t)
		secondStarted := make(chan struct{})
		releaseSecond := newGate(t)
		var calls atomic.Int32
		c := mustNew(t, context.Background(), 1, 1, func(context.Context, string) (State, error) {
			call := calls.Add(1)
			switch call {
			case 1:
				close(firstStarted)
				releaseFirst.wait()
			case 2:
				close(secondStarted)
				releaseSecond.wait()
			default:
				t.Errorf("unexpected loader call %d", call)
			}
			return State{SourceVersion: uint64(call), Value: "value"}, nil
		})

		mustTrigger(t, c, "resource")
		<-firstStarted
		for range 10 {
			mustTrigger(t, c, "resource")
		}
		releaseFirst.open()
		synctest.Wait()
		select {
		case <-secondStarted:
		default:
			t.Fatal("dirty follow-up load did not start after the running load returned")
		}
		releaseSecond.open()
		mustShutdown(t, c)

		if got := calls.Load(); got != 2 {
			t.Fatalf("loader calls = %d, want 2", got)
		}
		snapshot := c.Current()
		if snapshot.Version() != 3 {
			t.Fatalf("snapshot version = %d, want 3", snapshot.Version())
		}
		if state, ok := snapshot.Get("resource"); !ok || state.SourceVersion != 2 {
			t.Fatalf("published state = (%+v, %v), want source version 2", state, ok)
		}
	})
}

func TestTriggerDuringFollowUpSchedulesOneMore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan uint64, 1)
		release := []*gate{newGate(t), newGate(t), newGate(t), newGate(t)}
		var calls atomic.Uint64
		c := mustNew(t, context.Background(), 1, 1, func(context.Context, string) (State, error) {
			call := calls.Add(1)
			if call > uint64(len(release)) {
				t.Errorf("unexpected loader call %d", call)
				return State{}, errors.New("unexpected call")
			}
			started <- call
			release[call-1].wait()
			return State{SourceVersion: call}, nil
		})

		waitStarted := func(want uint64) {
			t.Helper()
			synctest.Wait()
			select {
			case got := <-started:
				if got != want {
					t.Fatalf("started load %d, want %d", got, want)
				}
			default:
				t.Fatalf("load %d did not start", want)
			}
		}
		mustTrigger(t, c, "r")
		waitStarted(1)
		mustTrigger(t, c, "r", "r") // dirty during load 1
		release[0].open()
		waitStarted(2)
		mustTrigger(t, c, "r", "r") // dirty during follow-up load 2
		release[1].open()
		waitStarted(3)
		mustTrigger(t, c, "r") // dirty during load 3
		release[2].open()
		waitStarted(4)
		release[3].open()
		mustShutdown(t, c)

		if got := calls.Load(); got != 4 {
			t.Fatalf("loader calls = %d, want 4", got)
		}
		if state, _ := c.Current().Get("r"); state.SourceVersion != 4 {
			t.Fatalf("published source version = %d, want 4", state.SourceVersion)
		}
	})
}

func TestSuccessfulLoadIsPublishedBeforeFollowUpFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		releaseFirst := newGate(t)
		secondStarted := make(chan struct{})
		releaseSecond := newGate(t)
		var calls atomic.Int32
		c := mustNew(t, context.Background(), 1, 1, func(context.Context, string) (State, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				releaseFirst.wait()
				return State{SourceVersion: 1, Value: "first"}, nil
			default:
				close(secondStarted)
				releaseSecond.wait()
				return State{SourceVersion: 2, Value: "second"}, nil
			}
		})

		mustTrigger(t, c, "resource")
		<-firstStarted
		mustTrigger(t, c, "resource")
		releaseFirst.open()
		synctest.Wait()
		select {
		case <-secondStarted:
		default:
			t.Fatal("dirty follow-up load did not start after the running load returned")
		}

		snapshot := c.Current()
		if state, ok := snapshot.Get("resource"); !ok || state.Value != "first" || snapshot.Version() != 2 {
			t.Fatalf("while the follow-up runs: state = (%+v, %v), version %d; want the first result published at version 2", state, ok, snapshot.Version())
		}
		releaseSecond.open()
		mustShutdown(t, c)
		if state, _ := c.Current().Get("resource"); state.Value != "second" {
			t.Fatalf("final state = %+v, want the follow-up result", state)
		}
	})
}

func TestSuccessfulLoadRemainsPublishedWhenFollowUpFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		releaseFirst := newGate(t)
		var calls atomic.Int32
		c := mustNew(t, context.Background(), 1, 1, func(context.Context, string) (State, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				releaseFirst.wait()
				return State{SourceVersion: 1, Value: "first"}, nil
			}
			return State{}, errors.New("follow-up failed")
		})

		mustTrigger(t, c, "resource")
		<-firstStarted
		mustTrigger(t, c, "resource")
		releaseFirst.open()
		mustShutdown(t, c)

		if got := calls.Load(); got != 2 {
			t.Fatalf("loader calls = %d, want 2", got)
		}
		snapshot := c.Current()
		if snapshot.Version() != 2 {
			t.Fatalf("snapshot version = %d, want 2 (a failed load publishes nothing)", snapshot.Version())
		}
		if state, ok := snapshot.Get("resource"); !ok || state.Value != "first" {
			t.Fatalf("published state = (%+v, %v), want the first success", state, ok)
		}
	})
}

func TestFailedLoadDoesNotBlockLaterTriggers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := mustNew(t, context.Background(), 1, 1, func(_ context.Context, id string) (State, error) {
			if calls.Add(1) == 1 {
				return State{}, errors.New("load failed")
			}
			return State{Value: id}, nil
		})

		mustTrigger(t, c, "resource")
		synctest.Wait()
		mustTrigger(t, c, "resource") // the failed load must have cleared its state
		synctest.Wait()
		mustShutdown(t, c)
		if got := calls.Load(); got != 2 {
			t.Fatalf("loader calls = %d, want 2", got)
		}
		if state, ok := c.Current().Get("resource"); !ok || state.Value != "resource" {
			t.Fatalf("published state = (%+v, %v), want the second load", state, ok)
		}
	})
}

func TestDifferentResourcesLoadConcurrentlyWithoutLostPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running atomic.Int32
		release := newGate(t)
		c := mustNew(t, context.Background(), 2, 2, func(_ context.Context, id string) (State, error) {
			running.Add(1)
			release.wait()
			return State{Value: id}, nil
		})
		old := c.Current()

		mustTrigger(t, c, "a", "b")
		if !eventually(func() bool { return running.Load() == 2 }) {
			t.Fatalf("loads running = %d, want 2; is a mutex held across the loader call?", running.Load())
		}
		release.open()
		mustShutdown(t, c)

		current := c.Current()
		if current.Version() != 3 || current.Len() != 2 {
			t.Fatalf("current snapshot = version %d, len %d; want version 3, len 2", current.Version(), current.Len())
		}
		for _, id := range []string{"a", "b"} {
			if state, ok := current.Get(id); !ok || state.Value != id {
				t.Fatalf("state %q = (%+v, %v)", id, state, ok)
			}
		}
		if old.Version() != 1 || old.Len() != 0 {
			t.Fatalf("old snapshot changed: version %d, len %d", old.Version(), old.Len())
		}
	})
}

func TestConcurrencyLimitAndNoOverlapPerResource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const concurrency = 3
		var mu sync.Mutex
		active := map[string]bool{}
		var current, maximum, total int
		var overlap bool
		release := newGate(t)
		c := mustNew(t, context.Background(), concurrency, 16, func(_ context.Context, id string) (State, error) {
			mu.Lock()
			if active[id] {
				overlap = true
			}
			active[id] = true
			current++
			total++
			maximum = max(maximum, current)
			mu.Unlock()

			release.wait()
			runtime.Gosched()

			mu.Lock()
			delete(active, id)
			current--
			mu.Unlock()
			return State{Value: id}, nil
		})
		running := func() int {
			mu.Lock()
			defer mu.Unlock()
			return current
		}

		ids := make([]string, 8)
		for i := range ids {
			ids[i] = "r" + strconv.Itoa(i)
		}
		mustTrigger(t, c, ids...)
		if !eventually(func() bool { return running() == concurrency }) {
			t.Fatalf("loads running = %d, want %d; is a mutex held across the loader call?", running(), concurrency)
		}
		synctest.Wait()
		mustTrigger(t, c, ids...) // 3 running become dirty, 5 queued are coalesced
		release.open()
		mustShutdown(t, c)

		mu.Lock()
		defer mu.Unlock()
		if maximum != concurrency {
			t.Fatalf("maximum concurrent loads = %d, want %d", maximum, concurrency)
		}
		if overlap {
			t.Fatal("two loads of the same resource ran at the same time")
		}
		if total != len(ids)+concurrency {
			t.Fatalf("loader calls = %d, want %d (one per resource plus one follow-up per running resource)", total, len(ids)+concurrency)
		}
		if got := c.Current().Len(); got != len(ids) {
			t.Fatalf("published resources = %d, want %d", got, len(ids))
		}
	})
}

func TestShutdownWaitsForRunningQueuedAndDirtyWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := newGate(t)
		var calls callLog
		c := mustNew(t, context.Background(), 1, 1, func(_ context.Context, id string) (State, error) {
			n := calls.add(id)
			if id == "a" && n == 1 {
				close(started)
				release.wait()
			}
			return State{SourceVersion: uint64(n), Value: id}, nil
		})

		mustTrigger(t, c, "a")
		<-started
		mustTrigger(t, c, "b", "a") // b queued, a dirty

		done := make(chan error, 1)
		go func() { done <- c.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v while a load was still running", err)
		default:
		}
		if err := c.Trigger(context.Background(), "late"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Trigger during shutdown = %v, want ErrClosed", err)
		}

		release.open()
		if err := <-done; err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
		if calls.count("a") != 2 || calls.count("b") != 1 || calls.count("late") != 0 {
			t.Fatalf("loader calls = %v, want a twice and b once", calls.get())
		}
		snapshot := c.Current()
		if a, _ := snapshot.Get("a"); a.SourceVersion != 2 {
			t.Fatalf("a = %+v, want the dirty follow-up result published before Shutdown returned", a)
		}
		if _, ok := snapshot.Get("b"); !ok {
			t.Fatal("queued resource b was not loaded before Shutdown returned")
		}
	})
}

func TestPreCancelledShutdownInitiatesDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		releaseFirst := newGate(t)
		var calls atomic.Int32
		var loaderCancelled atomic.Bool
		c := mustNew(t, context.Background(), 1, 1, func(ctx context.Context, id string) (State, error) {
			calls.Add(1)
			if id == "first" {
				close(firstStarted)
				releaseFirst.wait()
			}
			if ctx.Err() != nil {
				loaderCancelled.Store(true)
			}
			return State{Value: id}, nil
		})

		mustTrigger(t, c, "first")
		<-firstStarted
		mustTrigger(t, c, "queued")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := shutdownPromptly(t, c, ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Shutdown error = %v, want context.Canceled", err)
		}
		if err := trigger(t, c, "late"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Trigger after shutdown error = %v, want ErrClosed", err)
		}
		releaseFirst.open()
		mustShutdown(t, c)
		if got := calls.Load(); got != 2 {
			t.Fatalf("loader calls = %d, want 2", got)
		}
		if loaderCancelled.Load() {
			t.Fatal("a cancelled Shutdown waiter cancelled loader work")
		}
	})
}

func TestCancelledShutdownWaiterDoesNotCancelLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := newGate(t)
		loadErr := make(chan error, 1)
		c := mustNew(t, context.Background(), 1, 1, func(ctx context.Context, id string) (State, error) {
			close(started)
			release.wait()
			loadErr <- ctx.Err()
			return State{Value: id}, ctx.Err()
		})
		mustTrigger(t, c, "slow")
		<-started

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Shutdown(ctx) }()
		synctest.Wait()
		cancel()
		if err := receivePromptly(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown after its waiter was cancelled = %v, want context.Canceled", err)
		}
		release.open()
		mustShutdown(t, c)
		if err := <-loadErr; err != nil {
			t.Fatalf("loader context was cancelled (%v) by a shutdown waiter", err)
		}
		if _, ok := c.Current().Get("slow"); !ok {
			t.Fatal("the slow load was not published")
		}
	})
}

type ctxKey struct{}

func TestLoaderUsesWorkContextNotTriggerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "work"))
		defer cancelWork()
		started := make(chan struct{})
		release := newGate(t)
		type observed struct {
			value any
			err   error
		}
		seen := make(chan observed, 1)
		c := mustNew(t, workCtx, 1, 1, func(ctx context.Context, id string) (State, error) {
			close(started)
			release.wait()
			seen <- observed{ctx.Value(ctxKey{}), ctx.Err()}
			return State{Value: id}, nil
		})

		triggerCtx, cancelTrigger := context.WithCancel(context.Background())
		if err := c.Trigger(triggerCtx, "r"); err != nil {
			t.Fatalf("Trigger returned error: %v", err)
		}
		<-started
		cancelTrigger()
		release.open()
		got := <-seen
		if got.value != "work" {
			t.Fatalf("loader context value = %v, want the workCtx passed to New", got.value)
		}
		if got.err != nil {
			t.Fatalf("loader context error = %v after the trigger context was cancelled, want nil", got.err)
		}
		mustShutdown(t, c)
	})
}

func TestConcurrentTriggerAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls callLog
		c := mustNew(t, context.Background(), 2, 8, func(_ context.Context, id string) (State, error) {
			calls.add(id)
			runtime.Gosched()
			return State{Value: id}, nil
		})

		var accepted sync.Map
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Go(func() {
				id := strconv.Itoa(i)
				switch err := c.Trigger(context.Background(), id); {
				case err == nil:
					accepted.Store(id, true)
				case errors.Is(err, ErrClosed), errors.Is(err, ErrFull):
				default:
					t.Errorf("Trigger(%q) = %v, want nil, ErrClosed, or ErrFull", id, err)
				}
			})
		}
		for range 3 {
			wg.Go(func() {
				if err := c.Shutdown(context.Background()); err != nil {
					t.Errorf("Shutdown returned error: %v", err)
				}
			})
		}
		wg.Wait()

		snapshot := c.Current()
		accepted.Range(func(k, _ any) bool {
			id := k.(string)
			if n := calls.count(id); n != 1 {
				t.Errorf("accepted resource %q loaded %d times, want 1", id, n)
			}
			if _, ok := snapshot.Get(id); !ok {
				t.Errorf("accepted resource %q is not published", id)
			}
			return true
		})
		if got, want := uint64(snapshot.Len())+1, snapshot.Version(); got != want {
			t.Errorf("snapshot has %d resources at version %d, want version = resources + 1", snapshot.Len(), want)
		}
		for _, id := range calls.get() {
			if _, ok := accepted.Load(id); !ok {
				t.Errorf("resource %q was loaded but Trigger did not accept it", id)
			}
		}
	})
}

func TestValidationAndCancelledTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		validLoader := func(context.Context, string) (State, error) { return State{}, nil }
		if _, err := New(context.Background(), 0, 1, validLoader); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("zero concurrency error = %v, want ErrInvalidConfig", err)
		}
		if _, err := New(context.Background(), 1, 0, validLoader); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("zero capacity error = %v, want ErrInvalidConfig", err)
		}
		if _, err := New(context.Background(), 1, 1, nil); !errors.Is(err, ErrNilLoader) {
			t.Fatalf("nil loader error = %v, want ErrNilLoader", err)
		}

		var calls atomic.Int32
		c := mustNew(t, context.Background(), 1, 1, func(context.Context, string) (State, error) {
			calls.Add(1)
			return State{}, nil
		})
		if current := c.Current(); current == nil || current.Version() != 1 || current.Len() != 0 {
			t.Fatalf("initial snapshot = %+v, want empty at version 1", current)
		}
		if err := c.Trigger(context.Background(), ""); !errors.Is(err, ErrInvalidID) {
			t.Fatalf("empty ID error = %v, want ErrInvalidID", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Trigger(ctx, "resource"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Trigger error = %v, want context.Canceled", err)
		}
		mustShutdown(t, c)
		if got := calls.Load(); got != 0 {
			t.Fatalf("rejected triggers invoked the loader %d times", got)
		}
		if current := c.Current(); current.Version() != 1 || current.Len() != 0 {
			t.Fatalf("rejected triggers changed the snapshot: version %d, len %d", current.Version(), current.Len())
		}
	})
}

func mustNew(t *testing.T, workCtx context.Context, concurrency, queueCapacity int, loader Loader) *Coordinator {
	t.Helper()
	c, err := New(workCtx, concurrency, queueCapacity, loader)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	// Stop the workers even if the test fails early. Shutdown runs in its own
	// goroutine so that gates opened by later cleanups can unblock loaders.
	t.Cleanup(func() { go c.Shutdown(context.Background()) })
	return c
}

func mustTrigger(t *testing.T, c *Coordinator, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := trigger(t, c, id); err != nil {
			t.Fatalf("Trigger(%q) returned error: %v", id, err)
		}
	}
}

// trigger calls Trigger and fails the test if it does not return promptly,
// for example because a lock is held while a loader runs.
func trigger(t *testing.T, c *Coordinator, id string) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Trigger(context.Background(), id) }()
	return receivePromptly(t, done)
}

// shutdownPromptly calls Shutdown with a context that is already done and
// fails the test if it does not return promptly.
func shutdownPromptly(t *testing.T, c *Coordinator, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Shutdown(ctx) }()
	return receivePromptly(t, done)
}

func receivePromptly(t *testing.T, ch <-chan error) error {
	t.Helper()
	var err error
	if !eventually(func() bool {
		select {
		case err = <-ch:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("call did not return promptly; is a mutex held while a loader runs?")
	}
	return err
}

func mustShutdown(t *testing.T, c *Coordinator) {
	t.Helper()
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

// callLog records loader calls in start order.
type callLog struct {
	mu  sync.Mutex
	ids []string
}

// add records a call and returns how many times id has been loaded so far.
func (l *callLog) add(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids = append(l.ids, id)
	n := 0
	for _, v := range l.ids {
		if v == id {
			n++
		}
	}
	return n
}

func (l *callLog) count(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, v := range l.ids {
		if v == id {
			n++
		}
	}
	return n
}

func (l *callLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ids...)
}

// gate releases blocked loader calls. Cleanup opens it, so a failing test
// never leaves goroutines blocked in the bubble.
type gate struct {
	once sync.Once
	ch   chan struct{}
}

func newGate(t *testing.T) *gate {
	g := &gate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

func (g *gate) wait() { <-g.ch }

// eventually polls cond, yielding the processor between attempts, and gives
// up after a bounded number of attempts. It is used where a buggy
// implementation could block a goroutine on a mutex: such a goroutine is not
// "durably blocked" for synctest, so synctest.Wait would hang instead of
// failing.
func eventually(cond func() bool) bool {
	for range 100_000 {
		if cond() {
			return true
		}
		runtime.Gosched()
	}
	return false
}
