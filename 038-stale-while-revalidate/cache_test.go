package stalewhilerevalidate

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

const (
	freshFor = 100 * time.Millisecond
	staleFor = time.Second
)

type loaderFunc func(context.Context, string) (Value, error)

func (f loaderFunc) Load(ctx context.Context, key string) (Value, error) { return f(ctx, key) }

type result struct {
	value Value
	err   error
}

// mustNew creates a cache with freshFor and staleFor and closes it when the
// test ends, so that a leaked load shows up as a synctest failure.
func mustNew(t *testing.T, workCtx context.Context, loader Loader) *Cache {
	t.Helper()
	c, err := New(workCtx, loader, freshFor, staleFor)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// goGet runs Get in a new goroutine and returns a channel with its result.
func goGet(c *Cache, ctx context.Context, key string) <-chan result {
	ch := make(chan result, 1)
	go func() {
		v, err := c.Get(ctx, key)
		ch <- result{v, err}
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
		t.Fatalf("%s: Get returned %v, %v; want it still waiting", what, r.value, r.err)
	default:
	}
}

// mustGet calls Get with a background context and checks the value. Use it
// only when no load it could wait for is blocked.
func mustGet(t *testing.T, c *Cache, key, want, what string) {
	t.Helper()
	v, err := c.Get(context.Background(), key)
	if err != nil || v.Data != want {
		t.Fatalf("%s: Get(%q) = %v, %v; want %q, nil", what, key, v, err, want)
	}
}

// keyCounter counts Load calls per key.
type keyCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (k *keyCounter) inc(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.n == nil {
		k.n = map[string]int{}
	}
	k.n[key]++
	return k.n[key]
}

func (k *keyCounter) get(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.n[key]
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valid := loaderFunc(func(context.Context, string) (Value, error) { return Value{}, nil })
		for _, tc := range []struct {
			name     string
			loader   Loader
			freshFor time.Duration
			staleFor time.Duration
			wantErr  error
		}{
			{name: "nil loader", loader: nil, freshFor: freshFor, staleFor: staleFor, wantErr: ErrNilLoader},
			{name: "zero freshFor", loader: valid, freshFor: 0, staleFor: staleFor, wantErr: ErrInvalidConfig},
			{name: "negative freshFor", loader: valid, freshFor: -time.Second, staleFor: staleFor, wantErr: ErrInvalidConfig},
			{name: "zero staleFor", loader: valid, freshFor: freshFor, staleFor: 0, wantErr: ErrInvalidConfig},
			{name: "negative staleFor", loader: valid, freshFor: freshFor, staleFor: -time.Second, wantErr: ErrInvalidConfig},
		} {
			c, err := New(context.Background(), tc.loader, tc.freshFor, tc.staleFor)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if c != nil {
				t.Errorf("%s: New returned a non-nil cache with an error", tc.name)
				c.Close()
			}
		}
	})
}

func TestFreshHitDoesNotLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(_ context.Context, key string) (Value, error) {
			return Value{Data: fmt.Sprintf("%s-%d", key, calls.Add(1))}, nil
		}))

		mustGet(t, c, "key", "key-1", "missing key")
		for range 5 {
			mustGet(t, c, "key", "key-1", "fresh key")
		}
		time.Sleep(freshFor - time.Nanosecond)
		mustGet(t, c, "key", "key-1", "fresh key just before freshFor")
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("Load called %d times, want 1: fresh hits must not load", got)
		}
	})
}

func TestStaleReadReturnsOldValueAndStartsOneRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			n := calls.Add(1)
			if n > 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
			}
			return Value{Data: fmt.Sprintf("v%d", n)}, nil
		}))

		mustGet(t, c, "key", "v1", "missing key")
		time.Sleep(freshFor + freshFor/2) // stale

		const readers = 20
		results := make([]<-chan result, readers)
		for i := range results {
			results[i] = goGet(c, context.Background(), "key")
		}
		for i, ch := range results {
			r := receive(t, ch, fmt.Sprintf("stale reader %d (the refresh is still running)", i))
			if r.err != nil || r.value.Data != "v1" {
				t.Fatalf("stale reader %d got %v, %v; want the stale v1, nil", i, r.value, r.err)
			}
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times after %d concurrent stale reads, want 2: the initial load and exactly one refresh", got, readers)
		}

		// The refresh takes 10ms. The new value's age counts from its completion.
		time.Sleep(10 * time.Millisecond)
		close(release)
		synctest.Wait()
		mustGet(t, c, "key", "v2", "after the refresh completed")
		time.Sleep(freshFor - time.Nanosecond)
		mustGet(t, c, "key", "v2", "freshFor-1ns after the refresh completed")
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2: a refreshed value is fresh for freshFor after its load returned", got)
		}
	})
}

func TestFailedRefreshKeepsStaleValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errRefresh := errors.New("refresh failed")
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			n := calls.Add(1)
			if n == 2 {
				return Value{}, errRefresh
			}
			return Value{Data: fmt.Sprintf("v%d", n)}, nil
		}))

		mustGet(t, c, "key", "v1", "missing key")
		time.Sleep(2 * freshFor) // stale
		mustGet(t, c, "key", "v1", "stale read that starts a failing refresh")
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2: a stale read must start a refresh", got)
		}

		mustGet(t, c, "key", "v1", "stale read after a failed refresh")
		synctest.Wait()
		if got := calls.Load(); got != 3 {
			t.Fatalf("Load called %d times, want 3: a stale read after a failed refresh must start a new refresh", got)
		}
		mustGet(t, c, "key", "v3", "after a successful refresh")
		synctest.Wait()
		if got := calls.Load(); got != 3 {
			t.Fatalf("Load called %d times, want 3: the refreshed value is fresh", got)
		}
	})
}

func TestFreshnessBoundaries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errRefresh := errors.New("refresh failed")
		var calls atomic.Int32
		// Refreshes 2 and 3 fail, so the entry keeps the timestamp of load 1.
		c := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			n := calls.Add(1)
			if n == 2 || n == 3 {
				return Value{}, errRefresh
			}
			return Value{Data: fmt.Sprintf("v%d", n)}, nil
		}))

		mustGet(t, c, "key", "v1", "missing key")

		time.Sleep(freshFor - time.Nanosecond)
		mustGet(t, c, "key", "v1", "age freshFor-1ns")
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("at age freshFor-1ns: Load called %d times, want 1 (the entry is fresh)", got)
		}

		time.Sleep(time.Nanosecond)
		mustGet(t, c, "key", "v1", "age freshFor")
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("at age freshFor: Load called %d times, want 2 (the entry is stale and needs a refresh)", got)
		}

		time.Sleep(staleFor - time.Nanosecond)
		mustGet(t, c, "key", "v1", "age freshFor+staleFor-1ns")
		synctest.Wait()
		if got := calls.Load(); got != 3 {
			t.Fatalf("at age freshFor+staleFor-1ns: Load called %d times, want 3 (the entry is still stale)", got)
		}

		time.Sleep(time.Nanosecond)
		v, err := c.Get(context.Background(), "key")
		if err != nil || v.Data != "v4" {
			t.Fatalf("at age freshFor+staleFor: Get = %v, %v; want v4, nil (the entry is expired, Get must wait for a new load)", v, err)
		}
	})
}

func TestMissingOrExpiredKeyLoadsOnceForConcurrentCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls keyCounter
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, key string) (Value, error) {
			n := calls.inc(key)
			if key != "a" || n > 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
			}
			return Value{Data: fmt.Sprintf("%s-%d", key, n)}, nil
		}))

		mustGet(t, c, "a", "a-1", "missing key")
		time.Sleep(freshFor + staleFor) // expired

		const waiters = 20
		results := make([]<-chan result, waiters)
		for i := range results {
			results[i] = goGet(c, context.Background(), "a")
		}
		b := goGet(c, context.Background(), "b")
		for i, ch := range results {
			assertBlocked(t, ch, fmt.Sprintf("waiter %d for an expired key", i))
		}
		assertBlocked(t, b, "waiter for a missing key")
		if got := calls.get("a"); got != 2 {
			t.Fatalf("Load(a) called %d times while %d callers wait for the expired key, want 2", got, waiters)
		}
		if got := calls.get("b"); got != 1 {
			t.Fatalf("Load(b) called %d times, want 1: different keys load independently", got)
		}

		close(release)
		for i, ch := range results {
			if r := receive(t, ch, fmt.Sprintf("waiter %d", i)); r.err != nil || r.value.Data != "a-2" {
				t.Fatalf("waiter %d got %v, %v; want a-2, nil", i, r.value, r.err)
			}
		}
		if r := receive(t, b, "waiter for b"); r.err != nil || r.value.Data != "b-1" {
			t.Fatalf("waiter for b got %v, %v; want b-1, nil", r.value, r.err)
		}
		mustGet(t, c, "a", "a-2", "after the shared load")
		if got := calls.get("a"); got != 2 {
			t.Fatalf("Load(a) called %d times, want 2", got)
		}
	})
}

func TestFailedLoadReturnsErrorAndCachesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errLoad := errors.New("load failed")
		release := make(chan struct{})
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			switch n := calls.Add(1); n {
			case 1:
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
				return Value{}, errLoad
			case 3:
				return Value{}, errLoad
			default:
				return Value{Data: fmt.Sprintf("v%d", n)}, nil
			}
		}))

		results := []<-chan result{
			goGet(c, context.Background(), "key"),
			goGet(c, context.Background(), "key"),
			goGet(c, context.Background(), "key"),
		}
		synctest.Wait()
		close(release)
		for i, ch := range results {
			if r := receive(t, ch, fmt.Sprintf("waiter %d", i)); !errors.Is(r.err, errLoad) {
				t.Fatalf("waiter %d error = %v, want %v", i, r.err, errLoad)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("Load called %d times, want 1", got)
		}

		mustGet(t, c, "key", "v2", "after a failed load (nothing may be cached)")

		time.Sleep(freshFor + staleFor) // expired
		v, err := c.Get(context.Background(), "key")
		if !errors.Is(err, errLoad) {
			t.Fatalf("Get on an expired key whose load fails = %v, %v; want %v (never the expired value)", v, err, errLoad)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("Load called %d times, want 3", got)
		}
	})
}

func TestWaiterCancellationDoesNotCancelSharedLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		loadCtxs := make(chan context.Context, 8)
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, key string) (Value, error) {
			calls.Add(1)
			loadCtxs <- ctx
			select {
			case <-release:
				return Value{Data: key}, nil
			case <-ctx.Done():
				return Value{}, ctx.Err()
			}
		}))

		creatorCtx, cancelCreator := context.WithCancel(context.Background())
		creator := goGet(c, creatorCtx, "a")
		synctest.Wait()
		loadCtx := <-loadCtxs
		waiter := goGet(c, context.Background(), "a")
		synctest.Wait()

		cancelCreator()
		if r := receive(t, creator, "cancelled creator"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("creator error = %v, want context.Canceled", r.err)
		}
		if err := loadCtx.Err(); err != nil {
			t.Fatalf("the load's context was cancelled with its creator: %v", err)
		}
		assertBlocked(t, waiter, "remaining waiter")

		// Key b: the only waiter leaves; the load must still finish and publish.
		onlyCtx, cancelOnly := context.WithCancel(context.Background())
		only := goGet(c, onlyCtx, "b")
		synctest.Wait()
		cancelOnly()
		if r := receive(t, only, "only waiter of b"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("only waiter of b error = %v, want context.Canceled", r.err)
		}

		close(release)
		if r := receive(t, waiter, "remaining waiter"); r.err != nil || r.value.Data != "a" {
			t.Fatalf("remaining waiter got %v, %v; want a, nil", r.value, r.err)
		}
		synctest.Wait()
		mustGet(t, c, "a", "a", "after the shared load")
		mustGet(t, c, "b", "b", "after a load every waiter left")
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2: a load whose waiters left must still publish its value", got)
		}
	})
}

func TestExpiredReadJoinsRunningRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			n := calls.Add(1)
			if n > 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
			}
			return Value{Data: fmt.Sprintf("v%d", n)}, nil
		}))

		mustGet(t, c, "key", "v1", "missing key")
		time.Sleep(freshFor + freshFor/2) // stale
		if r := receive(t, goGet(c, context.Background(), "key"), "stale read"); r.err != nil || r.value.Data != "v1" {
			t.Fatalf("stale read got %v, %v; want v1, nil", r.value, r.err)
		}

		time.Sleep(staleFor) // expired while the refresh is still running
		late := goGet(c, context.Background(), "key")
		assertBlocked(t, late, "Get for an expired key while its refresh runs")
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2: an expired read must join the running refresh", got)
		}

		close(release)
		if r := receive(t, late, "Get that joined the refresh"); r.err != nil || r.value.Data != "v2" {
			t.Fatalf("Get that joined the refresh got %v, %v; want v2, nil", r.value, r.err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2", got)
		}
	})
}

func TestLoaderReceivesWorkContextNotCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		type seen struct {
			owner, caller any
			ctx           context.Context
		}
		workCtx := context.WithValue(context.Background(), ctxKey("owner"), "cache")
		loads := make(chan seen, 8)
		release := make(chan struct{})
		var calls atomic.Int32
		c := mustNew(t, workCtx, loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			n := calls.Add(1)
			loads <- seen{owner: ctx.Value(ctxKey("owner")), caller: ctx.Value(ctxKey("caller")), ctx: ctx}
			if n == 2 {
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
			}
			return Value{Data: fmt.Sprintf("v%d", n)}, nil
		}))

		callerCtx := context.WithValue(context.Background(), ctxKey("caller"), "get")
		if v, err := c.Get(callerCtx, "key"); err != nil || v.Data != "v1" {
			t.Fatalf("Get = %v, %v; want v1, nil", v, err)
		}
		s := <-loads
		if s.owner != "cache" {
			t.Fatal("synchronous load did not receive a context derived from workCtx")
		}
		if s.caller != nil {
			t.Fatal("synchronous load received a context derived from the caller's context")
		}

		time.Sleep(freshFor + freshFor/2) // stale
		readerCtx, cancelReader := context.WithCancel(context.WithValue(context.Background(), ctxKey("caller"), "stale reader"))
		if r := receive(t, goGet(c, readerCtx, "key"), "stale read"); r.err != nil || r.value.Data != "v1" {
			t.Fatalf("stale read got %v, %v; want v1, nil", r.value, r.err)
		}
		cancelReader() // the reader is gone; its refresh must go on
		synctest.Wait()
		s = <-loads
		if s.owner != "cache" {
			t.Fatal("background refresh did not receive a context derived from workCtx")
		}
		if s.caller != nil {
			t.Fatal("background refresh received a context derived from the stale reader's context")
		}
		if err := s.ctx.Err(); err != nil {
			t.Fatalf("background refresh was cancelled with the Get that triggered it: %v", err)
		}

		close(release)
		synctest.Wait()
		mustGet(t, c, "key", "v2", "after the refresh")
	})
}

func TestPreCancelledGetStartsNoLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			return Value{Data: fmt.Sprintf("v%d", calls.Add(1))}, nil
		}))
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := c.Get(cancelled, "key"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get on a missing key = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("Load called %d times for a pre-cancelled Get, want 0", got)
		}

		mustGet(t, c, "key", "v1", "missing key")
		if _, err := c.Get(cancelled, "key"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get on a fresh key = %v, want context.Canceled", err)
		}

		time.Sleep(freshFor + freshFor/2) // stale
		if _, err := c.Get(cancelled, "key"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get on a stale key = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("Load called %d times, want 1: a pre-cancelled Get must not start a refresh", got)
		}

		mustGet(t, c, "key", "v1", "stale key")
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times, want 2: the next stale Get starts the refresh", got)
		}
	})
}

func TestCloseCancelsWorkAndWaitsForLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleanup := make(chan struct{})
		var calls, cancelled atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			if calls.Add(1) == 1 {
				return Value{Data: "v1"}, nil
			}
			<-ctx.Done()
			cancelled.Add(1)
			<-cleanup // the loader takes a while to clean up
			return Value{}, ctx.Err()
		}))

		mustGet(t, c, "a", "v1", "missing key")
		time.Sleep(freshFor + freshFor/2) // stale
		if r := receive(t, goGet(c, context.Background(), "a"), "stale read"); r.err != nil || r.value.Data != "v1" {
			t.Fatalf("stale read got %v, %v; want v1, nil", r.value, r.err)
		}
		b := goGet(c, context.Background(), "b")
		assertBlocked(t, b, "waiter for a missing key")
		if got := calls.Load(); got != 3 {
			t.Fatalf("Load called %d times, want 3 (initial load, refresh of a, load of b)", got)
		}

		closed := make(chan struct{})
		go func() {
			c.Close()
			close(closed)
		}()
		synctest.Wait()
		if got := cancelled.Load(); got != 2 {
			t.Fatalf("%d loads saw their context cancelled, want 2: Close must cancel the background refresh and the synchronous load", got)
		}
		select {
		case <-closed:
			t.Fatal("Close returned while loads were still running")
		default:
		}

		for _, key := range []string{"a", "c"} {
			if _, err := c.Get(context.Background(), key); !errors.Is(err, ErrClosed) {
				t.Fatalf("Get(%q) during Close = %v, want ErrClosed", key, err)
			}
		}
		assertBlocked(t, b, "Get waiting for a load that has not returned")
		if got := calls.Load(); got != 3 {
			t.Fatalf("Load called %d times, want 3: Get after Close must not load", got)
		}

		close(cleanup)
		synctest.Wait()
		select {
		case <-closed:
		default:
			t.Fatal("Close did not return after every load returned")
		}
		if r := receive(t, b, "Get waiting when Close was called"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Get waiting when Close was called = %v, %v; want the load's context.Canceled", r.value, r.err)
		}
		c.Close() // after completion: returns immediately
	})
}

func TestRepeatedAndConcurrentCloseIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, active atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			active.Add(1)
			defer active.Add(-1)
			if calls.Add(1) == 1 {
				return Value{Data: "v1"}, nil
			}
			<-ctx.Done()
			return Value{}, ctx.Err()
		}))

		mustGet(t, c, "key", "v1", "missing key")
		time.Sleep(freshFor + freshFor/2) // stale
		if r := receive(t, goGet(c, context.Background(), "key"), "stale read"); r.err != nil || r.value.Data != "v1" {
			t.Fatalf("stale read got %v, %v; want v1, nil", r.value, r.err)
		}

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				c.Close()
				if n := active.Load(); n != 0 {
					t.Errorf("Close returned with %d loads still running", n)
				}
			})
		}
		wg.Wait()

		if _, err := c.Get(context.Background(), "key"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Get after Close = %v, want ErrClosed", err)
		}
		c.Close()
	})
}

func TestConcurrentGetAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var closeReturned atomic.Bool
		var active atomic.Int32
		c := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, key string) (Value, error) {
			if closeReturned.Load() {
				t.Errorf("Load(%q) started after Close returned", key)
			}
			active.Add(1)
			defer active.Add(-1)
			if err := ctx.Err(); err != nil {
				return Value{}, err
			}
			return Value{Data: key}, nil
		}))

		for i := range 4 {
			key := fmt.Sprintf("k%d", i)
			mustGet(t, c, key, key, "missing key")
		}
		time.Sleep(freshFor + freshFor/2) // k0..k3 are stale, k4..k7 are missing

		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				key := fmt.Sprintf("k%d", i%8)
				v, err := c.Get(context.Background(), key)
				switch {
				case err == nil:
					if v.Data != key {
						t.Errorf("Get(%q) = %q, want %q", key, v.Data, key)
					}
				case errors.Is(err, ErrClosed), errors.Is(err, context.Canceled):
				default:
					t.Errorf("Get(%q) = %v, want a value, ErrClosed, or the load's context.Canceled", key, err)
				}
			})
		}
		for range 2 {
			wg.Go(func() {
				c.Close()
				if n := active.Load(); n != 0 {
					t.Errorf("Close returned with %d loads still running", n)
				}
				closeReturned.Store(true)
			})
		}
		wg.Wait()
		synctest.Wait()

		if n := active.Load(); n != 0 {
			t.Fatalf("%d loads still running after Close returned", n)
		}
		if _, err := c.Get(context.Background(), "k0"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Get after Close = %v, want ErrClosed", err)
		}
	})
}
