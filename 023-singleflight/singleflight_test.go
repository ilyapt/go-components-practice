package singleflight

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type loaderFunc func(context.Context, string) (Value, error)

func (f loaderFunc) Load(ctx context.Context, key string) (Value, error) { return f(ctx, key) }

type result struct {
	value Value
	err   error
}

func mustNew(t *testing.T, workCtx context.Context, loader Loader) *Group {
	t.Helper()
	g, err := New(workCtx, loader)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return g
}

// goGet runs Get in a new goroutine and returns a channel with its result.
func goGet(g *Group, ctx context.Context, key string) <-chan result {
	ch := make(chan result, 1)
	go func() {
		v, err := g.Get(ctx, key)
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

func TestConcurrentSameKeySharesOneLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			calls.Add(1)
			<-release
			return Value{Data: "shared"}, nil
		}))

		const waiters = 20
		results := make([]<-chan result, waiters)
		for i := range results {
			results[i] = goGet(g, context.Background(), "key")
		}
		synctest.Wait() // every caller is now waiting on the one load

		if got := calls.Load(); got != 1 {
			t.Fatalf("Load called %d times while %d callers wait, want 1", got, waiters)
		}
		close(release)
		for i, ch := range results {
			r := receive(t, ch, fmt.Sprintf("waiter %d", i))
			if r.err != nil || r.value.Data != "shared" {
				t.Fatalf("waiter %d got %v, %v; want shared, nil", i, r.value, r.err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("Load called %d times, want 1", got)
		}
	})
}

func TestAllWaitersReceiveSameError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errLoad := errors.New("load failed")
		release := make(chan struct{})
		var calls atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			calls.Add(1)
			<-release
			return Value{}, errLoad
		}))

		results := []<-chan result{
			goGet(g, context.Background(), "key"),
			goGet(g, context.Background(), "key"),
			goGet(g, context.Background(), "key"),
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
	})
}

func TestCreatorCancellationDoesNotCancelSharedLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		loadCtxs := make(chan context.Context, 1)
		g := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			loadCtxs <- ctx
			select {
			case <-release:
				return Value{Data: "loaded"}, nil
			case <-ctx.Done():
				return Value{}, ctx.Err()
			}
		}))

		creatorCtx, cancelCreator := context.WithCancel(context.Background())
		creator := goGet(g, creatorCtx, "key")
		synctest.Wait()
		loadCtx := <-loadCtxs
		waiter := goGet(g, context.Background(), "key")
		synctest.Wait()

		cancelCreator()
		if r := receive(t, creator, "cancelled creator"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("creator error = %v, want context.Canceled", r.err)
		}
		if err := loadCtx.Err(); err != nil {
			t.Fatalf("the load's context was cancelled with its creator: %v", err)
		}
		assertBlocked(t, waiter, "remaining waiter")

		close(release)
		if r := receive(t, waiter, "remaining waiter"); r.err != nil || r.value.Data != "loaded" {
			t.Fatalf("remaining waiter got %v, %v; want loaded, nil", r.value, r.err)
		}
	})
}

func TestLoadCompletesAfterEveryWaiterLeaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls, completed atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			n := calls.Add(1)
			if n == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return Value{}, ctx.Err()
				}
			}
			completed.Add(1)
			return Value{Data: fmt.Sprintf("call-%d", n)}, nil
		}))

		ctx, cancel := context.WithCancel(context.Background())
		only := goGet(g, ctx, "key")
		synctest.Wait()
		cancel()
		if r := receive(t, only, "only waiter"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled waiter error = %v, want context.Canceled", r.err)
		}

		// A caller arriving while the abandoned load still runs joins it.
		joiner := goGet(g, context.Background(), "key")
		assertBlocked(t, joiner, "joiner")
		close(release)
		if r := receive(t, joiner, "joiner"); r.err != nil || r.value.Data != "call-1" {
			t.Fatalf("joiner got %v, %v; want call-1 from the running load", r.value, r.err)
		}
		if got := completed.Load(); got != 1 {
			t.Fatalf("completed loads = %d, want 1: the load must finish even with no waiters", got)
		}

		// The entry is gone afterwards, so the next Get starts a fresh load.
		if v, err := g.Get(context.Background(), "key"); err != nil || v.Data != "call-2" {
			t.Fatalf("Get after completion = %v, %v; want call-2", v, err)
		}
	})
}

func TestLoaderReceivesWorkContextNotWaiterContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		workCtx := context.WithValue(context.Background(), ctxKey("owner"), "group")
		var owner, caller any
		g := mustNew(t, workCtx, loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			owner = ctx.Value(ctxKey("owner"))
			caller = ctx.Value(ctxKey("caller"))
			return Value{Data: "ok"}, nil
		}))

		callerCtx := context.WithValue(context.Background(), ctxKey("caller"), "waiter")
		if _, err := g.Get(callerCtx, "key"); err != nil {
			t.Fatalf("Get returned error: %v", err)
		}
		if owner != "group" {
			t.Fatal("Load did not receive workCtx (or a context derived from it)")
		}
		if caller != nil {
			t.Fatal("Load received a context derived from the caller's context")
		}
	})
}

func TestWorkContextCancellationCancelsLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.Background())
		var calls atomic.Int32
		g := mustNew(t, workCtx, loaderFunc(func(ctx context.Context, _ string) (Value, error) {
			calls.Add(1)
			<-ctx.Done()
			return Value{}, ctx.Err()
		}))

		results := []<-chan result{
			goGet(g, context.Background(), "a"),
			goGet(g, context.Background(), "a"),
			goGet(g, context.Background(), "b"),
		}
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("Load called %d times for two keys, want 2", got)
		}
		cancelWork()
		for i, ch := range results {
			if r := receive(t, ch, fmt.Sprintf("waiter %d", i)); !errors.Is(r.err, context.Canceled) {
				t.Fatalf("waiter %d error = %v, want context.Canceled", i, r.err)
			}
		}
	})
}

func TestDifferentKeysRunIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var started atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(_ context.Context, key string) (Value, error) {
			started.Add(1)
			<-release
			return Value{Data: key}, nil
		}))

		a := goGet(g, context.Background(), "a")
		b := goGet(g, context.Background(), "b")
		synctest.Wait()
		if got := started.Load(); got != 2 {
			t.Fatalf("started loads = %d, want 2: different keys must not wait for each other", got)
		}
		close(release)
		for key, ch := range map[string]<-chan result{"a": a, "b": b} {
			if r := receive(t, ch, key); r.err != nil || r.value.Data != key {
				t.Fatalf("Get(%q) = %v, %v", key, r.value, r.err)
			}
		}
	})
}

func TestCompletedEntryIsNotCached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			return Value{Data: fmt.Sprintf("call-%d", calls.Add(1))}, nil
		}))

		first, err := g.Get(context.Background(), "key")
		if err != nil {
			t.Fatalf("first Get returned error: %v", err)
		}
		second, err := g.Get(context.Background(), "key")
		if err != nil {
			t.Fatalf("second Get returned error: %v", err)
		}
		if first.Data != "call-1" || second.Data != "call-2" {
			t.Fatalf("values = %q, %q; want call-1, call-2", first.Data, second.Data)
		}
	})
}

func TestPreCancelledCallerStartsNoWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		g := mustNew(t, context.Background(), loaderFunc(func(context.Context, string) (Value, error) {
			calls.Add(1)
			return Value{Data: "ok"}, nil
		}))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := g.Get(ctx, "key"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Get error = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("Load called %d times for a pre-cancelled caller, want 0", got)
		}

		// The rejected caller must leave no state behind: the next caller loads normally.
		next := goGet(g, context.Background(), "key")
		if r := receive(t, next, "next caller"); r.err != nil || r.value.Data != "ok" {
			t.Fatalf("Get after a pre-cancelled caller = %v, %v; want ok, nil", r.value, r.err)
		}
	})
}

func TestRejectsNilLoader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if _, err := New(context.Background(), nil); err == nil {
			t.Fatal("New(nil loader) returned nil error")
		}
	})
}
