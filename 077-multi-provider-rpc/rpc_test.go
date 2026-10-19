package multiproviderrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestFirstFreshResponseCancelsLoser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slowStarted := make(chan struct{})
		slowCancelled := make(chan struct{})
		slow := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			close(slowStarted)
			<-ctx.Done()
			close(slowCancelled)
			return Response{}, ctx.Err()
		})
		fast := providerFunc(func(context.Context, Request) (Response, error) {
			<-slowStarted
			return Response{Version: 100, Source: "fresh"}, nil
		})
		c := mustNew(t, []Provider{slow, fast}, 2, time.Second)

		resp, err := c.Fetch(context.Background(), Request{MinVersion: 100})
		if err != nil {
			t.Fatalf("Fetch returned error: %v", err)
		}
		if resp.Version != 100 || resp.Source != "fresh" {
			t.Fatalf("response = %+v, want the fresh response", resp)
		}
		synctest.Wait()
		select {
		case <-slowCancelled:
		default:
			t.Error("the losing call was not cancelled when Fetch selected a fresh response")
			<-slowCancelled // let a per-call timeout end it so the bubble can exit
		}
	})
}

func TestReturnsFreshestStaleResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, []Provider{
			after(1*time.Millisecond, Response{Version: 9, Source: "nine"}, nil),
			after(2*time.Millisecond, Response{}, errors.New("provider failed")),
			after(3*time.Millisecond, Response{Version: 7, Source: "seven"}, nil),
		}, 3, time.Second)

		resp, err := c.Fetch(context.Background(), Request{MinVersion: 10})
		if !errors.Is(err, ErrNoFreshResponse) {
			t.Fatalf("Fetch error = %v, want ErrNoFreshResponse", err)
		}
		if resp.Version != 9 || resp.Source != "nine" {
			t.Fatalf("response = %+v, want the freshest stale response (version 9), not the latest one", resp)
		}
	})
}

func TestStaleThenFreshReturnsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		c := mustNew(t, []Provider{
			after(1*time.Millisecond, Response{Version: 5, Source: "stale"}, nil),
			after(2*time.Millisecond, Response{}, errors.New("provider failed")),
			after(3*time.Millisecond, Response{Version: 10, Source: "fresh"}, nil),
			after(time.Hour, Response{Version: 11, Source: "too late"}, nil),
		}, 4, 2*time.Hour)

		resp, err := c.Fetch(context.Background(), Request{MinVersion: 10})
		if err != nil || resp.Source != "fresh" {
			t.Fatalf("Fetch = (%+v, %v), want the fresh response and nil", resp, err)
		}
		if elapsed := time.Since(start); elapsed != 3*time.Millisecond {
			t.Fatalf("Fetch took %v, want 3ms (return as soon as a fresh response arrives)", elapsed)
		}
	})
}

func TestAllProvidersFailedReturnsZeroResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, []Provider{
			after(time.Millisecond, Response{Version: 50, Source: "ignored"}, errors.New("failed with data")),
			after(2*time.Millisecond, Response{}, errors.New("failed")),
		}, 2, time.Second)

		resp, err := c.Fetch(context.Background(), Request{MinVersion: 1})
		if !errors.Is(err, ErrAllProvidersFailed) {
			t.Fatalf("Fetch error = %v, want ErrAllProvidersFailed", err)
		}
		if resp.Version != 0 || resp.Source != "" || resp.Payload != nil {
			t.Fatalf("response = %+v, want the zero Response", resp)
		}
	})
}

func TestPerCallTimeoutCountsAsProviderFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hang := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			return Response{}, ctx.Err()
		})

		c := mustNew(t, []Provider{hang}, 1, 20*time.Millisecond)
		start := time.Now()
		if _, err := c.Fetch(context.Background(), Request{}); !errors.Is(err, ErrAllProvidersFailed) {
			t.Fatalf("Fetch error = %v, want ErrAllProvidersFailed", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Fatalf("Fetch took %v, want exactly the 20ms per-call timeout", elapsed)
		}

		c = mustNew(t, []Provider{hang, after(5*time.Millisecond, Response{Version: 3}, nil)}, 2, 20*time.Millisecond)
		resp, err := c.Fetch(context.Background(), Request{MinVersion: 10})
		if !errors.Is(err, ErrNoFreshResponse) || resp.Version != 3 {
			t.Fatalf("Fetch = (%+v, %v), want the stale response with ErrNoFreshResponse", resp, err)
		}
	})
}

func TestTimeoutStartsAfterCapacityIsAcquired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 20 * time.Millisecond
		var mu sync.Mutex
		var budgets []time.Duration
		p := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("provider context has no deadline")
			}
			mu.Lock()
			budgets = append(budgets, deadline.Sub(time.Now()))
			mu.Unlock()
			time.Sleep(15 * time.Millisecond)
			return Response{Version: 1}, nil
		})
		c := mustNew(t, []Provider{p, p, p}, 1, timeout)

		if _, err := c.Fetch(context.Background(), Request{MinVersion: 100}); !errors.Is(err, ErrNoFreshResponse) {
			t.Fatalf("Fetch error = %v, want ErrNoFreshResponse (every call should finish within its own timeout)", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(budgets) != 3 {
			t.Fatalf("provider calls = %d, want 3", len(budgets))
		}
		for i, b := range budgets {
			if b != timeout {
				t.Fatalf("call %d started with %v left before its deadline, want the full %v; time spent waiting for capacity must not count", i+1, b, timeout)
			}
		}
	})
}

func TestProviderContextIsBoundedByRequestContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadlines := make(chan time.Time, 1)
		c := mustNew(t, []Provider{providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			d, _ := ctx.Deadline()
			deadlines <- d
			<-ctx.Done()
			return Response{}, ctx.Err()
		})}, 1, time.Hour)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		want, _ := ctx.Deadline()
		if _, err := c.Fetch(ctx, Request{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Fetch error = %v, want context.DeadlineExceeded from the request context", err)
		}
		if got := <-deadlines; !got.Equal(want) {
			t.Fatalf("provider deadline = %v, want the request deadline %v", got, want)
		}
	})
}

func TestGlobalConcurrencyLimitAcrossFetchCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var active, maximum, total atomic.Int32
		p := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			current := active.Add(1)
			defer active.Add(-1)
			total.Add(1)
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			select {
			case <-release:
				return Response{Version: 1}, nil
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
		})
		c := mustNew(t, []Provider{p, p, p}, 2, time.Second)

		done := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := c.Fetch(context.Background(), Request{MinVersion: 1})
				done <- err
			}()
		}
		synctest.Wait()
		if got := active.Load(); got != 2 {
			t.Fatalf("active provider calls = %d, want 2 (the limit is shared by both Fetch calls)", got)
		}
		close(release)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatalf("Fetch returned error: %v", err)
			}
		}
		synctest.Wait()
		if got := maximum.Load(); got != 2 {
			t.Fatalf("maximum concurrent provider calls = %d, want 2", got)
		}
		if got := active.Load(); got != 0 {
			t.Fatalf("provider calls still running after both Fetch calls returned: %d", got)
		}
	})
}

func TestCancelledWhileWaitingForCapacityStartsNoCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var waitingCalls atomic.Int32
		blocker := providerFunc(func(ctx context.Context, req Request) (Response, error) {
			if req.Key == "waiting" {
				waitingCalls.Add(1)
				return Response{Version: 1}, nil
			}
			<-release
			return Response{Version: 1}, nil
		})
		c := mustNew(t, []Provider{blocker}, 1, time.Hour)

		holder := make(chan error, 1)
		go func() {
			_, err := c.Fetch(context.Background(), Request{Key: "holder", MinVersion: 1})
			holder <- err
		}()
		synctest.Wait() // the holder occupies the only slot

		ctx, cancel := context.WithCancel(context.Background())
		waiting := make(chan error, 1)
		go func() {
			_, err := c.Fetch(ctx, Request{Key: "waiting", MinVersion: 1})
			waiting <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-waiting:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Fetch cancelled while waiting for capacity = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("Fetch did not return after its context was cancelled while waiting for capacity")
		}

		close(release) // the slot becomes free only now
		if err := <-holder; err != nil {
			t.Fatalf("holder Fetch returned error: %v", err)
		}
		synctest.Wait()
		if got := waitingCalls.Load(); got != 0 {
			t.Fatalf("a cancelled request started %d provider calls after capacity was freed", got)
		}
	})
}

func TestParentCancellationAndPreCancelledRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, cancelled atomic.Int32
		p := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			calls.Add(1)
			<-ctx.Done()
			cancelled.Add(1)
			return Response{}, ctx.Err()
		})
		c := mustNew(t, []Provider{p}, 1, time.Hour)

		preCancelled, cancelPre := context.WithCancel(context.Background())
		cancelPre()
		for range 20 {
			if _, err := c.Fetch(preCancelled, Request{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-cancelled Fetch error = %v, want context.Canceled", err)
			}
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("pre-cancelled Fetch invoked a provider %d times", got)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.Fetch(ctx, Request{})
			done <- err
		}()
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("provider calls = %d, want 1", got)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Fetch error = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if cancelled.Load() != 1 {
			t.Fatal("the in-flight provider call was not cancelled with the request")
		}
	})
}

func TestLateResultsDoNotBlockAfterFetchReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var finished atomic.Int32
		late := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			defer finished.Add(1)
			return Response{Version: 1}, nil // a result nobody is waiting for any more
		})
		fresh := after(time.Millisecond, Response{Version: 10}, nil)
		c := mustNew(t, []Provider{late, late, fresh, late}, 4, time.Hour)

		if resp, err := c.Fetch(context.Background(), Request{MinVersion: 10}); err != nil || resp.Version != 10 {
			t.Fatalf("Fetch = (%+v, %v), want version 10", resp, err)
		}
		synctest.Wait()
		if got := finished.Load(); got != 3 {
			t.Fatalf("late providers finished = %d, want 3", got)
		}
		// synctest.Test fails if any goroutine is still blocked publishing a late result.
	})
}

func TestCapacityIsReleasedAfterEveryOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running atomic.Int32
		both := make(chan struct{})
		provider := func(idx int) Provider {
			return providerFunc(func(ctx context.Context, req Request) (Response, error) {
				wait := func(d time.Duration) error {
					select {
					case <-time.After(d):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				switch req.Key {
				case "fail":
					return Response{}, errors.New("failed")
				case "stale":
					return Response{Version: 1}, nil
				case "fresh":
					if idx == 0 {
						return Response{Version: 10}, nil
					}
				case "check":
					if running.Add(1) == 2 {
						close(both)
					}
					select {
					case <-both:
						return Response{Version: 10}, nil
					case <-ctx.Done():
						return Response{}, ctx.Err()
					}
				}
				// "hang", "cancel", and the losing "fresh" call wait for their context.
				if err := wait(time.Hour); err != nil {
					return Response{}, err
				}
				return Response{Version: 1}, nil
			})
		}
		c := mustNew(t, []Provider{provider(0), provider(1)}, 2, 10*time.Millisecond)

		for _, key := range []string{"fail", "hang", "stale", "fresh", "cancel", "fail", "hang", "stale", "fresh", "cancel"} {
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			if key == "cancel" {
				go func() {
					time.Sleep(time.Millisecond)
					cancel()
				}()
			}
			_, _ = c.Fetch(ctx, Request{Key: key, MinVersion: 5})
			cancel()
		}
		synctest.Wait()

		// Both slots must be free again: two calls can run at the same time.
		resp, err := c.Fetch(context.Background(), Request{Key: "check", MinVersion: 5})
		if err != nil || resp.Version != 10 {
			t.Fatalf("Fetch after mixed outcomes = (%+v, %v), want version 10; capacity was not released after some outcome", resp, err)
		}
	})
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := providerFunc(func(context.Context, Request) (Response, error) { return Response{}, nil })
		for _, tc := range []struct {
			name        string
			providers   []Provider
			concurrency int
			timeout     time.Duration
			want        error
		}{
			{"no providers", nil, 1, time.Second, ErrInvalidConfig},
			{"zero concurrency", []Provider{p}, 0, time.Second, ErrInvalidConfig},
			{"zero timeout", []Provider{p}, 1, 0, ErrInvalidConfig},
			{"nil provider", []Provider{p, nil}, 1, time.Second, ErrNilProvider},
		} {
			if _, err := New(tc.providers, tc.concurrency, tc.timeout); !errors.Is(err, tc.want) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.want)
			}
		}
	})
}

type providerFunc func(context.Context, Request) (Response, error)

func (f providerFunc) Fetch(ctx context.Context, req Request) (Response, error) {
	return f(ctx, req)
}

// after returns a provider that answers with resp and err after d, or with
// ctx.Err() if its context ends first.
func after(d time.Duration, resp Response, err error) Provider {
	return providerFunc(func(ctx context.Context, _ Request) (Response, error) {
		select {
		case <-time.After(d):
			return resp, err
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	})
}

func mustNew(t *testing.T, providers []Provider, maxConcurrency int, perCallTimeout time.Duration) *Client {
	t.Helper()
	c, err := New(providers, maxConcurrency, perCallTimeout)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return c
}
