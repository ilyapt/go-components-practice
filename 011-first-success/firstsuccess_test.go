package firstsuccess

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type providerFunc func(context.Context, Request) (Response, error)

func (f providerFunc) Fetch(ctx context.Context, request Request) (Response, error) {
	return f(ctx, request)
}

// after returns a provider that answers after d unless its context is
// cancelled first.
func after(d time.Duration, resp Response, err error) providerFunc {
	return func(ctx context.Context, _ Request) (Response, error) {
		select {
		case <-time.After(d):
			return resp, err
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	}
}

type outcome struct {
	response Response
	err      error
}

func TestReturnsFirstSuccessAndCancelsLoser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		releaseFast := make(chan struct{})
		releaseLoser := make(chan struct{})
		loserCancelled := make(chan struct{})

		fast := providerFunc(func(context.Context, Request) (Response, error) {
			<-releaseFast
			return Response{Source: "fast", Value: "value"}, nil
		})
		slow := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			close(loserCancelled)
			<-releaseLoser // the loser takes a while to clean up
			return Response{}, ctx.Err()
		})

		result := make(chan outcome, 1)
		go func() {
			response, err := FirstSuccess(context.Background(), []Provider{slow, fast}, Request{Key: "key"})
			result <- outcome{response, err}
		}()
		synctest.Wait() // both calls are in flight

		close(releaseFast)
		synctest.Wait()
		select {
		case got := <-result:
			if got.err != nil {
				t.Fatalf("FirstSuccess returned error: %v", got.err)
			}
			if got.response.Source != "fast" {
				t.Fatalf("source = %q, want fast", got.response.Source)
			}
		default:
			t.Fatal("FirstSuccess waited for the losing provider to return")
		}

		select {
		case <-loserCancelled:
		default:
			t.Fatal("losing provider was not cancelled")
		}
		close(releaseLoser)
	})
}

func TestReturnsInCompletionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providers := []Provider{
			after(30*time.Millisecond, Response{Source: "slow"}, nil),
			after(10*time.Millisecond, Response{Source: "fast"}, nil),
			after(20*time.Millisecond, Response{Source: "medium"}, nil),
		}
		start := time.Now()
		response, err := FirstSuccess(context.Background(), providers, Request{})
		if err != nil {
			t.Fatalf("FirstSuccess returned error: %v", err)
		}
		if response.Source != "fast" {
			t.Fatalf("source = %q, want fast", response.Source)
		}
		if got := time.Since(start); got != 10*time.Millisecond {
			t.Fatalf("FirstSuccess returned after %v, want 10ms", got)
		}
	})
}

func TestFailureDoesNotEndTheRace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providers := []Provider{
			after(10*time.Millisecond, Response{}, errors.New("fast failure")),
			after(20*time.Millisecond, Response{Source: "slow", Value: "ok"}, nil),
		}
		response, err := FirstSuccess(context.Background(), providers, Request{})
		if err != nil {
			t.Fatalf("FirstSuccess = %v, want the slower success", err)
		}
		if response.Source != "slow" {
			t.Fatalf("source = %q, want slow", response.Source)
		}
	})
}

func TestAllProvidersFail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providerErr := errors.New("provider failed")
		providers := []Provider{
			after(0, Response{}, providerErr),
			after(10*time.Millisecond, Response{}, providerErr),
			after(20*time.Millisecond, Response{}, providerErr),
		}

		start := time.Now()
		response, err := FirstSuccess(context.Background(), providers, Request{})
		if err == nil {
			t.Fatalf("FirstSuccess returned (%+v, nil) although every provider failed", response)
		}
		if got := time.Since(start); got != 20*time.Millisecond {
			t.Fatalf("FirstSuccess returned after %v, want 20ms (when the last provider failed)", got)
		}
	})
}

func TestAlreadyCancelledParentWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		provider := providerFunc(func(context.Context, Request) (Response, error) {
			return Response{Source: "provider"}, nil
		})
		if _, err := FirstSuccess(ctx, []Provider{provider}, Request{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("FirstSuccess error = %v, want context.Canceled", err)
		}
	})
}

func TestParentCancellationCancelsAllCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cancelled atomic.Int32
		blocking := providerFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			cancelled.Add(1)
			return Response{}, ctx.Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan outcome, 1)
		go func() {
			response, err := FirstSuccess(ctx, []Provider{blocking, blocking, blocking}, Request{})
			result <- outcome{response, err}
		}()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case got := <-result:
			if !errors.Is(got.err, context.Canceled) {
				t.Fatalf("FirstSuccess error = %v, want context.Canceled", got.err)
			}
		default:
			t.Fatal("FirstSuccess did not return after the parent was cancelled")
		}
		if got := cancelled.Load(); got != 3 {
			t.Fatalf("%d providers observed cancellation, want 3", got)
		}
	})
}

func TestLateSuccessesDoNotBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		providers := make([]Provider, 20)
		for i := range providers {
			providers[i] = providerFunc(func(context.Context, Request) (Response, error) {
				<-release // every provider ignores cancellation and succeeds at once
				return Response{Source: strconv.Itoa(i)}, nil
			})
		}

		result := make(chan outcome, 1)
		go func() {
			response, err := FirstSuccess(context.Background(), providers, Request{})
			result <- outcome{response, err}
		}()
		synctest.Wait()
		close(release)

		got := <-result
		if got.err != nil || got.response.Source == "" {
			t.Fatalf("FirstSuccess = (%+v, %v), want one provider's success", got.response, got.err)
		}
		// synctest fails the test if a provider goroutine is still blocked
		// publishing its result when the test returns.
	})
}

func TestRejectsInvalidProviders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if _, err := FirstSuccess(context.Background(), nil, Request{}); !errors.Is(err, ErrNoProviders) {
			t.Fatalf("FirstSuccess with no providers = %v, want ErrNoProviders", err)
		}

		var calls atomic.Int32
		provider := providerFunc(func(context.Context, Request) (Response, error) {
			calls.Add(1)
			return Response{}, nil
		})
		if _, err := FirstSuccess(context.Background(), []Provider{provider, nil}, Request{}); err == nil {
			t.Fatal("FirstSuccess returned nil error for a nil provider")
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("provider called %d times, want 0", got)
		}
	})
}
