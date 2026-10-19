package timeoutwrapper

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type clientFunc func(context.Context, Request) (Response, error)

func (f clientFunc) Call(ctx context.Context, request Request) (Response, error) {
	return f(ctx, request)
}

func mustNew(t *testing.T, next Client, timeout time.Duration) Client {
	t.Helper()
	c, err := New(next, timeout)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return c
}

func TestPreservesCompletedClientResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientErr := errors.New("client error")
		wrapper := mustNew(t, clientFunc(func(context.Context, Request) (Response, error) {
			time.Sleep(10 * time.Millisecond)
			return Response{Value: "response"}, clientErr
		}), time.Second)

		response, err := wrapper.Call(context.Background(), Request{})
		if response.Value != "response" {
			t.Fatalf("response = %q, want response", response.Value)
		}
		if !errors.Is(err, clientErr) {
			t.Fatalf("error = %v, want the client's error", err)
		}
	})
}

func TestClientReceivesChildContextWithTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		start := time.Now()
		var deadline time.Time
		var hasDeadline bool
		var value any
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			deadline, hasDeadline = ctx.Deadline()
			value = ctx.Value(key{})
			return Response{}, nil
		}), 50*time.Millisecond)

		parent := context.WithValue(context.Background(), key{}, "from parent")
		if _, err := wrapper.Call(parent, Request{}); err != nil {
			t.Fatalf("Call returned error: %v", err)
		}
		if !hasDeadline || !deadline.Equal(start.Add(50*time.Millisecond)) {
			t.Fatalf("client deadline = %v (set=%v), want now+50ms", deadline.Sub(start), hasDeadline)
		}
		if value != "from parent" {
			t.Fatal("client context is not derived from the caller's context")
		}
	})
}

func TestReturnsWrapperTimeoutIndependentlyOfClientError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientErr := errors.New("client chose another error")
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			return Response{Value: "partial"}, clientErr
		}), 10*time.Millisecond)

		start := time.Now()
		if _, err := wrapper.Call(context.Background(), Request{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call error = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 10*time.Millisecond {
			t.Fatalf("Call returned after %v, want 10ms", got)
		}
	})
}

func TestReturnsParentCancellationIndependentlyOfClientError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientErr := errors.New("client chose another error")
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			return Response{}, clientErr
		}), time.Hour)

		parent, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := wrapper.Call(parent, Request{})
			result <- err
		}()
		synctest.Wait()
		cancel()

		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Call error = %v, want context.Canceled", err)
		}
	})
}

func TestEarlierParentDeadlineWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			<-ctx.Done()
			return Response{}, errors.New("client error")
		}), time.Hour)

		start := time.Now()
		parent, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if _, err := wrapper.Call(parent, Request{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call error = %v, want context.DeadlineExceeded from the parent", err)
		}
		if got := time.Since(start); got != 10*time.Millisecond {
			t.Fatalf("Call returned after %v, want 10ms (the parent's deadline)", got)
		}
	})
}

func TestAlreadyCancelledParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			return Response{Value: "ignored"}, ctx.Err()
		}), time.Hour)

		parent, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := wrapper.Call(parent, Request{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Call error = %v, want context.Canceled", err)
		}
	})
}

func TestCallWaitsForClientThatIgnoresContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		returned := false
		wrapper := mustNew(t, clientFunc(func(context.Context, Request) (Response, error) {
			time.Sleep(30 * time.Millisecond) // ignores its context
			returned = true
			return Response{Value: "late"}, nil
		}), 10*time.Millisecond)

		start := time.Now()
		_, err := wrapper.Call(context.Background(), Request{})
		if !returned {
			t.Fatal("Call returned before the wrapped client returned")
		}
		if got := time.Since(start); got != 30*time.Millisecond {
			t.Fatalf("Call returned after %v, want 30ms (when the client returned)", got)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call error = %v, want context.DeadlineExceeded: the timeout expired before the client returned", err)
		}
	})
}

func TestDerivedContextIsReleasedAfterCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var seen context.Context
		wrapper := mustNew(t, clientFunc(func(ctx context.Context, _ Request) (Response, error) {
			seen = ctx
			return Response{Value: "ok"}, nil
		}), time.Hour)

		if _, err := wrapper.Call(context.Background(), Request{}); err != nil {
			t.Fatalf("Call returned error: %v", err)
		}
		if seen == nil {
			t.Fatal("client was not called")
		}
		if err := seen.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("derived context error after Call = %v, want context.Canceled: cancel must run when Call returns", err)
		}
	})
}

func TestNewValidatesConfiguration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valid := clientFunc(func(context.Context, Request) (Response, error) {
			return Response{}, nil
		})
		if _, err := New(nil, time.Second); err == nil {
			t.Error("New returned nil error for a nil client")
		}
		if _, err := New(valid, 0); err == nil {
			t.Error("New returned nil error for a zero timeout")
		}
		if _, err := New(valid, -time.Second); err == nil {
			t.Error("New returned nil error for a negative timeout")
		}
	})
}
