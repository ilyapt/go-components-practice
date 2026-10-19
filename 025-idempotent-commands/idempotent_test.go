package idempotentcommands

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type callResult struct {
	result Result
	err    error
}

func mustNew(t *testing.T, workCtx context.Context, handler Handler) *Processor {
	t.Helper()
	p, err := New(workCtx, handler)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return p
}

func handleAsync(p *Processor, ctx context.Context, cmd Command) <-chan callResult {
	done := make(chan callResult, 1)
	go func() {
		result, err := p.Handle(ctx, cmd)
		done <- callResult{result: result, err: err}
	}()
	return done
}

// This test runs first: if a mutex is held across the handler, later tests
// hang in synctest.Wait instead of failing with a clear message.
func TestDifferentIDsDoNotWaitForEachOther(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		p := mustNew(t, context.Background(), func(_ context.Context, cmd Command) (Result, error) {
			if cmd.ID == "slow" {
				<-release
			}
			return Result{Value: cmd.ID}, nil
		})

		slow := handleAsync(p, context.Background(), Command{ID: "slow"})
		synctest.Wait() // the slow handler is running

		fast := handleAsync(p, context.Background(), Command{ID: "fast"})
		var got callResult
		// synctest.Wait cannot be used here: if the implementation holds a mutex
		// across the slow handler, the fast caller is blocked on that mutex and
		// synctest.Wait would never return. Yield instead and report the failure.
		returned := yieldUntil(func() bool {
			select {
			case got = <-fast:
				return true
			default:
				return false
			}
		})
		close(release)
		if !returned {
			<-fast
			t.Fatal("Handle for a different ID waited for a running handler: is a mutex held across the handler call?")
		}
		if got.err != nil || got.result.Value != "fast" {
			t.Fatalf("fast Handle = (%q, %v), want (fast, nil)", got.result.Value, got.err)
		}
		if got := <-slow; got.err != nil || got.result.Value != "slow" {
			t.Fatalf("slow Handle = (%q, %v), want (slow, nil)", got.result.Value, got.err)
		}
	})
}

func TestConcurrentAndLaterDuplicatesShareOneResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(context.Context, Command) (Result, error) {
			calls.Add(1)
			<-release
			return Result{Value: "result"}, nil
		})

		cmd := Command{ID: "id", Payload: []byte("payload")}
		first := handleAsync(p, context.Background(), cmd)
		synctest.Wait() // the handler is now running
		second := handleAsync(p, context.Background(), cmd)
		synctest.Wait()
		close(release)

		for _, done := range []<-chan callResult{first, second} {
			got := <-done
			if got.err != nil || got.result.Value != "result" {
				t.Fatalf("Handle returned (%q, %v), want (result, nil)", got.result.Value, got.err)
			}
		}

		result, err := p.Handle(context.Background(), cmd)
		if err != nil || result.Value != "result" {
			t.Fatalf("later Handle returned (%q, %v), want (result, nil)", result.Value, err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
	})
}

func TestManyConcurrentDuplicatesInvokeHandlerOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(_ context.Context, cmd Command) (Result, error) {
			calls.Add(1)
			<-release
			return Result{Value: string(cmd.Payload)}, nil
		})

		const callers = 20
		cmd := Command{ID: "id", Payload: []byte("payload")}
		results := make([]callResult, callers)
		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				result, err := p.Handle(context.Background(), cmd)
				results[i] = callResult{result: result, err: err}
			})
		}
		synctest.Wait() // every caller has either claimed the ID or is waiting
		close(release)
		wg.Wait()

		for i, got := range results {
			if got.err != nil || got.result.Value != "payload" {
				t.Fatalf("caller %d got (%q, %v), want (payload, nil)", i, got.result.Value, got.err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
	})
}

func TestPayloadConflictDoesNotInvokeHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(context.Context, Command) (Result, error) {
			calls.Add(1)
			<-release
			return Result{Value: "result"}, nil
		})

		first := handleAsync(p, context.Background(), Command{ID: "id", Payload: []byte("one")})
		synctest.Wait()

		// A conflict is detected immediately, without waiting for the running command.
		_, err := p.Handle(context.Background(), Command{ID: "id", Payload: []byte("two")})
		if !errors.Is(err, ErrPayloadConflict) {
			t.Fatalf("conflicting Handle error = %v, want ErrPayloadConflict", err)
		}
		close(release)
		if got := <-first; got.err != nil {
			t.Fatalf("first Handle returned error: %v", got.err)
		}

		_, err = p.Handle(context.Background(), Command{ID: "id", Payload: []byte("two")})
		if !errors.Is(err, ErrPayloadConflict) {
			t.Fatalf("later conflicting Handle error = %v, want ErrPayloadConflict", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
	})
}

func TestHandlerErrorIsCached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("failed")
		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(context.Context, Command) (Result, error) {
			calls.Add(1)
			return Result{}, wantErr
		})

		cmd := Command{ID: "id", Payload: []byte("payload")}
		for range 2 {
			if _, err := p.Handle(context.Background(), cmd); !errors.Is(err, wantErr) {
				t.Fatalf("Handle error = %v, want %v", err, wantErr)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1: failures must not be retried", got)
		}
	})
}

func TestWaiterCancellationDoesNotCancelSharedExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.Background())
		defer cancelWork()
		release := make(chan struct{})
		var calls atomic.Int32
		p := mustNew(t, workCtx, func(ctx context.Context, _ Command) (Result, error) {
			calls.Add(1)
			if ctx != workCtx {
				t.Errorf("handler received a different context, want the workCtx passed to New")
			}
			select {
			case <-release:
				return Result{Value: "result"}, nil
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		})

		waiterCtx, cancelWaiter := context.WithCancel(context.Background())
		cmd := Command{ID: "id", Payload: []byte("payload")}
		first := handleAsync(p, waiterCtx, cmd)
		synctest.Wait()
		cancelWaiter()
		synctest.Wait()

		select {
		case got := <-first:
			if !errors.Is(got.err, context.Canceled) {
				t.Fatalf("cancelled waiter error = %v, want context.Canceled", got.err)
			}
		default:
			t.Fatal("cancelled waiter did not return promptly")
		}

		second := handleAsync(p, context.Background(), cmd)
		synctest.Wait()
		close(release)
		got := <-second
		if got.err != nil || got.result.Value != "result" {
			t.Fatalf("second waiter returned (%q, %v), want (result, nil)", got.result.Value, got.err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
	})
}

func TestPreCancelledCallerDoesNotClaimID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(_ context.Context, cmd Command) (Result, error) {
			calls.Add(1)
			return Result{Value: string(cmd.Payload)}, nil
		})

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := p.Handle(cancelled, Command{ID: "id", Payload: []byte("one")}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Handle error = %v, want context.Canceled", err)
		}
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("handler calls = %d after a pre-cancelled Handle, want 0", got)
		}

		// The ID was never claimed, so a different payload is not a conflict.
		result, err := p.Handle(context.Background(), Command{ID: "id", Payload: []byte("two")})
		if err != nil || result.Value != "two" {
			t.Fatalf("Handle after pre-cancelled caller = (%q, %v), want (two, nil)", result.Value, err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
	})
}

func TestValidationAndCancelledWorkContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if _, err := New(context.Background(), nil); !errors.Is(err, ErrNilHandler) {
			t.Fatalf("New nil handler error = %v, want ErrNilHandler", err)
		}

		var calls atomic.Int32
		p := mustNew(t, context.Background(), func(context.Context, Command) (Result, error) {
			calls.Add(1)
			return Result{}, nil
		})
		if _, err := p.Handle(context.Background(), Command{}); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("empty ID error = %v, want ErrInvalidCommand", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("handler calls = %d, want 0", got)
		}

		// A processor whose work context is already cancelled does not run commands.
		workCtx, cancelWork := context.WithCancel(context.Background())
		cancelWork()
		workProcessor := mustNew(t, workCtx, func(context.Context, Command) (Result, error) {
			calls.Add(1)
			return Result{}, nil
		})
		for range 2 {
			if _, err := workProcessor.Handle(context.Background(), Command{ID: "work"}); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled work context error = %v, want context.Canceled", err)
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("handler calls = %d, want 0", got)
		}
	})
}

// yieldUntil lets other goroutines run until cond holds or a generous number of
// scheduling rounds has passed. It is used only where a buggy implementation
// would leave a goroutine blocked on a sync.Mutex, which synctest does not
// treat as durably blocked.
func yieldUntil(cond func() bool) bool {
	for range 10000 {
		if cond() {
			return true
		}
		runtime.Gosched()
	}
	return cond()
}
