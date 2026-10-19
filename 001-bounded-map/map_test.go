package boundedmap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestPreservesOrderAndBoundsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inputs := make([]Input, 12)
		for i := range inputs {
			inputs[i] = Input{Value: i}
		}

		var active, maximum atomic.Int32
		outputs, err := Map(context.Background(), inputs, 3, func(_ context.Context, in Input) (Output, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			// Later inputs finish first, so completion order differs from input order.
			time.Sleep(time.Duration(12-in.Value) * time.Millisecond)
			return Output{Value: in.Value * 10}, nil
		})
		if err != nil {
			t.Fatalf("Map returned error: %v", err)
		}
		if got := maximum.Load(); got != 3 {
			t.Fatalf("maximum concurrent calls = %d, want exactly 3 (the limit must be used, not exceeded)", got)
		}
		if len(outputs) != len(inputs) {
			t.Fatalf("len(outputs) = %d, want %d", len(outputs), len(inputs))
		}
		for i, out := range outputs {
			if out.Value != i*10 {
				t.Fatalf("outputs[%d] = %d, want %d", i, out.Value, i*10)
			}
		}
	})
}

func TestReturnsParentCancellationEvenIfFunctionSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var calls atomic.Int32
		done := make(chan error, 1)
		go func() {
			_, err := Map(ctx, []Input{{Value: 1}, {Value: 2}, {Value: 3}}, 1, func(ctx context.Context, _ Input) (Output, error) {
				calls.Add(1)
				<-ctx.Done()
				// Deliberately reports success: Map must not rely on fn to surface cancellation.
				return Output{}, nil
			})
			done <- err
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Map error = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("Map did not return after parent cancellation")
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("fn calls = %d, want 1: work was started after cancellation", got)
		}
	})
}

func TestErrorCancelsInFlightAndStopsNewWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errBoom := errors.New("boom")
		inputs := make([]Input, 10)
		for i := range inputs {
			inputs[i] = Input{Value: i}
		}

		var calls atomic.Int32
		_, err := Map(context.Background(), inputs, 2, func(ctx context.Context, in Input) (Output, error) {
			calls.Add(1)
			if in.Value == 0 {
				return Output{}, errBoom
			}
			// Every other call runs until it is cancelled.
			<-ctx.Done()
			return Output{}, ctx.Err()
		})
		if !errors.Is(err, errBoom) {
			t.Fatalf("Map error = %v, want %v", err, errBoom)
		}
		if got := calls.Load(); got > 3 {
			t.Fatalf("fn calls = %d, want at most 3: Map kept starting work after the first failure", got)
		}
	})
}

func TestDoesNotReturnWhileStartedCallIsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errFirst := errors.New("first call failed")
		secondStarted := make(chan struct{})
		releaseSecond := make(chan struct{})

		done := make(chan error, 1)
		go func() {
			_, err := Map(context.Background(), []Input{{Value: 1}, {Value: 2}}, 2, func(ctx context.Context, in Input) (Output, error) {
				if in.Value == 1 {
					<-secondStarted
					return Output{}, errFirst
				}
				close(secondStarted)
				<-ctx.Done()
				// Still running after cancellation, e.g. finishing a write.
				<-releaseSecond
				return Output{}, nil
			})
			done <- err
		}()

		synctest.Wait()
		select {
		case err := <-done:
			close(releaseSecond)
			t.Fatalf("Map returned %v while a started call was still running", err)
		default:
		}

		close(releaseSecond)
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, errFirst) {
				t.Fatalf("Map error = %v, want %v", err, errFirst)
			}
		default:
			t.Fatal("Map did not return after every started call returned")
		}
	})
}

func TestEmptyInputAndInvalidArguments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		fn := func(context.Context, Input) (Output, error) {
			calls.Add(1)
			return Output{}, nil
		}

		outputs, err := Map(context.Background(), nil, 2, fn)
		if err != nil || len(outputs) != 0 {
			t.Fatalf("Map(empty) = %v, %v; want empty output and nil error", outputs, err)
		}
		for _, limit := range []int{0, -1} {
			if _, err := Map(context.Background(), []Input{{Value: 1}}, limit, fn); err == nil {
				t.Fatalf("Map(limit=%d) returned nil error", limit)
			}
		}
		if _, err := Map(context.Background(), []Input{{Value: 1}}, 1, nil); err == nil {
			t.Fatal("Map(nil fn) returned nil error")
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn calls = %d, want 0 for invalid arguments", got)
		}
	})
}
