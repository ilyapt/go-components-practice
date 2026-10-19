package orderedparallel

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func item(i int) Item { return Item{ID: i, Value: "v" + strconv.Itoa(i)} }

func echo(_ context.Context, it Item) (Output, error) {
	return Output{Value: "out-" + it.Value}, nil
}

// bufferedInput returns a channel pre-filled with items 0..n-1. The number of
// items the stage has taken from it is always cap(in) - len(in).
func bufferedInput(n int, closeIt bool) chan Item {
	in := make(chan Item, n)
	for i := range n {
		in <- item(i)
	}
	if closeIt {
		close(in)
	}
	return in
}

func taken(in chan Item) int { return cap(in) - len(in) }

func mustRun(t *testing.T, ctx context.Context, in <-chan Item, workers, window int, fn ProcessFunc) <-chan Result {
	t.Helper()
	out, err := Run(ctx, in, workers, window, fn)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if out == nil {
		t.Fatal("Run returned a nil channel and no error")
	}
	return out
}

// goCollect reads out until it is closed, then sends everything it received.
func goCollect(out <-chan Result) <-chan []Result {
	ch := make(chan []Result, 1)
	go func() {
		var got []Result
		for r := range out {
			got = append(got, r)
		}
		ch <- got
	}()
	return ch
}

// receive waits for the stage to settle and takes the next emitted result.
func receive(t *testing.T, out <-chan Result, what string) Result {
	t.Helper()
	synctest.Wait()
	select {
	case r, ok := <-out:
		if !ok {
			t.Fatalf("%s: returned channel is closed, want a result", what)
		}
		return r
	default:
		t.Fatalf("%s: no result was emitted", what)
		return Result{}
	}
}

func assertNothingEmitted(t *testing.T, out <-chan Result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r, ok := <-out:
		if !ok {
			t.Fatalf("%s: returned channel is closed, want it open with nothing emitted", what)
		}
		t.Fatalf("%s: result for item %d was emitted, want nothing", what, r.Item.ID)
	default:
	}
}

func assertClosed(t *testing.T, out <-chan Result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r, ok := <-out:
		if ok {
			t.Fatalf("%s: result for item %d was emitted, want the channel closed", what, r.Item.ID)
		}
	default:
		t.Fatalf("%s: returned channel is still open, want it closed", what)
	}
}

// assertInOrder checks that got holds exactly the echo results of items 0..n-1.
func assertInOrder(t *testing.T, got []Result, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("received %d results, want %d", len(got), n)
	}
	for i, r := range got {
		if r.Item != item(i) {
			t.Fatalf("result %d is for item %+v, want %+v: results must follow input order", i, r.Item, item(i))
		}
		if want := "out-" + item(i).Value; r.Err != nil || r.Output.Value != want {
			t.Fatalf("result %d = %q, %v; want %q, nil", i, r.Output.Value, r.Err, want)
		}
	}
}

// gauge counts concurrent calls of fn and remembers the peak.
type gauge struct {
	mu           sync.Mutex
	active, peak int
}

func (g *gauge) enter() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active++
	g.peak = max(g.peak, g.active)
}

func (g *gauge) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
}

func (g *gauge) get() (active, peak int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active, g.peak
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Never closed: a goroutine started despite a validation error would
		// block on it forever, and synctest would report it.
		in := make(chan Item)
		for _, tc := range []struct {
			name            string
			in              <-chan Item
			workers, window int
			fn              ProcessFunc
			wantErr         error
		}{
			{name: "zero workers", in: in, workers: 0, window: 1, fn: echo, wantErr: ErrInvalidConfig},
			{name: "negative workers", in: in, workers: -1, window: 1, fn: echo, wantErr: ErrInvalidConfig},
			{name: "zero window", in: in, workers: 1, window: 0, fn: echo, wantErr: ErrInvalidConfig},
			{name: "window smaller than workers", in: in, workers: 3, window: 2, fn: echo, wantErr: ErrInvalidConfig},
			{name: "nil fn", in: in, workers: 1, window: 1, fn: nil, wantErr: ErrNilFunc},
			{name: "nil input", in: nil, workers: 1, window: 1, fn: echo, wantErr: ErrNilInput},
		} {
			out, err := Run(context.Background(), tc.in, tc.workers, tc.window, tc.fn)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: Run error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if out != nil {
				t.Errorf("%s: Run returned a non-nil channel with an error", tc.name)
			}
		}

		// window == workers is the smallest valid window.
		out := mustRun(t, context.Background(), bufferedInput(3, true), 2, 2, echo)
		assertInOrder(t, <-goCollect(out), 3)
	})
}

func TestEmitsInInputOrderDespiteCompletionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 20
		// Earlier items are slower, so completion order is the reverse of
		// input order within every batch the workers pick up.
		out := mustRun(t, context.Background(), bufferedInput(n, true), 4, 8, func(ctx context.Context, it Item) (Output, error) {
			time.Sleep(time.Duration(n-it.ID) * time.Millisecond)
			return echo(ctx, it)
		})
		if c := cap(out); c != 0 {
			t.Fatalf("returned channel has capacity %d, want an unbuffered channel", c)
		}
		var got []Result
		for r := range out {
			got = append(got, r)
		}
		assertInOrder(t, got, n)
	})
}

func TestAtMostWorkersCallsRunConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		release := make(chan struct{})
		var g gauge
		out := mustRun(t, context.Background(), bufferedInput(30, true), workers, 6, func(ctx context.Context, it Item) (Output, error) {
			g.enter()
			defer g.leave()
			<-release
			time.Sleep(time.Duration(it.ID%4) * time.Millisecond)
			return echo(ctx, it)
		})
		results := goCollect(out)

		synctest.Wait()
		if active, _ := g.get(); active != workers {
			t.Fatalf("%d calls of fn running with input available and the window not full, want exactly %d", active, workers)
		}

		close(release)
		assertInOrder(t, <-results, 30)
		if _, peak := g.get(); peak > workers {
			t.Fatalf("peak concurrent calls of fn = %d, want <= %d", peak, workers)
		}
	})
}

func TestWindowBoundsItemsTakenButNotEmitted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const window, n = 5, 20
		in := bufferedInput(n, true)
		out := mustRun(t, context.Background(), in, 2, window, echo)

		synctest.Wait()
		if got := taken(in); got != window {
			t.Fatalf("stage took %d items while the consumer read none, want exactly window = %d", got, window)
		}

		var got []Result
		for received := 1; received <= n; received++ {
			got = append(got, receive(t, out, fmt.Sprintf("result %d", received-1)))
			synctest.Wait()
			if want, have := min(n, received+window), taken(in); have != want {
				t.Fatalf("after the consumer received %d results the stage had taken %d items, want %d (received + window)", received, have, want)
			}
		}
		assertClosed(t, out, "after the last result")
		assertInOrder(t, got, n)
	})
}

func TestBackpressureBlocksProducerWhenWindowIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const window, n = 3, 10
		in := make(chan Item)
		var sent atomic.Int32
		go func() {
			for i := range n {
				in <- item(i)
				sent.Add(1)
			}
			close(in)
		}()
		out := mustRun(t, context.Background(), in, 2, window, echo)

		synctest.Wait()
		if got := sent.Load(); got != window {
			t.Fatalf("producer completed %d sends while the consumer read nothing, want %d: a full window must stop the stage from reading", got, window)
		}

		first := receive(t, out, "head result")
		synctest.Wait()
		if got := sent.Load(); got != window+1 {
			t.Fatalf("producer completed %d sends after the consumer received one result, want %d", got, window+1)
		}

		rest := <-goCollect(out)
		assertInOrder(t, append([]Result{first}, rest...), n)
	})
}

func TestSlowHeadStallsTheStageAtTheWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers, window = 2, 4
		release := make(chan struct{})
		var calls atomic.Int32
		in := bufferedInput(10, true)
		out := mustRun(t, context.Background(), in, workers, window, func(ctx context.Context, it Item) (Output, error) {
			calls.Add(1)
			if it.ID == 0 {
				<-release
			}
			return echo(ctx, it)
		})

		assertNothingEmitted(t, out, "while item 0 is still running")
		if got := calls.Load(); got != window {
			t.Fatalf("fn was called for %d items while item 0 was running, want %d: items 1-3 must be processed ahead within the window, item 4 must wait", got, window)
		}
		if got := taken(in); got != window {
			t.Fatalf("stage took %d items while item 0 was running, want %d", got, window)
		}

		close(release)
		assertInOrder(t, <-goCollect(out), 10)
	})
}

func TestPerItemErrorIsDeliveredInPlaceAndStageContinues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errBoom := errors.New("boom")
		out := mustRun(t, context.Background(), bufferedInput(6, true), 2, 3, func(ctx context.Context, it Item) (Output, error) {
			if it.ID == 2 {
				return Output{Value: "partial"}, errBoom
			}
			return echo(ctx, it)
		})

		got := <-goCollect(out)
		if len(got) != 6 {
			t.Fatalf("received %d results, want 6: an error from fn must not stop the stage", len(got))
		}
		for i, r := range got {
			if r.Item != item(i) {
				t.Fatalf("result %d is for item %+v, want %+v", i, r.Item, item(i))
			}
			switch {
			case i == 2:
				if !errors.Is(r.Err, errBoom) || r.Output.Value != "partial" {
					t.Fatalf("failed item result = %q, %v; want what fn returned: %q, %v", r.Output.Value, r.Err, "partial", errBoom)
				}
			case r.Err != nil || r.Output.Value != "out-"+r.Item.Value:
				t.Fatalf("result %d = %q, %v; want %q, nil", i, r.Output.Value, r.Err, "out-"+r.Item.Value)
			}
		}
	})
}

func TestEmptyInputClosesOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		out := mustRun(t, context.Background(), bufferedInput(0, true), 2, 2, func(ctx context.Context, it Item) (Output, error) {
			calls.Add(1)
			return echo(ctx, it)
		})
		if got := <-goCollect(out); len(got) != 0 {
			t.Fatalf("received %d results from an empty input, want 0", len(got))
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn called %d times for an empty input, want 0", got)
		}
	})
}

func TestOutputStaysOpenUntilInputIsClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := bufferedInput(3, false)
		out := mustRun(t, context.Background(), in, 2, 4, echo)

		var got []Result
		for i := range 3 {
			got = append(got, receive(t, out, fmt.Sprintf("result %d", i)))
		}
		assertNothingEmitted(t, out, "every taken item emitted, input still open")

		in <- item(3)
		got = append(got, receive(t, out, "result 3"))
		close(in)
		assertClosed(t, out, "after the input was closed and every item emitted")
		assertInOrder(t, got, 4)
	})
}

func TestProcessFuncReceivesContextDerivedFromRunContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey struct{}
		ctx := context.WithValue(context.Background(), ctxKey{}, "run")
		var mu sync.Mutex
		var seen []any
		out := mustRun(t, ctx, bufferedInput(3, true), 2, 2, func(ctx context.Context, it Item) (Output, error) {
			mu.Lock()
			seen = append(seen, ctx.Value(ctxKey{}))
			mu.Unlock()
			return echo(ctx, it)
		})

		assertInOrder(t, <-goCollect(out), 3)
		mu.Lock()
		defer mu.Unlock()
		for i, v := range seen {
			if v != "run" {
				t.Fatalf("call %d of fn did not receive the Run context or a context derived from it", i)
			}
		}
	})
}

func TestCancellationStopsTakingInputAndStartsNoNewCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 2
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls atomic.Int32
		in := bufferedInput(10, true)
		out := mustRun(t, ctx, in, workers, 4, func(ctx context.Context, _ Item) (Output, error) {
			calls.Add(1)
			<-ctx.Done() // returns only through cancellation of the Run context
			return Output{}, ctx.Err()
		})
		results := goCollect(out)

		synctest.Wait()
		if got := calls.Load(); got != workers {
			t.Fatalf("fn running for %d items before cancellation, want %d", got, workers)
		}
		takenBefore := taken(in)

		cancel()
		got := <-results // the stage must close the channel after cancellation
		if n := taken(in); n != takenBefore {
			t.Fatalf("stage took %d more items after ctx was cancelled, want 0", n-takenBefore)
		}
		if n := calls.Load(); n != workers {
			t.Fatalf("fn was called %d times, want %d: no new call may start after ctx is cancelled", n, workers)
		}
		if len(got) > takenBefore {
			t.Fatalf("received %d results but only %d items were taken", len(got), takenBefore)
		}
		for i, r := range got {
			if r.Item != item(i) {
				t.Fatalf("result %d is for item %+v: results emitted around cancellation must still be a gapless prefix in input order", i, r.Item)
			}
		}
	})
}

func TestCancellationClosesOutputOnlyAfterRunningCallsReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		var g gauge
		out := mustRun(t, ctx, bufferedInput(10, true), 2, 4, func(ctx context.Context, _ Item) (Output, error) {
			g.enter()
			defer g.leave()
			<-ctx.Done()
			<-release // cleanup after cancellation takes a while
			return Output{}, ctx.Err()
		})
		results := goCollect(out)
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case got := <-results:
			active, _ := g.get()
			t.Fatalf("returned channel closed (after %d results) while %d calls of fn were still running", len(got), active)
		default:
		}

		close(release)
		<-results
		if active, _ := g.get(); active != 0 {
			t.Fatalf("returned channel closed while %d calls of fn were still running", active)
		}
	})
}

func TestCancelledStageClosesOutputWithoutAReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := mustRun(t, ctx, bufferedInput(10, true), 2, 4, echo)

		// Item 0 is received; results 1-3 are finished and waiting to be emitted.
		if r := receive(t, out, "head result"); r.Item != item(0) {
			t.Fatalf("first result is for item %+v, want %+v", r.Item, item(0))
		}

		// The consumer stops reading by cancelling ctx and never reads again.
		cancel()
		synctest.Wait()
		select {
		case r, ok := <-out:
			if ok {
				t.Fatalf("stage was still trying to emit item %d after ctx was cancelled with nobody reading, want the channel closed", r.Item.ID)
			}
		default:
			t.Fatal("returned channel is still open after ctx was cancelled and fn returned; the stage must close it even without a reader")
		}
	})
}

func TestPreCancelledContextReadsNothingAndCallsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls atomic.Int32
		fn := func(ctx context.Context, it Item) (Output, error) {
			calls.Add(1)
			return echo(ctx, it)
		}

		// Repeated because a select with both ctx.Done() and in ready picks a
		// case at random.
		for attempt := range 16 {
			in := bufferedInput(5, false) // open: the stage must not wait for it to close
			out, err := Run(ctx, in, 2, 4, fn)
			if err != nil || out == nil {
				t.Fatalf("attempt %d: Run with a done ctx = %v, %v; want a non-nil channel and a nil error: a done ctx is not a validation error", attempt, out, err)
			}
			if got := <-goCollect(out); len(got) != 0 {
				t.Fatalf("attempt %d: received %d results with a ctx that was already done, want 0", attempt, len(got))
			}
			if n := taken(in); n != 0 {
				t.Fatalf("attempt %d: stage took %d items with a ctx that was already done, want 0", attempt, n)
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fn called %d times with a ctx that was already done, want 0", got)
		}
	})
}
