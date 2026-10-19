package hedgedrequest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

const delay = 10 * time.Millisecond

type providerFunc func(context.Context, Request) (Response, error)

func (f providerFunc) Fetch(ctx context.Context, req Request) (Response, error) { return f(ctx, req) }

func mustNew(t *testing.T, providers []Provider, hedgeDelay time.Duration, maxAttempts int) *Hedger {
	t.Helper()
	h, err := New(providers, hedgeDelay, maxAttempts)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return h
}

// behavior is what a provider does in one Fetch call. n is the call's index
// in the tracker's start order, counted over all providers of the tracker.
type behavior func(ctx context.Context, req Request, n int) (Response, error)

// hang blocks until the attempt is cancelled.
func hang(ctx context.Context, _ Request, _ int) (Response, error) {
	<-ctx.Done()
	return Response{}, ctx.Err()
}

// succeedAfter answers with source after d, unless cancelled first.
func succeedAfter(d time.Duration, source string) behavior {
	return func(ctx context.Context, _ Request, _ int) (Response, error) {
		select {
		case <-time.After(d):
			return Response{Source: source, Payload: []byte("payload-" + source)}, nil
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	}
}

// failAfter returns err after d, unless cancelled first.
func failAfter(d time.Duration, err error) behavior {
	return func(ctx context.Context, _ Request, _ int) (Response, error) {
		select {
		case <-time.After(d):
			return Response{}, err
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	}
}

// attempt is one recorded Fetch call.
type attempt struct {
	provider string
	key      string
	at       time.Duration // start time relative to the tracker's creation
	ctx      context.Context
}

// tracker records every Fetch call of its providers in start order.
type tracker struct {
	start    time.Time
	mu       sync.Mutex
	attempts []attempt
}

func newTracker() *tracker { return &tracker{start: time.Now()} }

func (tr *tracker) provider(name string, b behavior) Provider {
	return providerFunc(func(ctx context.Context, req Request) (Response, error) {
		tr.mu.Lock()
		n := len(tr.attempts)
		tr.attempts = append(tr.attempts, attempt{provider: name, key: req.Key, at: time.Since(tr.start), ctx: ctx})
		tr.mu.Unlock()
		return b(ctx, req, n)
	})
}

func (tr *tracker) snapshot() []attempt {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]attempt(nil), tr.attempts...)
}

// schedule renders the attempts for key ("" means all keys) as "A@0s B@10ms".
func (tr *tracker) schedule(key string) string {
	var parts []string
	for _, a := range tr.snapshot() {
		if key == "" || a.key == key {
			parts = append(parts, fmt.Sprintf("%s@%v", a.provider, a.at))
		}
	}
	return strings.Join(parts, " ")
}

// assertAllCancelled checks that the context of every recorded attempt is done.
func (tr *tracker) assertAllCancelled(t *testing.T, key string) {
	t.Helper()
	for i, a := range tr.snapshot() {
		if (key == "" || a.key == key) && a.ctx.Err() == nil {
			t.Errorf("attempt %d (%s@%v) still has a live context after Do returned", i, a.provider, a.at)
		}
	}
}

type result struct {
	resp Response
	err  error
	at   time.Duration // how long Do took, in virtual time
}

// call runs Do on the test goroutine, so virtual time advances while it waits.
func call(h *Hedger, ctx context.Context, req Request) result {
	start := time.Now()
	resp, err := h.Do(ctx, req)
	return result{resp, err, time.Since(start)}
}

// goDo runs Do in a new goroutine and returns a channel with its result.
func goDo(h *Hedger, ctx context.Context, req Request) <-chan result {
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		resp, err := h.Do(ctx, req)
		ch <- result{resp, err, time.Since(start)}
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
		t.Fatalf("%s: Do has not returned", what)
		return result{}
	}
}

func assertBlocked(t *testing.T, ch <-chan result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: Do returned %+v, %v; want it still waiting", what, r.resp, r.err)
	default:
	}
}

func assertSuccess(t *testing.T, r result, source string, at time.Duration) {
	t.Helper()
	if r.err != nil || r.resp.Source != source || string(r.resp.Payload) != "payload-"+source {
		t.Fatalf("Do = %+v, %v; want the response of %s unchanged and nil", r.resp, r.err, source)
	}
	if r.at != at {
		t.Fatalf("Do returned after %v, want %v", r.at, at)
	}
}

// assertNoLateAttempts lets a long time pass and checks the schedule is unchanged.
func assertNoLateAttempts(t *testing.T, tr *tracker, key, want string) {
	t.Helper()
	time.Sleep(10 * delay)
	synctest.Wait()
	if got := tr.schedule(key); got != want {
		t.Fatalf("attempts after Do returned = %q, want %q: no attempt may start after Do returns", got, want)
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ok := providerFunc(func(context.Context, Request) (Response, error) { return Response{}, nil })
		for _, tc := range []struct {
			name        string
			providers   []Provider
			hedgeDelay  time.Duration
			maxAttempts int
			wantErr     error
		}{
			{name: "nil providers", providers: nil, hedgeDelay: delay, maxAttempts: 2, wantErr: ErrInvalidConfig},
			{name: "empty providers", providers: []Provider{}, hedgeDelay: delay, maxAttempts: 2, wantErr: ErrInvalidConfig},
			{name: "zero delay", providers: []Provider{ok}, hedgeDelay: 0, maxAttempts: 2, wantErr: ErrInvalidConfig},
			{name: "negative delay", providers: []Provider{ok}, hedgeDelay: -delay, maxAttempts: 2, wantErr: ErrInvalidConfig},
			{name: "zero attempts", providers: []Provider{ok}, hedgeDelay: delay, maxAttempts: 0, wantErr: ErrInvalidConfig},
			{name: "negative attempts", providers: []Provider{ok}, hedgeDelay: delay, maxAttempts: -1, wantErr: ErrInvalidConfig},
			{name: "nil provider", providers: []Provider{ok, nil}, hedgeDelay: delay, maxAttempts: 2, wantErr: ErrNilProvider},
		} {
			h, err := New(tc.providers, tc.hedgeDelay, tc.maxAttempts)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if h != nil {
				t.Errorf("%s: New returned a non-nil Hedger with an error", tc.name)
			}
		}
	})
}

func TestFastAnswerSendsNoHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", succeedAfter(5*time.Millisecond, "A")),
			tr.provider("B", hang),
		}, delay, 3)

		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "A", 5*time.Millisecond)
		if got := tr.schedule(""); got != "A@0s" {
			t.Fatalf("attempts = %q, want only A@0s: an answer before hedgeDelay needs no hedge", got)
		}
		assertNoLateAttempts(t, tr, "", "A@0s")
	})
}

func TestHedgeStartsNextProviderAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", succeedAfter(30*time.Millisecond, "A")),
			tr.provider("B", succeedAfter(5*time.Millisecond, "B")),
			tr.provider("C", hang),
		}, delay, 3)

		// B started later but answered first (at 15ms): completion order wins.
		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "B", 15*time.Millisecond)
		if got, want := tr.schedule(""), "A@0s B@10ms"; got != want {
			t.Fatalf("attempts = %q, want %q", got, want)
		}
		tr.assertAllCancelled(t, "")
		assertNoLateAttempts(t, tr, "", "A@0s B@10ms")
	})
}

func TestEarlierAttemptKeepsRunningAfterHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", succeedAfter(25*time.Millisecond, "A")),
			tr.provider("B", hang),
		}, delay, 2)

		// The hedge delay is not a timeout: A must still be able to win at 25ms.
		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "A", 25*time.Millisecond)
		if got, want := tr.schedule(""), "A@0s B@10ms"; got != want {
			t.Fatalf("attempts = %q, want %q", got, want)
		}
		tr.assertAllCancelled(t, "")
	})
}

func TestHedgesUpToMaxAttemptsInProviderOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", hang),
			tr.provider("B", hang),
			tr.provider("C", hang),
			tr.provider("D", hang),
		}, delay, 3)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		r := call(h, ctx, Request{Key: "k"})
		if !errors.Is(r.err, context.DeadlineExceeded) || errors.Is(r.err, ErrAllAttemptsFailed) {
			t.Fatalf("Do error = %v, want the caller's context.DeadlineExceeded, not ErrAllAttemptsFailed", r.err)
		}
		if r.at != 100*time.Millisecond {
			t.Fatalf("Do returned after %v, want 100ms (the caller's deadline)", r.at)
		}
		if got, want := tr.schedule(""), "A@0s B@10ms C@20ms"; got != want {
			t.Fatalf("attempts = %q, want %q: maxAttempts bounds the attempts, in provider order", got, want)
		}
		tr.assertAllCancelled(t, "")
	})
}

func TestAttemptsWrapAroundProviders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		thirdWins := func(name string) behavior {
			return func(ctx context.Context, req Request, n int) (Response, error) {
				if n == 2 {
					return succeedAfter(time.Millisecond, fmt.Sprintf("%s#%d", name, n))(ctx, req, n)
				}
				return hang(ctx, req, n)
			}
		}
		h := mustNew(t, []Provider{
			tr.provider("A", thirdWins("A")),
			tr.provider("B", thirdWins("B")),
		}, delay, 4)

		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "A#2", 21*time.Millisecond)
		if got, want := tr.schedule(""), "A@0s B@10ms A@20ms"; got != want {
			t.Fatalf("attempts = %q, want %q: attempt i goes to providers[i %% len(providers)]", got, want)
		}
		tr.assertAllCancelled(t, "")
		assertNoLateAttempts(t, tr, "", "A@0s B@10ms A@20ms")
	})
}

func TestFailureStartsNextAttemptAndRestartsHedgeTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", failAfter(3*time.Millisecond, errors.New("A failed"))),
			tr.provider("B", hang),
			tr.provider("C", succeedAfter(time.Millisecond, "C")),
		}, delay, 3)

		// A fails at 3ms: B starts at once, and the hedge timer is re-armed
		// for 10ms after B's start, so C starts at 13ms.
		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "C", 14*time.Millisecond)
		if got, want := tr.schedule(""), "A@0s B@3ms C@13ms"; got != want {
			t.Fatalf("attempts = %q, want %q", got, want)
		}
		tr.assertAllCancelled(t, "")
	})
}

func TestFailureOfEarlierAttemptStartsNextAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", failAfter(12*time.Millisecond, errors.New("A failed"))),
			tr.provider("B", hang),
			tr.provider("C", succeedAfter(time.Millisecond, "C")),
		}, delay, 3)

		// B is the latest attempt, but A's failure at 12ms still starts C at once.
		r := call(h, context.Background(), Request{Key: "k"})
		assertSuccess(t, r, "C", 13*time.Millisecond)
		if got, want := tr.schedule(""), "A@0s B@10ms C@12ms"; got != want {
			t.Fatalf("attempts = %q, want %q", got, want)
		}
		tr.assertAllCancelled(t, "")
	})
}

func TestAllAttemptsFailedReturnsSentinelAfterLastFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errA := errors.New("A failed")
		errB := errors.New("B failed")
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", failAfter(15*time.Millisecond, errA)),
			tr.provider("B", failAfter(2*time.Millisecond, errB)),
		}, delay, 2)

		// B fails at 12ms while A is still in flight; A fails at 15ms.
		r := call(h, context.Background(), Request{Key: "k"})
		if !errors.Is(r.err, ErrAllAttemptsFailed) {
			t.Fatalf("Do error = %v, want ErrAllAttemptsFailed", r.err)
		}
		if !errors.Is(r.err, errA) || !errors.Is(r.err, errB) {
			t.Fatalf("Do error = %v, want it to match every attempt's error with errors.Is", r.err)
		}
		if !reflect.DeepEqual(r.resp, Response{}) {
			t.Fatalf("Do response = %+v, want the zero Response with an error", r.resp)
		}
		if r.at != 15*time.Millisecond {
			t.Fatalf("Do returned after %v, want 15ms: wait for the last in-flight attempt, and no longer", r.at)
		}
		if got, want := tr.schedule(""), "A@0s B@10ms"; got != want {
			t.Fatalf("attempts = %q, want %q", got, want)
		}
	})
}

func TestSingleAttemptDoesNotHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errA := errors.New("A failed")
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", failAfter(50*time.Millisecond, errA)),
			tr.provider("B", succeedAfter(time.Millisecond, "B")),
		}, delay, 1)

		r := call(h, context.Background(), Request{Key: "k"})
		if !errors.Is(r.err, ErrAllAttemptsFailed) || !errors.Is(r.err, errA) {
			t.Fatalf("Do error = %v, want ErrAllAttemptsFailed matching A's error", r.err)
		}
		if r.at != 50*time.Millisecond {
			t.Fatalf("Do returned after %v, want 50ms", r.at)
		}
		if got := tr.schedule(""); got != "A@0s" {
			t.Fatalf("attempts = %q, want only A@0s with maxAttempts = 1", got)
		}
	})
}

func TestCallerCancellationCancelsAttemptsAndStopsHedging(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", hang),
			tr.provider("B", hang),
			tr.provider("C", hang),
		}, delay, 3)

		ctx, cancel := context.WithCancel(context.Background())
		ch := goDo(h, ctx, Request{Key: "k"})
		time.Sleep(15 * time.Millisecond)
		assertBlocked(t, ch, "before cancellation")
		if got, want := tr.schedule(""), "A@0s B@10ms"; got != want {
			t.Fatalf("attempts at 15ms = %q, want %q", got, want)
		}

		// The attempts fail with context.Canceled; those failures must neither
		// start attempt C nor turn the result into ErrAllAttemptsFailed.
		cancel()
		r := receive(t, ch, "after cancellation")
		if !errors.Is(r.err, context.Canceled) || errors.Is(r.err, ErrAllAttemptsFailed) {
			t.Fatalf("Do error = %v, want the caller's context.Canceled, not ErrAllAttemptsFailed", r.err)
		}
		if r.at != 15*time.Millisecond {
			t.Fatalf("Do returned after %v, want 15ms (the moment of cancellation)", r.at)
		}
		tr.assertAllCancelled(t, "")
		assertNoLateAttempts(t, tr, "", "A@0s B@10ms")
	})
}

func TestPreCancelledContextStartsNoAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{tr.provider("A", succeedAfter(time.Millisecond, "A"))}, delay, 3)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := call(h, ctx, Request{Key: "k"})
		if !errors.Is(r.err, context.Canceled) || errors.Is(r.err, ErrAllAttemptsFailed) {
			t.Fatalf("Do error = %v, want context.Canceled", r.err)
		}
		synctest.Wait()
		if got := tr.schedule(""); got != "" {
			t.Fatalf("attempts = %q, want none for a pre-cancelled caller", got)
		}
	})
}

func TestDoesNotWaitForCancelledAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		tr := newTracker()
		h := mustNew(t, []Provider{
			// A notices cancellation late and then reports a success anyway.
			tr.provider("A", func(ctx context.Context, _ Request, _ int) (Response, error) {
				<-ctx.Done()
				<-release
				return Response{Source: "A-late"}, nil
			}),
			tr.provider("B", succeedAfter(time.Millisecond, "B")),
		}, delay, 2)

		ch := goDo(h, context.Background(), Request{Key: "k"})
		time.Sleep(20 * time.Millisecond)
		r := receive(t, ch, "while A is still returning")
		assertSuccess(t, r, "B", 11*time.Millisecond)
		tr.assertAllCancelled(t, "")

		// A's late result must be dropped without blocking. A goroutine stuck
		// publishing it makes synctest report "blocked goroutines remain".
		close(release)
		synctest.Wait()
		assertNoLateAttempts(t, tr, "", "A@0s B@10ms")
	})
}

func TestAttemptsRunUnderCallerContextAndAreCancelledOnReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", hang),
			tr.provider("B", succeedAfter(time.Millisecond, "B")),
		}, delay, 2)

		callerCtx := context.WithValue(context.Background(), ctxKey("caller"), "marker")
		r := call(h, callerCtx, Request{Key: "k-42"})
		assertSuccess(t, r, "B", 11*time.Millisecond)

		attempts := tr.snapshot()
		if len(attempts) != 2 {
			t.Fatalf("attempts = %q, want A@0s B@10ms", tr.schedule(""))
		}
		for _, a := range attempts {
			if a.ctx.Value(ctxKey("caller")) != "marker" {
				t.Errorf("attempt %s did not receive a context derived from the caller's context", a.provider)
			}
			if a.key != "k-42" {
				t.Errorf("attempt %s received key %q, want the request unchanged (k-42)", a.provider, a.key)
			}
			if a.ctx.Err() == nil {
				t.Errorf("attempt %s still has a live context after Do returned; Do must cancel every attempt, the winner's included", a.provider)
			}
		}
	})
}

func TestConcurrentCallsAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		h := mustNew(t, []Provider{
			tr.provider("A", hang),
			tr.provider("B", func(ctx context.Context, req Request, n int) (Response, error) {
				if strings.HasPrefix(req.Key, "y") {
					return succeedAfter(time.Millisecond, "B")(ctx, req, n)
				}
				return hang(ctx, req, n)
			}),
		}, delay, 2)

		xCtx, cancelX := context.WithCancel(context.Background())
		x := goDo(h, xCtx, Request{Key: "x"}) // A@0, B@10, never answers
		time.Sleep(5 * time.Millisecond)
		y := goDo(h, context.Background(), Request{Key: "y"}) // A@5, B@15, answers at 16
		time.Sleep(7 * time.Millisecond)

		cancelX() // at 12ms
		if r := receive(t, x, "cancelled call x"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("call x error = %v, want context.Canceled", r.err)
		}
		assertBlocked(t, y, "call y after x was cancelled")
		for _, a := range tr.snapshot() {
			if a.key == "y" && a.ctx.Err() != nil {
				t.Fatalf("cancelling call x cancelled attempt %s of call y", a.provider)
			}
		}

		time.Sleep(8 * time.Millisecond) // 20ms
		assertSuccess(t, receive(t, y, "call y"), "B", 11*time.Millisecond)
		if got, want := tr.schedule("x"), "A@0s B@10ms"; got != want {
			t.Fatalf("attempts of call x = %q, want %q", got, want)
		}
		if got, want := tr.schedule("y"), "A@5ms B@15ms"; got != want {
			t.Fatalf("attempts of call y = %q, want %q: each call has its own hedge timer", got, want)
		}

		// Many concurrent calls, each with its own timeline.
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Go(func() {
				r := call(h, context.Background(), Request{Key: fmt.Sprintf("y-%d", i)})
				if r.err != nil || r.resp.Source != "B" || r.at != 11*time.Millisecond {
					t.Errorf("call y-%d = %+v, %v after %v; want B after 11ms", i, r.resp, r.err, r.at)
				}
			})
		}
		wg.Wait()
	})
}
