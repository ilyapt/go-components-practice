package circuitbreaker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// The tests pass time.Now as the breaker clock. Inside a synctest bubble time
// is virtual, so sleep(d) advances the breaker clock by exactly d.

var errClassified = errors.New("classified provider failure")

func TestClassifiedFailuresOpenAtThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		b := mustNew(t, 2, time.Minute, func(context.Context, Request) (Response, error) {
			calls.Add(1)
			return Response{}, errClassified
		})

		for i := range 2 {
			if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, errClassified) {
				t.Fatalf("failure %d error = %v, want the provider failure (threshold not reached yet)", i+1, err)
			}
		}
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("open breaker error = %v, want ErrCircuitOpen", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("client calls = %d, want 2 (an open breaker must not call the client)", got)
		}
	})
}

func TestSuccessResetsFailuresAndNonClassifiedErrorsDoNotCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nonClassified := errors.New("caller error")
		var calls atomic.Int32
		b := mustNew(t, 2, time.Minute, func(context.Context, Request) (Response, error) {
			switch calls.Add(1) {
			case 1, 4:
				return Response{}, errClassified
			case 2:
				return Response{}, nonClassified
			default:
				return Response{Value: "ok"}, nil
			}
		})

		_, _ = b.Call(context.Background(), Request{}) // classified: 1
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, nonClassified) {
			t.Fatalf("non-classified error = %v, want it returned to the caller", err)
		}
		_, _ = b.Call(context.Background(), Request{}) // success: reset to 0
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, errClassified) {
			t.Fatalf("classified error after reset = %v, want the provider failure", err)
		}
		if _, err := b.Call(context.Background(), Request{}); err != nil {
			t.Fatalf("breaker opened too early after a reset: %v", err)
		}
	})
}

func TestCooldownBoundaryIsInclusive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			if calls.Add(1) == 1 {
				return Response{}, errClassified
			}
			return Response{Value: "probe"}, nil
		})

		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute - time.Nanosecond)
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call 1ns before the cooldown deadline = %v, want ErrCircuitOpen", err)
		}
		sleep(time.Nanosecond)
		if resp, err := b.Call(context.Background(), Request{}); err != nil || resp.Value != "probe" {
			t.Fatalf("call exactly at the cooldown deadline = (%q, %v), want (probe, nil)", resp.Value, err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("client calls = %d, want 2", got)
		}
	})
}

func TestSingleProbeAndSuccessCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probeStarted := make(chan struct{})
		releaseProbe := newGate(t)
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			switch calls.Add(1) {
			case 1:
				return Response{}, errClassified
			case 2:
				close(probeStarted)
				releaseProbe.wait()
				return Response{Value: "probe"}, nil
			default:
				return Response{Value: "closed"}, nil
			}
		})

		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute)

		probe := callAsync(b, Request{})
		<-probeStarted
		if _, err := call(t, b, Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call during the half-open probe = %v, want ErrCircuitOpen", err)
		}
		releaseProbe.open()
		if r := <-probe; r.err != nil || r.resp.Value != "probe" {
			t.Fatalf("probe result = (%q, %v), want (probe, nil)", r.resp.Value, r.err)
		}
		if resp, err := b.Call(context.Background(), Request{}); err != nil || resp.Value != "closed" {
			t.Fatalf("call after recovery = (%q, %v), want (closed, nil)", resp.Value, err)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("client calls = %d, want 3", got)
		}
	})
}

func TestConcurrentCallersAtCooldownGetOneProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := newGate(t)
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			if calls.Add(1) == 1 {
				return Response{}, errClassified
			}
			release.wait()
			return Response{Value: "probe"}, nil
		})
		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute)

		const callers = 20
		results := make(chan callResult, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				resp, err := b.Call(context.Background(), Request{})
				results <- callResult{resp, err}
			})
		}
		// Every caller except the probe must return while the probe is running.
		if !eventually(func() bool { return len(results) == callers-1 && calls.Load() == 2 }) {
			t.Fatalf("while the probe runs: %d of %d callers returned, %d client calls after the cooldown; want %d rejected and exactly 1 probe",
				len(results), callers, calls.Load()-1, callers-1)
		}
		synctest.Wait()
		if got := calls.Load() - 1; got != 1 {
			t.Fatalf("client calls after the cooldown = %d, want exactly 1 probe", got)
		}
		release.open()
		wg.Wait()
		close(results)

		var successes, rejected int
		for r := range results {
			switch {
			case r.err == nil:
				successes++
			case errors.Is(r.err, ErrCircuitOpen):
				rejected++
			default:
				t.Errorf("unexpected error %v", r.err)
			}
		}
		if successes != 1 || rejected != callers-1 {
			t.Fatalf("successes = %d, rejected = %d; want 1 and %d", successes, rejected, callers-1)
		}
	})
}

func TestAnyProbeErrorReopensForNewCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probeErr := errors.New("not classified")
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			switch calls.Add(1) {
			case 1:
				return Response{}, errClassified
			case 2:
				return Response{}, probeErr
			default:
				return Response{Value: "recovered"}, nil
			}
		})

		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute)
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, probeErr) {
			t.Fatalf("probe error = %v, want the non-classified probe error", err)
		}
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call during the restarted cooldown = %v, want ErrCircuitOpen", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("client calls = %d, want 2", got)
		}
		sleep(time.Minute)
		if resp, err := b.Call(context.Background(), Request{}); err != nil || resp.Value != "recovered" {
			t.Fatalf("second probe = (%q, %v), want (recovered, nil)", resp.Value, err)
		}
	})
}

func TestReopenCooldownCountsFromProbeCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			switch calls.Add(1) {
			case 1:
				return Response{}, errClassified
			case 2:
				time.Sleep(30 * time.Second) // a slow failing probe
				return Response{}, errClassified
			default:
				return Response{Value: "ok"}, nil
			}
		})

		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute)
		_, _ = b.Call(context.Background(), Request{}) // probe fails 30s after it started

		sleep(time.Minute - time.Nanosecond)
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call 1ns before cooldown measured from probe completion = %v, want ErrCircuitOpen", err)
		}
		sleep(time.Nanosecond)
		if _, err := b.Call(context.Background(), Request{}); err != nil {
			t.Fatalf("probe at cooldown measured from probe completion = %v, want nil", err)
		}
	})
}

func TestStaleFailureCannotAffectNewClosedPeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slowStarted := make(chan struct{})
		releaseSlow := newGate(t)
		b := mustNew(t, 1, time.Minute, func(_ context.Context, req Request) (Response, error) {
			switch req.Value {
			case "slow-old":
				close(slowStarted)
				releaseSlow.wait()
				return Response{}, errClassified
			case "open":
				return Response{}, errClassified
			default:
				return Response{Value: req.Value}, nil
			}
		})

		slow := callAsync(b, Request{Value: "slow-old"})
		<-slowStarted
		if _, err := call(t, b, Request{Value: "open"}); !errors.Is(err, errClassified) {
			t.Fatalf("opening call error = %v, want the provider failure", err)
		}
		sleep(time.Minute)
		if _, err := call(t, b, Request{Value: "probe"}); err != nil {
			t.Fatalf("probe returned error: %v", err)
		}

		releaseSlow.open()
		if r := <-slow; !errors.Is(r.err, errClassified) {
			t.Fatalf("stale call error = %v, want the provider failure returned to its caller", r.err)
		}
		if resp, err := b.Call(context.Background(), Request{Value: "new"}); err != nil || resp.Value != "new" {
			t.Fatalf("call in the new closed period = (%q, %v), want (new, nil); a stale failure must not count", resp.Value, err)
		}
	})
}

func TestStaleSuccessCannotCloseOpenBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slowStarted := make(chan struct{})
		releaseSlow := newGate(t)
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(_ context.Context, req Request) (Response, error) {
			calls.Add(1)
			if req.Value == "slow-old" {
				close(slowStarted)
				releaseSlow.wait()
				return Response{Value: "late"}, nil
			}
			return Response{}, errClassified
		})

		slow := callAsync(b, Request{Value: "slow-old"})
		<-slowStarted
		if _, err := call(t, b, Request{Value: "open"}); !errors.Is(err, errClassified) {
			t.Fatalf("opening call error = %v, want the provider failure", err)
		}

		releaseSlow.open()
		if r := <-slow; r.err != nil || r.resp.Value != "late" {
			t.Fatalf("stale call = (%q, %v), want (late, nil)", r.resp.Value, r.err)
		}
		if _, err := b.Call(context.Background(), Request{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call after a stale success = %v, want ErrCircuitOpen; a success from the old closed period must not close the breaker", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("client calls = %d, want 2", got)
		}
	})
}

func TestStaleCompletionDuringProbeIsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slowStarted := make(chan struct{})
		releaseSlow := newGate(t)
		probeStarted := make(chan struct{})
		releaseProbe := newGate(t)
		b := mustNew(t, 1, time.Minute, func(_ context.Context, req Request) (Response, error) {
			switch req.Value {
			case "slow-old":
				close(slowStarted)
				releaseSlow.wait()
				return Response{Value: "late"}, nil
			case "probe":
				close(probeStarted)
				releaseProbe.wait()
				return Response{}, errClassified
			default:
				return Response{}, errClassified
			}
		})

		slow := callAsync(b, Request{Value: "slow-old"})
		<-slowStarted
		_, _ = call(t, b, Request{Value: "open"})
		sleep(time.Minute)
		probe := callAsync(b, Request{Value: "probe"})
		<-probeStarted

		releaseSlow.open()
		<-slow
		if _, err := call(t, b, Request{Value: "other"}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call during the probe after a stale success = %v, want ErrCircuitOpen", err)
		}
		releaseProbe.open()
		<-probe
		if _, err := b.Call(context.Background(), Request{Value: "other"}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call after a failed probe = %v, want ErrCircuitOpen", err)
		}
	})
}

func TestClosedCallsDoNotWaitForEachOther(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slowStarted := make(chan struct{})
		releaseSlow := newGate(t)
		b := mustNew(t, 10, time.Minute, func(_ context.Context, req Request) (Response, error) {
			if req.Value == "slow" {
				close(slowStarted)
				releaseSlow.wait()
			}
			return Response{Value: req.Value}, nil
		})

		slow := callAsync(b, Request{Value: "slow"})
		<-slowStarted
		if resp, err := call(t, b, Request{Value: "fast"}); err != nil || resp.Value != "fast" {
			t.Fatalf("fast call = (%q, %v), want (fast, nil)", resp.Value, err)
		}
		releaseSlow.open()
		<-slow
	})
}

func TestPreCancelledCallDoesNotConsumeProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		b := mustNew(t, 1, time.Minute, func(context.Context, Request) (Response, error) {
			if calls.Add(1) == 1 {
				return Response{}, errClassified
			}
			return Response{Value: "probe"}, nil
		})
		_, _ = b.Call(context.Background(), Request{})
		sleep(time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := b.Call(ctx, Request{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled call = %v, want context.Canceled", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("pre-cancelled call invoked the client (calls = %d)", got)
		}
		if resp, err := b.Call(context.Background(), Request{}); err != nil || resp.Value != "probe" {
			t.Fatalf("call after a pre-cancelled one = (%q, %v), want (probe, nil); the pre-cancelled caller must not take the probe", resp.Value, err)
		}
	})
}

func TestValidationAndPreCancelledCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := clientFunc(func(context.Context, Request) (Response, error) { return Response{}, nil })
		classifier := func(error) bool { return true }
		now := time.Now
		for _, tc := range []struct {
			name       string
			client     Client
			threshold  int
			cooldown   time.Duration
			now        func() time.Time
			classifier FailureClassifier
			want       error
		}{
			{"nil client", nil, 1, time.Second, now, classifier, ErrNilClient},
			{"zero threshold", client, 0, time.Second, now, classifier, ErrInvalidConfig},
			{"zero cooldown", client, 1, 0, now, classifier, ErrInvalidConfig},
			{"nil clock", client, 1, time.Second, nil, classifier, ErrInvalidConfig},
			{"nil classifier", client, 1, time.Second, now, nil, ErrNilClassifier},
		} {
			if _, err := New(tc.client, tc.threshold, tc.cooldown, tc.now, tc.classifier); !errors.Is(err, tc.want) {
				t.Errorf("%s: New error = %v, want %v", tc.name, err, tc.want)
			}
		}

		var calls atomic.Int32
		b := mustNew(t, 1, time.Second, func(context.Context, Request) (Response, error) {
			calls.Add(1)
			return Response{}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := b.Call(ctx, Request{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Call error = %v, want context.Canceled", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("pre-cancelled Call invoked the client %d times", got)
		}
	})
}

type clientFunc func(context.Context, Request) (Response, error)

func (f clientFunc) Call(ctx context.Context, req Request) (Response, error) {
	return f(ctx, req)
}

type callResult struct {
	resp Response
	err  error
}

func mustNew(t *testing.T, threshold int, cooldown time.Duration, fn clientFunc) *Breaker {
	t.Helper()
	b, err := New(fn, threshold, cooldown, time.Now, func(err error) bool { return errors.Is(err, errClassified) })
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return b
}

func callAsync(b *Breaker, req Request) <-chan callResult {
	ch := make(chan callResult, 1)
	go func() {
		resp, err := b.Call(context.Background(), req)
		ch <- callResult{resp, err}
	}()
	return ch
}

// call runs one Call while another call may be blocked inside the client and
// fails the test if it does not return promptly.
func call(t *testing.T, b *Breaker, req Request) (Response, error) {
	t.Helper()
	ch := callAsync(b, req)
	var r callResult
	if !eventually(func() bool {
		select {
		case r = <-ch:
			return true
		default:
			return false
		}
	}) {
		t.Fatalf("Call(%q) did not return while another call was inside the client; is the breaker mutex held across the client call?", req.Value)
	}
	return r.resp, r.err
}

// eventually polls cond, yielding the processor between attempts, and gives
// up after a bounded number of attempts. It exists because a goroutine blocked
// on a mutex is not "durably blocked" for synctest: synctest.Wait would hang
// instead of reporting a call stuck behind a lock held across the client call.
func eventually(cond func() bool) bool {
	for range 100_000 {
		if cond() {
			return true
		}
		runtime.Gosched()
	}
	return false
}

// gate releases blocked client calls. Cleanup opens it, so a failing test
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

// sleep advances virtual time (and therefore the breaker clock) by d and lets
// every goroutine in the bubble settle.
func sleep(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}
