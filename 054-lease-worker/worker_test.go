package leaseworker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

const testWorker = "worker-1"

// runKey marks the context passed to Run, so the tests can check that the
// handler and every store call receive a context derived from it.
type runKey struct{}

type handlerFunc func(context.Context, Job) ([]byte, error)

func (f handlerFunc) Handle(ctx context.Context, job Job) ([]byte, error) { return f(ctx, job) }

type claim struct {
	job Job
	gen uint64
}

// serve returns a claim function that hands out claims in order, then
// ErrNoAvailableJob forever.
func serve(claims ...claim) func(context.Context, int) (Job, uint64, error) {
	return func(_ context.Context, n int) (Job, uint64, error) {
		if n <= len(claims) {
			return claims[n-1].job, claims[n-1].gen, nil
		}
		return Job{}, 0, ErrNoAvailableJob
	}
}

type storeCall struct {
	op     string
	at     time.Duration
	jobID  string
	result string
}

// fakeStore records every call with its virtual time and fails the test on
// any call the requirements forbid: a wrong generation, worker, or lease; a
// context not derived from Run's ctx; overlapping Renew calls; a Renew after
// Complete or after ErrLeaseLost; a Complete while a Renew is running, after
// ErrLeaseLost, or twice; a Claim before every claimed job was reported.
// Hooks decide the results; by default Claim finds no job and Renew and
// Complete succeed. Like a real store, every method returns ctx.Err() for a
// done context.
type fakeStore struct {
	t     *testing.T
	start time.Time
	lease time.Duration

	claimFn    func(ctx context.Context, n int) (Job, uint64, error)
	renewFn    func(ctx context.Context, jobID string, n int) error
	completeFn func(ctx context.Context, jobID string) error

	mu        sync.Mutex
	calls     []storeCall
	claims    int
	gens      map[string]uint64
	renews    map[string]int
	renewing  map[string]int
	lost      map[string]bool
	completed map[string]bool
	outcomes  []Outcome
	reporting atomic.Int32
}

func newFakeStore(t *testing.T, lease time.Duration) *fakeStore {
	return &fakeStore{
		t:     t,
		start: time.Now(),
		lease: lease,
		claimFn: func(context.Context, int) (Job, uint64, error) {
			return Job{}, 0, ErrNoAvailableJob
		},
		renewFn:    func(context.Context, string, int) error { return nil },
		completeFn: func(context.Context, string) error { return nil },
		gens:       map[string]uint64{},
		renews:     map[string]int{},
		renewing:   map[string]int{},
		lost:       map[string]bool{},
		completed:  map[string]bool{},
	}
}

func (s *fakeStore) since() time.Duration { return time.Since(s.start) }

func (s *fakeStore) checkCall(ctx context.Context, op, workerID string, lease time.Duration) {
	if ctx.Value(runKey{}) != "run" {
		s.t.Errorf("%s received a context that is not derived from Run's ctx", op)
	}
	if workerID != testWorker {
		s.t.Errorf("%s called with worker %q, want %q", op, workerID, testWorker)
	}
	if lease != s.lease {
		s.t.Errorf("%s called with lease %v, want %v", op, lease, s.lease)
	}
}

func (s *fakeStore) Claim(ctx context.Context, workerID string, lease time.Duration) (Job, uint64, error) {
	s.checkCall(ctx, "Claim", workerID, lease)
	s.mu.Lock()
	if claimed, reported := len(s.gens), len(s.outcomes); claimed != reported {
		s.t.Errorf("Claim called at %v while %d claimed job(s) have not been reported: the previous job has not ended", s.since(), claimed-reported)
	}
	s.claims++
	n := s.claims
	s.calls = append(s.calls, storeCall{op: "claim", at: s.since()})
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Job{}, 0, err
	}
	job, gen, err := s.claimFn(ctx, n)
	if err == nil {
		s.mu.Lock()
		s.gens[job.ID] = gen
		s.mu.Unlock()
	}
	return job, gen, err
}

func (s *fakeStore) checkGeneration(op, jobID string, generation uint64) {
	want, ok := s.gens[jobID]
	if !ok {
		s.t.Errorf("%s called for job %q, which was never claimed", op, jobID)
	} else if generation != want {
		s.t.Errorf("%s(%q) called with generation %d, want the claimed generation %d", op, jobID, generation, want)
	}
}

func (s *fakeStore) Renew(ctx context.Context, jobID, workerID string, generation uint64, lease time.Duration) error {
	s.checkCall(ctx, "Renew", workerID, lease)
	s.mu.Lock()
	s.checkGeneration("Renew", jobID, generation)
	switch {
	case s.completed[jobID]:
		s.t.Errorf("Renew(%q) at %v after Complete was called", jobID, s.since())
	case s.lost[jobID]:
		s.t.Errorf("Renew(%q) at %v after an earlier Renew returned ErrLeaseLost", jobID, s.since())
	case s.renewing[jobID] > 0:
		s.t.Errorf("Renew(%q) at %v overlaps another Renew: want exactly one renewal loop per job", jobID, s.since())
	}
	s.renewing[jobID]++
	s.renews[jobID]++
	n := s.renews[jobID]
	s.calls = append(s.calls, storeCall{op: "renew", at: s.since(), jobID: jobID})
	s.mu.Unlock()

	err := ctx.Err()
	if err == nil {
		err = s.renewFn(ctx, jobID, n)
	}

	s.mu.Lock()
	s.renewing[jobID]--
	if errors.Is(err, ErrLeaseLost) {
		s.lost[jobID] = true
	}
	s.mu.Unlock()
	return err
}

func (s *fakeStore) Complete(ctx context.Context, jobID, workerID string, generation uint64, result []byte) error {
	s.checkCall(ctx, "Complete", workerID, s.lease)
	s.mu.Lock()
	s.checkGeneration("Complete", jobID, generation)
	switch {
	case s.completed[jobID]:
		s.t.Errorf("Complete(%q) called twice", jobID)
	case s.lost[jobID]:
		s.t.Errorf("Complete(%q) called after Renew returned ErrLeaseLost: a worker that lost its lease must not complete", jobID)
	case s.renewing[jobID] > 0:
		s.t.Errorf("Complete(%q) called at %v while a Renew is still running", jobID, s.since())
	}
	s.completed[jobID] = true
	s.calls = append(s.calls, storeCall{op: "complete", at: s.since(), jobID: jobID, result: string(result)})
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	return s.completeFn(ctx, jobID)
}

// report is the worker's report callback.
func (s *fakeStore) report(o Outcome) {
	if s.reporting.Add(1) != 1 {
		s.t.Errorf("report calls overlap")
	}
	defer s.reporting.Add(-1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomes = append(s.outcomes, o)
}

// times returns the virtual times of the calls of op for jobID ("" for Claim).
func (s *fakeStore) times(op, jobID string) []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []time.Duration
	for _, c := range s.calls {
		if c.op == op && c.jobID == jobID {
			out = append(out, c.at)
		}
	}
	return out
}

func (s *fakeStore) completions(jobID string) []storeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storeCall
	for _, c := range s.calls {
		if c.op == "complete" && c.jobID == jobID {
			out = append(out, c)
		}
	}
	return out
}

func (s *fakeStore) renewsInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.renewing {
		n += v
	}
	return n
}

func (s *fakeStore) reported() []Outcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.outcomes)
}

func mustNew(t *testing.T, s *fakeStore, renewEvery, pollInterval time.Duration) *Worker {
	t.Helper()
	w, err := New(s, testWorker, s.lease, renewEvery, pollInterval, s.report)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return w
}

// goRun runs Run in a new goroutine under a marked ctx and returns a channel
// with its result.
func goRun(w *Worker, ctx context.Context, h Handler) <-chan error {
	ctx = context.WithValue(ctx, runKey{}, "run")
	ch := make(chan error, 1)
	go func() { ch <- w.Run(ctx, h) }()
	return ch
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		return err
	default:
		t.Fatalf("%s: Run has not returned", what)
		return nil
	}
}

func assertRunning(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-ch:
		t.Fatalf("%s: Run returned %v; want it still running", what, err)
	default:
	}
}

// stop cancels Run, expects it to return context.Canceled, and checks that
// every claimed job was reported.
func stop(t *testing.T, s *fakeStore, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := receive(t, done, "after cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v after ctx was cancelled, want context.Canceled", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.outcomes) != len(s.gens) {
		t.Fatalf("Run returned with %d claimed jobs but %d outcomes reported", len(s.gens), len(s.outcomes))
	}
}

func assertOutcome(t *testing.T, got Outcome, jobID string, gen uint64, want error) {
	t.Helper()
	if got.JobID != jobID || got.Generation != gen {
		t.Fatalf("outcome for (%q, %d), want (%q, %d)", got.JobID, got.Generation, jobID, gen)
	}
	if want == nil && got.Err != nil || want != nil && !errors.Is(got.Err, want) {
		t.Fatalf("outcome for %q has Err = %v, want %v", jobID, got.Err, want)
	}
}

func durations(ds ...time.Duration) []time.Duration { return ds }

// cancelEvent is what a handler saw when its job context was done.
type cancelEvent struct {
	at    time.Duration
	cause error
}

// waitForCancel is a handler that waits for its job context, reports when it
// was done and why, and returns ctx.Err().
func waitForCancel(s *fakeStore, events chan<- cancelEvent) handlerFunc {
	return func(ctx context.Context, _ Job) ([]byte, error) {
		<-ctx.Done()
		events <- cancelEvent{at: s.since(), cause: context.Cause(ctx)}
		return nil, ctx.Err()
	}
}

func receiveCancel(t *testing.T, events <-chan cancelEvent) cancelEvent {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	default:
		t.Fatal("the handler's job context was never cancelled")
		return cancelEvent{}
	}
}

func receiveCtx(t *testing.T, ch <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-ch:
		return ctx
	default:
		t.Fatal("the handler was never called")
		return nil
	}
}

func TestValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		const lease, renew, poll = 10 * time.Second, 3 * time.Second, time.Minute
		for _, tc := range []struct {
			name                    string
			store                   Store
			workerID                string
			lease, renewEvery, poll time.Duration
		}{
			{"nil store", nil, testWorker, lease, renew, poll},
			{"empty worker ID", s, "", lease, renew, poll},
			{"zero lease", s, testWorker, 0, renew, poll},
			{"negative lease", s, testWorker, -lease, renew, poll},
			{"zero renewEvery", s, testWorker, lease, 0, poll},
			{"negative renewEvery", s, testWorker, lease, -renew, poll},
			{"renewEvery equal to lease", s, testWorker, lease, lease, poll},
			{"renewEvery longer than lease", s, testWorker, lease, 2 * lease, poll},
			{"zero pollInterval", s, testWorker, lease, renew, 0},
			{"negative pollInterval", s, testWorker, lease, renew, -poll},
		} {
			w, err := New(tc.store, tc.workerID, tc.lease, tc.renewEvery, tc.poll, nil)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("%s: New error = %v, want ErrInvalidConfig", tc.name, err)
			}
			if w != nil {
				t.Errorf("%s: New returned a non-nil worker with an error", tc.name)
			}
		}

		w, err := New(s, testWorker, lease, renew, poll, nil)
		if err != nil {
			t.Fatalf("New with a nil report returned error: %v", err)
		}
		if err := receive(t, goRun(w, context.Background(), nil), "nil handler"); !errors.Is(err, ErrNilHandler) {
			t.Fatalf("Run(nil handler) = %v, want ErrNilHandler", err)
		}
		if got := s.times("claim", ""); len(got) != 0 {
			t.Fatalf("Run with a nil handler called Claim %d times, want 0", len(got))
		}
	})
}

func TestPreCancelledRunClaimsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		w := mustNew(t, s, 3*time.Second, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := goRun(w, ctx, handlerFunc(func(context.Context, Job) ([]byte, error) {
			t.Error("handler called for a pre-cancelled Run")
			return nil, nil
		}))
		if err := receive(t, done, "pre-cancelled Run"); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Run = %v, want context.Canceled", err)
		}
		if got := s.times("claim", ""); len(got) != 0 {
			t.Fatalf("pre-cancelled Run called Claim %d times, want 0", len(got))
		}
	})
}

func TestRenewsAtCadenceThenCompletesWithClaimedGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1", Payload: []byte("payload")}, 7})
		w := mustNew(t, s, 3*time.Second, time.Minute)

		type seen struct {
			job       Job
			runCtx    bool
			cancelled bool
		}
		seenCh := make(chan seen, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, job Job) ([]byte, error) {
			select {
			case <-time.After(10 * time.Second):
				seenCh <- seen{job: job, runCtx: ctx.Value(runKey{}) == "run"}
				return []byte("result"), nil
			case <-ctx.Done():
				seenCh <- seen{job: job, cancelled: true}
				return nil, ctx.Err()
			}
		}))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "idle worker")
		var got seen
		select {
		case got = <-seenCh:
		default:
			t.Fatal("the handler was never called or never returned")
		}
		if got.cancelled {
			t.Fatal("the handler's job context was cancelled while the lease was being renewed")
		}
		if got.job.ID != "job-1" || string(got.job.Payload) != "payload" {
			t.Fatalf("handler got job (%q, %q), want (job-1, payload)", got.job.ID, got.job.Payload)
		}
		if !got.runCtx {
			t.Fatal("the handler's context is not derived from Run's ctx")
		}
		if renews, want := s.times("renew", "job-1"), durations(3*time.Second, 6*time.Second, 9*time.Second); !slices.Equal(renews, want) {
			t.Fatalf("Renew issued at %v, want %v: every renewEvery while the handler runs, and never after it returned", renews, want)
		}
		completions := s.completions("job-1")
		if len(completions) != 1 || completions[0].at != 10*time.Second || completions[0].result != "result" {
			t.Fatalf("Complete calls = %+v, want one at 10s with result %q", completions, "result")
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 7, nil)

		stop(t, s, cancel, done)
	})
}

func TestLostLeaseCancelsHandlerAndSuppressesComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 3})
		s.renewFn = func(_ context.Context, _ string, n int) error {
			if n == 2 {
				return fmt.Errorf("renew: %w", ErrLeaseLost)
			}
			return nil
		}
		w := mustNew(t, s, 3*time.Second, time.Minute)

		events := make(chan cancelEvent, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, waitForCancel(s, events))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "after the lost job")
		ev := receiveCancel(t, events)
		if ev.at != 6*time.Second {
			t.Fatalf("job context cancelled at %v, want 6s, when Renew returned ErrLeaseLost", ev.at)
		}
		if !errors.Is(ev.cause, ErrLeaseLost) {
			t.Fatalf("context.Cause(jobCtx) = %v, want ErrLeaseLost", ev.cause)
		}
		if renews, want := s.times("renew", "job-1"), durations(3*time.Second, 6*time.Second); !slices.Equal(renews, want) {
			t.Fatalf("Renew issued at %v, want %v: renewal stops when the lease is lost", renews, want)
		}
		if got := s.completions("job-1"); len(got) != 0 {
			t.Fatalf("Complete called %d times for a lost lease, want 0", len(got))
		}
		if claims, want := s.times("claim", ""), durations(0, 6*time.Second); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v: the next Claim follows the end of the job immediately", claims, want)
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 3, ErrLeaseLost)

		stop(t, s, cancel, done)
	})
}

func TestStaleHandlerResultIsNeverCompleted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 1})
		s.renewFn = func(context.Context, string, int) error { return ErrLeaseLost }
		w := mustNew(t, s, 3*time.Second, time.Minute)

		jobCtxs := make(chan context.Context, 1)
		ctx, cancel := context.WithCancel(context.Background())
		// The handler ignores its context and returns a result long after
		// the lease was lost.
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, _ Job) ([]byte, error) {
			jobCtxs <- ctx
			time.Sleep(20 * time.Second)
			return []byte("stale"), nil
		}))

		time.Sleep(15 * time.Second)
		assertRunning(t, done, "stale handler still running")
		jobCtx := receiveCtx(t, jobCtxs)
		if !errors.Is(context.Cause(jobCtx), ErrLeaseLost) {
			t.Fatalf("job context cause = %v after Renew returned ErrLeaseLost, want ErrLeaseLost", context.Cause(jobCtx))
		}
		if claims := s.times("claim", ""); len(claims) != 1 {
			t.Fatalf("Claim called %d times while the stale handler still runs, want 1: one job at a time", len(claims))
		}
		if got := s.reported(); len(got) != 0 {
			t.Fatalf("reported %v while the handler still runs, want nothing: the job has not ended", got)
		}

		time.Sleep(5 * time.Second) // the handler returns its result at 20s
		synctest.Wait()
		if got := s.completions("job-1"); len(got) != 0 {
			t.Fatalf("Complete called with the result of a lost lease: %+v", got)
		}
		if renews := s.times("renew", "job-1"); len(renews) != 1 {
			t.Fatalf("Renew called %d times, want 1: no renewal after ErrLeaseLost", len(renews))
		}
		if claims, want := s.times("claim", ""), durations(0, 20*time.Second); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v", claims, want)
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 1, ErrLeaseLost)

		stop(t, s, cancel, done)
	})
}

func TestTransientRenewErrorsRetriedUntilLocalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errUnavailable := errors.New("store unavailable")
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 2})
		s.renewFn = func(ctx context.Context, _ string, n int) error {
			if n == 1 {
				// Issued at 3s, succeeds at 5s: the store may have set the
				// expiry anywhere between 13s and 15s.
				select {
				case <-time.After(2 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return errUnavailable
		}
		w := mustNew(t, s, 3*time.Second, time.Minute)

		events := make(chan cancelEvent, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, waitForCancel(s, events))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "after the lost job")
		ev := receiveCancel(t, events)
		if ev.at != 13*time.Second {
			t.Fatalf("job context cancelled at %v, want 13s: lease after the last successful Renew was issued (3s), not earlier and not measured from when it returned", ev.at)
		}
		if !errors.Is(ev.cause, ErrLeaseLost) {
			t.Fatalf("context.Cause(jobCtx) = %v, want ErrLeaseLost", ev.cause)
		}
		renews := s.times("renew", "job-1")
		if len(renews) < 3 {
			t.Fatalf("Renew issued at %v, want the failed renewals retried before the deadline", renews)
		}
		if last := renews[len(renews)-1]; last > 13*time.Second {
			t.Fatalf("Renew issued at %v, after the lease was lost at 13s", last)
		}
		if got := s.completions("job-1"); len(got) != 0 {
			t.Fatalf("Complete called %d times for a lost lease, want 0", len(got))
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 2, ErrLeaseLost)

		stop(t, s, cancel, done)
	})
}

func TestHungRenewDoesNotExtendLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 1})
		s.renewFn = func(ctx context.Context, _ string, _ int) error {
			<-ctx.Done() // the store never answers
			return ctx.Err()
		}
		w := mustNew(t, s, 3*time.Second, time.Minute)

		events := make(chan cancelEvent, 1)
		ctx, cancel := context.WithCancel(context.Background())
		// The handler notices the cancellation, but still returns a result
		// 2s later.
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, _ Job) ([]byte, error) {
			<-ctx.Done()
			events <- cancelEvent{at: s.since(), cause: context.Cause(ctx)}
			time.Sleep(2 * time.Second)
			return []byte("stale"), nil
		}))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "after the lost job")
		ev := receiveCancel(t, events)
		if ev.at != 10*time.Second {
			t.Fatalf("job context cancelled at %v, want 10s: lease after the Claim, since no Renew succeeded", ev.at)
		}
		if !errors.Is(ev.cause, ErrLeaseLost) {
			t.Fatalf("context.Cause(jobCtx) = %v, want ErrLeaseLost", ev.cause)
		}
		if n := s.renewsInFlight(); n != 0 {
			t.Fatalf("%d Renew calls still running after the job ended, want 0", n)
		}
		if claims, want := s.times("claim", ""), durations(0, 12*time.Second); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v: the job ends once the handler and its renewal have returned", claims, want)
		}
		if got := s.completions("job-1"); len(got) != 0 {
			t.Fatalf("Complete called with a result that arrived after the local lease deadline: %+v", got)
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 1, ErrLeaseLost)

		stop(t, s, cancel, done)
	})
}

func TestHandlerErrorStopsRenewalWithoutComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errHandler := errors.New("handler failed")
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 2})
		w := mustNew(t, s, 3*time.Second, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, _ Job) ([]byte, error) {
			select {
			case <-time.After(4 * time.Second):
				return nil, errHandler
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "after the failed job")
		if renews, want := s.times("renew", "job-1"), durations(3*time.Second); !slices.Equal(renews, want) {
			t.Fatalf("Renew issued at %v, want %v: renewal stops when the handler returns", renews, want)
		}
		if got := s.completions("job-1"); len(got) != 0 {
			t.Fatalf("Complete called %d times after the handler failed, want 0", len(got))
		}
		if claims, want := s.times("claim", ""), durations(0, 4*time.Second); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v", claims, want)
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 2, errHandler)
		if errors.Is(outcomes[0].Err, ErrLeaseLost) {
			t.Fatalf("outcome Err = %v: a handler failure is not a lost lease", outcomes[0].Err)
		}

		stop(t, s, cancel, done)
	})
}

func TestCompleteLeaseLostIsReportedAndRunContinues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 1}, claim{Job{ID: "job-2"}, 5})
		s.completeFn = func(_ context.Context, jobID string) error {
			if jobID == "job-1" {
				return ErrLeaseLost // fenced by the store
			}
			return nil
		}
		w := mustNew(t, s, 3*time.Second, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, job Job) ([]byte, error) {
			select {
			case <-time.After(time.Second):
				return []byte(job.ID), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))

		time.Sleep(30 * time.Second)
		assertRunning(t, done, "after both jobs")
		for _, id := range []string{"job-1", "job-2"} {
			if got := s.completions(id); len(got) != 1 || got[0].result != id {
				t.Fatalf("Complete calls for %s = %+v, want exactly one with result %q", id, got, id)
			}
		}
		if claims, want := s.times("claim", ""), durations(0, time.Second, 2*time.Second); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v: a fenced Complete does not stop the loop", claims, want)
		}
		outcomes := s.reported()
		if len(outcomes) != 2 {
			t.Fatalf("reported %d outcomes, want 2", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 1, ErrLeaseLost)
		assertOutcome(t, outcomes[1], "job-2", 5, nil)

		stop(t, s, cancel, done)
	})
}

// The handler returns at the same virtual instant as a renewal tick, and
// Complete takes longer than several ticks. The fake store fails the test if
// a Renew overlaps or follows Complete for the same job.
func TestNoRenewOverlapsOrFollowsComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const jobs = 20
		s := newFakeStore(t, 5*time.Second)
		var claims []claim
		for i := range jobs {
			claims = append(claims, claim{Job{ID: fmt.Sprintf("job-%d", i)}, uint64(i + 1)})
		}
		s.claimFn = serve(claims...)
		s.completeFn = func(ctx context.Context, _ string) error {
			select {
			case <-time.After(3 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w := mustNew(t, s, time.Second, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, _ Job) ([]byte, error) {
			select {
			case <-time.After(2 * time.Second):
				return []byte("ok"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))

		time.Sleep(2 * time.Minute)
		assertRunning(t, done, "after all jobs")
		outcomes := s.reported()
		if len(outcomes) != jobs {
			t.Fatalf("reported %d outcomes, want %d", len(outcomes), jobs)
		}
		for i, o := range outcomes {
			assertOutcome(t, o, claims[i].job.ID, claims[i].gen, nil)
		}

		stop(t, s, cancel, done)
	})
}

func TestPollsAfterFailedClaimAndStopsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = func(_ context.Context, n int) (Job, uint64, error) {
			switch n {
			case 1:
				return Job{}, 0, ErrNoAvailableJob
			case 2:
				return Job{}, 0, errors.New("store unavailable")
			case 3:
				return Job{}, 0, fmt.Errorf("claim: %w", ErrNoAvailableJob)
			case 4:
				return Job{ID: "job-1"}, 1, nil
			default:
				return Job{}, 0, ErrNoAvailableJob
			}
		}
		w := mustNew(t, s, 3*time.Second, 5*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(context.Context, Job) ([]byte, error) {
			return []byte("ok"), nil
		}))

		time.Sleep(22 * time.Second)
		assertRunning(t, done, "polling worker")
		want := durations(0, 5*time.Second, 10*time.Second, 15*time.Second, 15*time.Second, 20*time.Second)
		if claims := s.times("claim", ""); !slices.Equal(claims, want) {
			t.Fatalf("Claim issued at %v, want %v: wait pollInterval after any failed Claim, not after a job", claims, want)
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes, want 1", len(outcomes))
		}
		assertOutcome(t, outcomes[0], "job-1", 1, nil)

		stop(t, s, cancel, done) // receive does not advance time
		if now := s.since(); now != 22*time.Second {
			t.Fatalf("Run returned at %v, want 22s: waiting for the next poll must end when ctx is cancelled", now)
		}
	})
}

func TestCancellationDuringClaimReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = func(ctx context.Context, _ int) (Job, uint64, error) {
			<-ctx.Done() // the store is slow to answer
			return Job{}, 0, ctx.Err()
		}
		w := mustNew(t, s, 3*time.Second, time.Minute)

		ctx, cancel := context.WithCancel(context.Background())
		done := goRun(w, ctx, handlerFunc(func(context.Context, Job) ([]byte, error) {
			t.Error("handler called without a claimed job")
			return nil, nil
		}))

		time.Sleep(time.Second)
		assertRunning(t, done, "waiting in Claim")
		stop(t, s, cancel, done)
		if claims := s.times("claim", ""); len(claims) != 1 {
			t.Fatalf("Claim called %d times, want 1", len(claims))
		}
	})
}

func TestCancellationCancelsJobAndWaitsForHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newFakeStore(t, 10*time.Second)
		s.claimFn = serve(claim{Job{ID: "job-1"}, 4})
		w := mustNew(t, s, 3*time.Second, time.Minute)

		jobCtxs := make(chan context.Context, 1)
		ctx, cancel := context.WithCancel(context.Background())
		// The handler takes 3s to wind down after its context is cancelled,
		// then returns a result anyway.
		done := goRun(w, ctx, handlerFunc(func(ctx context.Context, _ Job) ([]byte, error) {
			jobCtxs <- ctx
			<-ctx.Done()
			time.Sleep(3 * time.Second)
			return []byte("late"), nil
		}))

		time.Sleep(2 * time.Second)
		synctest.Wait()
		cancel()
		assertRunning(t, done, "handler still winding down")
		jobCtx := receiveCtx(t, jobCtxs)
		if jobCtx.Err() == nil {
			t.Fatal("cancelling Run's ctx did not cancel the job context")
		}
		if errors.Is(context.Cause(jobCtx), ErrLeaseLost) {
			t.Fatal("job context cause is ErrLeaseLost after a shutdown: the lease was not lost")
		}

		time.Sleep(2 * time.Second) // 4s: past the 3s renewal tick
		assertRunning(t, done, "handler still winding down")
		if renews := s.times("renew", "job-1"); len(renews) != 0 {
			t.Fatalf("Renew issued at %v after ctx was cancelled, want none", renews)
		}

		time.Sleep(time.Second) // 5s: the handler returns
		if err := receive(t, done, "after the handler returned"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
		if claims := s.times("claim", ""); len(claims) != 1 {
			t.Fatalf("Claim called %d times, want 1: no Claim after ctx is cancelled", len(claims))
		}
		outcomes := s.reported()
		if len(outcomes) != 1 {
			t.Fatalf("reported %d outcomes before Run returned, want 1", len(outcomes))
		}
		// Completing a result that arrives after cancellation is the
		// solver's choice; abandoning it reports ctx.Err().
		o := outcomes[0]
		if o.JobID != "job-1" || o.Generation != 4 {
			t.Fatalf("outcome for (%q, %d), want (job-1, 4)", o.JobID, o.Generation)
		}
		if o.Err != nil && !errors.Is(o.Err, context.Canceled) || errors.Is(o.Err, ErrLeaseLost) {
			t.Fatalf("outcome Err = %v, want nil (completed) or context.Canceled (abandoned)", o.Err)
		}
		if o.Err == nil && len(s.completions("job-1")) != 1 {
			t.Fatal("outcome Err is nil but Complete was not called")
		}
	})
}
