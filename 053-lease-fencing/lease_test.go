package leasefencing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// fakeClock is the injected clock. It deliberately starts far from synctest's
// virtual time and moves only when the test advances it, so an implementation
// that consults time.Now anywhere gets the wrong answer.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var bg = context.Background()

func mustClaim(t *testing.T, s *Store, workerID string, lease time.Duration, wantID string) uint64 {
	t.Helper()
	job, generation, err := s.Claim(bg, workerID, lease)
	if err != nil || job.ID != wantID {
		t.Fatalf("Claim(%q) = (%q, %d, %v), want job %q", workerID, job.ID, generation, err, wantID)
	}
	return generation
}

func TestClaimUsesInputOrderAndExcludesActiveLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{
			{ID: "first", Payload: []byte("one")},
			{ID: "second", Payload: []byte("two")},
		}, clock.Now)

		job, generation, err := store.Claim(bg, "worker-a", time.Minute)
		if err != nil || job.ID != "first" || string(job.Payload) != "one" || generation != 1 {
			t.Fatalf("first Claim = (%q, %q, %d, %v), want (first, one, 1, nil)", job.ID, job.Payload, generation, err)
		}

		job, generation, err = store.Claim(bg, "worker-b", time.Minute)
		if err != nil || job.ID != "second" || generation != 1 {
			t.Fatalf("second Claim = (%q, %d, %v), want (second, 1, nil)", job.ID, generation, err)
		}

		if _, _, err := store.Claim(bg, "worker-c", time.Minute); !errors.Is(err, ErrNoAvailableJob) {
			t.Fatalf("third Claim error = %v, want ErrNoAvailableJob", err)
		}
	})
}

func TestClaimPrefersEarlierExpiredJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "a"}, {ID: "b"}}, clock.Now)
		mustClaim(t, store, "worker-1", 10*time.Second, "a")

		clock.Advance(10 * time.Second)
		job, generation, err := store.Claim(bg, "worker-2", time.Minute)
		if err != nil || job.ID != "a" || generation != 2 {
			t.Fatalf("Claim after expiry = (%q, %d, %v), want (a, 2, nil): the first available job in order", job.ID, generation, err)
		}
	})
}

func TestRenewExtendsLeaseFromCurrentTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		generation := mustClaim(t, store, "worker", 10*time.Second, "job")

		clock.Advance(5 * time.Second)
		if err := store.Renew(bg, "job", "worker", generation, 20*time.Second); err != nil {
			t.Fatalf("Renew returned error: %v", err)
		}

		clock.Advance(19 * time.Second)
		if _, _, err := store.Claim(bg, "other", time.Minute); !errors.Is(err, ErrNoAvailableJob) {
			t.Fatalf("Claim before renewed expiry error = %v, want ErrNoAvailableJob", err)
		}
		if err := store.Complete(bg, "job", "worker", generation, []byte("result")); err != nil {
			t.Fatalf("Complete before renewed expiry returned error: %v", err)
		}
	})
}

func TestExpiredLeaseIsReclaimedAndOldGenerationIsFenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		oldGeneration := mustClaim(t, store, "worker", 10*time.Second, "job")

		clock.Advance(10 * time.Second) // exactly at expiry: the lease is expired
		if err := store.Renew(bg, "job", "worker", oldGeneration, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Renew at expiry error = %v, want ErrLeaseLost", err)
		}

		// The same worker ID reclaims the job: only the generation tells the epochs apart.
		job, newGeneration, err := store.Claim(bg, "worker", time.Minute)
		if err != nil || job.ID != "job" || newGeneration != oldGeneration+1 {
			t.Fatalf("reclaim = (%q, %d, %v), want (job, %d, nil)", job.ID, newGeneration, err, oldGeneration+1)
		}

		if err := store.Renew(bg, "job", "worker", oldGeneration, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale Renew error = %v, want ErrLeaseLost", err)
		}
		if err := store.Complete(bg, "job", "worker", oldGeneration, nil); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale Complete error = %v, want ErrLeaseLost", err)
		}
		if err := store.Complete(bg, "job", "worker", newGeneration, []byte("done")); err != nil {
			t.Fatalf("current Complete returned error: %v", err)
		}
		if _, _, err := store.Claim(bg, "other", time.Minute); !errors.Is(err, ErrNoAvailableJob) {
			t.Fatalf("Claim after completion error = %v, want ErrNoAvailableJob", err)
		}
	})
}

func TestCompleteBoundaryAndCompletedJobIsFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "late"}, {ID: "done"}}, clock.Now)
		lateGeneration := mustClaim(t, store, "worker", 10*time.Second, "late")
		doneGeneration := mustClaim(t, store, "worker", 10*time.Second, "done")

		clock.Advance(10*time.Second - time.Nanosecond)
		if err := store.Complete(bg, "done", "worker", doneGeneration, []byte("ok")); err != nil {
			t.Fatalf("Complete just before expiry returned error: %v", err)
		}
		clock.Advance(time.Nanosecond)
		if err := store.Complete(bg, "late", "worker", lateGeneration, []byte("late")); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Complete at expiry error = %v, want ErrLeaseLost", err)
		}

		// "late" is available again; "done" never is, even though its lease expired.
		clock.Advance(time.Hour)
		mustClaim(t, store, "other", time.Minute, "late")
		if _, _, err := store.Claim(bg, "other", time.Minute); !errors.Is(err, ErrNoAvailableJob) {
			t.Fatalf("Claim of a completed job error = %v, want ErrNoAvailableJob", err)
		}
		if err := store.Renew(bg, "done", "worker", doneGeneration, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Renew of a completed job error = %v, want ErrLeaseLost", err)
		}
		if err := store.Complete(bg, "done", "worker", doneGeneration, nil); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("second Complete error = %v, want ErrLeaseLost", err)
		}
	})
}

func TestWrongWorkerGenerationOrJobIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		generation := mustClaim(t, store, "worker", time.Minute, "job")

		for _, tc := range []struct {
			name       string
			jobID      string
			workerID   string
			generation uint64
		}{
			{"other worker", "job", "intruder", generation},
			{"future generation", "job", "worker", generation + 1},
			{"zero generation", "job", "worker", 0},
			{"unknown job", "missing", "worker", generation},
		} {
			if err := store.Renew(bg, tc.jobID, tc.workerID, tc.generation, time.Minute); !errors.Is(err, ErrLeaseLost) {
				t.Errorf("%s: Renew error = %v, want ErrLeaseLost", tc.name, err)
			}
			if err := store.Complete(bg, tc.jobID, tc.workerID, tc.generation, nil); !errors.Is(err, ErrLeaseLost) {
				t.Errorf("%s: Complete error = %v, want ErrLeaseLost", tc.name, err)
			}
		}
		// None of the rejected calls disturbed the real owner.
		if err := store.Complete(bg, "job", "worker", generation, []byte("ok")); err != nil {
			t.Fatalf("owner Complete returned error: %v", err)
		}
	})
}

func TestUsesOnlyInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		generation := mustClaim(t, store, "worker", time.Minute, "job")

		// Real (virtual) time passes; the injected clock does not.
		time.Sleep(time.Hour)
		if err := store.Renew(bg, "job", "worker", generation, time.Minute); err != nil {
			t.Fatalf("Renew error = %v after only time.Now moved, want nil: expiry must use the injected clock", err)
		}
		if _, _, err := store.Claim(bg, "other", time.Minute); !errors.Is(err, ErrNoAvailableJob) {
			t.Fatalf("Claim error = %v, want ErrNoAvailableJob: the lease is still valid on the injected clock", err)
		}

		clock.Advance(time.Minute)
		if err := store.Renew(bg, "job", "worker", generation, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Renew error = %v after the injected clock passed expiry, want ErrLeaseLost", err)
		}
	})
}

func TestConcurrentClaimHasOneWinnerPerJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		jobs := []Job{{ID: "a"}, {ID: "b"}, {ID: "c"}}
		store := New(jobs, clock.Now)

		const callers = 20
		var mu sync.Mutex
		won := map[string]int{}
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				job, generation, err := store.Claim(bg, "worker", time.Minute)
				switch {
				case err == nil:
					if generation != 1 {
						t.Errorf("Claim(%q) generation = %d, want 1", job.ID, generation)
					}
					mu.Lock()
					won[job.ID]++
					mu.Unlock()
				case errors.Is(err, ErrNoAvailableJob):
				default:
					t.Errorf("Claim returned unexpected error: %v", err)
				}
			})
		}
		wg.Wait()

		if len(won) != len(jobs) {
			t.Fatalf("claimed jobs = %v, want each of %d jobs claimed", won, len(jobs))
		}
		for id, n := range won {
			if n != 1 {
				t.Fatalf("job %q claimed %d times, want 1", id, n)
			}
		}
	})
}

func TestConcurrentCompleteHasOneWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		generation := mustClaim(t, store, "worker", time.Minute, "job")

		const callers = 10
		errs := make(chan error, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() { errs <- store.Complete(bg, "job", "worker", generation, []byte("done")) })
		}
		wg.Wait()
		close(errs)

		winners := 0
		for err := range errs {
			switch {
			case err == nil:
				winners++
			case errors.Is(err, ErrLeaseLost):
			default:
				t.Fatalf("Complete returned unexpected error: %v", err)
			}
		}
		if winners != 1 {
			t.Fatalf("successful completions = %d, want 1", winners)
		}
	})
}

func TestCancelledCallsDoNotMutateState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)
		ctx, cancel := context.WithCancel(bg)
		cancel()

		if _, _, err := store.Claim(ctx, "worker", time.Minute); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Claim error = %v, want context.Canceled", err)
		}
		generation := mustClaim(t, store, "worker", 10*time.Second, "job")
		if generation != 1 {
			t.Fatalf("generation after cancelled Claim = %d, want 1", generation)
		}

		clock.Advance(5 * time.Second)
		if err := store.Renew(ctx, "job", "worker", generation, time.Hour); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Renew error = %v, want context.Canceled", err)
		}
		if err := store.Complete(ctx, "job", "worker", generation, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Complete error = %v, want context.Canceled", err)
		}

		// The cancelled Renew did not extend the lease: it expires at the original time...
		clock.Advance(5 * time.Second)
		if err := store.Renew(bg, "job", "worker", generation, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Renew at the original expiry error = %v, want ErrLeaseLost", err)
		}
		// ...and the cancelled Complete did not complete the job: it can be reclaimed.
		job, next, err := store.Claim(bg, "other", time.Minute)
		if err != nil || job.ID != "job" || next != 2 {
			t.Fatalf("reclaim = (%q, %d, %v), want (job, 2, nil)", job.ID, next, err)
		}
	})
}

func TestInvalidArguments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFakeClock()
		store := New([]Job{{ID: "job"}}, clock.Now)

		if _, _, err := store.Claim(bg, "", time.Minute); !errors.Is(err, ErrInvalidWorker) {
			t.Fatalf("empty worker Claim error = %v, want ErrInvalidWorker", err)
		}
		for _, lease := range []time.Duration{0, -time.Second} {
			if _, _, err := store.Claim(bg, "worker", lease); !errors.Is(err, ErrInvalidLease) {
				t.Fatalf("Claim(lease=%v) error = %v, want ErrInvalidLease", lease, err)
			}
		}
		generation := mustClaim(t, store, "worker", time.Minute, "job")
		if generation != 1 {
			t.Fatalf("generation after invalid Claims = %d, want 1", generation)
		}

		if err := store.Renew(bg, "job", "", generation, time.Minute); !errors.Is(err, ErrInvalidWorker) {
			t.Fatalf("empty worker Renew error = %v, want ErrInvalidWorker", err)
		}
		if err := store.Renew(bg, "job", "worker", generation, 0); !errors.Is(err, ErrInvalidLease) {
			t.Fatalf("zero lease Renew error = %v, want ErrInvalidLease", err)
		}
		if err := store.Complete(bg, "job", "", generation, nil); !errors.Is(err, ErrInvalidWorker) {
			t.Fatalf("empty worker Complete error = %v, want ErrInvalidWorker", err)
		}
	})
}
