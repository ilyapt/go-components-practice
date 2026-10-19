package immutablesnapshot

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// Several tests below run a refresh while another one is blocked inside its
// loader. A correct implementation may legitimately keep the second refresh
// blocked on a sync.Mutex, which synctest does not treat as durably blocked, so
// those tests avoid synctest.Wait and yield to the scheduler instead.

func TestNewCopiesInitialMapAndPublishesVersionOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		initial := map[string]Entry{"a": {Block: 1, Value: "old"}}
		store := New(initial)
		initial["a"] = Entry{Block: 2, Value: "mutated"}
		initial["b"] = Entry{Block: 2, Value: "new"}

		current := store.Current()
		if current.Version() != 1 {
			t.Fatalf("version = %d, want 1", current.Version())
		}
		if current.Len() != 1 {
			t.Fatalf("length = %d, want 1: New must copy the initial map", current.Len())
		}
		assertEntry(t, current, "a", Entry{Block: 1, Value: "old"})
	})
}

func TestRefreshPublishesAllChangesTogetherAndKeepsOldSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{
			"a": {Block: 1, Value: "old-a"},
			"b": {Block: 1, Value: "old-b"},
		})
		old := store.Current()

		err := store.Refresh(context.Background(), []string{"a", "c"}, func(_ context.Context, id string) (Entry, error) {
			return Entry{Block: 2, Value: "new-" + id}, nil
		})
		if err != nil {
			t.Fatalf("Refresh returned error: %v", err)
		}

		current := store.Current()
		if current.Version() != 2 {
			t.Fatalf("version = %d, want 2", current.Version())
		}
		if current.Len() != 3 {
			t.Fatalf("length = %d, want 3", current.Len())
		}
		assertEntry(t, current, "a", Entry{Block: 2, Value: "new-a"})
		assertEntry(t, current, "b", Entry{Block: 1, Value: "old-b"})
		assertEntry(t, current, "c", Entry{Block: 2, Value: "new-c"})

		if old.Version() != 1 || old.Len() != 2 {
			t.Fatalf("old snapshot changed: version=%d len=%d", old.Version(), old.Len())
		}
		assertEntry(t, old, "a", Entry{Block: 1, Value: "old-a"})
		if _, ok := old.Get("c"); ok {
			t.Fatal("old snapshot contains a newly published entry")
		}
	})
}

func TestRefreshFailurePublishesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("load failed")
		store := New(map[string]Entry{"a": {Block: 1, Value: "old"}})
		err := store.Refresh(context.Background(), []string{"a", "b"}, func(_ context.Context, id string) (Entry, error) {
			if id == "b" {
				return Entry{}, wantErr
			}
			return Entry{Block: 2, Value: "new"}, nil
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Refresh error = %v, want %v", err, wantErr)
		}

		current := store.Current()
		if current.Version() != 1 || current.Len() != 1 {
			t.Fatalf("snapshot changed after failure: version=%d len=%d", current.Version(), current.Len())
		}
		assertEntry(t, current, "a", Entry{Block: 1, Value: "old"})
	})
}

func TestCancellationBeforePublicationPublishesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"a": {Block: 1, Value: "old"}})
		ctx, cancel := context.WithCancel(context.Background())
		// The loader succeeds, but the caller cancelled before publication.
		err := store.Refresh(ctx, []string{"a"}, func(context.Context, string) (Entry, error) {
			cancel()
			return Entry{Block: 2, Value: "new"}, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Refresh error = %v, want context.Canceled", err)
		}
		current := store.Current()
		if current.Version() != 1 {
			t.Fatalf("version = %d after cancellation, want 1", current.Version())
		}
		assertEntry(t, current, "a", Entry{Block: 1, Value: "old"})
	})
}

func TestCurrentDoesNotWaitForLoader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"a": {Block: 1, Value: "old"}})
		started := make(chan struct{})
		release := make(chan struct{})
		refreshDone := make(chan error, 1)
		go func() {
			refreshDone <- store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				close(started)
				<-release
				return Entry{Block: 2, Value: "new"}, nil
			})
		}()
		<-started

		currentDone := make(chan *Snapshot, 1)
		go func() { currentDone <- store.Current() }()
		var current *Snapshot
		returned := yieldUntil(func() bool {
			select {
			case current = <-currentDone:
				return true
			default:
				return false
			}
		})
		close(release)
		if !returned {
			<-currentDone
			t.Fatal("Current blocked on a slow loader: is a reader-facing lock held across the loader call?")
		}
		assertEntry(t, current, "a", Entry{Block: 1, Value: "old"})
		if err := <-refreshDone; err != nil {
			t.Fatalf("Refresh returned error: %v", err)
		}
	})
}

func TestRefreshWritersAreSerialized(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"a": {Block: 1, Value: "old"}})
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				close(firstStarted)
				<-releaseFirst
				return Entry{Block: 2, Value: "first"}, nil
			})
		}()
		<-firstStarted

		var secondStarted atomic.Bool
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				secondStarted.Store(true)
				return Entry{Block: 3, Value: "second"}, nil
			})
		}()
		overlapped := yieldUntil(secondStarted.Load)
		close(releaseFirst)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Refresh returned error: %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second Refresh returned error: %v", err)
		}
		if overlapped {
			t.Fatal("second refresh called its loader while the first refresh was still loading")
		}
		if !secondStarted.Load() {
			t.Fatal("second refresh never called its loader")
		}

		current := store.Current()
		if current.Version() != 3 {
			t.Fatalf("version = %d, want 3", current.Version())
		}
		assertEntry(t, current, "a", Entry{Block: 3, Value: "second"})
	})
}

func TestConcurrentRefreshesDoNotLoseUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"x": {Block: 1, Value: "untouched"}})
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				close(firstStarted)
				<-releaseFirst
				return Entry{Block: 2, Value: "new-a"}, nil
			})
		}()
		<-firstStarted

		secondDone := make(chan error, 1)
		go func() {
			secondDone <- store.Refresh(context.Background(), []string{"b"}, func(context.Context, string) (Entry, error) {
				return Entry{Block: 2, Value: "new-b"}, nil
			})
		}()
		yieldUntil(func() bool { return false }) // let the second refresh get as far as it can
		close(releaseFirst)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Refresh returned error: %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second Refresh returned error: %v", err)
		}

		current := store.Current()
		if current.Version() != 3 {
			t.Fatalf("version = %d, want 3 after two successful refreshes", current.Version())
		}
		assertEntry(t, current, "a", Entry{Block: 2, Value: "new-a"})
		assertEntry(t, current, "b", Entry{Block: 2, Value: "new-b"})
		assertEntry(t, current, "x", Entry{Block: 1, Value: "untouched"})
	})
}

func TestRefreshCancelledWhileWaitingPublishesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"a": {Block: 1, Value: "old"}})
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				close(firstStarted)
				<-releaseFirst
				return Entry{Block: 2, Value: "first"}, nil
			})
		}()
		<-firstStarted

		ctx, cancel := context.WithCancel(context.Background())
		secondDone := make(chan error, 1)
		go func() {
			// This loader ignores cancellation: the store itself must notice it.
			secondDone <- store.Refresh(ctx, []string{"a"}, func(context.Context, string) (Entry, error) {
				return Entry{Block: 3, Value: "second"}, nil
			})
		}()
		yieldUntil(func() bool { return false })
		cancel()
		close(releaseFirst)

		if err := <-firstDone; err != nil {
			t.Fatalf("first Refresh returned error: %v", err)
		}
		if err := <-secondDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Refresh error = %v, want context.Canceled", err)
		}
		current := store.Current()
		if current.Version() != 2 {
			t.Fatalf("version = %d, want 2: a cancelled refresh must publish nothing", current.Version())
		}
		assertEntry(t, current, "a", Entry{Block: 2, Value: "first"})
	})
}

func TestConcurrentReadersAndRefreshes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(map[string]Entry{"a": {Block: 1, Value: "initial"}})
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				for range 100 {
					current := store.Current()
					entry, ok := current.Get("a")
					if !ok {
						t.Error("snapshot did not contain a")
						return
					}
					// A snapshot's version and contents belong together.
					if entry.Block != current.Version() {
						t.Errorf("snapshot version %d has entry from block %d", current.Version(), entry.Block)
						return
					}
					runtime.Gosched()
				}
			})
		}
		for i := range 10 {
			block := uint64(i + 2)
			if err := store.Refresh(context.Background(), []string{"a"}, func(context.Context, string) (Entry, error) {
				return Entry{Block: block, Value: "updated"}, nil
			}); err != nil {
				t.Fatalf("Refresh returned error: %v", err)
			}
		}
		wg.Wait()
		if got := store.Current().Version(); got != 11 {
			t.Fatalf("version = %d, want 11", got)
		}
	})
}

func TestEmptyRefreshAndNilLoader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(nil)
		if err := store.Refresh(context.Background(), nil, func(context.Context, string) (Entry, error) {
			return Entry{}, nil
		}); err != nil {
			t.Fatalf("empty Refresh returned error: %v", err)
		}
		if current := store.Current(); current.Version() != 1 || current.Len() != 0 {
			t.Fatalf("empty Refresh changed snapshot: version=%d len=%d", current.Version(), current.Len())
		}
		if err := store.Refresh(context.Background(), []string{"a"}, nil); !errors.Is(err, ErrNilLoader) {
			t.Fatalf("nil loader error = %v, want ErrNilLoader", err)
		}
	})
}

func assertEntry(t *testing.T, snapshot *Snapshot, id string, want Entry) {
	t.Helper()
	got, ok := snapshot.Get(id)
	if !ok {
		t.Fatalf("entry %q not found", id)
	}
	if got != want {
		t.Fatalf("entry %q = %+v, want %+v", id, got, want)
	}
}

// yieldUntil lets other goroutines run until cond holds or a generous number of
// scheduling rounds has passed, and reports whether cond held.
func yieldUntil(cond func() bool) bool {
	for range 10000 {
		if cond() {
			return true
		}
		runtime.Gosched()
	}
	return cond()
}
