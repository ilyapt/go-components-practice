package dagexecutor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type runFunc = func(context.Context, map[string]Output) (Output, error)

type result struct {
	results map[string]Result
	err     error
}

// goExecute runs Execute in a new goroutine and returns a channel with its result.
func goExecute(ctx context.Context, nodes []Node, limit int) <-chan result {
	ch := make(chan result, 1)
	go func() {
		r, err := Execute(ctx, nodes, limit)
		ch <- result{r, err}
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
		t.Fatalf("%s: Execute has not returned", what)
		return result{}
	}
}

func assertBlocked(t *testing.T, ch <-chan result, what string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: Execute returned %v; want it still waiting", what, r.err)
	default:
	}
}

func mustExecute(t *testing.T, nodes []Node, limit int) map[string]Result {
	t.Helper()
	results, err := Execute(context.Background(), nodes, limit)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if len(results) != len(nodes) {
		t.Fatalf("Execute returned %d results for %d nodes", len(results), len(nodes))
	}
	return results
}

func succeed(out Output) runFunc {
	return func(context.Context, map[string]Output) (Output, error) { return out, nil }
}

// tracker records how often each node started, the order of starts and
// ends, and how many Run calls execute at once.
type tracker struct {
	mu        sync.Mutex
	active    int
	maxActive int
	seq       int
	starts    map[string]int
	startSeq  map[string]int
	endSeq    map[string]int
}

func newTracker() *tracker {
	return &tracker{starts: map[string]int{}, startSeq: map[string]int{}, endSeq: map[string]int{}}
}

// node returns a Node whose Run is fn wrapped with start and end bookkeeping.
func (tr *tracker) node(id string, deps []string, fn runFunc) Node {
	return Node{ID: id, DependsOn: deps, Run: func(ctx context.Context, d map[string]Output) (Output, error) {
		tr.mu.Lock()
		tr.active++
		tr.maxActive = max(tr.maxActive, tr.active)
		tr.seq++
		tr.starts[id]++
		tr.startSeq[id] = tr.seq
		tr.mu.Unlock()
		defer func() {
			tr.mu.Lock()
			tr.active--
			tr.seq++
			tr.endSeq[id] = tr.seq
			tr.mu.Unlock()
		}()
		return fn(ctx, d)
	}}
}

func (tr *tracker) started(id string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.starts[id]
}

func (tr *tracker) totalStarted() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	total := 0
	for _, n := range tr.starts {
		total += n
	}
	return total
}

func (tr *tracker) running() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.active
}

// finishedBefore reports whether dep's Run returned before node's Run started.
func (tr *tracker) finishedBefore(dep, node string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	end, ok := tr.endSeq[dep]
	return ok && end < tr.startSeq[node]
}

func TestValidationRejectsInvalidGraphBeforeRunningAnything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		run := func(context.Context, map[string]Output) (Output, error) {
			calls.Add(1)
			return nil, nil
		}
		n := func(id string, deps ...string) Node { return Node{ID: id, DependsOn: deps, Run: run} }

		for _, tc := range []struct {
			name    string
			nodes   []Node
			limit   int
			wantErr error
			named   []string // the error message must name at least one of these
		}{
			{"zero limit", []Node{n("root")}, 0, ErrInvalidLimit, nil},
			{"negative limit", []Node{n("root")}, -1, ErrInvalidLimit, nil},
			{"nil run", []Node{n("root"), {ID: "no-func", DependsOn: []string{"root"}}}, 2, ErrNilRun, []string{"no-func"}},
			{"duplicate id", []Node{n("root"), n("dup-node", "root"), n("dup-node")}, 2, ErrDuplicateNode, []string{"dup-node"}},
			{"unknown dependency", []Node{n("root"), n("child", "root", "missing-dep")}, 2, ErrUnknownDependency, []string{"missing-dep"}},
			{"self dependency", []Node{n("root"), n("self-loop", "root", "self-loop")}, 2, ErrSelfDependency, []string{"self-loop"}},
			{"two-node cycle", []Node{n("root"), n("cycle-a", "root", "cycle-b"), n("cycle-b", "cycle-a")}, 2, ErrCycle, []string{"cycle-a", "cycle-b"}},
			{"cycle behind runnable nodes", []Node{
				n("root"), n("mid", "root"),
				n("cycle-x", "mid", "cycle-z"), n("cycle-y", "cycle-x"), n("cycle-z", "cycle-y"),
				n("leaf", "cycle-z"),
			}, 4, ErrCycle, []string{"cycle-x", "cycle-y", "cycle-z"}},
		} {
			results, err := Execute(context.Background(), tc.nodes, tc.limit)
			synctest.Wait()
			if got := calls.Swap(0); got != 0 {
				t.Errorf("%s: %d Run calls for an invalid graph, want 0: validate before starting anything", tc.name, got)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: Execute error = %v, want %v", tc.name, err, tc.wantErr)
				continue
			}
			if results != nil {
				t.Errorf("%s: Execute returned results %v with a validation error, want nil", tc.name, results)
			}
			if len(tc.named) > 0 && !containsAny(err.Error(), tc.named) {
				t.Errorf("%s: error %q does not name any of %v", tc.name, err, tc.named)
			}
		}
	})
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func TestEmptyGraphSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		results, err := Execute(context.Background(), nil, 1)
		if err != nil || len(results) != 0 {
			t.Fatalf("Execute(empty graph) = %v, %v; want an empty map and nil", results, err)
		}
	})
}

func TestDependencyFinishesBeforeDependentStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const count = 12
		type payload struct{ id string }
		id := func(i int) string { return fmt.Sprintf("n%02d", i) }
		outputs := map[string]*payload{}
		edges := map[string][]string{}
		for i := range count {
			outputs[id(i)] = &payload{id(i)}
			if i%3 != 0 {
				edges[id(i)] = append(edges[id(i)], id(i-1))
			}
			if i >= 2 && i/2 != i-1 {
				edges[id(i)] = append(edges[id(i)], id(i/2))
			}
		}

		tr := newTracker()
		var nodes []Node
		// Dependents are listed before their dependencies, and every
		// dependency is slower than the nodes that depend on it.
		for i := count - 1; i >= 0; i-- {
			self, deps, delay := id(i), edges[id(i)], time.Duration(count-i)*time.Millisecond
			nodes = append(nodes, tr.node(self, deps, func(_ context.Context, got map[string]Output) (Output, error) {
				if len(got) != len(deps) {
					t.Errorf("%s received %d dependency outputs, want %d", self, len(got), len(deps))
				}
				for _, d := range deps {
					if got[d] != Output(outputs[d]) {
						t.Errorf("%s received %v for dependency %s, want the value %s returned", self, got[d], d, d)
					}
				}
				time.Sleep(delay)
				return outputs[self], nil
			}))
		}

		results := mustExecute(t, nodes, 3)
		for i := range count {
			self := id(i)
			if got := tr.started(self); got != 1 {
				t.Errorf("%s ran %d times, want 1", self, got)
			}
			for _, d := range edges[self] {
				if !tr.finishedBefore(d, self) {
					t.Errorf("%s started before its dependency %s returned", self, d)
				}
			}
			if r := results[self]; r.Err != nil || r.Output != Output(outputs[self]) {
				t.Errorf("result of %s = %v, %v; want its own output and nil", self, r.Output, r.Err)
			}
		}
		if tr.maxActive > 3 {
			t.Errorf("max concurrent Run calls = %d, want <= 3", tr.maxActive)
		}
	})
}

func TestOutputsArePassedToDependentsUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type config struct{ region string }
		cfg := &config{region: "eu-west-1"}
		rendered := &strings.Builder{}

		var rootDeps, renderDeps, publishDeps map[string]Output
		nodes := []Node{
			{ID: "publish", DependsOn: []string{"render"}, Run: func(_ context.Context, d map[string]Output) (Output, error) {
				publishDeps = d
				return "published", nil
			}},
			{ID: "render", DependsOn: []string{"config", "schema"}, Run: func(_ context.Context, d map[string]Output) (Output, error) {
				renderDeps = d
				return rendered, nil
			}},
			{ID: "config", Run: func(_ context.Context, d map[string]Output) (Output, error) {
				rootDeps = d
				return cfg, nil
			}},
			{ID: "schema", Run: succeed("schema-v42")},
		}

		results := mustExecute(t, nodes, 2)
		if len(rootDeps) != 0 {
			t.Errorf("a node without dependencies received %v, want an empty map", rootDeps)
		}
		if len(renderDeps) != 2 || renderDeps["config"] != Output(cfg) || renderDeps["schema"] != Output("schema-v42") {
			t.Errorf("render received %v, want exactly config=%p and schema=schema-v42", renderDeps, cfg)
		}
		if len(publishDeps) != 1 || publishDeps["render"] != Output(rendered) {
			t.Errorf("publish received %v, want exactly the value render returned", publishDeps)
		}
		if results["config"].Output != Output(cfg) || results["publish"].Output != Output("published") {
			t.Errorf("results = %v, want each node's own output", results)
		}
	})
}

func TestDiamondDependencyRunsEachNodeOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		middles := []string{"m0", "m1", "m2", "m3", "m4"}
		var joinDeps map[string]Output
		nodes := []Node{
			tr.node("join", middles, func(_ context.Context, d map[string]Output) (Output, error) {
				joinDeps = d
				return "joined", nil
			}),
			tr.node("source", nil, succeed("source-out")),
		}
		for i, m := range middles {
			delay := time.Duration(len(middles)-i) * time.Millisecond
			nodes = append(nodes, tr.node(m, []string{"source"}, func(_ context.Context, d map[string]Output) (Output, error) {
				if d["source"] != Output("source-out") {
					t.Errorf("%s received %v, want source-out", m, d["source"])
				}
				time.Sleep(delay) // middles finish in reverse order
				return m + "-out", nil
			}))
		}

		mustExecute(t, nodes, 4)
		for _, id := range append([]string{"source", "join"}, middles...) {
			if got := tr.started(id); got != 1 {
				t.Errorf("%s ran %d times, want exactly 1", id, got)
			}
		}
		for _, m := range middles {
			if !tr.finishedBefore(m, "join") {
				t.Errorf("join started before %s returned", m)
			}
			if joinDeps[m] != Output(m+"-out") {
				t.Errorf("join received %v for %s, want %s-out", joinDeps[m], m, m)
			}
		}
		if len(joinDeps) != len(middles) {
			t.Errorf("join received %d dependency outputs, want %d", len(joinDeps), len(middles))
		}
	})
}

func TestRepeatedDependencyIsCountedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		var got map[string]Output
		nodes := []Node{
			tr.node("a", nil, succeed("a-out")),
			tr.node("b", []string{"a", "a"}, func(_ context.Context, d map[string]Output) (Output, error) {
				got = d
				return "b-out", nil
			}),
		}

		r := receive(t, goExecute(context.Background(), nodes, 1), "graph with a repeated dependency")
		if r.err != nil {
			t.Fatalf("Execute returned error: %v", r.err)
		}
		if tr.started("b") != 1 {
			t.Fatalf("b ran %d times, want 1: a dependency listed twice is still one dependency", tr.started("b"))
		}
		if len(got) != 1 || got["a"] != Output("a-out") {
			t.Fatalf("b received %v, want exactly a=a-out", got)
		}
	})
}

func TestRunsAtMostLimitNodesConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 3
		release := make(chan struct{})
		tr := newTracker()
		var nodes []Node
		for i := range 8 {
			root := fmt.Sprintf("root-%d", i)
			nodes = append(nodes, tr.node(root, nil, func(context.Context, map[string]Output) (Output, error) {
				<-release
				return root, nil
			}))
			if i%2 == 1 {
				deps := []string{fmt.Sprintf("root-%d", i-1), root}
				nodes = append(nodes, tr.node(fmt.Sprintf("child-%d", i/2), deps, func(context.Context, map[string]Output) (Output, error) {
					time.Sleep(time.Millisecond)
					return nil, nil
				}))
			}
		}

		done := goExecute(context.Background(), nodes, limit)
		synctest.Wait()
		if got := tr.running(); got != limit {
			t.Fatalf("%d Run calls executing with 8 ready nodes, want exactly %d", got, limit)
		}

		close(release)
		if r := receive(t, done, "after release"); r.err != nil {
			t.Fatalf("Execute returned error: %v", r.err)
		}
		if tr.maxActive > limit {
			t.Fatalf("max concurrent Run calls = %d, want <= %d", tr.maxActive, limit)
		}
		if got := tr.totalStarted(); got != len(nodes) {
			t.Fatalf("%d Run calls, want %d", got, len(nodes))
		}
	})
}

func TestIndependentNodesRunInParallel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		tr := newTracker()
		var nodes []Node
		for i := range 5 {
			nodes = append(nodes, tr.node(fmt.Sprintf("n%d", i), nil, func(context.Context, map[string]Output) (Output, error) {
				<-release
				return nil, nil
			}))
		}

		done := goExecute(context.Background(), nodes, 10)
		synctest.Wait()
		if got := tr.running(); got != 5 {
			t.Fatalf("%d of 5 independent nodes running with limit 10, want all 5", got)
		}
		assertBlocked(t, done, "while nodes run")
		close(release)
		if r := receive(t, done, "after release"); r.err != nil {
			t.Fatalf("Execute returned error: %v", r.err)
		}
	})
}

func TestReadyNodeDoesNotWaitForUnrelatedSlowNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		tr := newTracker()
		nodes := []Node{
			tr.node("slow", nil, func(context.Context, map[string]Output) (Output, error) {
				<-release
				return "slow-out", nil
			}),
			tr.node("fast", nil, succeed("fast-out")),
			tr.node("next", []string{"fast"}, succeed("next-out")),
			tr.node("after-next", []string{"next"}, succeed("after-next-out")),
		}

		done := goExecute(context.Background(), nodes, 2)
		synctest.Wait()
		for _, id := range []string{"fast", "next", "after-next"} {
			if tr.started(id) != 1 {
				t.Fatalf("%s has not run while slow holds one of two slots: ready nodes must not wait for unrelated nodes", id)
			}
		}
		assertBlocked(t, done, "while slow runs")

		close(release)
		if r := receive(t, done, "after slow returns"); r.err != nil {
			t.Fatalf("Execute returned error: %v", r.err)
		}
	})
}

func TestFailureSkipsTransitiveDependents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errBoom := errors.New("extract failed")
		tr := newTracker()
		nodes := []Node{
			tr.node("extract", nil, func(context.Context, map[string]Output) (Output, error) {
				return "partial", errBoom
			}),
			tr.node("transform", []string{"extract"}, succeed("transformed")),
			tr.node("load", []string{"transform"}, succeed("loaded")),
			tr.node("audit", nil, succeed("audited")),
			tr.node("report", []string{"load", "audit"}, succeed("reported")),
			tr.node("notify", []string{"audit"}, succeed("notified")),
		}

		results, err := Execute(context.Background(), nodes, 2)
		if !errors.Is(err, ErrGraphFailed) || !errors.Is(err, errBoom) {
			t.Fatalf("Execute error = %v, want one matching both ErrGraphFailed and the node's error", err)
		}
		if len(results) != len(nodes) {
			t.Fatalf("Execute returned %d results for %d nodes", len(results), len(nodes))
		}
		if r := results["extract"]; r.Output != Output("partial") || r.Err != errBoom {
			t.Errorf("result of extract = %v, %v; want exactly what Run returned", r.Output, r.Err)
		}
		for id, named := range map[string][]string{
			"transform": {"extract"},
			"load":      {"transform", "extract"},
			"report":    {"load", "extract"},
		} {
			r := results[id]
			if tr.started(id) != 0 {
				t.Errorf("%s ran although a dependency failed", id)
			}
			if !errors.Is(r.Err, ErrDependencyFailed) || r.Output != nil {
				t.Errorf("result of %s = %v, %v; want nil output and ErrDependencyFailed", id, r.Output, r.Err)
				continue
			}
			if !containsAny(r.Err.Error(), named) {
				t.Errorf("error of %s = %q, want it to name one of %v", id, r.Err, named)
			}
		}
		for _, id := range []string{"audit", "notify"} {
			if r := results[id]; r.Err != nil || tr.started(id) != 1 {
				t.Errorf("%s: ran %d times with error %v; want exactly one successful run", id, tr.started(id), r.Err)
			}
		}
	})
}

func TestIndependentBranchContinuesAfterFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errBoom := errors.New("boom")
		release := make(chan struct{})
		var slowCtxErr error
		var afterDeps map[string]Output
		tr := newTracker()
		nodes := []Node{
			tr.node("fails", nil, func(context.Context, map[string]Output) (Output, error) {
				return nil, errBoom
			}),
			tr.node("slow", nil, func(ctx context.Context, _ map[string]Output) (Output, error) {
				<-release
				slowCtxErr = ctx.Err()
				return "slow-out", nil
			}),
			tr.node("join", []string{"fails", "slow"}, succeed("joined")),
			tr.node("after-slow", []string{"slow"}, func(_ context.Context, d map[string]Output) (Output, error) {
				afterDeps = d
				return "after-out", nil
			}),
		}

		done := goExecute(context.Background(), nodes, 3)
		assertBlocked(t, done, "while an independent branch still runs")
		if tr.started("fails") != 1 || tr.started("slow") != 1 {
			t.Fatal("fails and slow did not both start")
		}

		close(release)
		r := receive(t, done, "after slow returns")
		if slowCtxErr != nil {
			t.Fatalf("slow saw a done context (%v): a failure must not cancel other nodes", slowCtxErr)
		}
		if tr.started("join") != 0 || !errors.Is(r.results["join"].Err, ErrDependencyFailed) {
			t.Fatalf("join ran %d times with result error %v; want skipped with ErrDependencyFailed", tr.started("join"), r.results["join"].Err)
		}
		if res := r.results["after-slow"]; res.Err != nil || afterDeps["slow"] != Output("slow-out") {
			t.Fatalf("after-slow = %v with deps %v; want it to run on slow's output after the failure", res.Err, afterDeps)
		}
		if !errors.Is(r.err, ErrGraphFailed) || !errors.Is(r.err, errBoom) {
			t.Fatalf("Execute error = %v, want one matching both ErrGraphFailed and boom", r.err)
		}
	})
}

func TestCancellationStopsStartingNodesAndWaitsForRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 2
		release := make(chan struct{})
		tr := newTracker()
		var nodes []Node
		for i := range 3 {
			root, child := fmt.Sprintf("root-%d", i), fmt.Sprintf("child-%d", i)
			// A root notices the cancellation, cleans up for a while, and
			// still returns a successful result.
			nodes = append(nodes,
				tr.node(root, nil, func(ctx context.Context, _ map[string]Output) (Output, error) {
					<-ctx.Done()
					<-release
					return root + "-out", nil
				}),
				tr.node(child, []string{root}, succeed(child+"-out")),
			)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := goExecute(ctx, nodes, limit)
		synctest.Wait()
		if got := tr.totalStarted(); got != limit {
			t.Fatalf("%d nodes started with three ready roots, want %d", got, limit)
		}

		cancel()
		assertBlocked(t, done, "after cancellation, while started nodes still run")
		close(release)
		r := receive(t, done, "after started nodes return")

		if got := tr.totalStarted(); got != limit {
			t.Fatalf("%d nodes started in total, want %d: nothing may start after ctx is done", got, limit)
		}
		for i := range 3 {
			root, child := fmt.Sprintf("root-%d", i), fmt.Sprintf("child-%d", i)
			rr, cr := r.results[root], r.results[child]
			if tr.started(root) == 1 {
				if rr.Err != nil || rr.Output != Output(root+"-out") {
					t.Errorf("%s = %v, %v; want the result its Run returned", root, rr.Output, rr.Err)
				}
				if !errors.Is(cr.Err, context.Canceled) {
					t.Errorf("%s (dependency succeeded, never started) error = %v, want context.Canceled", child, cr.Err)
				}
				continue
			}
			if !errors.Is(rr.Err, context.Canceled) {
				t.Errorf("%s (never started) error = %v, want context.Canceled", root, rr.Err)
			}
			if !errors.Is(cr.Err, context.Canceled) && !errors.Is(cr.Err, ErrDependencyFailed) {
				t.Errorf("%s (never started) error = %v, want context.Canceled or ErrDependencyFailed", child, cr.Err)
			}
		}
		if !errors.Is(r.err, ErrGraphFailed) || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Execute error = %v, want one matching both ErrGraphFailed and context.Canceled", r.err)
		}
	})
}

func TestPreCancelledContextRunsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		run := func(context.Context, map[string]Output) (Output, error) {
			calls.Add(1)
			return "out", nil
		}
		nodes := []Node{
			{ID: "a", Run: run},
			{ID: "b", DependsOn: []string{"a"}, Run: run},
			{ID: "c", Run: run},
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		results, err := Execute(ctx, nodes, 2)
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("%d Run calls with an expired context, want 0", got)
		}
		if len(results) != len(nodes) {
			t.Fatalf("Execute returned %d results for %d nodes", len(results), len(nodes))
		}
		for _, id := range []string{"a", "c"} {
			if r := results[id]; !errors.Is(r.Err, context.DeadlineExceeded) || r.Output != nil {
				t.Errorf("result of %s = %v, %v; want nil output and ctx.Err() (deadline exceeded)", id, r.Output, r.Err)
			}
		}
		if r := results["b"]; !errors.Is(r.Err, context.DeadlineExceeded) && !errors.Is(r.Err, ErrDependencyFailed) {
			t.Errorf("result of b = %v, want ctx.Err() or ErrDependencyFailed", r.Err)
		}
		if !errors.Is(err, ErrGraphFailed) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Execute error = %v, want one matching both ErrGraphFailed and context.DeadlineExceeded", err)
		}
	})
}

func TestNodeContextCarriesCallerValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type ctxKey string
		ctx := context.WithValue(context.Background(), ctxKey("trace"), "trace-1")
		var seen sync.Map
		run := func(id string) runFunc {
			return func(ctx context.Context, _ map[string]Output) (Output, error) {
				seen.Store(id, ctx.Value(ctxKey("trace")))
				return nil, nil
			}
		}
		nodes := []Node{
			{ID: "a", Run: run("a")},
			{ID: "b", DependsOn: []string{"a"}, Run: run("b")},
		}

		if _, err := Execute(ctx, nodes, 1); err != nil {
			t.Fatalf("Execute returned error: %v", err)
		}
		for _, id := range []string{"a", "b"} {
			if v, _ := seen.Load(id); v != "trace-1" {
				t.Errorf("%s saw trace value %v, want trace-1: Run must receive a context derived from ctx", id, v)
			}
		}
	})
}
