package engine

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

type rec struct {
	mu      sync.Mutex
	order   []string
	current int32
	maxSeen int32
	fails   map[string]int
}

func newRec() *rec { return &rec{fails: map[string]int{}} }

func (r *rec) enter(id string) {
	atomic.AddInt32(&r.current, 1)
	for {
		v := atomic.LoadInt32(&r.maxSeen)
		c := atomic.LoadInt32(&r.current)
		if c <= v || atomic.CompareAndSwapInt32(&r.maxSeen, v, c) {
			break
		}
	}
}

func (r *rec) leave(id string) {
	r.mu.Lock()
	r.order = append(r.order, id)
	r.mu.Unlock()
	atomic.AddInt32(&r.current, -1)
}

func testEngine(t *testing.T, g *dag.DAG, conc int) (*Engine, *rec, string) {
	t.Helper()
	r := newRec()
	path := filepath.Join(t.TempDir(), "run.jsonl")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng, err := New(g, Config{MaxConcurrency: conc, Store: store, LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	return eng, r, path
}

// trackedTask builds a task that delays briefly and may fail the first
// `failNTimes` attempts with a retriable error.
func trackedTask(r *rec, id string, deps []string, delay time.Duration, failNTimes int) *dag.Task {
	var tries int32
	return &dag.Task{
		ID:      id,
		Deps:    deps,
		Timeout: 5 * time.Second,
		Retry:   dag.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond},
		Fn: func(ctx dag.Context) error {
			r.enter(id)
			defer r.leave(id)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			n := atomic.AddInt32(&tries, 1)
			if int(n) <= failNTimes {
				return dag.NewRetriableError("boom")
			}
			return nil
		},
	}
}

func TestDependencyOrderAndSuccess(t *testing.T) {
	r := newRec()
	g := dag.New([]*dag.Task{
		trackedTask(r, "a", nil, 10*time.Millisecond, 0),
		trackedTask(r, "b", []string{"a"}, 10*time.Millisecond, 0),
		trackedTask(r, "c", []string{"a"}, 10*time.Millisecond, 0),
		trackedTask(r, "d", []string{"b", "c"}, 10*time.Millisecond, 0),
	})
	eng, _, _ := testEngine(t, g, 4)
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Success {
		t.Fatalf("expected success: %+v", snap.Tasks)
	}
	// a before b,c before d
	idx := map[string]int{}
	for i, id := range r.order {
		idx[id] = i
	}
	if !(idx["a"] < idx["b"] && idx["a"] < idx["c"] &&
		idx["b"] < idx["d"] && idx["c"] < idx["d"]) {
		t.Fatalf("dependency order violated: %v", r.order)
	}
}

func TestUpstreamFailureBlocksDownstream(t *testing.T) {
	var downstreamRan int32
	build := func() *dag.DAG {
		fail := &dag.Task{
			ID: "bad", Timeout: time.Second,
			Retry: dag.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond},
			Fn:    func(dag.Context) error { return dag.NewTerminalError("permanent") },
		}
		good := &dag.Task{
			ID: "good", Deps: []string{"bad"}, Timeout: time.Second,
			Fn: func(dag.Context) error {
				atomic.StoreInt32(&downstreamRan, 1)
				return nil
			},
		}
		indirect := &dag.Task{
			ID: "indirect", Deps: []string{"good"}, Timeout: time.Second,
			Fn: func(dag.Context) error {
				atomic.StoreInt32(&downstreamRan, 1)
				return nil
			},
		}
		return dag.New([]*dag.Task{fail, good, indirect})
	}
	eng, _, _ := testEngine(t, build(), 4)
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Success {
		t.Fatal("expected failed run")
	}
	st := map[string]TaskState{}
	for _, ts := range snap.Tasks {
		st[ts.TaskID] = ts
	}
	if st["bad"].Status != StatusFailed || st["bad"].Reason != string(dag.ReasonTerminalFailure) {
		t.Fatalf("bad = %+v", st["bad"])
	}
	if st["good"].Status != StatusSkipped || st["good"].Reason != string(dag.ReasonUpstreamFailed) {
		t.Fatalf("good = %+v", st["good"])
	}
	if st["indirect"].Status != StatusSkipped {
		t.Fatalf("indirect = %+v", st["indirect"])
	}
	if atomic.LoadInt32(&downstreamRan) != 0 {
		t.Fatal("downstream task executed despite upstream failure")
	}
}

func TestConcurrencyLimit(t *testing.T) {
	r := newRec()
	ids := []string{"a", "b", "c", "d", "e"}
	var tasks []*dag.Task
	for _, id := range ids {
		tasks = append(tasks, trackedTask(r, id, nil, 30*time.Millisecond, 0))
	}
	g := dag.New(tasks)
	store, err := NewFileStore(filepath.Join(t.TempDir(), "run.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err := New(g, Config{MaxConcurrency: 2, Store: store, LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Success {
		t.Fatalf("run failed: %+v", snap.Tasks)
	}
	if got := atomic.LoadInt32(&r.maxSeen); got > 2 {
		t.Fatalf("concurrency limit exceeded: max=%d", got)
	}
	if got := atomic.LoadInt32(&r.maxSeen); got != 2 {
		t.Fatalf("expected to reach limit 2, max=%d (order=%v)", got, r.order)
	}
}
