package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	s, err := NewFileStore(filepath.Join(t.TempDir(), "j.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestTaskPanicIsTerminalFailure(t *testing.T) {
	task := &dag.Task{
		ID: "p", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 5},
		Fn:    func(dag.Context) error { panic("boom") },
	}
	eng, err := New(dag.New([]*dag.Task{task}),
		Config{MaxConcurrency: 1, Store: newTestStore(t), LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	snap, rerr := eng.Run(context.Background())
	if rerr != nil {
		t.Fatal(rerr)
	}
	ts := snap.Tasks[0]
	if ts.Status != StatusFailed || ts.Reason != string(dag.ReasonTerminalFailure) {
		t.Fatalf("panic must be terminal: %+v", ts)
	}
}

func TestRunIDMismatchRejected(t *testing.T) {
	store := newTestStore(t)
	task := &dag.Task{ID: "a", Fn: func(dag.Context) error { return nil }}
	eng1, err := New(dag.New([]*dag.Task{task}), Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.Close()

	store2, err := NewFileStore(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	_, err = New(dag.New([]*dag.Task{task}), Config{Store: store2, RunID: "run-different"})
	if !errors.Is(err, ErrRunIDMismatch) {
		t.Fatalf("expected ErrRunIDMismatch, got %v", err)
	}
}

func TestConcurrentRunRejected(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	task := &dag.Task{
		ID: "a", Timeout: time.Second,
		Fn: func(dag.Context) error { <-release; return nil },
	}
	eng, err := New(dag.New([]*dag.Task{task}), Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); eng.Run(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	if _, err := eng.Run(context.Background()); !errors.Is(err, ErrRunInProgress) {
		t.Fatalf("expected ErrRunInProgress, got %v", err)
	}
	close(release)
	wg.Wait()
}

func TestDefaultRetryPolicyIsSingleAttempt(t *testing.T) {
	store := newTestStore(t)
	var calls int
	task := &dag.Task{
		ID: "a",
		Fn: func(dag.Context) error {
			calls++
			return dag.NewRetriableError("x")
		},
	}
	eng, err := New(dag.New([]*dag.Task{task}), Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := eng.Run(context.Background())
	if calls != 1 {
		t.Fatalf("zero-value policy must mean 1 attempt, got %d", calls)
	}
	if snap.Tasks[0].Reason != string(dag.ReasonRetriesExhausted) {
		t.Fatalf("single failed attempt => retries_exhausted, got %s", snap.Tasks[0].Reason)
	}
}

func TestSnapshotBeforeRunFromJournal(t *testing.T) {
	store := newTestStore(t)
	if err := store.Append(context.Background(),
		Event{Type: EventRunStarted, RunID: "r", Time: time.Now()},
		Event{Type: EventTaskStarted, RunID: "r", TaskID: "a", Attempt: 1, Time: time.Now()},
		Event{Type: EventTaskFinished, RunID: "r", TaskID: "a", Attempt: 1,
			Status: StatusSucceeded, Time: time.Now()},
	); err != nil {
		t.Fatal(err)
	}
	task := &dag.Task{ID: "a", Fn: func(dag.Context) error { return nil }}
	eng, err := New(dag.New([]*dag.Task{task}), Config{Store: store, RunID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	snap := eng.Snapshot()
	if snap.Tasks[0].Status != StatusSucceeded || snap.Seq != 3 {
		t.Fatalf("unexpected pre-run snapshot: %+v", snap)
	}
}

func TestCycleRejectedAtEngineConstruction(t *testing.T) {
	store := newTestStore(t)
	g := dag.New([]*dag.Task{
		{ID: "a", Deps: []string{"b"}, Fn: func(dag.Context) error { return nil }},
		{ID: "b", Deps: []string{"a"}, Fn: func(dag.Context) error { return nil }},
	})
	if _, err := New(g, Config{Store: store}); err == nil {
		t.Fatal("expected validation error")
	}
}
