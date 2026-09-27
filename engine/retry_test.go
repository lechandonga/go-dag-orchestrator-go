package engine

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

func runOne(t *testing.T, task *dag.Task, ctx context.Context) (*RunSnapshot, []Event) {
	t.Helper()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "run.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err := New(dag.New([]*dag.Task{task}), Config{
		MaxConcurrency: 1, Store: store, LogSink: testLogger{t},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := eng.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return snap, eng.Events()
}

func TestRetriesThenSucceeds(t *testing.T) {
	var calls int32
	task := &dag.Task{
		ID: "t", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond},
		Fn: func(dag.Context) error {
			if atomic.AddInt32(&calls, 1) < 3 {
				return dag.NewRetriableError("flaky")
			}
			return nil
		},
	}
	snap, events := runOne(t, task, context.Background())
	if !snap.Success {
		t.Fatalf("expected success: %+v", snap.Tasks)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
	var retries int
	for _, ev := range events {
		if ev.Type == EventTaskRetry {
			retries++
			if ev.Attempt != retries+1 {
				t.Fatalf("retry event attempt=%d want %d", ev.Attempt, retries+1)
			}
		}
	}
	if retries != 2 {
		t.Fatalf("expected 2 retry events, got %d", retries)
	}
}

func TestRetriesExhaustedReason(t *testing.T) {
	task := &dag.Task{
		ID: "t", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond},
		Fn:    func(dag.Context) error { return dag.NewRetriableError("nope") },
	}
	snap, _ := runOne(t, task, context.Background())
	ts := snap.Tasks[0]
	if ts.Status != StatusFailed {
		t.Fatalf("status=%s", ts.Status)
	}
	if ts.Reason != string(dag.ReasonRetriesExhausted) {
		t.Fatalf("reason=%s want %s", ts.Reason, dag.ReasonRetriesExhausted)
	}
	if ts.Attempts != 3 {
		t.Fatalf("attempts=%d want 3", ts.Attempts)
	}
}

func TestTerminalFailureNotRetried(t *testing.T) {
	var calls int32
	task := &dag.Task{
		ID: "t", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 5, Backoff: time.Millisecond},
		Fn: func(dag.Context) error {
			atomic.AddInt32(&calls, 1)
			return dag.NewTerminalError("bad input")
		},
	}
	snap, events := runOne(t, task, context.Background())
	ts := snap.Tasks[0]
	if ts.Status != StatusFailed || ts.Reason != string(dag.ReasonTerminalFailure) {
		t.Fatalf("unexpected state: %+v", ts)
	}
	if calls != 1 {
		t.Fatalf("terminal error must not retry, calls=%d", calls)
	}
	for _, ev := range events {
		if ev.Type == EventTaskRetry {
			t.Fatal("terminal failure produced a retry event")
		}
	}
}

func TestTimeoutIsRetried(t *testing.T) {
	var calls int32
	task := &dag.Task{
		ID: "t", Timeout: 30 * time.Millisecond,
		Retry: dag.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond},
		Fn: func(ctx dag.Context) error {
			if atomic.AddInt32(&calls, 1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}
	snap, _ := runOne(t, task, context.Background())
	if !snap.Success {
		t.Fatalf("expected recovery from timeout: %+v", snap.Tasks)
	}
	if calls != 2 {
		t.Fatalf("want 2 attempts, got %d", calls)
	}
}

func TestCancelMarksPendingSkipped(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	slow := &dag.Task{
		ID: "slow", Timeout: time.Second,
		Fn: func(dag.Context) error {
			started <- struct{}{}
			<-release
			return nil
		},
	}
	down := &dag.Task{
		ID: "down", Deps: []string{"slow"}, Timeout: time.Second,
		Fn: func(dag.Context) error { return nil },
	}
	store, err := NewFileStore(filepath.Join(t.TempDir(), "run.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err := New(dag.New([]*dag.Task{slow, down}),
		Config{MaxConcurrency: 1, Store: store, LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		eng.Run(ctx)
	}()
	<-started
	cancel()
	close(release)
	<-runDone

	snap := eng.Snapshot()
	for _, ts := range snap.Tasks {
		if ts.TaskID == "down" {
			if ts.Status != StatusSkipped || ts.Reason != string(dag.ReasonCanceled) {
				t.Fatalf("down expected skipped/canceled: %+v", ts)
			}
		}
	}
}
