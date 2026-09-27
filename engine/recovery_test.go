package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

// TestRecoveryAcrossProcessCrash drives a real subprocess that:
//   - executes a DAG a -> b -> c,
//   - crashes (os.Exit) inside b on the first launch,
//   - is restarted by the test against the same journal directory.
//
// On restart a must never re-execute, b must resume and finish, and c
// must run afterwards.
func TestRecoveryAcrossProcessCrash(t *testing.T) {
	if os.Getenv("DAG_CRASH_HELPER") == "1" {
		crashHelperMain()
		return
	}

	dir := t.TempDir()
	// First launch: crash in b.
	cmd := exec.Command(os.Args[0], "-test.run=TestRecoveryAcrossProcessCrash")
	cmd.Env = append(os.Environ(),
		"DAG_CRASH_HELPER=1",
		"DAG_DIR="+dir,
		"DAG_CRASH_POINT=b")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 42 {
		t.Fatalf("expected crash exit 42, got %v: %s", err, out)
	}

	// Marker for a exists; b's marker must not (crashed before success).
	if _, err := os.Stat(filepath.Join(dir, "ran-a")); err != nil {
		t.Fatalf("a should have completed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran-b")); err == nil {
		t.Fatal("b must not be marked complete before crash")
	}

	// Restart: no crash point; run must complete without re-running a.
	cmd = exec.Command(os.Args[0], "-test.run=TestRecoveryAcrossProcessCrash")
	cmd.Env = append(os.Environ(),
		"DAG_CRASH_HELPER=1",
		"DAG_DIR="+dir)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restart failed: %v\n%s", err, out)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := os.Stat(filepath.Join(dir, "ran-"+id)); err != nil {
			t.Fatalf("task %s missing completion marker: %v", id, err)
		}
	}
	// a must have run exactly once across both processes.
	if data, err := os.ReadFile(filepath.Join(dir, "ran-a")); err != nil ||
		strings.TrimSpace(string(data)) != "1" {
		t.Fatalf("a ran more than once, marker=%q err=%v", data, err)
	}
	// b's first attempt was killed without a verdict; it resumes at
	// attempt 1 (the interruption must not consume a retry).
	if data, _ := os.ReadFile(filepath.Join(dir, "attempts-b")); strings.TrimSpace(string(data)) != "1" {
		t.Fatalf("b should resume on attempt 1 (interrupted attempt does not count), got %q", data)
	}
}

func crashHelperMain() {
	dir := os.Getenv("DAG_DIR")
	crashPoint := os.Getenv("DAG_CRASH_POINT")

	task := func(id string, deps []string) *dag.Task {
		return &dag.Task{
			ID: id, Deps: deps, Timeout: 5 * time.Second,
			Retry: dag.RetryPolicy{MaxAttempts: 5, Backoff: time.Millisecond},
			Fn: func(ctx dag.Context) error {
				// Count total executions per task (detects re-runs).
				cntPath := filepath.Join(dir, "runs-"+id)
				n := bumpCounter(cntPath)
				fmt.Fprintf(os.Stderr, "[helper] executing %s execution=%d attempt=%d\n", id, n, ctx.Attempt())
				if id == crashPoint && ctx.Attempt() == 1 {
					// Simulate a hard crash (SIGKILL-like): no cleanup.
					os.Exit(42)
				}
				if err := os.WriteFile(filepath.Join(dir, "ran-"+id), []byte(fmt.Sprint(n)), 0o644); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(dir, "attempts-"+id),
					[]byte(fmt.Sprint(ctx.Attempt())), 0o644); err != nil {
					return err
				}
				return nil
			},
		}
	}

	g := dag.New([]*dag.Task{
		task("a", nil),
		task("b", []string{"a"}),
		task("c", []string{"b"}),
	})
	store, err := NewFileStore(filepath.Join(dir, "journal.jsonl"))
	if err != nil {
		failHelper(err)
	}
	defer store.Close()
	eng, err := New(g, Config{MaxConcurrency: 1, Store: store, LogSink: os.Stderr})
	if err != nil {
		failHelper(err)
	}
	snap, err := eng.Run(context.Background())
	if err != nil {
		failHelper(err)
	}
	if !snap.Success {
		failHelper(fmt.Errorf("run not successful: %+v", snap.Tasks))
	}
}

func bumpCounter(path string) int {
	n := 1
	if data, err := os.ReadFile(path); err == nil {
		var v int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &v); err == nil {
			n = v + 1
		}
	}
	if err := os.WriteFile(path, []byte(fmt.Sprint(n)), 0o644); err != nil {
		panic(err)
	}
	return n
}

func failHelper(err error) {
	fmt.Fprintln(os.Stderr, "helper error:", err)
	os.Exit(1)
}

// TestResumeInterruptedAttempt reconstructs a journal whose last task
// is stuck in running state (process killed after task_started but
// before task_finished) and asserts the engine resumes that task on the
// next attempt and completes the run.
func TestResumeInterruptedAttempt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.jsonl")

	// Build journal manually: run_started + task_started(attempt=2),
	// representing a crash during the second attempt.
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(),
		Event{Type: EventRunStarted, RunID: "run-x", Time: time.Now()},
		Event{Type: EventTaskStarted, RunID: "run-x", TaskID: "t", Attempt: 2, Time: time.Now()},
	); err != nil {
		t.Fatal(err)
	}
	store.Close()

	var observedAttempt int32
	realTask := &dag.Task{
		ID: "t", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 3},
		Fn: func(ctx dag.Context) error {
			atomic.StoreInt32(&observedAttempt, int32(ctx.Attempt()))
			return nil
		},
	}
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	eng, err := New(dag.New([]*dag.Task{realTask}),
		Config{MaxConcurrency: 1, Store: store2, RunID: "run-x", LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Success {
		t.Fatalf("expected success after resume: %+v", snap.Tasks)
	}
	if atomic.LoadInt32(&observedAttempt) != 2 {
		t.Fatalf("expected resume on attempt 2, got %d", observedAttempt)
	}
}

// TestRepeatedRecoveryIsIdempotent runs the same completed journal
// through two more engine instances: nothing re-executes and snapshots
// are byte-identical.
func TestRepeatedRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.jsonl")

	mkEngine := func(runID string) (*Engine, *FileStore, *int32) {
		var calls int32
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatal(err)
		}
		task := &dag.Task{
			ID: "a", Timeout: time.Second,
			Fn: func(dag.Context) error {
				atomic.AddInt32(&calls, 1)
				return nil
			},
		}
		eng, err := New(dag.New([]*dag.Task{task}),
			Config{MaxConcurrency: 1, Store: store, RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return eng, store, &calls
	}

	eng1, store1, calls1 := mkEngine("")
	if _, err := eng1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	events1 := eng1.Events()
	snap1 := eng1.Snapshot()
	store1.Close()
	if *calls1 != 1 {
		t.Fatalf("expected single execution, got %d", *calls1)
	}

	eng2, store2, calls2 := mkEngine(eng1.RunID())
	snap2, err := eng2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store2.Close()
	if *calls2 != 0 {
		t.Fatalf("recovered run re-executed task %d times", *calls2)
	}
	events2 := eng2.Events()
	if len(events2) != len(events1) {
		t.Fatalf("journal grew on recovery: %d vs %d", len(events2), len(events1))
	}
	if !snapshotsEqual(snap1, snap2) {
		t.Fatalf("snapshots differ after recovery:\n%+v\n%+v", snap1, snap2)
	}
}

func snapshotsEqual(a, b *RunSnapshot) bool {
	if a.RunID != b.RunID || a.Seq != b.Seq || a.Finished != b.Finished ||
		a.Success != b.Success || len(a.Tasks) != len(b.Tasks) {
		return false
	}
	for i := range a.Tasks {
		if a.Tasks[i] != b.Tasks[i] {
			return false
		}
	}
	return true
}

// TestResumeAfterRetryRecorded: the journal contains a task_retry(2)
// event but no following task_started (crash during backoff/wait). The
// task must resume on attempt 2 exactly once.
func TestResumeAfterRetryRecorded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.jsonl")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(),
		Event{Type: EventRunStarted, RunID: "run-y", Time: time.Now()},
		Event{Type: EventTaskStarted, RunID: "run-y", TaskID: "t", Attempt: 1, Time: time.Now()},
		Event{Type: EventTaskRetry, RunID: "run-y", TaskID: "t", Attempt: 2,
			Reason: "task_failed", Message: "flaky", Time: time.Now()},
	); err != nil {
		t.Fatal(err)
	}
	store.Close()

	var observed []int32
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	task := &dag.Task{
		ID: "t", Timeout: time.Second,
		Retry: dag.RetryPolicy{MaxAttempts: 3},
		Fn: func(ctx dag.Context) error {
			observed = append(observed, int32(ctx.Attempt()))
			return nil
		},
	}
	eng, err := New(dag.New([]*dag.Task{task}),
		Config{MaxConcurrency: 1, Store: store2, RunID: "run-y", LogSink: testLogger{t}})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Success {
		t.Fatalf("expected success: %+v", snap.Tasks)
	}
	if len(observed) != 1 || observed[0] != 2 {
		t.Fatalf("expected exactly one resume execution at attempt 2, got %v", observed)
	}
	if snap.Tasks[0].Attempts != 2 {
		t.Fatalf("expected final attempts=2, got %d", snap.Tasks[0].Attempts)
	}
}

// TestConcurrentSnapshotsAreConsistent hammers Snapshot/Events while a
// run with retries executes; every observed snapshot must be internally
// coherent and events must remain gap-free with no duplicate seqs.
func TestConcurrentSnapshotsAreConsistent(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(filepath.Join(dir, "j.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var tasks []*dag.Task
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("t%d", i)
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("t%d", i-1)}
		}
		var calls int32
		tasks = append(tasks, &dag.Task{
			ID: id, Deps: deps, Timeout: time.Second,
			Retry: dag.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond},
			Fn: func(ctx dag.Context) error {
				if atomic.AddInt32(&calls, 1) == 1 && i%2 == 0 {
					return dag.NewRetriableError("x")
				}
				time.Sleep(2 * time.Millisecond)
				return nil
			},
		})
	}
	eng, err := New(dag.New(tasks),
		Config{MaxConcurrency: 3, Store: store})
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := eng.Snapshot()
					if snap == nil || len(snap.Tasks) != 8 {
						t.Errorf("incoherent snapshot: %+v", snap)
						return
					}
					for _, ts := range snap.Tasks {
						if ts.Attempts < 0 {
							t.Errorf("negative attempts: %+v", ts)
						}
					}
					evs := eng.Events()
					for i, ev := range evs {
						if ev.Seq != int64(i+1) {
							t.Errorf("event seq gap: idx=%d seq=%d", i, ev.Seq)
							return
						}
					}
				}
			}
		}()
	}

	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	if !snap.Success {
		t.Fatalf("run failed: %+v", snap.Tasks)
	}
	// Final event stream: contiguous seqs, exactly one run_finished.
	evs := eng.Events()
	var finishes int
	for i, ev := range evs {
		if ev.Seq != int64(i+1) {
			t.Fatalf("final seq gap at %d", i)
		}
		if ev.Type == EventRunFinished {
			finishes++
		}
	}
	if finishes != 1 {
		t.Fatalf("expected exactly one run_finished, got %d", finishes)
	}
}
