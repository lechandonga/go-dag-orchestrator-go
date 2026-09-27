package dag

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func okTask(id string, deps ...string) Task {
	return Task{ID: id, Deps: deps, Run: func(ctx Context) error { return nil }}
}

func TestRunLinearDAG(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mk := func(id string, deps ...string) Task {
		return Task{ID: id, Deps: deps, Run: func(ctx Context) error {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}}
	}
	spec := Spec{Name: "linear", Tasks: []Task{
		mk("a"), mk("b", "a"), mk("c", "b"),
	}}
	e := NewEngine(nil, Options{})
	snap, err := e.Run(context.Background(), "run-linear", spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if snap.Status != RunSucceeded {
		t.Fatalf("expected succeeded, got %s", snap.Status)
	}
	for _, id := range []string{"a", "b", "c"} {
		if snap.Tasks[id].Status != StatusSucceeded {
			t.Fatalf("task %s status=%s", id, snap.Tasks[id].Status)
		}
		if snap.Tasks[id].Attempts != 1 {
			t.Fatalf("task %s attempts=%d, want 1", id, snap.Tasks[id].Attempts)
		}
	}
	t.Logf("execution order: %v", order)
}

func TestUpstreamFailureBlocksDownstream(t *testing.T) {
	boom := errors.New("boom")
	var downstreamRan atomic.Bool
	spec := Spec{Name: "fail", Tasks: []Task{
		{ID: "a", Run: func(ctx Context) error { return boom }}, // 终止性失败
		{ID: "b", Deps: []string{"a"}, Run: func(ctx Context) error {
			downstreamRan.Store(true)
			return nil
		}},
		{ID: "c", Deps: []string{"b"}, Run: func(ctx Context) error {
			downstreamRan.Store(true)
			return nil
		}},
		{ID: "d", Run: func(ctx Context) error { return nil }}, // 独立分支不受影响
	}}
	e := NewEngine(nil, Options{})
	snap, err := e.Run(context.Background(), "run-fail", spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if snap.Status != RunFailed {
		t.Fatalf("expected run failed, got %s", snap.Status)
	}
	if downstreamRan.Load() {
		t.Fatal("downstream task ran despite upstream failure")
	}
	if got := snap.Tasks["a"].Status; got != StatusFailed {
		t.Fatalf("a status=%s", got)
	}
	if got := snap.Tasks["a"].Failure; got != FailureFatal {
		t.Fatalf("a failure kind=%s, want fatal", got)
	}
	for _, id := range []string{"b", "c"} {
		ts := snap.Tasks[id]
		if ts.Status != StatusSkipped || ts.Failure != FailureUpstream {
			t.Fatalf("task %s status=%s failure=%s, want skipped/upstream", id, ts.Status, ts.Failure)
		}
	}
	if got := snap.Tasks["d"].Status; got != StatusSucceeded {
		t.Fatalf("independent branch d status=%s", got)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	const limit = 2
	const n = 6
	var cur, max atomic.Int32
	var tasks []Task
	for i := 0; i < n; i++ {
		tasks = append(tasks, Task{
			ID: fmt.Sprintf("t%d", i),
			Run: func(ctx Context) error {
				c := cur.Add(1)
				for {
					m := max.Load()
					if c <= m || max.CompareAndSwap(m, c) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				cur.Add(-1)
				return nil
			},
		})
	}
	e := NewEngine(nil, Options{Concurrency: limit})
	snap, err := e.Run(context.Background(), "run-conc", Spec{Name: "conc", Tasks: tasks})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if snap.Status != RunSucceeded {
		t.Fatalf("status=%s", snap.Status)
	}
	if got := max.Load(); got > limit {
		t.Fatalf("concurrency %d exceeded limit %d", got, limit)
	}
	t.Logf("max observed concurrency: %d (limit %d)", max.Load(), limit)
}

func TestTimeoutAndRetry(t *testing.T) {
	var attempts atomic.Int32
	spec := Spec{Name: "timeout", Tasks: []Task{{
		ID:      "slow",
		Timeout: 30 * time.Millisecond,
		Retries: 2, // 共 3 次尝试
		Run: func(ctx Context) error {
			attempts.Add(1)
			select {
			case <-ctx.Done():
				// 模拟任务不尊重取消：继续睡，让引擎超时判定生效
				time.Sleep(80 * time.Millisecond)
				return nil
			case <-time.After(200 * time.Millisecond):
				return nil
			}
		},
	}}}
	e := NewEngine(nil, Options{})
	snap, err := e.Run(context.Background(), "run-timeout", spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	ts := snap.Tasks["slow"]
	if ts.Status != StatusFailed {
		t.Fatalf("status=%s, want failed", ts.Status)
	}
	if ts.Failure != FailureTimeout {
		t.Fatalf("failure=%s, want timeout", ts.Failure)
	}
	if ts.Attempts != 3 {
		t.Fatalf("attempts=%d, want 3", ts.Attempts)
	}
	if ts.Err == "" {
		t.Fatal("expected failure reason recorded")
	}
	t.Logf("timeout task: attempts=%d failure=%s err=%q", ts.Attempts, ts.Failure, ts.Err)
}

func TestRetryableVsFatal(t *testing.T) {
	var retryableAttempts, fatalAttempts atomic.Int32
	spec := Spec{Name: "retry", Tasks: []Task{
		{
			ID:      "flaky",
			Retries: 3,
			Run: func(ctx Context) error {
				n := retryableAttempts.Add(1)
				if n < 3 {
					return Retryable(fmt.Errorf("transient error %d", n))
				}
				return nil
			},
		},
		{
			ID:      "fatal",
			Retries: 5, // 终止性失败不应重试
			Run: func(ctx Context) error {
				fatalAttempts.Add(1)
				return errors.New("permanent failure")
			},
		},
	}}
	e := NewEngine(nil, Options{})
	snap, err := e.Run(context.Background(), "run-retry", spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if snap.Status != RunFailed {
		t.Fatalf("status=%s, want failed (fatal task)", snap.Status)
	}
	flaky := snap.Tasks["flaky"]
	if flaky.Status != StatusSucceeded || flaky.Attempts != 3 {
		t.Fatalf("flaky: status=%s attempts=%d, want succeeded/3", flaky.Status, flaky.Attempts)
	}
	fatal := snap.Tasks["fatal"]
	if fatal.Status != StatusFailed || fatal.Failure != FailureFatal {
		t.Fatalf("fatal: status=%s failure=%s", fatal.Status, fatal.Failure)
	}
	if fatal.Attempts != 1 {
		t.Fatalf("fatal attempts=%d, want 1 (no retry on fatal)", fatal.Attempts)
	}
	if got := fatalAttempts.Load(); got != 1 {
		t.Fatalf("fatal task executed %d times, want 1", got)
	}
}

func TestConcurrentSnapshotConsistency(t *testing.T) {
	release := make(chan struct{})
	var tasks []Task
	for i := 0; i < 4; i++ {
		tasks = append(tasks, Task{
			ID: fmt.Sprintf("t%d", i),
			Run: func(ctx Context) error {
				<-release
				return nil
			},
		})
	}
	e := NewEngine(nil, Options{Concurrency: 4})
	done := make(chan Snapshot, 1)
	go func() {
		snap, _ := e.Run(context.Background(), "run-snap", Spec{Name: "snap", Tasks: tasks})
		done <- snap
	}()
	// 并发查询快照：状态计数必须守恒（总数恒为任务数，无重复无丢失）
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap, ok := e.Snapshot("run-snap")
				if !ok {
					continue
				}
				if len(snap.Tasks) != 4 {
					t.Errorf("snapshot has %d tasks, want 4", len(snap.Tasks))
					return
				}
				total := 0
				for _, ts := range snap.Tasks {
					switch ts.Status {
					case StatusPending, StatusReady, StatusRunning, StatusSucceeded:
						total++
					default:
						t.Errorf("unexpected mid-run status %s", ts.Status)
					}
				}
				if total != 4 {
					t.Errorf("status count not conserved: %d", total)
					return
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	snap := <-done
	close(stop)
	wg.Wait()
	if snap.Status != RunSucceeded {
		t.Fatalf("status=%s", snap.Status)
	}
}
