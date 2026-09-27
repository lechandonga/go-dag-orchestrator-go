package dag

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestInterruptAndResume 模拟进程中断：第一次运行在 a 完成后被取消，
// b 未完成；用同一 runID 与同一存储恢复后，a 不得重跑，b 继续完成。
func TestInterruptAndResume(t *testing.T) {
	dir := t.TempDir()
	var aRuns, bRuns atomic.Int32

	aStarted := make(chan struct{}, 1)
	blockB := make(chan struct{})
	mkSpec := func() Spec {
		return Spec{Name: "recover", Tasks: []Task{
			{ID: "a", Run: func(ctx Context) error {
				aRuns.Add(1)
				aStarted <- struct{}{}
				return nil
			}},
			{ID: "b", Deps: []string{"a"}, Run: func(ctx Context) error {
				bRuns.Add(1)
				select {
				case <-blockB:
					return nil
				case <-ctx.Done():
					<-blockB // 模拟被中断前还在执行的任务
					return nil
				}
			}},
		}}
	}

	// 第一次运行：a 完成后取消（模拟进程中断）
	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e1 := NewEngine(store1, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Snapshot, 1)
	go func() {
		snap, _ := e1.Run(ctx, "run-recover", mkSpec())
		done <- snap
	}()
	<-aStarted // 等 a 完成、b 进入执行
	time.Sleep(20 * time.Millisecond)
	cancel()      // 中断
	close(blockB) // 让被中断的 b 退出
	snap1 := <-done
	t.Logf("after interrupt: run=%s a=%s b=%s", snap1.Status,
		snap1.Tasks["a"].Status, snap1.Tasks["b"].Status)
	if snap1.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("a should be succeeded, got %s", snap1.Tasks["a"].Status)
	}

	// 恢复：新引擎、同一存储目录、同一 runID
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e2 := NewEngine(store2, Options{})
	snap2, err := e2.Run(context.Background(), "run-recover", mkSpec())
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if snap2.Status != RunSucceeded {
		t.Fatalf("resumed run status=%s, want succeeded", snap2.Status)
	}
	if got := aRuns.Load(); got != 1 {
		t.Fatalf("task a ran %d times, want 1 (completed tasks must not re-run)", got)
	}
	if got := bRuns.Load(); got < 1 {
		t.Fatalf("task b should have run, got %d", got)
	}
	t.Logf("after resume: a runs=%d b runs=%d", aRuns.Load(), bRuns.Load())

	// 重复触发恢复：运行已成功结束，再跑一次不得重复执行任何任务
	snap3, err := e2.Run(context.Background(), "run-recover", mkSpec())
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if snap3.Status != RunSucceeded {
		t.Fatalf("re-run status=%s", snap3.Status)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("task a re-ran after completion: %d", aRuns.Load())
	}
	bAfter := bRuns.Load()
	if bAfter != 2 { // 中断时 1 次 + 恢复后 1 次
		t.Fatalf("task b ran %d times, want 2", bAfter)
	}

	// 事件流一致性：Seq 单调、无重复
	store3, _ := NewFileStore(dir)
	evs, err := store3.Load("run-recover")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	var prev int64
	for _, ev := range evs {
		if seen[ev.Seq] {
			t.Fatalf("duplicate event seq=%d", ev.Seq)
		}
		seen[ev.Seq] = true
		if ev.Seq <= prev {
			t.Fatalf("events not monotonic: %d after %d", ev.Seq, prev)
		}
		prev = ev.Seq
	}
	t.Logf("event log: %d events, seq monotonic, no duplicates", len(evs))
}

// TestCrashMidAttempt 模拟任务执行到一半进程崩溃（连取消事件都没来得及写）：
// 直接往存储里写入 attempt 事件后“崩溃”，恢复后该任务应按剩余重试次数继续。
func TestCrashMidAttempt(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 手工构造崩溃现场：run_started + a 成功 + b 第一次尝试开始（无终态事件）
	now := time.Now()
	for _, ev := range []Event{
		{Seq: 1, RunID: "run-crash", Type: EventRunStarted, Tasks: []string{"a", "b"}, Time: now},
		{Seq: 2, RunID: "run-crash", Type: EventTaskAttempt, TaskID: "a", Attempt: 1, Time: now},
		{Seq: 3, RunID: "run-crash", Type: EventTaskSucceeded, TaskID: "a", Attempt: 1, Time: now},
		{Seq: 4, RunID: "run-crash", Type: EventTaskAttempt, TaskID: "b", Attempt: 1, Time: now},
	} {
		if err := store.Append(ev); err != nil {
			t.Fatal(err)
		}
	}

	var aRuns, bAttempts atomic.Int32
	spec := Spec{Name: "crash", Tasks: []Task{
		{ID: "a", Run: func(ctx Context) error { aRuns.Add(1); return nil }},
		{ID: "b", Deps: []string{"a"}, Retries: 2, Run: func(ctx Context) error {
			bAttempts.Add(1)
			return nil
		}},
	}}
	e := NewEngine(store, Options{})
	snap, err := e.Run(context.Background(), "run-crash", spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if snap.Status != RunSucceeded {
		t.Fatalf("status=%s", snap.Status)
	}
	if got := aRuns.Load(); got != 0 {
		t.Fatalf("completed task a re-ran %d times", got)
	}
	if got := bAttempts.Load(); got != 1 {
		t.Fatalf("b should run once more after crash, got %d", got)
	}
	// b 的尝试计数应包含崩溃前那次（attempt=1）+ 恢复后（attempt=2）
	if got := snap.Tasks["b"].Attempts; got != 2 {
		t.Fatalf("b attempts=%d, want 2 (crash attempt preserved)", got)
	}
}

// TestConcurrentRunSameID 同一 runID 不允许并发执行两次。
func TestConcurrentRunSameID(t *testing.T) {
	release := make(chan struct{})
	spec := Spec{Tasks: []Task{
		{ID: "a", Run: func(ctx Context) error { <-release; return nil }},
	}}
	e := NewEngine(nil, Options{})
	done := make(chan error, 1)
	go func() {
		_, err := e.Run(context.Background(), "run-dup", spec)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_, err := e.Run(context.Background(), "run-dup", spec)
	if err != ErrRunInProgress {
		t.Fatalf("expected ErrRunInProgress, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first run: %v", err)
	}
}
