package dag

import (
	"fmt"
	"testing"
	"time"
)

func TestFileStoreAppendIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// 单调追加 + 重复追加：Load 应去重并保持有序
	for _, ev := range []Event{
		{Seq: 1, RunID: "r", Type: EventRunStarted, Tasks: []string{"a"}, Time: now},
		{Seq: 2, RunID: "r", Type: EventTaskAttempt, TaskID: "a", Attempt: 1, Time: now},
		{Seq: 2, RunID: "r", Type: EventTaskAttempt, TaskID: "a", Attempt: 1, Time: now}, // 重复
		{Seq: 3, RunID: "r", Type: EventTaskSucceeded, TaskID: "a", Time: now},
		{Seq: 3, RunID: "r", Type: EventTaskSucceeded, TaskID: "a", Time: now}, // 重复
	} {
		if err := s.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := s.Load("r")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 {
		t.Fatalf("loaded %d events, want 3 (dedup)", len(evs))
	}
	for i, want := range []int64{1, 2, 3} {
		if evs[i].Seq != want {
			t.Fatalf("evs[%d].Seq=%d, want %d", i, evs[i].Seq, want)
		}
	}
	// 不存在的 run：空而非报错
	evs, err = s.Load("nonexistent")
	if err != nil || len(evs) != 0 {
		t.Fatalf("missing run: evs=%v err=%v", evs, err)
	}
}

func TestFileStoreReloadAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	s1, _ := NewFileStore(dir)
	for i := 1; i <= 5; i++ {
		if err := s1.Append(Event{Seq: int64(i), RunID: "r", Type: EventTaskAttempt, TaskID: "a", Attempt: i, Time: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟进程重启：新实例读取同一目录
	s2, _ := NewFileStore(dir)
	evs, err := s2.Load("r")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 5 {
		t.Fatalf("reloaded %d events, want 5", len(evs))
	}
	// 重启后继续追加：Seq 必须接续而非覆盖
	if err := s2.Append(Event{Seq: 6, RunID: "r", Type: EventTaskSucceeded, TaskID: "a", Time: time.Now()}); err != nil {
		t.Fatal(err)
	}
	evs, _ = s2.Load("r")
	if len(evs) != 6 || evs[5].Seq != 6 {
		t.Fatalf("after restart append: %d events, last seq=%v", len(evs), evs[len(evs)-1].Seq)
	}
}

func TestMemStoreConcurrent(t *testing.T) {
	s := NewMemStore()
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func(g int) {
			runID := fmt.Sprintf("r%d", g) // 每个 run 内保持 Seq 单调（Store 契约）
			for i := 0; i < 50; i++ {
				_ = s.Append(Event{Seq: int64(i + 1), RunID: runID, Type: EventTaskAttempt, Time: time.Now()})
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 4; g++ {
		<-done
	}
	for g := 0; g < 4; g++ {
		evs, _ := s.Load(fmt.Sprintf("r%d", g))
		if len(evs) != 50 {
			t.Fatalf("run r%d loaded %d events, want 50", g, len(evs))
		}
	}
}

func TestSnapshotBeforeAndAfterRun(t *testing.T) {
	e := NewEngine(nil, Options{})
	if _, ok := e.Snapshot("nope"); ok {
		t.Fatal("snapshot of unknown run should not be ok")
	}
	spec := Spec{Tasks: []Task{{ID: "a", Run: func(ctx Context) error { return nil }}}}
	if _, err := e.Run(t.Context(), "r1", spec); err != nil {
		t.Fatal(err)
	}
	snap, ok := e.Snapshot("r1")
	if !ok {
		t.Fatal("snapshot should exist after run")
	}
	if snap.Status != RunSucceeded || snap.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	// 快照是深拷贝：修改返回值不影响内部状态
	snap.Tasks["a"] = TaskSnapshot{ID: "a", Status: StatusFailed}
	snap2, _ := e.Snapshot("r1")
	if snap2.Tasks["a"].Status != StatusSucceeded {
		t.Fatal("snapshot mutation leaked into engine state")
	}
}
