// dagrun 是 DAG 编排引擎的演示 CLI。
//
// 用法：
//
//	dagrun -dir ./rundata -run-id demo [-concurrency 2]
//
// 首次执行会运行内置示例编排；用相同 -run-id 再次执行时，
// 已完成的任务不会重跑，未完成的任务按原语义继续（恢复语义）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

func main() {
	var (
		dir         = flag.String("dir", "./rundata", "事件存储目录（每个 run-id 一个 JSONL 文件）")
		runID       = flag.String("run-id", "demo", "运行 ID；相同 ID 重复执行会触发恢复语义")
		concurrency = flag.Int("concurrency", 2, "并发上限")
	)
	flag.Parse()

	store, err := dag.NewFileStore(*dir)
	if err != nil {
		log.Fatalf("create store: %v", err)
	}
	engine := dag.NewEngine(store, dag.Options{Concurrency: *concurrency})

	// 收到 SIGINT/SIGTERM 时取消运行，模拟进程中断；
	// 重新以相同 run-id 启动即可从断点恢复。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	spec := demoSpec()
	log.Printf("starting run=%s dir=%s concurrency=%d tasks=%d", *runID, *dir, *concurrency, len(spec.Tasks))

	snap, err := engine.Run(ctx, *runID, spec)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Printf("run %s finished: %s\n", snap.RunID, snap.Status)
	for id, ts := range snap.Tasks {
		fmt.Printf("  %-10s %-9s attempts=%d failure=%q err=%q\n",
			id, ts.Status, ts.Attempts, ts.Failure, ts.Err)
	}
	if snap.Status != dag.RunSucceeded {
		os.Exit(1)
	}
}

// demoSpec 构造一个包含依赖、重试与超时的示例编排。
func demoSpec() dag.Spec {
	work := func(d time.Duration) dag.TaskFunc {
		return func(ctx dag.Context) error {
			ctx.Logf("working for %s", d)
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			}
		}
	}
	flaky := func() dag.TaskFunc {
		remaining := 2
		return func(ctx dag.Context) error {
			if remaining > 0 {
				remaining--
				ctx.Logf("simulating transient failure (%d left)", remaining)
				return dag.Retryable(fmt.Errorf("transient error"))
			}
			if rand.Intn(2) == 0 {
				ctx.Logf("unlucky")
			}
			return nil
		}
	}
	return dag.Spec{
		Name: "demo",
		Tasks: []dag.Task{
			{ID: "extract", Run: work(300 * time.Millisecond), Timeout: 2 * time.Second},
			{ID: "transform", Deps: []string{"extract"}, Run: flaky(), Retries: 3, Backoff: 100 * time.Millisecond},
			{ID: "load", Deps: []string{"transform"}, Run: work(200 * time.Millisecond)},
			{ID: "audit", Deps: []string{"extract"}, Run: work(100 * time.Millisecond)},
			{ID: "notify", Deps: []string{"load", "audit"}, Run: work(50 * time.Millisecond)},
		},
	}
}
