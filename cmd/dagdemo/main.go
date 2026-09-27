// Command dagdemo runs a small fixed DAG against ./data/demo.jsonl so
// that interrupting and restarting the process demonstrates recovery:
// succeeded tasks keep marker files and are never re-executed.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
	"github.com/lechandonga/go-dag-orchestrator-go/engine"
)

func main() {
	dir := flag.String("dir", "data", "directory holding journal and marker files")
	slow := flag.Bool("slow", false, "make extract slow (for kill/restart demos)")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
		os.Exit(1)
	}

	store, err := engine.NewFileStore(filepath.Join(*dir, "demo.jsonl"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "store:", err)
		os.Exit(1)
	}
	defer store.Close()

	tasks := buildTasks(*dir, *slow)
	g := dag.New(tasks)

	eng, err := engine.New(g, engine.Config{
		MaxConcurrency: 2,
		Store:          store,
		LogSink:        os.Stdout,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snap, err := eng.Run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
	}

	fmt.Println("\n=== final snapshot ===")
	for _, t := range snap.Tasks {
		line := fmt.Sprintf("- %-8s status=%-9s attempts=%d", t.TaskID, t.Status, t.Attempts)
		if t.Reason != "" {
			line += " reason=" + t.Reason
		}
		if t.Message != "" {
			line += " (" + t.Message + ")"
		}
		fmt.Println(line)
	}
	if !snap.Success {
		os.Exit(2)
	}
}

// buildTasks constructs:
//
//	extract ─┬─> transform ─┬─> load
//	         └─> validate ──┘
//
// "transform" fails twice with a retriable error and then succeeds;
// "validate" has a tight timeout to exercise the timeout path once.
// Each successful task writes a marker file; on recovery an existing
// marker would indicate a re-run (it never happens with the engine).
func buildTasks(dir string, slow bool) []*dag.Task {
	marker := func(id string, f func(dag.Context) error) dag.Func {
		path := filepath.Join(dir, "marker-"+id)
		return func(ctx dag.Context) error {
			if data, err := os.ReadFile(path); err == nil {
				ctx.Logf("MARKER ALREADY EXISTS from previous run: %s (this task is being re-run!)",
					string(data))
			}
			if err := f(ctx); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return os.WriteFile(path,
				[]byte(fmt.Sprintf("attempt=%d at=%s", ctx.Attempt(), time.Now().Format(time.RFC3339))),
				0o644)
		}
	}

	return []*dag.Task{
		{
			ID: "extract",
			Fn: marker("extract", func(ctx dag.Context) error {
				if slow {
					select {
					case <-time.After(3 * time.Second):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}),
			Timeout: 2 * time.Second,
			Retry:   dag.RetryPolicy{MaxAttempts: 1},
		},
		{
			ID:   "transform",
			Deps: []string{"extract"},
			Fn: marker("transform", func(ctx dag.Context) error {
				if ctx.Attempt() < 3 {
					return dag.NewRetriableError("simulated flaky service")
				}
				return nil
			}),
			Timeout: 2 * time.Second,
			Retry:   dag.RetryPolicy{MaxAttempts: 4, Backoff: 10 * time.Millisecond},
		},
		{
			ID:   "validate",
			Deps: []string{"extract"},
			Fn: marker("validate", func(ctx dag.Context) error {
				if ctx.Attempt() == 1 {
					select {
					case <-time.After(150 * time.Millisecond): // exceeds timeout
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}),
			Timeout: 50 * time.Millisecond,
			Retry:   dag.RetryPolicy{MaxAttempts: 2, Backoff: 10 * time.Millisecond},
		},
		{
			ID:      "load",
			Deps:    []string{"transform", "validate"},
			Fn:      marker("load", func(dag.Context) error { return nil }),
			Timeout: 2 * time.Second,
			Retry:   dag.RetryPolicy{MaxAttempts: 1},
		},
	}
}
