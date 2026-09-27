package dag

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Options 是引擎的可配置项。
type Options struct {
	Concurrency int         // 并发上限，<=0 表示不限制
	Logger      *log.Logger // 为 nil 时使用默认 logger（输出到 stderr）
}

// ErrRunInProgress 表示同一 runID 已有正在执行的运行。
var ErrRunInProgress = errors.New("dag: run already in progress")

// Engine 引擎：按依赖调度任务，记录事件，支持恢复。
type Engine struct {
	store Store
	opts  Options
	log   *log.Logger

	mu   sync.Mutex
	live map[string]bool // 正在执行的 runID
}

// NewEngine 创建引擎。store 为 nil 时使用内存存储（不持久化）。
func NewEngine(store Store, opts Options) *Engine {
	if store == nil {
		store = NewMemStore()
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[dag] ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Engine{store: store, opts: opts, log: logger, live: map[string]bool{}}
}

// Run 执行一次编排。
// runID 相同且 store 中已有事件时，按恢复语义继续：已完成任务不重跑，
// 中断时处于运行中的任务按剩余重试次数重新执行。
// 返回最终一致快照；编排定义非法时 err 为 *ValidationError。
func (e *Engine) Run(ctx context.Context, runID string, spec Spec) (Snapshot, error) {
	e.mu.Lock()
	if e.live[runID] {
		e.mu.Unlock()
		return Snapshot{}, ErrRunInProgress
	}
	e.live[runID] = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.live, runID)
		e.mu.Unlock()
	}()

	e.log.Printf("run=%s spec=%q tasks=%d concurrency=%d", runID, spec.Name, len(spec.Tasks), e.opts.Concurrency)

	// 1. 校验编排定义
	if err := Validate(spec); err != nil {
		e.log.Printf("run=%s validation failed: %v", runID, err)
		return Snapshot{RunID: runID, Status: RunInvalid, Tasks: map[string]TaskSnapshot{}}, err
	}

	// 2. 重放历史事件，构建恢复状态
	st := newRunState(runID, spec)
	evs, err := e.store.Load(runID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load events: %w", err)
	}
	for _, ev := range evs {
		st.apply(ev)
	}
	if len(evs) > 0 {
		e.log.Printf("run=%s recovered %d events, status=%s", runID, len(evs), st.status)
	}
	// 中断时处于非终态的任务（如 running）重置为 pending，按剩余重试次数继续
	st.resetNonTerminal()
	if st.status == RunSucceeded || st.status == RunFailed {
		// 幂等：运行已结束，直接返回重放结果，不重跑任何任务。
		// 注意：RunCanceled 不算结束——被中断的运行可通过相同 runID 恢复继续。
		e.log.Printf("run=%s already finished with %s, skip re-run", runID, st.status)
		return st.snapshot(), nil
	}

	// 3. 记录运行开始（携带任务清单，保证重放/快照能看到全部任务）
	e.emit(st, Event{Type: EventRunStarted, Tasks: append([]string(nil), st.order...)})

	// 4. 调度循环
	e.schedule(ctx, st, spec)

	// 5. 记录运行结束
	final := RunSucceeded
	for _, t := range st.tasks {
		switch t.Status {
		case StatusFailed, StatusSkipped:
			final = RunFailed
		case StatusCanceled:
			final = RunCanceled
		}
	}
	if ctx.Err() != nil && final == RunSucceeded {
		final = RunCanceled
	}
	e.emit(st, Event{Type: EventRunFinished, RunStatus: final})
	e.log.Printf("run=%s finished status=%s", runID, final)
	return st.snapshot(), nil
}

// Snapshot 返回指定运行当前的一致快照（可并发调用）。
// 通过重放事件流构建，保证不丢事件、不重复计数。
func (e *Engine) Snapshot(runID string) (Snapshot, bool) {
	evs, err := e.store.Load(runID)
	if err != nil || len(evs) == 0 {
		return Snapshot{}, false
	}
	st := newRunState(runID, Spec{})
	for _, ev := range evs {
		st.apply(ev)
	}
	return st.snapshot(), true
}

// emit 追加事件到存储并应用到内存状态，两者在同一把锁下完成，保证一致。
func (e *Engine) emit(st *runState, ev Event) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.seq++
	ev.Seq = st.seq
	ev.RunID = st.runID
	ev.Time = time.Now()
	if err := e.store.Append(ev); err != nil {
		e.log.Printf("run=%s store append failed seq=%d type=%s: %v", st.runID, ev.Seq, ev.Type, err)
	}
	st.applyLocked(ev)
}

// ---- 调度器 ----

type taskResult struct {
	id     string
	status TaskStatus
}

func (e *Engine) schedule(ctx context.Context, st *runState, spec Spec) {
	tasks := make(map[string]Task, len(spec.Tasks))
	for _, t := range spec.Tasks {
		tasks[t.ID] = t
	}
	conc := e.opts.Concurrency
	if conc <= 0 {
		conc = len(spec.Tasks) + 1
	}
	sem := make(chan struct{}, conc)
	results := make(chan taskResult, len(spec.Tasks))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	running := 0
	for {
		// 1. 传播跳过：上游未成功的任务标记为 skipped
		for e.propagateSkips(st, tasks) {
		}
		// 2. 全部终态则结束
		if st.allTerminal() {
			return
		}
		// 3. 取消：不再派发新任务，等待在途任务结束
		if ctx.Err() != nil {
			cancel()
			e.cancelPending(st, tasks)
			if running == 0 {
				return
			}
		} else {
			// 4. 派发就绪任务（受并发上限约束）
			for running < conc {
				id := st.nextReady(tasks)
				if id == "" {
					break
				}
				sem <- struct{}{}
				running++
				st.setStatus(id, StatusRunning)
				go e.executeTask(runCtx, st, tasks[id], results)
			}
			if running == 0 {
				// 没有可派发也没有在途任务，但非全终态：
				// 剩余任务只能是被跳过传播遗漏的，再传播一次后退出
				if !e.propagateSkips(st, tasks) {
					return
				}
				continue
			}
		}
		// 5. 等待一个任务结束或取消信号。
		// 已取消时只等在途任务结束，避免对已关闭的 Done 通道空转。
		if ctx.Err() != nil {
			<-results
			<-sem
			running--
			continue
		}
		select {
		case <-results:
			<-sem
			running--
		case <-ctx.Done():
			// 下一轮循环处理取消
		}
	}
}

// propagateSkips 把上游存在失败/跳过/取消的 pending 任务标记为 skipped。
// 返回本轮是否有新的跳过。
func (e *Engine) propagateSkips(st *runState, tasks map[string]Task) bool {
	changed := false
	for id, t := range tasks {
		if st.getStatus(id) != StatusPending {
			continue
		}
		blocked := false
		for _, dep := range t.Deps {
			switch st.getStatus(dep) {
			case StatusFailed, StatusSkipped, StatusCanceled:
				blocked = true
			}
		}
		if blocked {
			e.emit(st, Event{Type: EventTaskSkipped, TaskID: id, Failure: FailureUpstream,
				Err: "upstream task did not succeed"})
			e.log.Printf("run=%s task=%s skipped: upstream not succeeded", st.runID, id)
			changed = true
		}
	}
	return changed
}

// cancelPending 把所有非终态任务标记为 canceled。
func (e *Engine) cancelPending(st *runState, tasks map[string]Task) {
	for id := range tasks {
		switch st.getStatus(id) {
		case StatusPending, StatusReady:
			e.emit(st, Event{Type: EventTaskCanceled, TaskID: id, Failure: FailureCanceled, Err: "run canceled"})
		}
	}
}

// executeTask 执行单个任务（含超时与重试），结果通过 results 回报。
// 重试预算按“失败次数”计算：被中断/取消的尝试不消耗预算，
// 因此恢复后任务总能继续，且崩溃前的失败次数不会丢失。
func (e *Engine) executeTask(ctx context.Context, st *runState, task Task, results chan<- taskResult) {
	attempt := st.attempts(task.ID) + 1
	failures := st.failures(task.ID)
	for {
		e.emit(st, Event{Type: EventTaskAttempt, TaskID: task.ID, Attempt: attempt})
		e.log.Printf("run=%s task=%s attempt=%d failures=%d/%d timeout=%s",
			st.runID, task.ID, attempt, failures, task.Retries, task.Timeout)

		err := e.runAttempt(ctx, task)

		if err == nil {
			e.emit(st, Event{Type: EventTaskSucceeded, TaskID: task.ID, Attempt: attempt})
			e.log.Printf("run=%s task=%s succeeded attempt=%d", st.runID, task.ID, attempt)
			results <- taskResult{id: task.ID, status: StatusSucceeded}
			return
		}

		kind := classify(err, ctx)
		if kind == FailureCanceled {
			e.emit(st, Event{Type: EventTaskCanceled, TaskID: task.ID, Attempt: attempt, Failure: kind, Err: err.Error()})
			e.log.Printf("run=%s task=%s canceled: %v", st.runID, task.ID, err)
			results <- taskResult{id: task.ID, status: StatusCanceled}
			return
		}
		failures++
		retryable := kind == FailureRetryable || kind == FailureTimeout
		if retryable && failures <= task.Retries {
			e.emit(st, Event{Type: EventTaskRetrying, TaskID: task.ID, Attempt: attempt, Failure: kind, Err: err.Error()})
			e.log.Printf("run=%s task=%s attempt=%d failed kind=%s retryable, will retry: %v",
				st.runID, task.ID, attempt, kind, err)
			if !sleepOrDone(ctx, task.Backoff) {
				e.emit(st, Event{Type: EventTaskCanceled, TaskID: task.ID, Attempt: attempt, Failure: FailureCanceled, Err: "run canceled during backoff"})
				results <- taskResult{id: task.ID, status: StatusCanceled}
				return
			}
			attempt++
			continue
		}
		e.emit(st, Event{Type: EventTaskFailed, TaskID: task.ID, Attempt: attempt, Failure: kind, Err: err.Error()})
		e.log.Printf("run=%s task=%s failed terminally kind=%s attempts=%d: %v",
			st.runID, task.ID, kind, attempt, err)
		results <- taskResult{id: task.ID, status: StatusFailed}
		return
	}
}

// runAttempt 执行单次尝试，应用任务超时。
func (e *Engine) runAttempt(ctx context.Context, task Task) error {
	attemptCtx := ctx
	cancel := func() {}
	if task.Timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, task.Timeout)
	}
	defer cancel()

	done := make(chan error, 1)
	tc := &taskContext{ctx: attemptCtx, log: e.log, taskID: task.ID}
	go func() {
		done <- task.Run(tc)
	}()
	select {
	case err := <-done:
		return err
	case <-attemptCtx.Done():
		if ctx.Err() != nil {
			return ctx.Err() // 运行级取消
		}
		return &timeoutError{d: task.Timeout}
	}
}

// classify 把尝试错误归类为失败性质。
func classify(err error, runCtx context.Context) FailureKind {
	if runCtx.Err() != nil || errors.Is(err, context.Canceled) {
		return FailureCanceled
	}
	var te *timeoutError
	if errors.As(err, &te) || errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout
	}
	var re *RetryableError
	if errors.As(err, &re) {
		return FailureRetryable
	}
	return FailureFatal
}

type timeoutError struct{ d time.Duration }

func (e *timeoutError) Error() string { return fmt.Sprintf("task timed out after %s", e.d) }

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// taskContext 实现传给任务执行体的 Context。
type taskContext struct {
	ctx    context.Context
	log    *log.Logger
	taskID string
}

func (c *taskContext) Done() <-chan struct{} { return c.ctx.Done() }
func (c *taskContext) Logf(format string, args ...any) {
	c.log.Printf("task=%s %s", c.taskID, fmt.Sprintf(format, args...))
}
