package dag

import "sync"

// runState 是一次运行的内存状态，由事件流重放构建，保证一致性。
type runState struct {
	mu     sync.Mutex
	runID  string
	status RunStatus
	tasks  map[string]*TaskSnapshot
	order  []string // 任务登记顺序（spec 顺序），保证调度确定性
	seq    int64
}

func newRunState(runID string, spec Spec) *runState {
	st := &runState{
		runID:  runID,
		status: "",
		tasks:  map[string]*TaskSnapshot{},
	}
	for _, t := range spec.Tasks {
		st.tasks[t.ID] = &TaskSnapshot{ID: t.ID, Status: StatusPending}
		st.order = append(st.order, t.ID)
	}
	return st
}

// apply 按事件推进状态（加锁版本，用于重放）。
func (st *runState) apply(ev Event) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.applyLocked(ev)
}

// applyLocked 调用方须持有 st.mu。
func (st *runState) applyLocked(ev Event) {
	if ev.Seq > st.seq {
		st.seq = ev.Seq
	}
	ts := st.ensureTask(ev.TaskID)
	switch ev.Type {
	case EventRunStarted:
		for _, id := range ev.Tasks {
			st.ensureTask(id)
		}
	case EventTaskAttempt:
		ts.Status = StatusRunning
		if ev.Attempt > ts.Attempts {
			ts.Attempts = ev.Attempt
		}
		ts.StartedAt = ev.Time
	case EventTaskRetrying:
		// 保持 running：任务 goroutine 仍持有该任务（退避中），
		// 调度器不得再次派发，否则会并发重复执行同一任务。
		ts.Status = StatusRunning
		ts.Failure = ev.Failure
		ts.Err = ev.Err
		ts.failures++
	case EventTaskSucceeded:
		ts.Status = StatusSucceeded
		ts.Failure = FailureNone
		ts.Err = ""
		ts.FinishedAt = ev.Time
	case EventTaskFailed:
		ts.Status = StatusFailed
		ts.Failure = ev.Failure
		ts.Err = ev.Err
		ts.FinishedAt = ev.Time
		ts.failures++
	case EventTaskSkipped:
		ts.Status = StatusSkipped
		ts.Failure = ev.Failure
		ts.Err = ev.Err
		ts.FinishedAt = ev.Time
	case EventTaskCanceled:
		ts.Status = StatusCanceled
		ts.Failure = ev.Failure
		ts.Err = ev.Err
		ts.FinishedAt = ev.Time
	case EventRunFinished:
		st.status = ev.RunStatus
	}
}

func (st *runState) ensureTask(id string) *TaskSnapshot {
	if id == "" {
		return &TaskSnapshot{}
	}
	ts, ok := st.tasks[id]
	if !ok {
		ts = &TaskSnapshot{ID: id, Status: StatusPending}
		st.tasks[id] = ts
		st.order = append(st.order, id)
	}
	return ts
}

// snapshot 返回当前状态的一致深拷贝。
func (st *runState) snapshot() Snapshot {
	st.mu.Lock()
	defer st.mu.Unlock()
	tasks := make(map[string]TaskSnapshot, len(st.tasks))
	for id, ts := range st.tasks {
		tasks[id] = *ts
	}
	status := st.status
	if status == "" {
		status = "running"
	}
	return Snapshot{RunID: st.runID, Status: status, Tasks: tasks}
}

// resetNonTerminal 把中断时处于非终态的任务重置为 pending，供恢复后继续调度。
// 已完成的（succeeded/failed/skipped）保持不变，绝不重跑。
// canceled 视为“被中断而未完成”，同样重置为 pending 以支持断点续跑。
func (st *runState) resetNonTerminal() {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, ts := range st.tasks {
		switch ts.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped:
		default:
			ts.Status = StatusPending
		}
	}
}

func (st *runState) getStatus(id string) TaskStatus {
	st.mu.Lock()
	defer st.mu.Unlock()
	if ts, ok := st.tasks[id]; ok {
		return ts.Status
	}
	return StatusPending
}

func (st *runState) setStatus(id string, s TaskStatus) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.ensureTask(id).Status = s
}

func (st *runState) attempts(id string) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	if ts, ok := st.tasks[id]; ok {
		return ts.Attempts
	}
	return 0
}

// failures 返回已计入重试预算的失败次数。
func (st *runState) failures(id string) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	if ts, ok := st.tasks[id]; ok {
		return ts.failures
	}
	return 0
}

// allTerminal 报告所有任务是否都已到达终态。
func (st *runState) allTerminal() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, ts := range st.tasks {
		switch ts.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped, StatusCanceled:
		default:
			return false
		}
	}
	return true
}

// nextReady 按 spec 顺序找到第一个上游全部成功的 pending 任务。
// 调用方负责随后 setStatus(id, StatusRunning)。
func (st *runState) nextReady(tasks map[string]Task) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, id := range st.order {
		ts := st.tasks[id]
		if ts.Status != StatusPending {
			continue
		}
		ready := true
		for _, dep := range tasks[id].Deps {
			if st.tasks[dep].Status != StatusSucceeded {
				ready = false
				break
			}
		}
		if ready {
			return id
		}
	}
	return ""
}
