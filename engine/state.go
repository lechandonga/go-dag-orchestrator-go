package engine

import (
	"sort"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

// runState is the mutable internal projection of the event journal.
// Every mutation happens exclusively via applyEvent, so replaying the
// journal always reconstructs exactly the same state (determinism and
// idempotence across repeated recoveries).
type runState struct {
	runID    string
	seq      int64
	started  bool
	finished bool
	success  bool
	tasks    map[string]*TaskState
	// inflight tracks tasks whose latest event is task_started, i.e.
	// attempts interrupted by a crash. Only meaningful in-memory during
	// a live run; not derivable as a simple status.
	inflight map[string]bool
}

func newRunState(ids []string) *runState {
	st := &runState{
		tasks:    make(map[string]*TaskState, len(ids)),
		inflight: make(map[string]bool),
	}
	for _, id := range ids {
		st.tasks[id] = &TaskState{TaskID: id, Status: StatusPending}
	}
	return st
}

// applyEvent folds one journal event into the state. Unknown task ids
// are ignored defensively; unknown events are counted in seq only.
func (st *runState) applyEvent(ev Event) {
	st.seq = ev.Seq
	if st.runID == "" && ev.RunID != "" {
		st.runID = ev.RunID
	}
	switch ev.Type {
	case EventRunStarted:
		st.started = true
	case EventRunFinished:
		st.finished = true
		st.success = ev.Status == StatusSucceeded
	case EventTaskStarted:
		if t := st.tasks[ev.TaskID]; t != nil {
			if t.StartedAt.IsZero() {
				t.StartedAt = ev.Time
			}
			t.Status = StatusRunning
			if ev.Attempt > t.Attempts {
				t.Attempts = ev.Attempt
			}
			st.inflight[ev.TaskID] = true
		}
	case EventTaskRetry:
		if t := st.tasks[ev.TaskID]; t != nil {
			if ev.Attempt > t.Attempts {
				t.Attempts = ev.Attempt
			}
			// A retry record means the next attempt is authorized but
			// has not started yet. Keep status Running with no inflight
			// marker: the scheduler resumes at the recorded attempt.
			t.Status = StatusRunning
			delete(st.inflight, ev.TaskID)
			t.Reason = ev.Reason
			t.Message = ev.Message
		}
	case EventTaskFinished:
		if t := st.tasks[ev.TaskID]; t != nil {
			t.Status = ev.Status
			t.Attempts = ev.Attempt
			t.Reason = ev.Reason
			t.Message = ev.Message
			t.FinishedAt = ev.Time
			delete(st.inflight, ev.TaskID)
		}
	case EventTaskSkipped:
		if t := st.tasks[ev.TaskID]; t != nil {
			t.Status = StatusSkipped
			t.Reason = ev.Reason
			t.Message = ev.Message
			t.FinishedAt = ev.Time
			delete(st.inflight, ev.TaskID)
		}
	}
}

// replay rebuilds state from events. The supplied state is reset.
func replay(g *dag.DAG, events []Event) *runState {
	st := newRunState(g.TaskIDs())
	for _, ev := range events {
		st.applyEvent(ev)
	}
	return st
}

// snapshot produces an immutable, deeply-copied view of the run.
func (st *runState) snapshot() *RunSnapshot {
	tasks := make([]TaskState, 0, len(st.tasks))
	for _, t := range st.tasks {
		cp := *t
		tasks = append(tasks, cp)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
	return &RunSnapshot{
		RunID:    st.runID,
		Tasks:    tasks,
		Seq:      st.seq,
		Finished: st.finished,
		Success:  st.success,
	}
}

// terminal reports whether the task will never change state again.
func taskTerminal(t *TaskState) bool {
	switch t.Status {
	case StatusSucceeded, StatusFailed, StatusSkipped:
		return true
	default:
		return false
	}
}
