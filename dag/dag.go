package dag

import (
	"fmt"
	"sort"
	"time"
)

// Func is the executable body of a task. A *TaskError with Retriable
// false is terminal; every other non-nil error is treated as retriable.
// The context is canceled on timeout, engine cancel or shutdown;
// implementations should respect it.
type Func func(ctx Context) error

// Context exposes logging and cancellation to task bodies.
type Context interface {
	TaskID() string
	Attempt() int
	Logf(format string, args ...any)
	// Done is closed when the task must stop (timeout / run canceled).
	Done() <-chan struct{}
	Err() error
}

// RetryPolicy describes how a task is retried after a retriable failure.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (>=1). Zero means 1.
	MaxAttempts int
	// Backoff is the fixed delay between attempts.
	Backoff time.Duration
}

// Task is a node definition in the workflow.
type Task struct {
	ID      string
	Deps    []string
	Fn      Func
	Timeout time.Duration // zero => no timeout (still responds to cancel)
	Retry   RetryPolicy
}

// DAG is an immutable, validated set of tasks.
type DAG struct {
	tasks map[string]*Task
	deps  map[string][]string // task -> direct dependencies
	next  map[string][]string // task -> direct dependents
	ids   []string            // sorted
}

// New builds a DAG from task definitions. Call Validate before running.
func New(tasks []*Task) *DAG {
	d := &DAG{
		tasks: make(map[string]*Task, len(tasks)),
		deps:  make(map[string][]string, len(tasks)),
		next:  make(map[string][]string, len(tasks)),
	}
	for _, t := range tasks {
		if t == nil {
			continue
		}
		d.tasks[t.ID] = t
		d.deps[t.ID] = append(d.deps[t.ID], t.Deps...)
	}
	for id := range d.tasks {
		d.ids = append(d.ids, id)
	}
	sort.Strings(d.ids)
	for _, id := range d.ids {
		sorted := append([]string(nil), d.deps[id]...)
		sort.Strings(sorted)
		d.deps[id] = sorted
		for _, dep := range sorted {
			d.next[dep] = append(d.next[dep], id)
		}
	}
	for id := range d.next {
		next := d.next[id]
		sort.Strings(next)
		d.next[id] = next
	}
	return d
}

// Validate checks the graph: empty/duplicate ids, missing dependencies
// and cycles. The returned *ValidationError carries a stable Reason.
func (d *DAG) Validate() error {
	if len(d.tasks) == 0 {
		return &ValidationError{Reason: ReasonMissingDependency, Message: "dag contains no tasks"}
	}
	// Missing dependency check (ids order is sorted so messages are stable).
	for _, id := range d.ids {
		for _, dep := range d.deps[id] {
			if _, ok := d.tasks[dep]; !ok {
				return &ValidationError{
					Reason:  ReasonMissingDependency,
					Message: fmt.Sprintf("task %q depends on unknown task %q", id, dep),
				}
			}
			if dep == id {
				return &ValidationError{
					Reason:  ReasonCycle,
					Message: fmt.Sprintf("task %q depends on itself", id),
					Cycle:   []string{id, id},
				}
			}
		}
	}
	if cyc := d.findCycle(); cyc != nil {
		return &ValidationError{
			Reason:  ReasonCycle,
			Message: fmt.Sprintf("dependency cycle: %v", cyc),
			Cycle:   cyc,
		}
	}
	return nil
}

// findCycle returns the ids of one cycle if present, else nil.
func (d *DAG) findCycle() []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(d.ids))
	var stack []string
	var dfs func(id string) []string
	dfs = func(id string) []string {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range d.deps[id] {
			switch color[dep] {
			case gray:
				// Extract the cycle from the stack.
				for i, n := range stack {
					if n == dep {
						return append(append([]string(nil), stack[i:]...), dep)
					}
				}
			case white:
				if cyc := dfs(dep); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return nil
	}
	for _, id := range d.ids {
		if color[id] == white {
			if cyc := dfs(id); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// ValidationError describes a structural problem with the graph.
type ValidationError struct {
	Reason  Reason
	Message string
	// Cycle holds the ids participating in a cycle when Reason is
	// ReasonCycle, otherwise nil.
	Cycle []string
}

func (e *ValidationError) Error() string {
	return string(e.Reason) + ": " + e.Message
}

// Task returns the task definition by id.
func (d *DAG) Task(id string) (*Task, bool) {
	t, ok := d.tasks[id]
	return t, ok
}

// TaskIDs returns all task ids in stable (sorted) order.
func (d *DAG) TaskIDs() []string {
	return append([]string(nil), d.ids...)
}

// Dependents returns the ids that directly depend on id (sorted).
func (d *DAG) Dependents(id string) []string {
	return append([]string(nil), d.next[id]...)
}

// Dependencies returns the direct dependencies of id (sorted).
func (d *DAG) Dependencies(id string) []string {
	return append([]string(nil), d.deps[id]...)
}

// Roots returns tasks with no dependencies (sorted).
func (d *DAG) Roots() []string {
	var roots []string
	for _, id := range d.ids {
		if len(d.deps[id]) == 0 {
			roots = append(roots, id)
		}
	}
	return roots
}
