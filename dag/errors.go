// Package dag provides the static task-graph definition and validation
// primitives for the orchestration engine.
package dag

// Reason classifies why a task could not run or why a run ended.
// Values are stable and used by callers to distinguish failure modes
// (cycle vs missing dependency vs upstream failure, etc.).
type Reason string

const (
	ReasonNone              Reason = ""
	ReasonCycle             Reason = "cycle_detected"
	ReasonMissingDependency Reason = "missing_dependency"
	ReasonUpstreamFailed    Reason = "upstream_failed"
	ReasonTimeout           Reason = "task_timeout"
	ReasonRetriesExhausted  Reason = "retries_exhausted"
	ReasonTaskFailed        Reason = "task_failed"
	ReasonTerminalFailure   Reason = "terminal_failure"
	ReasonCanceled          Reason = "canceled"
)

// TaskError is the structured error returned for an unsuccessful task
// attempt handling. Retriable distinguishes transient failures (eligible
// for retry) from terminal failures (fail fast, skip downstream).
type TaskError struct {
	Reason    Reason
	Message   string
	Retriable bool
}

func (e *TaskError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Reason) + ": " + e.Message
}

// NewRetriableError builds a transient error that may be retried.
func NewRetriableError(msg string) *TaskError {
	return &TaskError{Reason: ReasonTaskFailed, Message: msg, Retriable: true}
}

// NewTerminalError builds a non-retriable error. Downstream tasks are
// blocked with ReasonUpstreamFailed.
func NewTerminalError(msg string) *TaskError {
	return &TaskError{Reason: ReasonTerminalFailure, Message: msg, Retriable: false}
}
