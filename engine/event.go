// Package engine implements the runtime that schedules a validated DAG:
// dependency-aware dispatch, concurrency limiting, timeout/retry,
// failure propagation and crash recovery from an event journal.
package engine

import "time"

// Status is the externally visible lifecycle state of a task.
// Values are stable strings persisted in the journal.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusSkipped   Status = "skipped" // upstream failed/canceled; never ran
)

// EventType enumerates journal record kinds.
type EventType string

const (
	EventRunStarted   EventType = "run_started"
	EventTaskStarted  EventType = "task_started"
	EventTaskRetry    EventType = "task_retry"
	EventTaskFinished EventType = "task_finished"
	EventTaskSkipped  EventType = "task_skipped"
	EventRunFinished  EventType = "run_finished"
)

// Event is one durable journal record.
type Event struct {
	Seq     int64     `json:"seq"`
	Type    EventType `json:"type"`
	RunID   string    `json:"run_id"`
	TaskID  string    `json:"task_id,omitempty"`
	Attempt int       `json:"attempt,omitempty"`
	Status  Status    `json:"status,omitempty"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Time    time.Time `json:"time"`
}

// TaskState is an immutable snapshot of one task's state.
type TaskState struct {
	TaskID     string
	Status     Status
	Attempts   int
	Reason     string
	Message    string
	StartedAt  time.Time
	FinishedAt time.Time
}

// RunSnapshot is a consistent point-in-time view of the whole run.
type RunSnapshot struct {
	RunID    string
	Tasks    []TaskState // sorted by task id
	Seq      int64       // journal sequence the snapshot reflects
	Finished bool
	Success  bool
}
