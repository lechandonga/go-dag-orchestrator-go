package dag

import "time"

// EventType 是运行过程中记录的事件类型。
type EventType string

const (
	EventRunStarted    EventType = "run_started"
	EventTaskAttempt   EventType = "task_attempt"   // 一次尝试开始
	EventTaskSucceeded EventType = "task_succeeded" // 任务成功（终态）
	EventTaskFailed    EventType = "task_failed"    // 任务终止性失败（终态）
	EventTaskRetrying  EventType = "task_retrying"  // 一次尝试失败，将重试
	EventTaskSkipped   EventType = "task_skipped"   // 因上游失败被跳过（终态）
	EventTaskCanceled  EventType = "task_canceled"  // 任务被取消（终态）
	EventRunFinished   EventType = "run_finished"   // 运行结束（终态）
)

// Event 是追加到日志中的一条不可变记录。
type Event struct {
	Seq       int64       `json:"seq"` // 单调递增序号，重放时据此去重/排序
	RunID     string      `json:"run_id"`
	Type      EventType   `json:"type"`
	TaskID    string      `json:"task_id,omitempty"`
	Attempt   int         `json:"attempt,omitempty"`
	Failure   FailureKind `json:"failure,omitempty"`
	Err       string      `json:"err,omitempty"`
	RunStatus RunStatus   `json:"run_status,omitempty"`
	Tasks     []string    `json:"tasks,omitempty"` // run_started 携带：本次运行的全部任务 ID
	Time      time.Time   `json:"time"`
}
