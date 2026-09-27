// Package dag 实现一个本地可运行的 DAG 任务编排引擎。
package dag

import "time"

// TaskStatus 表示单个任务在一次运行中的稳定状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"   // 等待上游完成
	StatusReady     TaskStatus = "ready"     // 上游已全部成功，等待调度
	StatusRunning   TaskStatus = "running"   // 正在执行
	StatusSucceeded TaskStatus = "succeeded" // 执行成功
	StatusFailed    TaskStatus = "failed"    // 终止性失败（重试耗尽或不可重试）
	StatusSkipped   TaskStatus = "skipped"   // 因上游失败/取消而被阻断
	StatusCanceled  TaskStatus = "canceled"  // 运行被取消
)

// FailureKind 区分失败性质，决定是否可以重试。
type FailureKind string

const (
	FailureNone       FailureKind = ""           // 无失败
	FailureRetryable  FailureKind = "retryable"  // 可重试失败
	FailureFatal      FailureKind = "fatal"      // 终止性失败，不重试
	FailureTimeout    FailureKind = "timeout"    // 超时（按重试策略处理）
	FailureUpstream   FailureKind = "upstream"   // 上游失败导致被跳过
	FailureCanceled   FailureKind = "canceled"   // 运行被取消
	FailureValidation FailureKind = "validation" // 编排定义非法（环/缺失依赖）
)

// TaskFunc 是任务的执行体。返回的 error 决定失败性质：
// 返回 nil 表示成功；返回 *RetryableError 表示可重试失败；其他 error 视为终止性失败。
type TaskFunc func(ctx Context) error

// Context 是传给任务执行体的上下文，携带取消信号与日志能力。
type Context interface {
	Done() <-chan struct{}
	Logf(format string, args ...any)
}

// Task 描述编排中的一个节点。
type Task struct {
	ID      string        // 唯一标识
	Deps    []string      // 上游任务 ID
	Run     TaskFunc      // 执行体
	Timeout time.Duration // 单次尝试超时，0 表示不限制
	Retries int           // 失败后的最大重试次数（不含首次）
	Backoff time.Duration // 重试间隔，0 表示立即重试
}

// Spec 是一次编排定义。
type Spec struct {
	Name  string
	Tasks []Task
}

// TaskSnapshot 是某个任务对外可见的稳定状态。
type TaskSnapshot struct {
	ID         string
	Status     TaskStatus
	Attempts   int         // 已执行的尝试次数
	Failure    FailureKind // 失败性质（无失败时为空）
	Err        string      // 最近一次失败原因
	StartedAt  time.Time
	FinishedAt time.Time

	failures int // 已计入重试预算的失败次数（取消/中断不计入）
}

// RunStatus 是整次运行的判定结果。
type RunStatus string

const (
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"   // 有任务终止性失败
	RunCanceled  RunStatus = "canceled" // 运行被取消
	RunInvalid   RunStatus = "invalid"  // 编排定义非法
)

// Snapshot 是某次运行的一致快照。
type Snapshot struct {
	RunID  string
	Status RunStatus
	Tasks  map[string]TaskSnapshot
}
