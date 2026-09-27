package dag

import "fmt"

// RetryableError 包装一个错误，表示该失败可重试。
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable 把 err 标记为可重试失败。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// ValidationError 描述编排定义中的问题，Kind 可区分环与缺失依赖。
type ValidationError struct {
	Kind    FailureKind // FailureValidation
	Detail  string
	TaskIDs []string // 涉及的任务（如环上的节点）
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid spec: %s: %v", e.Detail, e.TaskIDs)
}

// ErrCycle 在存在环依赖时返回（*ValidationError）。
// ErrMissingDep 在依赖不存在的任务时返回（*ValidationError）。
