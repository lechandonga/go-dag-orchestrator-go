package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

// emitEvents durably appends events and folds them into state under the
// engine write lock, guaranteeing journal and projection never diverge.
// Journal writes intentionally ignore the caller's cancellation: a
// terminal transition that has already been decided must be durable even
// while the run is shutting down.
func (e *Engine) emitEvents(_ context.Context, evs ...Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.cfg.Store.Append(context.Background(), evs...); err != nil {
		return err
	}
	for _, ev := range evs {
		e.st.applyEvent(ev)
	}
	e.events = append(e.events, evs...)
	return nil
}

// classifyOutcome turns a task error into (reason, retriable).
// External cancellation always wins over timeout.
func classifyOutcome(parent context.Context, timedOut bool, err error) (dag.Reason, bool) {
	if parent.Err() != nil {
		return dag.ReasonCanceled, false
	}
	if timedOut {
		return dag.ReasonTimeout, true
	}
	var te *dag.TaskError
	if errors.As(err, &te) {
		if te.Retriable {
			if te.Reason != dag.ReasonNone {
				return te.Reason, true
			}
			return dag.ReasonTaskFailed, true
		}
		return dag.ReasonTerminalFailure, false
	}
	return dag.ReasonTaskFailed, true
}

// executeTask runs the attempt loop for one task. It blocks until the
// task reaches a terminal state, the parent context is canceled or a
// journal write fails. Every state transition is persisted before the
// next step, so crashes leave a fully reconstructable trail.
func (e *Engine) executeTask(parent context.Context, task *dag.Task, firstAttempt int) error {
	maxAttempts := task.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	attempt := firstAttempt
	var lastMsg string

	for {
		e.logger.Logf("[task=%s attempt=%d] start (max=%d timeout=%s deps=%v)",
			task.ID, attempt, maxAttempts, task.Timeout, task.Deps)

		startedAt := e.cfg.Clock.Now()
		if err := e.emitEvents(parent, Event{
			Type:    EventTaskStarted,
			RunID:   e.runID,
			TaskID:  task.ID,
			Attempt: attempt,
			Time:    startedAt,
		}); err != nil {
			return err
		}

		err, timedOut := e.runOnce(parent, task, attempt)

		if err == nil {
			e.logger.Logf("[task=%s attempt=%d] success", task.ID, attempt)
			return e.emitEvents(parent, Event{
				Type:    EventTaskFinished,
				RunID:   e.runID,
				TaskID:  task.ID,
				Attempt: attempt,
				Status:  StatusSucceeded,
				Time:    e.cfg.Clock.Now(),
			})
		}

		reason, retriable := classifyOutcome(parent, timedOut, err)
		lastMsg = err.Error()
		e.logger.Logf("[task=%s attempt=%d] failed reason=%s retriable=%t: %s",
			task.ID, attempt, reason, retriable, err.Error())

		canRetry := retriable && attempt < maxAttempts
		// External cancellation is never retried even if policy allows.
		if parent.Err() != nil {
			canRetry = false
		}
		if canRetry {
			next := attempt + 1
			if err := e.emitEvents(parent, Event{
				Type:    EventTaskRetry,
				RunID:   e.runID,
				TaskID:  task.ID,
				Attempt: next,
				Reason:  string(reason),
				Message: lastMsg,
				Time:    e.cfg.Clock.Now(),
			}); err != nil {
				return err
			}
			e.logger.Logf("[task=%s] retry %d/%d after %s (last_reason=%s)",
				task.ID, next, maxAttempts, task.Retry.Backoff, reason)
			if berr := e.cfg.Clock.Sleep(parent, task.Retry.Backoff); berr != nil {
				// Interrupted during backoff: surface as canceled failure
				// so the attempt is recorded terminal on shutdown.
				return e.failTask(parent, task, attempt, dag.ReasonCanceled,
					"canceled while waiting to retry", false)
			}
			attempt = next
			continue
		}

		finalReason := reason
		if retriable && attempt >= maxAttempts {
			finalReason = dag.ReasonRetriesExhausted
		}
		return e.failTask(parent, task, attempt, finalReason, lastMsg, retriable)
	}
}

// failTask persists the terminal task_finished(failed) event.
func (e *Engine) failTask(parent context.Context, task *dag.Task, attempt int,
	reason dag.Reason, msg string, _ bool) error {
	return e.emitEvents(parent, Event{
		Type:    EventTaskFinished,
		RunID:   e.runID,
		TaskID:  task.ID,
		Attempt: attempt,
		Status:  StatusFailed,
		Reason:  string(reason),
		Message: msg,
		Time:    e.cfg.Clock.Now(),
	})
}

// runOnce performs exactly one attempt with an optional timeout. It
// always waits for the task function to return, so no task goroutine
// outlives the run.
func (e *Engine) runOnce(parent context.Context, task *dag.Task, attempt int) (err error, timedOut bool) {
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	var deadline <-chan time.Time
	if task.Timeout > 0 {
		t := time.NewTimer(task.Timeout)
		defer t.Stop()
		deadline = t.C
	}

	tc := &taskContext{
		ctx:     runCtx,
		taskID:  task.ID,
		attempt: attempt,
		engine:  e,
	}

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("%w: %v", dag.NewTerminalError("task panicked"), r)
			}
		}()
		done <- task.Fn(tc)
	}()

	select {
	case <-parent.Done():
		cancel()
		<-done // let the function observe cancellation and return
		return parent.Err(), false
	case <-deadline:
		cancel()
		<-done // join before retrying so attempts cannot overlap
		// A deadline that coincides with run cancellation loses: the
		// run is over and no retry must happen.
		if parent.Err() != nil {
			return parent.Err(), false
		}
		return context.DeadlineExceeded, true
	case err := <-done:
		// Cancellation wins even if the function returned at the same
		// instant: a canceled run never records a late success/failure
		// with retry semantics.
		if parent.Err() != nil {
			cancel()
			return parent.Err(), false
		}
		return err, false
	}
}

// taskContext adapts the engine to the dag.Context interface.
type taskContext struct {
	ctx     context.Context
	taskID  string
	attempt int
	engine  *Engine
}

func (c *taskContext) TaskID() string { return c.taskID }
func (c *taskContext) Attempt() int   { return c.attempt }

func (c *taskContext) Logf(format string, args ...any) {
	c.engine.logger.Logf("[task=%s attempt=%d] "+format,
		append([]any{c.taskID, c.attempt}, args...)...)
}

// Deadline/Value/Done support lets task bodies treat the dag.Context as
// a plain context when they need cancellation.
func (c *taskContext) Done() <-chan struct{}       { return c.ctx.Done() }
func (c *taskContext) Err() error                  { return c.ctx.Err() }
func (c *taskContext) Value(key any) any           { return c.ctx.Value(key) }
func (c *taskContext) Deadline() (time.Time, bool) { return c.ctx.Deadline() }
