package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lechandonga/go-dag-orchestrator-go/dag"
)

// Config controls engine-wide behavior.
type Config struct {
	// MaxConcurrency bounds simultaneously running tasks; <=0 => 1.
	MaxConcurrency int
	// Store is the durability backend. Required.
	Store Store
	// LogSink receives structured log lines. nil => discard.
	LogSink io.Writer
	// Clock is used for timestamps/backoff; nil => real clock.
	Clock Clock
	// RunID, when set, must match an existing journal (recovery) or is
	// used for a fresh run. Empty => generated for fresh runs.
	RunID string
}

// ErrRunInProgress is returned when Run is called concurrently.
var ErrRunInProgress = errors.New("run already in progress")

// ErrRunIDMismatch means the configured RunID does not match the journal.
var ErrRunIDMismatch = errors.New("run id mismatch with existing journal")

// Engine runs one DAG definition against one durable run journal.
// Snapshot/Events are safe to call concurrently with Run.
type Engine struct {
	cfg    Config
	g      *dag.DAG
	runID  string
	logger *logger

	mu     sync.RWMutex
	st     *runState
	events []Event

	runMu   sync.Mutex
	running bool
}

// New constructs an engine, validates the graph and replays any journal
// found in the store, so Snapshot is usable before Run.
func New(g *dag.DAG, cfg Config) (*Engine, error) {
	if g == nil {
		return nil, errors.New("nil dag")
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	if cfg.Store == nil {
		return nil, errors.New("engine config requires a Store")
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}

	stored, err := cfg.Store.LoadAll(context.Background())
	if err != nil && !errors.Is(err, ErrNoJournal) {
		return nil, fmt.Errorf("load journal: %w", err)
	}
	st := replay(g, stored)

	runID := cfg.RunID
	if len(stored) > 0 {
		if cfg.RunID != "" && cfg.RunID != st.runID {
			return nil, fmt.Errorf("%w: config=%q journal=%q",
				ErrRunIDMismatch, cfg.RunID, st.runID)
		}
		runID = st.runID
	} else if runID == "" {
		runID = generateRunID()
	}

	return &Engine{
		cfg:    cfg,
		g:      g,
		runID:  runID,
		logger: newLogger(cfg.LogSink, cfg.Clock),
		st:     st,
		events: stored,
	}, nil
}

// Run executes (or resumes) the workflow. After a completed run it is a
// idempotent no-op; concurrent invocation returns ErrRunInProgress.
func (e *Engine) Run(ctx context.Context) (*RunSnapshot, error) {
	e.runMu.Lock()
	if e.running {
		e.runMu.Unlock()
		return nil, ErrRunInProgress
	}
	e.running = true
	defer func() {
		e.runMu.Lock()
		e.running = false
		e.runMu.Unlock()
	}()
	e.runMu.Unlock()

	e.mu.RLock()
	finished := e.st.finished
	e.mu.RUnlock()
	if finished {
		e.logger.Logf("[run=%s] already finished; resume is a no-op", e.runID)
		return e.Snapshot(), nil
	}

	// Fresh run: persist run_started before any task can run.
	e.mu.RLock()
	started := e.st.started
	e.mu.RUnlock()
	if !started {
		if err := e.emitEvents(ctx, Event{
			Type:   EventRunStarted,
			RunID:  e.runID,
			Status: StatusRunning,
			Time:   e.cfg.Clock.Now(),
		}); err != nil {
			return nil, fmt.Errorf("persist run_started: %w", err)
		}
		e.logger.Logf("[run=%s] started tasks=%d concurrency=%d",
			e.runID, len(e.g.TaskIDs()), e.cfg.MaxConcurrency)
	} else {
		e.logger.Logf("[run=%s] recovered journal seq=%d; resuming",
			e.runID, e.st.seq)
	}

	// Recovery fixpoint: a crash may have happened between a task failure
	// and downstream skip propagation. Emit deterministic skips before
	// (re)starting anything.
	if err := e.propagateSkips(ctx); err != nil {
		return nil, err
	}

	snap, err := e.schedule(ctx)
	if err != nil {
		return snap, err
	}
	return snap, nil
}

// schedule is the coordinator loop. Exactly one goroutine mutates the
// run state here (via events); workers only execute task bodies.
func (e *Engine) schedule(ctx context.Context) (*RunSnapshot, error) {
	type completion struct {
		id  string
		err error
	}
	doneCh := make(chan completion, len(e.g.TaskIDs()))
	dispatched := make(map[string]bool)
	var workers sync.WaitGroup

	launch := func(id string, attempt int) {
		dispatched[id] = true
		task, _ := e.g.Task(id)
		e.logger.Logf("[scheduler] dispatch task=%s attempt=%d inflight=%d/%d",
			id, attempt, len(dispatched), e.cfg.MaxConcurrency)
		workers.Add(1)
		go func() {
			defer workers.Done()
			perr := e.executeTask(ctx, task, attempt)
			doneCh <- completion{id: id, err: perr}
		}()
	}

	var fatal error
loop:
	for {
		if fatal != nil {
			break loop
		}
		// Emit any skips unlocked-propagation now (failure/cancel driven).
		if err := e.propagateSkips(ctx); err != nil {
			fatal = err
			break loop
		}

		e.mu.Lock()
		// Plan a scheduling wave.
		type plan struct {
			id      string
			attempt int
			resume  bool
		}
		var plans []plan
		inflight := 0
		for _, id := range e.g.TaskIDs() {
			if dispatched[id] {
				inflight++
				continue
			}
			t := e.st.tasks[id]
			// Running at the start of a scheduling wave means the
			// attempt was authorized but is not owned by this process
			// (left behind by a crash). Live, in-process attempts are
			// excluded by the dispatched set above.
			resume := t.Status == StatusRunning
			if t.Status != StatusPending && !resume {
				continue // terminal
			}
			blocked, failedDep := e.depsStatusLocked(id)
			if failedDep != "" {
				continue // propagateSkips handles it
			}
			if blocked {
				continue
			}
			if inflight+len(plans) >= e.cfg.MaxConcurrency {
				continue
			}
			plans = append(plans, plan{id: id, attempt: t.Attempts, resume: resume})
		}
		pendingCount := 0
		for _, id := range e.g.TaskIDs() {
			t := e.st.tasks[id]
			if !taskTerminal(t) {
				pendingCount++
			}
		}
		e.mu.Unlock()

		for _, p := range plans {
			attempt := 1
			if p.resume {
				// Resume at the attempt number recorded in the journal.
				// An attempt killed before any verdict is replayed at
				// its own number, so interruptions never consume a
				// retry; a task_retry record already advanced the number.
				attempt = p.attempt
				if attempt < 1 {
					attempt = 1
				}
			}
			launch(p.id, attempt)
		}

		// All terminal?
		e.mu.RLock()
		allDone := true
		for _, id := range e.g.TaskIDs() {
			if !taskTerminal(e.st.tasks[id]) {
				allDone = false
				break
			}
		}
		e.mu.RUnlock()
		if allDone {
			break loop
		}

		select {
		case c := <-doneCh:
			delete(dispatched, c.id)
			if c.err != nil {
				fatal = c.err
			}
		case <-ctx.Done():
			// Stop launching new waves; wait for inflight tasks to
			// acknowledge cancellation. The completion channel is sized
			// to the total task count, so workers never block sending.
			e.logger.Logf("[scheduler] run context canceled: %v; draining %d inflight task(s)",
				ctx.Err(), len(dispatched))
			workers.Wait()
			break loop
		}
	}

	workers.Wait()
	// Collect buffered completions for error reporting.
	for {
		select {
		case c := <-doneCh:
			delete(dispatched, c.id)
			if c.err != nil && fatal == nil {
				fatal = c.err
			}
		default:
			goto drained
		}
	}
drained:

	// Final fixpoint pass (handles everything not yet skipped), best
	// effort even when ctx is canceled.
	if perr := e.propagateSkips(context.Background()); perr != nil {
		return e.Snapshot(), perr
	}

	snap := e.Snapshot()
	if !snap.Finished {
		success := true
		for _, t := range snap.Tasks {
			if t.Status != StatusSucceeded {
				success = false
			}
		}
		reason := ""
		status := StatusSucceeded
		if !success {
			status = StatusFailed
			if ctx.Err() != nil && !e.anyFailed(snap) {
				reason = string(dag.ReasonCanceled)
			}
		}
		fin := Event{
			Type:   EventRunFinished,
			RunID:  e.runID,
			Status: status,
			Reason: reason,
			Time:   e.cfg.Clock.Now(),
		}
		// run_finished must persist even on cancel; use background ctx.
		if err := e.emitEvents(context.Background(), fin); err != nil {
			return e.Snapshot(), err
		}
		e.logger.Logf("[run=%s] finished success=%t tasks=%d",
			e.runID, success, len(snap.Tasks))
		snap = e.Snapshot()
	}
	if fatal != nil {
		return snap, fmt.Errorf("run aborted: %w", fatal)
	}
	return snap, nil
}

// depsStatusLocked reports dependency gating for a task (caller holds
// lock). blocked=true means deps are still pending/running; failedDep
// names a failed/skipped dependency when the task must be skipped.
func (e *Engine) depsStatusLocked(id string) (blocked bool, failedDep string) {
	for _, dep := range e.g.Dependencies(id) {
		switch e.st.tasks[dep].Status {
		case StatusSucceeded:
		case StatusPending, StatusRunning:
			blocked = true
		case StatusFailed, StatusSkipped:
			return false, dep
		}
	}
	return blocked, ""
}

// propagateSkips emits task_skipped events for every non-terminal,
// non-inflight task that cannot run anymore: an upstream is terminal
// non-success, or the run context was canceled before it started.
// Inflight (interrupted-on-crash) tasks are resumed, not skipped.
func (e *Engine) propagateSkips(ctx context.Context) error {
	for {
		var events []Event
		e.mu.Lock()
		canceled := ctx.Err() != nil
		for _, id := range e.g.TaskIDs() {
			t := e.st.tasks[id]
			if t.Status != StatusPending {
				continue
			}
			blocked, failedDep := e.depsStatusLocked(id)
			switch {
			case failedDep != "":
				dep := e.st.tasks[failedDep]
				// Attribute the skip to cancellation when the upstream
				// itself was a casualty of cancellation; otherwise the
				// downstream was blocked by a genuine upstream failure.
				reason := dag.ReasonUpstreamFailed
				if dep.Reason == string(dag.ReasonCanceled) {
					reason = dag.ReasonCanceled
				}
				events = append(events, Event{
					Type:    EventTaskSkipped,
					RunID:   e.runID,
					TaskID:  id,
					Status:  StatusSkipped,
					Reason:  string(reason),
					Message: fmt.Sprintf("upstream %q did not succeed (%s)", failedDep, dep.Reason),
					Time:    e.cfg.Clock.Now(),
				})
			case canceled && !blocked:
				events = append(events, Event{
					Type:    EventTaskSkipped,
					RunID:   e.runID,
					TaskID:  id,
					Status:  StatusSkipped,
					Reason:  string(dag.ReasonCanceled),
					Message: "run canceled before task started",
					Time:    e.cfg.Clock.Now(),
				})
			}
		}
		e.mu.Unlock()
		if len(events) == 0 {
			return nil
		}
		e.logger.Logf("[scheduler] skip %d task(s): reasons propagated", len(events))
		if err := e.emitEvents(context.Background(), events...); err != nil {
			return err
		}
	}
}

func (e *Engine) anyFailed(snap *RunSnapshot) bool {
	for _, t := range snap.Tasks {
		if t.Status == StatusFailed {
			return true
		}
	}
	return false
}

// Snapshot returns a consistent, deeply-copied view of the run.
func (e *Engine) Snapshot() *RunSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.st.snapshot()
}

// Events returns a defensive copy of journal events in seq order.
func (e *Engine) Events() []Event {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Event, len(e.events))
	copy(out, e.events)
	return out
}

// RunID reports the bound run id.
func (e *Engine) RunID() string { return e.runID }

func generateRunID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "run-" + time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b[:])
}
