package dag

import (
	"errors"
	"testing"
)

func noop(Context) error { return nil }

func TestValidateOK(t *testing.T) {
	g := New([]*Task{
		{ID: "a", Fn: noop},
		{ID: "b", Deps: []string{"a"}, Fn: noop},
		{ID: "c", Deps: []string{"a", "b"}, Fn: noop},
	})
	if err := g.Validate(); err != nil {
		t.Fatalf("expected valid graph, got %v", err)
	}
	if ids := g.TaskIDs(); len(ids) != 3 || ids[0] != "a" || ids[1] != "b" || ids[2] != "c" {
		t.Fatalf("unexpected sorted ids: %v", ids)
	}
	if roots := g.Roots(); len(roots) != 1 || roots[0] != "a" {
		t.Fatalf("unexpected roots: %v", roots)
	}
	if ds := g.Dependents("a"); len(ds) != 2 {
		t.Fatalf("expected 2 dependents, got %v", ds)
	}
}

func TestValidateMissingDependency(t *testing.T) {
	g := New([]*Task{
		{ID: "a", Deps: []string{"ghost"}, Fn: noop},
	})
	err := g.Validate()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T %v", err, err)
	}
	if ve.Reason != ReasonMissingDependency {
		t.Fatalf("expected reason %q, got %q", ReasonMissingDependency, ve.Reason)
	}
}

func TestValidateSelfCycle(t *testing.T) {
	g := New([]*Task{{ID: "a", Deps: []string{"a"}, Fn: noop}})
	var ve *ValidationError
	if err := g.Validate(); !errors.As(err, &ve) || ve.Reason != ReasonCycle {
		t.Fatalf("expected cycle, got %v", err)
	}
}

func TestValidateCycle(t *testing.T) {
	g := New([]*Task{
		{ID: "a", Deps: []string{"c"}, Fn: noop},
		{ID: "b", Deps: []string{"a"}, Fn: noop},
		{ID: "c", Deps: []string{"b"}, Fn: noop},
	})
	err := g.Validate()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T %v", err, err)
	}
	if ve.Reason != ReasonCycle {
		t.Fatalf("expected reason %q, got %q", ReasonCycle, ve.Reason)
	}
	if len(ve.Cycle) < 2 {
		t.Fatalf("expected cycle path, got %v", ve.Cycle)
	}
	t.Logf("cycle detected: %v", ve.Cycle)
}

func TestValidateEmpty(t *testing.T) {
	if err := New(nil).Validate(); err == nil {
		t.Fatal("expected error for empty dag")
	}
}

func TestTaskErrorClassification(t *testing.T) {
	r := NewRetriableError("flaky")
	if !r.Retriable || r.Reason != ReasonTaskFailed {
		t.Fatalf("unexpected retriable error: %+v", r)
	}
	term := NewTerminalError("bad input")
	if term.Retriable || term.Reason != ReasonTerminalFailure {
		t.Fatalf("unexpected terminal error: %+v", term)
	}
}
