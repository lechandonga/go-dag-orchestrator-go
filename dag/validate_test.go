package dag

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateCycle(t *testing.T) {
	spec := Spec{Name: "cyc", Tasks: []Task{
		{ID: "a", Deps: []string{"c"}},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
	}}
	err := Validate(spec)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if !strings.Contains(ve.Detail, "cycle") {
		t.Fatalf("expected cycle detail, got %q", ve.Detail)
	}
	if len(ve.TaskIDs) == 0 {
		t.Fatal("expected cycle members in TaskIDs")
	}
	t.Logf("cycle detected: %v", ve.TaskIDs)
}

func TestValidateSelfCycle(t *testing.T) {
	spec := Spec{Tasks: []Task{{ID: "a", Deps: []string{"a"}}}}
	if err := Validate(spec); err == nil {
		t.Fatal("expected self-cycle error")
	}
}

func TestValidateMissingDep(t *testing.T) {
	spec := Spec{Tasks: []Task{{ID: "a", Deps: []string{"ghost"}}}}
	err := Validate(spec)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %v", err)
	}
	if !strings.Contains(ve.Detail, "missing") {
		t.Fatalf("expected missing-dep detail, got %q", ve.Detail)
	}
}

func TestValidateDuplicateID(t *testing.T) {
	spec := Spec{Tasks: []Task{{ID: "a"}, {ID: "a"}}}
	if err := Validate(spec); err == nil {
		t.Fatal("expected duplicate id error")
	}
}

func TestValidateOK(t *testing.T) {
	spec := Spec{Tasks: []Task{
		{ID: "a"}, {ID: "b", Deps: []string{"a"}}, {ID: "c", Deps: []string{"a", "b"}},
	}}
	if err := Validate(spec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	layers, err := topoLayers(spec)
	if err != nil {
		t.Fatalf("topoLayers: %v", err)
	}
	if len(layers) != 3 || layers[0][0] != "a" || layers[1][0] != "b" || layers[2][0] != "c" {
		t.Fatalf("unexpected layers: %v", layers)
	}
}

func TestRunInvalidSpecDistinguishable(t *testing.T) {
	e := NewEngine(nil, Options{})
	snap, err := e.Run(t.Context(), "run-invalid", Spec{Tasks: []Task{
		{ID: "a", Deps: []string{"a"}},
	}})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %v", err)
	}
	if snap.Status != RunInvalid {
		t.Fatalf("expected RunInvalid, got %s", snap.Status)
	}
}
