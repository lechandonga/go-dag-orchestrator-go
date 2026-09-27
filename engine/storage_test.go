package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.jsonl")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.LoadAll(context.Background()); !errors.Is(err, ErrNoJournal) {
		t.Fatalf("expected ErrNoJournal, got %v", err)
	}

	evs := []Event{
		{Type: EventRunStarted, RunID: "r1", Time: s.nowForTest()},
		{Type: EventTaskStarted, RunID: "r1", TaskID: "a", Attempt: 1},
		{Type: EventTaskFinished, RunID: "r1", TaskID: "a", Attempt: 1, Status: StatusSucceeded},
	}
	if err := s.Append(context.Background(), evs...); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify contiguous seqs and content.
	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.LoadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 events, got %d", len(got))
	}
	for i, ev := range got {
		if ev.Seq != int64(i+1) {
			t.Fatalf("seq gap: event %d has seq %d", i, ev.Seq)
		}
	}
	// Appends continue the sequence after reopen.
	if err := s2.Append(context.Background(), Event{Type: EventRunFinished, RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s2.LoadAll(context.Background())
	if last := got[len(got)-1]; last.Seq != 4 {
		t.Fatalf("expected seq 4, got %d", last.Seq)
	}
}

func TestFileStoreLockExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.jsonl")
	s1, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()

	s2, err := NewFileStore(path)
	if !errors.Is(err, ErrJournalLocked) {
		if s2 != nil {
			s2.Close()
		}
		t.Fatalf("expected ErrJournalLocked, got %v", err)
	}
}

func TestFileStoreCorruptJournal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "j.jsonl")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), Event{Type: EventRunStarted, RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Tamper: duplicate seq by truncating file and rewriting two seq=1 rows.
	if err := os.WriteFile(path, []byte(
		"{\"seq\":1,\"type\":\"run_started\",\"run_id\":\"r1\"}\n"+
			"{\"seq\":1,\"type\":\"run_started\",\"run_id\":\"r1\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(path); err == nil {
		t.Fatal("expected corrupt journal error")
	}
}
