package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ErrNoJournal is returned when no run journal exists at the target path.
var ErrNoJournal = errors.New("no run journal found")

// ErrJournalLocked means another engine process/thread holds the run lock.
var ErrJournalLocked = errors.New("run journal is locked by another engine")

// Store is the durability layer. Writes are fsynced before the
// corresponding state transition is considered durable; reads return
// events in sequence order.
type Store interface {
	// LoadAll replays every persisted event of the run, in seq order.
	// It returns ErrNoJournal when nothing has been written yet.
	LoadAll(ctx context.Context) ([]Event, error)
	// Append atomically persists events, assigning monotonically
	// increasing seq numbers continuing from the journal, and fsyncs
	// before returning.
	Append(ctx context.Context, evs ...Event) error
}

// FileStore is a JSON-lines file backed Store with an advisory lock
// that guarantees a single live engine per journal.
type FileStore struct {
	path string
	f    *os.File

	mu       sync.Mutex
	nextSeq  int64
	cached   []Event
	lockFile *os.File
}

// NewFileStore opens (or creates) the journal at path and acquires an
// exclusive, non-blocking advisory lock on "<path>.lock".
func NewFileStore(path string) (*FileStore, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create journal dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	lockPath := path + ".lock"
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("open journal lock: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrJournalLocked
		}
		return nil, fmt.Errorf("acquire journal lock: %w", err)
	}
	s := &FileStore{path: path, f: f, nextSeq: 1}
	// lockFile is kept open for the lock lifetime; attach via finalizer-ish
	// field kept on the struct to prevent GC of the handle.
	s.lockFile = lf
	if err := s.replayLocked(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Path reports the journal file location.
func (s *FileStore) Path() string { return s.path }

func (s *FileStore) LoadAll(ctx context.Context) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cached) == 0 {
		return nil, ErrNoJournal
	}
	out := make([]Event, len(s.cached))
	copy(out, s.cached)
	return out, nil
}

func (s *FileStore) Append(ctx context.Context, evs ...Event) error {
	if len(evs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	for i := range evs {
		s.nextSeq++
		evs[i].Seq = s.nextSeq - 1 // mutate caller's copy; projection must agree
		raw, err := json.Marshal(evs[i])
		if err != nil {
			return fmt.Errorf("encode event seq=%d: %w", evs[i].Seq, err)
		}
		buf.Write(raw)
		buf.WriteByte('\n')
	}
	if _, err := s.f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write journal: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync journal: %w", err)
	}
	s.cached = append(s.cached, evs...)
	return nil
}

// Close releases the advisory lock and file handle.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.lockFile != nil {
		if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN); err != nil {
			firstErr = err
		}
		if err := s.lockFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.lockFile = nil
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.f = nil
	}
	return firstErr
}

// replayLocked reads the journal from disk, validates seq contiguity and
// primes the cache and seq counter.
func (s *FileStore) replayLocked() error {
	if _, err := s.f.Seek(0, 0); err != nil {
		return fmt.Errorf("seek journal: %w", err)
	}
	scanner := bufio.NewScanner(s.f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var events []Event
	var want int64 = 1
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("corrupt journal near seq=%d: %w", want, err)
		}
		if ev.Seq != want {
			return fmt.Errorf("corrupt journal: expected seq=%d, got %d", want, ev.Seq)
		}
		events = append(events, ev)
		want++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read journal: %w", err)
	}
	if _, err := s.f.Seek(0, 2); err != nil {
		return fmt.Errorf("seek journal end: %w", err)
	}
	s.cached = events
	s.nextSeq = want
	return nil
}
