package dag

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Store 持久化运行事件，支撑进程中断后的恢复。
// 实现必须保证：Append 按 Seq 单调追加；Load 返回完整有序事件流；
// 重复 Append 相同 Seq 的事件是幂等的（去重）。
type Store interface {
	Append(ev Event) error
	Load(runID string) ([]Event, error)
}

// MemStore 是内存版 Store，用于测试与不持久化的场景。
type MemStore struct {
	mu     sync.Mutex
	events map[string][]Event
}

func NewMemStore() *MemStore { return &MemStore{events: map[string][]Event{}} }

func (s *MemStore) Append(ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs := s.events[ev.RunID]
	if len(evs) > 0 && ev.Seq <= evs[len(evs)-1].Seq {
		return nil // 幂等：已存在相同或更新的事件
	}
	s.events[ev.RunID] = append(evs, ev)
	return nil
}

func (s *MemStore) Load(runID string) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events[runID]...), nil
}

// FileStore 以 JSONL 文件实现 Store，每个 runID 一个文件。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore 创建基于目录的存储，目录不存在时自动创建。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(runID string) string {
	return filepath.Join(s.dir, runID+".jsonl")
}

func (s *FileStore) Append(ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 幂等检查：若该 Seq 已存在则跳过
	last, err := s.lastSeq(ev.RunID)
	if err != nil {
		return err
	}
	if ev.Seq <= last {
		return nil
	}
	f, err := os.OpenFile(s.path(ev.RunID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open event log: %w", err)
	}
	defer f.Close()
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	return f.Sync()
}

func (s *FileStore) lastSeq(runID string) (int64, error) {
	evs, err := s.loadLocked(runID)
	if err != nil {
		return 0, err
	}
	if len(evs) == 0 {
		return 0, nil
	}
	return evs[len(evs)-1].Seq, nil
}

func (s *FileStore) Load(runID string) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(runID)
}

// loadLocked 不加锁版本，调用方须持有 s.mu。
func (s *FileStore) loadLocked(runID string) ([]Event, error) {
	f, err := os.Open(s.path(runID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}
	defer f.Close()
	seen := map[int64]bool{}
	var evs []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("corrupt event log %s: %w", s.path(runID), err)
		}
		if seen[ev.Seq] {
			continue // 重放去重
		}
		seen[ev.Seq] = true
		evs = append(evs, ev)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read event log: %w", err)
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].Seq < evs[j].Seq })
	return evs, nil
}
