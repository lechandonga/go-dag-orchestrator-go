package engine

import (
	"context"
	"sync"
	"time"
)

// Clock allows the engine to use deterministic time in tests.
type Clock interface {
	Now() time.Time
	// Sleep waits for d or until ctx is done.
	Sleep(ctx context.Context, d time.Duration) error
}

// realClock is the default wall-clock implementation.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// manualClock is a deterministic test clock. Time only moves via Advance;
// Sleep blocks until enough cumulative advancement or ctx cancellation.
type manualClock struct {
	mu       sync.Mutex
	now      time.Duration // virtual elapsed from start
	sleepers []*sleeper
}

type sleeper struct {
	wakeAt time.Duration
	done   chan struct{}
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{}
}

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return time.Unix(0, int64(m.now)).UTC()
}

func (m *manualClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	m.mu.Lock()
	deadline := m.now + d
	if m.now >= deadline {
		m.mu.Unlock()
		return nil
	}
	s := &sleeper{wakeAt: deadline, done: make(chan struct{})}
	m.sleepers = append(m.sleepers, s)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.mu.Lock()
		m.removeSleeper(s)
		m.mu.Unlock()
		return ctx.Err()
	case <-s.done:
		return nil
	}
}

func (m *manualClock) removeSleeper(target *sleeper) {
	out := m.sleepers[:0]
	for _, s := range m.sleepers {
		if s != target {
			out = append(out, s)
		}
	}
	m.sleepers = out
}

// Advance moves virtual time by d and wakes every expired sleeper.
func (m *manualClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now += d
	var ready []*sleeper
	out := m.sleepers[:0]
	for _, s := range m.sleepers {
		if s.wakeAt <= m.now {
			ready = append(ready, s)
		} else {
			out = append(out, s)
		}
	}
	m.sleepers = out
	m.mu.Unlock()
	for _, s := range ready {
		close(s.done)
	}
}
