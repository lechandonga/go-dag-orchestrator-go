package engine

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// logger serializes writes to the configured sink and stamps lines with
// a timestamp. A nil sink discards everything.
type logger struct {
	mu   sync.Mutex
	sink io.Writer
	clk  Clock
}

func newLogger(sink io.Writer, clk Clock) *logger {
	if sink == nil {
		sink = io.Discard
	}
	return &logger{sink: sink, clk: clk}
}

func (l *logger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := l.clk.Now().UTC().Format(time.RFC3339Nano)
	fmt.Fprintf(l.sink, "%s "+format+"\n", append([]any{ts}, args...)...)
}
