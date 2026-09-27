package engine

import (
	"testing"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Write(p []byte) (int, error) {
	l.t.Logf("%s", p)
	return len(p), nil
}
