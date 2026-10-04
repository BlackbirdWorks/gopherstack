package testleak

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

const (
	stackBuf     = 1 << 20
	baselineWait = 5 * time.Second
)

// Baseline waits for a goroutine stack containing marker, then ignores every live goroutine.
// Not for parallel tests: it inspects the whole process.
func Baseline(t *testing.T, marker string) []goleak.Option {
	t.Helper()

	deadline := time.Now().Add(baselineWait)

	for {
		buf := make([]byte, stackBuf)
		if strings.Contains(string(buf[:runtime.Stack(buf, true)]), marker) {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("no goroutine matching %q started", marker)
		}

		runtime.Gosched()
	}

	return append(defaultIgnores(), goleak.IgnoreCurrent())
}
