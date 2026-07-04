package logger_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// syncWriter serialises writes so the test's buffer access is race-free
// independently of the logger's own copy-on-write guarantees.
type syncWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf.Write(p)
}

// TestConcurrentDerivationIsCopyOnWrite verifies that deriving service/worker
// loggers off a single shared parent context from many goroutines never mutates
// the parent and never corrupts the logger carried in ctx. Run with -race.
func TestConcurrentDerivationIsCopyOnWrite(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)

	handler := slog.NewTextHandler(&syncWriter{mu: &mu, buf: &buf}, &slog.HandlerOptions{Level: slog.LevelDebug})
	base := logger.Save(t.Context(), slog.New(handler))
	parent := logger.Load(base)

	const n = 256

	var wg sync.WaitGroup

	wg.Add(n)

	for i := range n {
		go func(i int) {
			defer wg.Done()

			ctx := logger.WithService(base, "svc")
			ctx = logger.WithWorker(ctx, "svc", "job")
			ctx = logger.AddAttrs(ctx, slog.Int("iter", i))

			if logger.Load(ctx) == parent {
				t.Errorf("derived logger must differ from the shared parent")
			}

			logger.Load(ctx).InfoContext(ctx, "child")
		}(i)
	}

	wg.Wait()

	// Copy-on-write: the base context's logger pointer must be unchanged.
	if logger.Load(base) != parent {
		t.Fatal("parent logger was replaced in the base context")
	}

	// A record emitted through the untouched parent must not carry the child-only
	// service/worker/iter attributes — proving enrichment never leaked upward.
	mu.Lock()
	buf.Reset()
	mu.Unlock()

	parent.InfoContext(t.Context(), "parent-msg")

	mu.Lock()
	out := buf.String()
	mu.Unlock()

	for _, leaked := range []string{"worker=", "iter=", "service="} {
		if strings.Contains(out, leaked) {
			t.Fatalf("parent logger leaked child attribute %q: %q", leaked, out)
		}
	}
}
