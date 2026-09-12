package cloudfront_test

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// reconcilerFrame identifies the invalidation-reconciler goroutine
// (store.go's runInvalidationReconciler) in a runtime.Stack dump. Matching on
// this rather than on runtime.NumGoroutine()'s process-wide count means
// unrelated goroutines elsewhere in the test binary -- GC/race-detector
// bookkeeping, other tests' background work -- can never perturb the result;
// only a goroutine actually executing (or on the call stack of) this
// function counts (gopherstack-ndss).
const reconcilerFrame = "runInvalidationReconciler"

// countGoroutines returns how many currently running goroutines have frame
// somewhere in their stack trace.
func countGoroutines(frame string) int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]

			break
		}

		buf = make([]byte, 2*len(buf))
	}

	count := 0

	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, frame) {
			count++
		}
	}

	return count
}

// TestHandler_ShutdownStopsInvalidationReconciler_NoGoroutineLeak verifies
// that tearing a Handler down through the service.Shutdowner interface --
// the same discovery path cli.go's shutdownServices uses -- actually stops
// the backend's background invalidation-reconciler goroutine
// (store.go's runInvalidationReconciler).
//
// The assertion goes through a type assertion to service.Shutdowner rather
// than calling h.Shutdown directly, so it exercises exactly the mechanism
// cli.go relies on and still compiles against a Handler with no Shutdown
// method at all: before Handler implemented service.Shutdowner, the
// assertion silently failed, Close() was never reached, and the reconciler
// goroutine leaked forever.
//
// The leak check itself counts goroutines by stack identity (countGoroutines
// above), not runtime.NumGoroutine(). An earlier version of this test
// compared NumGoroutine() before/after and was flaky in CI: that counter is
// process-wide, and the whole package runs in one test binary under
// -race -shuffle on, so any unrelated goroutine starting or still unwinding
// between the two samples shifted the delta (gopherstack-ndss). Scoping to
// goroutines whose stack names the reconciler removes that source of noise
// entirely -- a goroutine with an unrelated stack can never match.
//
// The comparison uses require.Eventually to close a small, unrelated race:
// Close() joins the reconciler via a channel the goroutine closes with its
// last deferred call, so the receiver can observe the close a hair before
// the sender has fully unwound off the stack. Eventually's own internally
// spawned polling goroutine is safe to use here (unlike with
// runtime.NumGoroutine(), where it would permanently inflate the process-wide
// count for as long as polling continues): it never has reconcilerFrame on
// its stack, so it cannot affect this scoped count.
//
// Sequential (no t.Parallel()): keeping this test off the parallel scheduler
// means no other top-level test's body -- including another test that
// constructs and tears down a cloudfront.InMemoryBackend of its own -- can
// be mid-flight while this one samples, so the only source of
// reconcilerFrame goroutines in the count is this test's own loop.
//
//nolint:paralleltest // deliberately sequential, see doc comment
func TestHandler_ShutdownStopsInvalidationReconciler_NoGoroutineLeak(t *testing.T) {
	const iterations = 12

	baseline := countGoroutines(reconcilerFrame)

	for range iterations {
		b := cloudfront.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
		h := cloudfront.NewHandler(b)

		shutdowner, ok := any(h).(service.Shutdowner)
		require.True(t, ok,
			"cloudfront.Handler must implement service.Shutdowner so cli.go's shutdownServices reaches it")

		shutdowner.Shutdown(context.Background())
	}

	require.Eventually(t, func() bool {
		return countGoroutines(reconcilerFrame) <= baseline
	}, 200*time.Millisecond, 5*time.Millisecond,
		"invalidation reconciler goroutine leaked after %d construct/shutdown cycles (baseline=%d)",
		iterations, baseline)
}
