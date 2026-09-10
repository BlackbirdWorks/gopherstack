package elbv2_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestHandler_ShutdownStopsHealthReconciler_NoGoroutineLeak verifies that
// tearing a Handler down through the service.Shutdowner interface -- the
// same discovery path cli.go's shutdownServices uses -- actually stops the
// backend's background health-reconciler goroutine (targets.go's
// runHealthReconciler).
//
// The assertion goes through a type assertion to service.Shutdowner rather
// than calling h.Shutdown directly, so it exercises exactly the mechanism
// cli.go relies on and still compiles against a Handler with no Shutdown
// method at all: before Handler implemented service.Shutdowner, the
// assertion silently failed, Close() was never reached, and the reconciler
// goroutine leaked forever.
//
// No polling/require.Eventually here: Close() joins the reconciler
// goroutine synchronously (InMemoryBackend.healthWG.Wait()), so the
// goroutine is already gone by the time Shutdown returns -- a single
// measurement right after the loop is deterministic. (require.Eventually
// would not work for this: it runs its condition from an internally
// spawned goroutine, which inflates runtime.NumGoroutine() by one for as
// long as polling continues -- see services/cloudfront's twin of this test
// for how that was found.)
//
// Sequential (no t.Parallel()): runtime.NumGoroutine() is a process-wide
// counter, so running this alongside other parallel tests would make the
// delta assertion flaky -- see services/redshift/reconciler_test.go's
// assertStopsPromptly doc comment for the same rationale.
//
//nolint:paralleltest // deliberately sequential: runtime.NumGoroutine() is process-wide, see doc comment
func TestHandler_ShutdownStopsHealthReconciler_NoGoroutineLeak(t *testing.T) {
	const iterations = 12

	baseline := runtime.NumGoroutine()

	for range iterations {
		b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
		h := elbv2.NewHandler(b)

		shutdowner, ok := any(h).(service.Shutdowner)
		require.True(t, ok,
			"elbv2.Handler must implement service.Shutdowner so cli.go's shutdownServices reaches it")

		shutdowner.Shutdown(context.Background())
	}

	after := runtime.NumGoroutine()
	require.LessOrEqual(t, after, baseline,
		"goroutine count grew after %d construct/shutdown cycles: baseline=%d, after=%d (health reconciler leaked)",
		iterations, baseline, after)
}
