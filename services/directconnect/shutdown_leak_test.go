package directconnect_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

// TestHandler_ShutdownStopsScheduledTransitions_NoLeak verifies that
// tearing a Handler down through the service.Shutdowner interface -- the
// same discovery path cli.go's shutdownServices uses -- actually reaches
// the backend's worker.Group and stops it, so no scheduled
// state-transition timer fires after shutdown.
//
// Unlike services/cloudfront and services/elbv2's reconciler goroutines (a
// persistent goroutine parked in a select loop for the service's whole
// lifetime, identifiable by name in a runtime.Stack dump), this backend's
// worker.Group is only ever driven via After -- one-shot, delayed
// callbacks (store.go's scheduled state-transition helpers) that do not
// exist as a goroutine at all until they fire, and then run to completion
// in well under a microsecond. Sampling runtime.Stack for such a frame
// would almost always miss it and would flake -- exactly the
// gopherstack-ndss failure mode the cloudfront/elbv2 tests exist to avoid
// -- so this test instead probes the Group directly: after Shutdown, arm a
// fresh timer on the same Group (export_test.go's ArmProbeTimerForTest)
// and confirm it never fires. Per pkgs/worker's own
// TestGroupAfterIsNoOpAfterStop, After silently no-ops once Stop has run,
// so a probe timer firing proves Close() (and therefore work.Stop()) was
// never reached.
//
// The assertion goes through a type assertion to service.Shutdowner rather
// than calling h.Shutdown directly, so it exercises exactly the mechanism
// cli.go relies on and still compiles against a Handler with no Shutdown
// method at all: before Handler implemented service.Shutdowner, the
// assertion silently failed, Close() was never reached, and scheduled
// transitions kept firing on backend state after intended shutdown.
//
// Run inside a synctest bubble so the probe timer's delay is resolved
// deterministically against a fake clock rather than a real sleep --
// see pkgs/worker/group_test.go and .claude/memories's "no time.Sleep in
// tests" convention.
func TestHandler_ShutdownStopsScheduledTransitions_NoLeak(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const iterations = 5

		for range iterations {
			b := directconnect.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
			h := directconnect.NewHandler(b)

			shutdowner, ok := any(h).(service.Shutdowner)
			require.True(t, ok,
				"directconnect.Handler must implement service.Shutdowner so cli.go's shutdownServices reaches it")

			shutdowner.Shutdown(context.Background())

			fired := b.ArmProbeTimerForTest(time.Millisecond)

			time.Sleep(10 * time.Millisecond)
			synctest.Wait()

			select {
			case <-fired:
				t.Fatal("probe timer fired after Shutdown: worker.Group was never stopped, " +
					"so scheduled-transition timers leak past service shutdown")
			default:
			}
		}
	})
}
