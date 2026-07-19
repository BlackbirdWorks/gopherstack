package secretsmanager //nolint:testpackage // existing issue.

import (
	"testing"
	"time"
)

// assertStopsPromptly asserts stop returns within the timeout. StopRotationScheduler
// joins the scheduler goroutine, so a prompt return proves it exited; if it leaked,
// the join would block and this reports it. runtime.NumGoroutine() cannot be used:
// these tests run in parallel, so unrelated tests inflate the process-wide count.
func assertStopsPromptly(t *testing.T, timeout time.Duration, stop func()) {
	t.Helper()

	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		stop()
	}()

	select {
	case <-stopped:
	case <-time.After(timeout):
		t.Fatal("scheduler goroutine did not exit after stop")
	}
}

func TestStopRotationScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		start     bool // whether to start the scheduler before stopping
		stopTwice bool // call StopRotationScheduler twice to verify idempotency
	}{
		{
			name:      "started scheduler exits on stop",
			start:     true,
			stopTwice: false,
		},
		{
			name:      "stop is idempotent when called twice",
			start:     true,
			stopTwice: true,
		},
		{
			name:      "stop is safe when scheduler never started",
			start:     false,
			stopTwice: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_ = t.Context()

			b := NewInMemoryBackend()

			if tc.start {
				b.ensureRotationScheduler()
				// Give the goroutine a moment to be scheduled.
				time.Sleep(5 * time.Millisecond)
			}

			assertStopsPromptly(t, 2*time.Second, b.StopRotationScheduler)

			if tc.stopTwice {
				// A second stop must not panic (close-of-closed-channel) and
				// must remain a no-op.
				assertStopsPromptly(t, 2*time.Second, b.StopRotationScheduler)
			}

			// The stop channel must be closed (loop is guaranteed unblocked).
			select {
			case <-b.schedulerStop:
			default:
				t.Fatal("schedulerStop channel was not closed after StopRotationScheduler")
			}
		})
	}
}
