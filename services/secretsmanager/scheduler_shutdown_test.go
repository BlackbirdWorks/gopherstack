package secretsmanager

import (
	"runtime"
	"testing"
	"time"
)

// waitForGoroutineCount polls until the goroutine count drops to at most target
// or the deadline passes, returning the final observed count.
func waitForGoroutineCount(target int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()

		n := runtime.NumGoroutine()
		if n <= target || time.Now().After(deadline) {
			return n
		}

		time.Sleep(time.Millisecond)
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

			baseline := runtime.NumGoroutine()

			if tc.start {
				b.ensureRotationScheduler()
				// Give the goroutine a moment to be scheduled.
				time.Sleep(5 * time.Millisecond)
			}

			b.StopRotationScheduler()
			if tc.stopTwice {
				// A second stop must not panic (close-of-closed-channel) and
				// must remain a no-op.
				b.StopRotationScheduler()
			}

			// After stopping, the scheduler goroutine must exit so the count
			// returns to (at most) the baseline observed before starting it.
			if got := waitForGoroutineCount(baseline, time.Second); got > baseline {
				t.Fatalf("scheduler goroutine leaked: goroutines=%d baseline=%d", got, baseline)
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
