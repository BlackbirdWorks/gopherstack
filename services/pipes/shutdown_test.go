package pipes_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestBackendShutdown verifies that the backend's delayed state-transition
// goroutines are cancellable via Shutdown and that, absent shutdown,
// transitions still complete after the delay.
func TestBackendShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		shutdownBefore   bool
		wantFinalRunning bool
	}{
		{
			name:             "transition completes when not shut down",
			shutdownBefore:   false,
			wantFinalRunning: true,
		},
		{
			name:             "shutdown cancels pending transition",
			shutdownBefore:   true,
			wantFinalRunning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			b := pipes.NewInMemoryBackendWithContext(ctx, "123456789012", "us-east-1")

			created, err := b.CreatePipeSimple(
				"p1", "arn:aws:iam::123456789012:role/r",
				"arn:aws:sqs:us-east-1:123456789012:src",
				"arn:aws:sqs:us-east-1:123456789012:tgt",
				"", "RUNNING", nil,
			)
			require.NoError(t, err)
			require.Equal(t, "CREATING", created.CurrentState)

			if tt.shutdownBefore {
				// Cancel before the transition delay elapses; Shutdown must
				// return promptly and the pipe must remain in CREATING.
				shutCtx, shutCancel := context.WithTimeout(t.Context(), time.Second)
				defer shutCancel()
				b.Shutdown(shutCtx)

				p, getErr := b.GetPipe(context.Background(), "p1")
				require.NoError(t, getErr)
				require.Equal(t, "CREATING", p.CurrentState,
					"transition must not fire after shutdown")

				return
			}

			require.Eventually(t, func() bool {
				p, getErr := b.GetPipe(context.Background(), "p1")

				return getErr == nil && p.CurrentState == "RUNNING"
			}, time.Second, 5*time.Millisecond)
		})
	}
}

// TestBackendShutdownIdempotent verifies Shutdown can be called safely even when
// no transitions are in flight and returns within the bounded ctx.
func TestBackendShutdownIdempotent(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		b.Shutdown(ctx)
		b.Shutdown(ctx) // second call must not panic or block
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return within bound")
	}
}
