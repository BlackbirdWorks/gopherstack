package ses_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSESJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestSESJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "no_timeout_zero",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "with_30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())
			h.WithJanitor(time.Minute, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestSESJanitor_Run_ExitsOnCancel verifies that the janitor goroutine exits
// promptly when the parent context is cancelled, even with a TaskTimeout set.
func TestSESJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	j := ses.NewJanitor(b, 10*time.Millisecond)
	j.TaskTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "janitor did not exit after context cancellation")
	}
}
