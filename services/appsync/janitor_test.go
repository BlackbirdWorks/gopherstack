package appsync_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_StartWorker(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

func TestJanitor_NewAndRun(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	j := appsync.NewJanitor(b)
	require.NotNil(t, j)

	j.Interval = 1 * time.Millisecond

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	// Run should exit cleanly when context is cancelled.
	done := make(chan struct{})
	go func() {
		j.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}
}
