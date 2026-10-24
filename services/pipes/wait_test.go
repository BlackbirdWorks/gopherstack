package pipes_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// waitBudget is a backstop only; the transition itself takes ~10ms.
const waitBudget = 30 * time.Second

func waitPipeState(t *testing.T, b *pipes.InMemoryBackend, name, state string) {
	t.Helper()

	require.Eventually(t, func() bool {
		p, err := b.GetPipe(context.Background(), name)

		return err == nil && p.CurrentState == state
	}, waitBudget, 2*time.Millisecond, "pipe %q did not reach %s", name, state)
}

func waitPipeRunning(t *testing.T, b *pipes.InMemoryBackend, name string) {
	t.Helper()
	waitPipeState(t, b, name, "RUNNING")
}

func waitPipeStopped(t *testing.T, b *pipes.InMemoryBackend, name string) {
	t.Helper()
	waitPipeState(t, b, name, "STOPPED")
}
