package azurekeyvault_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestMain(m *testing.M) { testleak.VerifyTestMain(m) }

func TestJanitor_RunStopsOnCancel(t *testing.T) {
	t.Parallel()

	j := azurekeyvault.NewJanitor(azurekeyvault.NewInMemoryBackend(true), time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "janitor did not stop after cancel")
	}
}
