package resiliencehub_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

func TestHandler_SiblingTimersStopWithSibling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		drop func(t *testing.T, h *resiliencehub.Handler)
		name string
	}{
		{name: "reset", drop: func(_ *testing.T, h *resiliencehub.Handler) { h.Reset() }},
		{name: "shutdown", drop: func(_ *testing.T, h *resiliencehub.Handler) { h.Shutdown(context.Background()) }},
		{name: "restore", drop: func(t *testing.T, h *resiliencehub.Handler) {
			t.Helper()
			require.NoError(t, h.Restore(context.Background(), []byte(`{}`)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := resiliencehub.NewHandler(
					resiliencehub.NewInMemoryBackend(context.Background(), "000000000000", "us-east-1"),
				)
				h.EnableRegions(context.Background())

				sibling := h.BackendFor("eu-west-1")
				require.NotSame(t, h.Backend, sibling)

				tc.drop(t, h)

				fired := sibling.ArmProbeTimerForTest(time.Millisecond)

				time.Sleep(10 * time.Millisecond)
				synctest.Wait()

				select {
				case <-fired:
					assert.Fail(t, "sibling timer fired after its region was dropped")
				default:
				}
			})
		})
	}
}
