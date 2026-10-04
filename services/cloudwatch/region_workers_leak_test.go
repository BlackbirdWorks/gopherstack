package cloudwatch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

//nolint:paralleltest // goleak inspects every goroutine in the process
func TestSiblingJanitorStopsWithSibling(t *testing.T) {
	tests := []struct {
		drop func(t *testing.T, h *cloudwatch.Handler)
		name string
	}{
		{name: "reset", drop: func(_ *testing.T, h *cloudwatch.Handler) { h.Reset() }},
		{name: "restore", drop: func(t *testing.T, h *cloudwatch.Handler) {
			t.Helper()
			require.NoError(t, h.Restore(context.Background(), h.Snapshot(context.Background())))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
			h.EnableRegions()
			require.NoError(t, h.StartWorker(ctx))

			opts := testleak.Baseline(t, "worker.(*Group).Ticker.func1")

			require.NotNil(t, h.BackendFor("eu-west-1"))
			require.NotNil(t, h.BackendFor("ap-south-1"))

			tc.drop(t, h)
			h.Reset()

			require.NoError(t, goleak.Find(opts...))
		})
	}
}

//nolint:paralleltest // goleak inspects every goroutine in the process
func TestStartWorkerCoversEarlySiblings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	h.EnableRegions()
	require.NotNil(t, h.BackendFor("eu-west-1"))

	require.NoError(t, h.StartWorker(ctx))
	require.NoError(t, h.StartWorker(ctx))

	h.Reset()
	cancel()

	require.NoError(t, goleak.Find(testleak.DefaultIgnores()...))
}
