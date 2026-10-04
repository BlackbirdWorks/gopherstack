package lambda_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

//nolint:paralleltest // goleak inspects every goroutine in the process
func TestSiblingWorkersStopWithSibling(t *testing.T) {
	tests := []struct {
		drop func(h *lambda.Handler)
		name string
	}{
		{name: "close_regions", drop: func(h *lambda.Handler) { h.CloseRegions(context.Background()) }},
		{name: "reset", drop: func(h *lambda.Handler) { h.Reset() }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), mrAccount, mrHome)
			defer b.Close(context.Background())

			h := lambda.NewHandler(b)
			h.EnableRegions()
			require.NoError(t, h.StartWorker(ctx))

			opts := testleak.Baseline(t, "worker.(*Group).Ticker.func1")

			require.NotNil(t, h.BackendFor(mrEU))
			require.NotNil(t, h.BackendFor("ap-south-1"))
			require.Len(t, h.RegionBackends(), 3)

			tc.drop(h)

			require.Len(t, h.RegionBackends(), 1)
			require.NoError(t, goleak.Find(opts...))
		})
	}
}
