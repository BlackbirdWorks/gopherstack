package lakeformation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

//nolint:paralleltest // goleak inspects every goroutine in the process
func TestSiblingJanitorStopsWithSibling(t *testing.T) {
	tests := []struct {
		drop func(h *lakeformation.Handler)
		name string
	}{
		{name: "reset", drop: func(h *lakeformation.Handler) { h.Reset() }},
		{name: "shutdown", drop: func(h *lakeformation.Handler) { h.Shutdown(context.Background()) }},
		{name: "restore", drop: func(h *lakeformation.Handler) {
			require.NoError(t, h.Restore(context.Background(), []byte(`{}`)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			home := lakeformation.NewInMemoryBackend()
			home.StartJanitor(ctx)

			h := lakeformation.NewHandler(home)
			h.DefaultRegion = "us-east-1"
			h.EnableRegions(ctx)

			opts := testleak.Baseline(t, "StartJanitor.func1")

			require.NotNil(t, h.BackendFor("eu-west-1"))
			require.NotNil(t, h.BackendFor("ap-south-1"))
			require.Len(t, h.RegionBackends(), 3)

			tc.drop(h)

			if tc.name != "shutdown" {
				require.Len(t, h.RegionBackends(), 1)
			}

			require.NoError(t, goleak.Find(opts...))
		})
	}
}
