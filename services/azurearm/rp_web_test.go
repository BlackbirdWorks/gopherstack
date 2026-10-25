package azurearm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func newWebHandler(t *testing.T) *azurearm.Handler {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	return h
}

func TestWebProvider_WithoutDataPlane(t *testing.T) {
	t.Parallel()

	h := newWebHandler(t)
	ctx := context.Background()
	rid := azurearm.ResourceID{
		SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.Web",
		Types: []string{"sites"}, Names: []string{"app1"},
	}

	_, _, err := h.Registry.Put(ctx, rid, map[string]any{"kind": "functionapp,linux", "location": "local"})
	require.ErrorIs(t, err, azurearm.ErrWebFunctionsUnavailable, "no data plane, no app")

	_, _, err = h.Registry.Put(ctx, rid, map[string]any{"kind": "app", "location": "local"})
	require.ErrorIs(t, err, azurearm.ErrWebUnsupportedKind)
}

func TestWebProvider_PlanSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	src := newWebHandler(t)
	ctx := context.Background()
	pid := azurearm.ResourceID{
		SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.Web",
		Types: []string{"serverfarms"}, Names: []string{"plan1"},
	}

	_, _, err := src.Registry.Put(ctx, pid, map[string]any{
		"location": "local", "kind": "functionapp", "sku": map[string]any{"name": "EP1", "tier": "ElasticPremium"},
		"properties": map[string]any{"reserved": true},
	})
	require.NoError(t, err)

	dst := newWebHandler(t)
	require.NoError(t, dst.Restore(ctx, src.Snapshot(ctx)))

	got, err := dst.Registry.Get(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, "EP1", got["sku"].(map[string]any)["name"])
	assert.Equal(t, true, got["properties"].(map[string]any)["elasticScaleEnabled"])

	require.NoError(t, dst.Registry.Delete(ctx, pid))
	_, err = dst.Registry.Get(ctx, pid)
	require.ErrorIs(t, err, azurearm.ErrWebPlanNotFound)
}
