package azurearm_test

import (
	"context"
	"sync"
	"testing"
	"time"

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

type fnRecorder struct {
	azurearm.FunctionApps

	names []string
	mu    sync.Mutex
}

func (f *fnRecorder) PutApp(name string, _ azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local", SCMHost: name + ".scm.local"}, nil
}

func (f *fnRecorder) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local"}, true
}

func (f *fnRecorder) Settings(string) (map[string]string, error) { return map[string]string{}, nil }

func (f *fnRecorder) SetSettings(string, map[string]string) error { return nil }

func (f *fnRecorder) Functions(_ context.Context, name string) ([]azurearm.FunctionAppFunction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.names = append(f.names, name)

	return []azurearm.FunctionAppFunction{{Name: "fn1"}}, nil
}

func (f *fnRecorder) FunctionKeys(name, _ string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.names = append(f.names, "keys:"+name)

	return map[string]string{"default": "k"}, nil
}

func TestWebProvider_SlotFunctionLookupUsesSlotDataPlaneName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		slot  string
		want  string
		wantK string
	}{
		{name: "production", slot: "", want: "app1", wantK: "keys:app1"},
		{name: "slot", slot: "staging", want: "app1/staging", wantK: "keys:app1/staging"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWebHandler(t)
			rec := &fnRecorder{}
			h.Registry.SetFunctionApps(rec)

			ctx := context.Background()
			site := azurearm.ResourceID{
				SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.Web",
				Types: []string{"sites"}, Names: []string{"app1"},
			}
			body := map[string]any{"kind": "functionapp,linux", "location": "local"}

			_, _, err := h.Registry.Put(ctx, site, body)
			require.NoError(t, err)

			id := site
			if tt.slot != "" {
				id = azurearm.ResourceID{
					SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.Web",
					Types: []string{"sites", "slots"}, Names: []string{"app1", tt.slot},
				}
				_, _, err = h.Registry.Put(ctx, id, body)
				require.NoError(t, err)
			}

			p, ok := h.Registry.ProviderNamed("Microsoft.Web")
			require.True(t, ok)

			wp, ok := p.(*azurearm.WebProvider)
			require.True(t, ok)

			_, err = wp.ListFunctions(ctx, id)
			require.NoError(t, err)

			_, err = wp.FunctionKeys(ctx, id, "fn1")
			require.NoError(t, err)

			rec.mu.Lock()
			defer rec.mu.Unlock()

			assert.Equal(t, []string{tt.want, tt.want, tt.wantK}, rec.names)
		})
	}
}
