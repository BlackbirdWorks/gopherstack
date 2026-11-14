package azurearm_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type connRecorder struct {
	azurearm.FunctionApps

	env map[string]map[string]string
	mu  sync.Mutex
}

func (c *connRecorder) PutApp(name string, _ azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local"}, nil
}

func (c *connRecorder) Settings(string) (map[string]string, error) { return map[string]string{}, nil }

func (c *connRecorder) SetSettings(string, map[string]string) error { return nil }

func (c *connRecorder) WarmUp(context.Context, string) error { return nil }

func (c *connRecorder) SetConnectionStrings(name string, env map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.env[name] = env

	return nil
}

func (c *connRecorder) get(name string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.env[name]
}

func webID(types []string, names ...string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.Web", Types: types, Names: names,
	}
}

func TestWebAlwaysOnPlanValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sku     map[string]any
		name    string
		kind    string
		wantErr bool
	}{
		{name: "basic", sku: map[string]any{"name": "B1", "tier": "Basic"}, kind: "functionapp,linux"},
		{name: "premium v3", sku: map[string]any{"name": "P1v3", "tier": "PremiumV3"}, kind: "functionapp,linux"},
		{name: "consumption", sku: map[string]any{"name": "Y1", "tier": "Dynamic"}, kind: "functionapp,linux",
			wantErr: true},
		{name: "elastic premium", sku: map[string]any{"name": "EP1", "tier": "ElasticPremium"},
			kind: "functionapp,linux", wantErr: true},
		{name: "name only consumption", sku: map[string]any{"name": "Y1"}, kind: "functionapp,linux", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWebHandler(t)
			h.Registry.SetFunctionApps(&connRecorder{env: map[string]map[string]string{}})
			ctx := t.Context()

			_, _, err := h.Registry.Put(ctx, webID([]string{"serverfarms"}, "plan"), map[string]any{
				"location": "local", "kind": "linux", "sku": tt.sku, "properties": map[string]any{"reserved": true},
			})
			require.NoError(t, err)

			site := webID([]string{"sites"}, "app1")
			farm := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Web/serverfarms/plan"
			_, _, err = h.Registry.Put(ctx, site, map[string]any{
				"kind": tt.kind, "location": "local", "properties": map[string]any{"serverFarmId": farm},
			})
			require.NoError(t, err, "alwaysOn false never fails")

			p, _ := h.Registry.ProviderNamed("Microsoft.Web")
			wp, ok := p.(*azurearm.WebProvider)
			require.True(t, ok)

			_, err = wp.PutConfig(ctx, site, "web", map[string]any{
				"properties": map[string]any{"alwaysOn": true},
			}, true)

			if tt.wantErr {
				require.ErrorIs(t, err, azurearm.ErrWebInvalidSiteConfig)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestWebConnectionStringsReachHost(t *testing.T) {
	t.Parallel()

	h := newWebHandler(t)
	rec := &connRecorder{env: map[string]map[string]string{}}
	h.Registry.SetFunctionApps(rec)
	ctx := t.Context()

	site := webID([]string{"sites"}, "app1")
	_, _, err := h.Registry.Put(ctx, site, map[string]any{"kind": "functionapp,linux", "location": "local"})
	require.NoError(t, err)

	p, _ := h.Registry.ProviderNamed("Microsoft.Web")
	wp, ok := p.(*azurearm.WebProvider)
	require.True(t, ok)

	_, err = wp.PutConfig(ctx, site, "connectionstrings", map[string]any{"properties": map[string]any{
		"db":    map[string]any{"value": "Server=x", "type": "SQLAzure"},
		"cache": map[string]any{"value": "host:6380", "type": "RedisCache"},
		"misc":  map[string]any{"value": "v", "type": "Custom"},
		"odd":   map[string]any{"value": "v", "type": "NoSuchType"},
	}}, false)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"SQLAZURECONNSTR_db": "Server=x", "REDISCACHECONNSTR_cache": "host:6380", "CUSTOMCONNSTR_misc": "v",
	}, rec.get("app1"))

	slot := webID([]string{"sites", "slots"}, "app1", "staging")
	_, _, err = h.Registry.Put(ctx, slot, map[string]any{"kind": "functionapp,linux", "location": "local"})
	require.NoError(t, err)

	_, err = wp.PutConfig(ctx, slot, "connectionstrings", map[string]any{"properties": map[string]any{
		"db": map[string]any{"value": "Server=staging", "type": "SQLAzure"},
	}}, false)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"SQLAZURECONNSTR_db": "Server=staging"}, rec.get("app1/staging"))
}
