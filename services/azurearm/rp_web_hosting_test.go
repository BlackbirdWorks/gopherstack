package azurearm_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type cfgRecorder struct {
	azurearm.FunctionApps

	cfgs map[string]azurearm.FunctionAppConfig
	mu   sync.Mutex
}

func (c *cfgRecorder) PutApp(name string, cfg azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cfgs[name] = cfg

	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local", SCMHost: name + ".scm.local"}, nil
}

func (c *cfgRecorder) get(name string) azurearm.FunctionAppConfig {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cfgs[name]
}

func (c *cfgRecorder) Settings(string) (map[string]string, error) { return map[string]string{}, nil }

func (c *cfgRecorder) SetSettings(string, map[string]string) error { return nil }

func (c *cfgRecorder) SetConnectionStrings(string, map[string]string) error { return nil }

func (c *cfgRecorder) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local"}, true
}

func TestWebSiteHostingReachesDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sku      map[string]any
		name     string
		kind     string
		hosting  string
		alwaysOn bool
	}{
		{nil, "function app without plan", "functionapp,linux", "Consumption", false},
		{map[string]any{"name": "Y1", "tier": "Dynamic"}, "consumption", "functionapp,linux", "Consumption", false},
		{map[string]any{"name": "EP1", "tier": "ElasticPremium"}, "premium", "functionapp,linux", "Premium", false},
		{map[string]any{"name": "B1", "tier": "Basic"}, "dedicated", "functionapp,linux", "Dedicated", false},
		{
			map[string]any{"name": "S1", "tier": "Standard"},
			"dedicated always on",
			"functionapp,linux",
			"Dedicated",
			true,
		},
		{map[string]any{"name": "P1v3", "tier": "PremiumV3"}, "web app", "app,linux", "Dedicated", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWebHandler(t)
			rec := &cfgRecorder{cfgs: map[string]azurearm.FunctionAppConfig{}}
			h.Registry.SetFunctionApps(rec)
			ctx := t.Context()

			props := map[string]any{}

			if tt.sku != nil {
				_, _, err := h.Registry.Put(ctx, webID([]string{"serverfarms"}, "plan"), map[string]any{
					"location": "local", "kind": "linux", "sku": tt.sku, "properties": map[string]any{"reserved": true},
				})
				require.NoError(t, err)

				props["serverFarmId"] = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Web/serverfarms/plan"
			}

			site := webID([]string{"sites"}, "app1")
			_, _, err := h.Registry.Put(
				ctx,
				site,
				map[string]any{"kind": tt.kind, "location": "local", "properties": props},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.hosting, rec.get("app1").Hosting)
			assert.False(t, rec.get("app1").AlwaysOn)

			if !tt.alwaysOn {
				return
			}

			p, _ := h.Registry.ProviderNamed("Microsoft.Web")
			wp, ok := p.(*azurearm.WebProvider)
			require.True(t, ok)

			_, err = wp.PutConfig(
				ctx,
				site,
				"web",
				map[string]any{"properties": map[string]any{"alwaysOn": true}},
				true,
			)
			require.NoError(t, err)
			assert.True(t, rec.get("app1").AlwaysOn)
			assert.Equal(t, tt.hosting, rec.get("app1").Hosting)

			if tt.kind == "app,linux" {
				return
			}

			slot := webID([]string{"sites", "slots"}, "app1", "stage")
			_, _, err = h.Registry.Put(ctx, slot, map[string]any{"kind": tt.kind, "location": "local"})
			require.NoError(t, err)
			assert.Equal(t, tt.hosting, rec.get("app1/stage").Hosting, "a slot runs in its site's plan")
			assert.False(t, rec.get("app1/stage").AlwaysOn, "Always On is a per-slot setting")
		})
	}
}
