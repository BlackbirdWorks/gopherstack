package dashboard_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	appSub   = "00000000-0000-0000-0000-000000000000"
	appAPI   = "?api-version=2023-12-01"
	appRG    = "/subscriptions/" + appSub + "/resourceGroups/demo"
	appWeb   = appRG + "/providers/Microsoft.Web"
	appSite  = appWeb + "/sites/shop"
	appProxy = "/dashboard/api/v1/azure/appservice"
)

// fakeFunctionApps is an in-memory data plane for the Microsoft.Web provider.
type fakeFunctionApps struct {
	azurearm.FunctionApps

	apps map[string]*azurearm.FunctionAppInfo
	cfg  map[string]map[string]string
	mu   sync.Mutex
}

func newFakeFunctionApps() *fakeFunctionApps {
	return &fakeFunctionApps{
		apps: map[string]*azurearm.FunctionAppInfo{}, cfg: map[string]map[string]string{},
	}
}

func (f *fakeFunctionApps) PutApp(name string, _ azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.apps[name]; !ok {
		f.apps[name] = &azurearm.FunctionAppInfo{
			Created: time.Now(), HostName: name + ".azurewebsites.net", SCMHost: name + ".scm.azurewebsites.net",
		}
		f.cfg[name] = map[string]string{}
	}

	return *f.apps[name], nil
}

func (f *fakeFunctionApps) DeleteApp(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.apps, name)
	delete(f.cfg, name)

	return nil
}

func (f *fakeFunctionApps) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	a, ok := f.apps[name]
	if !ok {
		return azurearm.FunctionAppInfo{}, false
	}

	return *a, true
}

func (f *fakeFunctionApps) Settings(name string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.cfg[name], nil
}

func (f *fakeFunctionApps) SetSettings(name string, s map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cfg[name] = s

	return nil
}

func (f *fakeFunctionApps) SetStopped(name string, stopped bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if a, ok := f.apps[name]; ok {
		a.Stopped = stopped
	}

	return nil
}

func (f *fakeFunctionApps) Restart(string) error { return nil }

func (f *fakeFunctionApps) SwapContent(string, string) error { return nil }

func (f *fakeFunctionApps) WarmUp(context.Context, string) error { return nil }

func newAppServiceDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	backend := azurearm.NewInMemoryBackend()
	registry := azurearm.NewRegistry(backend)
	registry.Register(azurearm.NewWebProvider(newFakeFunctionApps()))

	issuer, err := aadauth.NewIssuer()
	require.NoError(t, err)

	h := dashboard.NewHandler(dashboard.Config{
		AzureARMOps: azurearm.NewHandler(backend, registry, issuer, azurearm.DefaultSettings()),
	})

	plan := appWeb + "/serverfarms/plan"

	for _, s := range []struct{ path, body string }{
		{appRG + "?api-version=2021-04-01", `{"location":"eastus"}`},
		{plan + appAPI, `{"location":"eastus","kind":"linux","sku":{"name":"S1","tier":"Standard"},` +
			`"properties":{"reserved":true}}`},
		{appSite + appAPI, `{"location":"eastus","kind":"app,linux","properties":{"serverFarmId":"` + plan + `",` +
			`"siteConfig":{"linuxFxVersion":"NODE|20-lts"}}}`},
		{appSite + "/slots/staging" + appAPI, `{"location":"eastus","kind":"app,linux","properties":{}}`},
		{appSite + "/config/slotConfigNames" + appAPI, `{"properties":{"appSettingNames":["STICKY_ONE"]}}`},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, strings.NewReader(s.body))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", s.path, rec.Body.String())
	}

	return h
}

func TestAzureAppServiceProxy(t *testing.T) {
	t.Parallel()

	px := appProxy + appSite
	list := appProxy + "/subscriptions/" + appSub + "/providers/Microsoft.Web/sites" + appAPI

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, list, "", `"name":"shop"`, 200},
			{http.MethodGet, px + appAPI, "", `"defaultHostName"`, 200},
			{http.MethodGet, px + "/slots" + appAPI, "", "shop/staging", 200},
			{http.MethodGet, px + "/slots/staging" + appAPI, "", "shop/staging", 200},
			{http.MethodGet, px + "/config/slotConfigNames" + appAPI, "", "STICKY_ONE", 200},
		}},
		{"lifecycle", []gcpStep{
			{http.MethodPost, px + "/stop" + appAPI, "", "", 200},
			{http.MethodGet, px + appAPI, "", `"state":"Stopped"`, 200},
			{http.MethodPost, px + "/start" + appAPI, "", "", 200},
			{http.MethodPost, px + "/restart" + appAPI, "", "", 200},
			{http.MethodPost, px + "/slots/staging/stop" + appAPI, "", "", 200},
			{http.MethodGet, px + "/slots/staging" + appAPI, "", `"state":"Stopped"`, 200},
		}},
		{"swap", []gcpStep{
			{http.MethodPost, px + "/slots/staging/slotsswap" + appAPI, `{"targetSlot":"production"}`, "", 200},
			{http.MethodPost, px + "/slots/staging/applySlotConfig" + appAPI, `{"targetSlot":"production"}`, "", 200},
			{http.MethodGet, px + "/slots/staging" + appAPI, "", `"targetSwapSlot":"production"`, 200},
			{http.MethodPost, px + "/slots/staging/resetSlotConfig" + appAPI, "", "", 200},
			{http.MethodPost, px + "/slots/staging/slotsdiffs" + appAPI, `{"targetSlot":"production"}`, `"value"`, 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodDelete, px + appAPI, "", "", http.StatusMethodNotAllowed},
			{http.MethodPut, px + appAPI, `{}`, "", http.StatusMethodNotAllowed},
			{http.MethodPost, px + "/config/appsettings/list" + appAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/config/appsettings" + appAPI, "", "unsupported", 400},
			{http.MethodPost, px + "/publishxml" + appAPI, "", "unsupported", 400},
			{http.MethodGet, appProxy + "/subscriptions/" + appSub + "/providers/Microsoft.Storage/x" + appAPI, "",
				"unsupported", 400},
			{http.MethodGet, px + "/slots/../config/web" + appAPI, "", "unsupported", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newAppServiceDashboard(t), tt.steps)
		})
	}

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, list, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
