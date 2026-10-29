package dashboard_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	flexAPIPG    = "?api-version=2025-08-01"
	flexAPIMy    = "?api-version=2024-12-30"
	flexPxBase   = "/dashboard/api/v1/azure/flexdb"
	flexPxSecret = "Adm1n-Secret-pw!"
	flexPgProv   = armBase + "Microsoft.DBforPostgreSQL/flexibleServers"
	flexMyProv   = armBase + "Microsoft.DBforMySQL/flexibleServers"
	flexPgServer = flexPgProv + "/pgone"
	flexMyServer = flexMyProv + "/myone"
)

// flexEnv is an ARM-backed dashboard whose flexible-server clock advances on demand so async operations settle.
type flexEnv struct {
	*echo.Echo

	skew atomic.Int64
}

func (f *flexEnv) advance() { f.skew.Add(int64(time.Hour)) }

func newFlexDashboard(t *testing.T) *flexEnv {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := svc.(*azurearm.Handler)
	require.True(t, ok)
	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	env := &flexEnv{Echo: echo.New()}
	clock := func() time.Time { return time.Now().Add(time.Duration(env.skew.Load())) }

	for _, ns := range []string{"Microsoft.DBforPostgreSQL", "Microsoft.DBforMySQL"} {
		p, found := armH.Registry.ProviderNamed(ns)
		require.True(t, found)

		fp, isFlex := p.(*azurearm.FlexDBProvider)
		require.True(t, isFlex)
		fp.SetClock(clock)
	}

	env.Any("/*", dashboard.NewHandler(dashboard.Config{AzureARMOps: armH}).Handler())

	seeds := []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{
			flexPgServer + flexAPIPG,
			`{"location":"eastus","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{` +
				`"administratorLogin":"pgadmin","administratorLoginPassword":"` + flexPxSecret + `","version":"16",` +
				`"storage":{"storageSizeGB":32}}}`,
		},
		{
			flexMyServer + flexAPIMy,
			`{"location":"eastus","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{` +
				`"administratorLogin":"myadmin","administratorLoginPassword":"` + flexPxSecret + `","version":"8.0.21",` +
				`"storage":{"storageSizeGB":20}}}`,
		},
		{
			flexPgServer + "/configurations/max_connections" + flexAPIPG,
			`{"properties":{"value":"150","source":"user-override"}}`,
		},
	}

	for i, s := range seeds {
		if i == 1 {
			env.advance()
		}

		rec := flexDo(t, env.Echo, http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, s.body)
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", s.path, rec.Body.String())
	}

	env.advance()

	return env
}

func flexDo(t *testing.T, e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestAzureFlexDBProxy(t *testing.T) {
	t.Parallel()

	px := flexPxBase + flexPgServer
	my := flexPxBase + flexMyServer
	subList := flexPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.DBforPostgreSQL/flexibleServers"
	rgList := flexPxBase + flexPgProv

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, subList + flexAPIPG, "", `"name":"pgone"`, 200},
			{
				http.MethodGet,
				flexPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.DBforMySQL/flexibleServers" + flexAPIMy,
				"",
				`"name":"myone"`,
				200,
			},
			{http.MethodGet, rgList + flexAPIPG, "", `"name":"pgone"`, 200},
			{http.MethodGet, px + flexAPIPG, "", `"administratorLogin":"pgadmin"`, 200},
			{http.MethodGet, my + flexAPIMy, "", `"administratorLogin":"myadmin"`, 200},
			{http.MethodGet, px + "/databases" + flexAPIPG, "", `"name":"postgres"`, 200},
			{http.MethodGet, px + "/firewallRules" + flexAPIPG, "", `value`, 200},
			{http.MethodGet, px + "/configurations" + flexAPIPG, "", `"name":"max_connections"`, 200},
			{http.MethodGet, px + "/configurations/max_connections" + flexAPIPG, "", `"value":"150"`, 200},
		}},
		{"children", []gcpStep{
			{
				http.MethodPut,
				px + "/databases/orders" + flexAPIPG,
				`{"properties":{"charset":"UTF8","collation":"en_US.utf8"}}`,
				`"name":"orders"`,
				201,
			},
			{http.MethodGet, px + "/databases" + flexAPIPG, "", `"name":"orders"`, 200},
			{http.MethodDelete, px + "/databases/orders" + flexAPIPG, "", "", http.StatusAccepted},
			{
				http.MethodPut,
				px + "/firewallRules/office" + flexAPIPG,
				`{"properties":{"startIpAddress":"10.0.0.1","endIpAddress":"10.0.0.9"}}`,
				`"startIpAddress":"10.0.0.1"`,
				201,
			},
			{http.MethodGet, px + "/firewallRules" + flexAPIPG, "", `"name":"office"`, 200},
			{http.MethodDelete, px + "/firewallRules/office" + flexAPIPG, "", "", http.StatusAccepted},
		}},
		{"power", []gcpStep{
			{http.MethodPost, px + "/stop" + flexAPIPG, "", "", http.StatusAccepted},
			{http.MethodGet, px + flexAPIPG, "", `"state":"Stopped"`, 200},
			{http.MethodPost, px + "/start" + flexAPIPG, "", "", http.StatusAccepted},
			{
				http.MethodPost,
				px + "/restart" + flexAPIPG,
				`{"restartWithFailover":true,"failoverMode":"x"}`,
				"",
				http.StatusAccepted,
			},
			{http.MethodPost, my + "/restart" + flexAPIMy, "", "", http.StatusAccepted},
		}},
		{"rejects", []gcpStep{
			{http.MethodPut, px + flexAPIPG, `{}`, "unsupported", 400},
			{http.MethodDelete, px + flexAPIPG, "", "unsupported", 400},
			{
				http.MethodPut,
				px + "/configurations/max_connections" + flexAPIPG,
				`{"properties":{"value":"1"}}`,
				"unsupported",
				400,
			},
			{http.MethodPost, px + "/failover" + flexAPIPG, "", "unsupported", 400},
			{http.MethodPost, px + "/resetAdminPassword" + flexAPIPG, `{}`, "unsupported", 400},
			{http.MethodPost, px + "/administrators/x" + flexAPIPG, `{}`, "unsupported", 400},
			{http.MethodGet, px + "/administrators" + flexAPIPG, "", "unsupported", 400},
			{http.MethodGet, px + "/databases/../x" + flexAPIPG, "", "unsupported", 400},
			{http.MethodPut, subList + flexAPIPG, `{}`, "unsupported", 400},
			{
				http.MethodPut,
				px + "/databases/bad" + flexAPIPG,
				`{"properties":{"charset":"UTF8; DROP"}}`,
				"invalid",
				400,
			},
			{
				http.MethodPut,
				px + "/firewallRules/bad" + flexAPIPG,
				`{"properties":{"startIpAddress":"nope","endIpAddress":"1.1.1.1"}}`,
				"invalid",
				400,
			},
			{http.MethodPut, px + "/firewallRules/bad" + flexAPIPG, `not json`, "invalid", 400},
			{
				http.MethodGet,
				flexPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.Compute/virtualMachines" + flexAPIPG,
				"",
				"unsupported",
				400,
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newFlexDashboard(t)

			for i, s := range tt.steps {
				e.advance()

				rec := flexDo(t, e.Echo, s.method, s.path, s.body)
				assert.Equal(t, s.wantCode, rec.Code, "step %d %s %s: %s", i, s.method, s.path, rec.Body.String())
				assert.Contains(t, rec.Body.String(), s.wantBody, "step %d", i)
			}
		})
	}

	t.Run("passwords stay server side", func(t *testing.T) {
		t.Parallel()

		e := newFlexDashboard(t)

		for _, p := range []string{subList + flexAPIPG, rgList + flexAPIPG, px + flexAPIPG, my + flexAPIMy} {
			body := flexDo(t, e.Echo, http.MethodGet, p, "").Body.String()
			assert.NotContains(t, body, flexPxSecret, p)
			assert.NotContains(t, strings.ToLower(body), "loginpassword", p)
		}
	})

	t.Run("write bodies are rebuilt", func(t *testing.T) {
		t.Parallel()

		e := newFlexDashboard(t)
		rec := flexDo(t, e.Echo, http.MethodPut, px+"/databases/extra"+flexAPIPG,
			`{"properties":{"charset":"UTF8","collation":"en_US.utf8"},"id":"evil","type":"x","extra":{"a":1}}`)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

		got := flexDo(t, e.Echo, http.MethodGet, px+"/databases/extra"+flexAPIPG, "").Body.String()
		assert.Contains(t, got, `"charset":"UTF8"`)
		assert.NotContains(t, got, "evil")
	})
}
