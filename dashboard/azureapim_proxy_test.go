package dashboard_test

import (
	"context"
	"encoding/json"
	"io"
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
	apimPxBase   = "/dashboard/api/v1/azure/apim"
	apimSvcPath  = "/subscriptions/" + armSub + "/resourceGroups/demo/providers/Microsoft.ApiManagement/service/gw1"
	apimVer      = "?api-version=2024-05-01"
	apimNamedVal = "top-secret-named-value"
)

type apimEnv struct {
	dash *echo.Echo
	key  string
	key2 string
}

func newAPIMDashboard(t *testing.T) *apimEnv {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := svc.(*azurearm.Handler)
	require.True(t, ok)
	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	pr, found := armH.Registry.ProviderNamed("Microsoft.ApiManagement")
	require.True(t, found)

	prov, isAPIM := pr.(*azurearm.APIMProvider)
	require.True(t, isAPIM)

	var skew atomic.Int64

	prov.SetClock(func() time.Time { return time.Now().Add(time.Duration(skew.Load())) })

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Echo-Key", r.Header.Get("Ocp-Apim-Subscription-Key"))
		_ = json.NewEncoder(w).Encode(map[string]any{"path": r.URL.Path, "header": r.Header, "query": r.URL.RawQuery})
	}))
	t.Cleanup(be.Close)

	arm := echo.New()
	arm.Any("/*", armH.Handler())

	put := func(path, body string) {
		rec := flexDo(t, arm, http.MethodPut, path, body)
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())
	}

	put("/subscriptions/"+armSub+"/resourceGroups/demo?api-version=2021-04-01", `{"location":"westus"}`)
	put(apimSvcPath+apimVer, `{"location":"westus","sku":{"name":"Developer","capacity":1},`+
		`"properties":{"publisherEmail":"ops@example.com","publisherName":"Ops"}}`)
	skew.Add(int64(time.Hour))

	put(apimSvcPath+"/apis/echo"+apimVer, `{"properties":{"displayName":"Echo","path":"echo","serviceUrl":"`+be.URL+
		`/base","protocols":["https"],"subscriptionRequired":true}}`)
	put(apimSvcPath+"/apis/plain"+apimVer, `{"properties":{"displayName":"Plain","path":"plain","serviceUrl":"`+be.URL+
		`","protocols":["http"],"subscriptionRequired":false}}`)
	put(apimSvcPath+"/apis/echo/operations/get-item"+apimVer,
		`{"properties":{"displayName":"Get item","method":"GET","urlTemplate":"/items/{id}"}}`)
	put(apimSvcPath+"/apis/plain/operations/x"+apimVer,
		`{"properties":{"displayName":"x","method":"GET","urlTemplate":"/x"}}`)
	put(apimSvcPath+"/subscriptions/k1"+apimVer, `{"properties":{"displayName":"k1","scope":"/apis","state":"active"}}`)
	put(apimSvcPath+"/subscriptions/off"+apimVer,
		`{"properties":{"displayName":"off","scope":"/apis","state":"suspended"}}`)
	put(apimSvcPath+"/namedValues/hush"+apimVer,
		`{"properties":{"displayName":"hush","value":"`+apimNamedVal+`","secret":true}}`)
	put(apimSvcPath+"/namedValues/shown"+apimVer, `{"properties":{"displayName":"shown","value":"visible-value"}}`)
	put(apimSvcPath+"/policies/policy"+apimVer, `{"properties":{"format":"rawxml","value":`+
		`"<policies><inbound><base /></inbound><backend><base /></backend><outbound><base /></outbound>`+
		`<on-error><base /></on-error></policies>"}}`)

	rec := flexDo(t, arm, http.MethodPost, apimSvcPath+"/subscriptions/k1/listSecrets"+apimVer, `{}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var keys struct {
		Primary   string `json:"primaryKey"`
		Secondary string `json:"secondaryKey"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &keys))
	require.NotEmpty(t, keys.Primary)

	dash := echo.New()
	dash.Any("/*", dashboard.NewHandler(dashboard.Config{AzureARMOps: armH}).Handler())

	return &apimEnv{dash: dash, key: keys.Primary, key2: keys.Secondary}
}

func TestAzureAPIMProxyReads(t *testing.T) {
	t.Parallel()

	env := newAPIMDashboard(t)
	px := apimPxBase + apimSvcPath

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			"service list",
			apimPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.ApiManagement/service" + apimVer,
			`"gatewayUrl"`,
		},
		{"service", px + apimVer, `"name":"gw1"`},
		{"apis", px + "/apis" + apimVer, `"displayName":"Echo"`},
		{"api", px + "/apis/echo" + apimVer, `"subscriptionRequired":true`},
		{"operations", px + "/apis/echo/operations" + apimVer, `"urlTemplate":"/items/{id}"`},
		{"subscriptions", px + "/subscriptions" + apimVer, `"state":"active"`},
		{"subscription", px + "/subscriptions/k1" + apimVer, `"displayName":"k1"`},
		{"named values", px + "/namedValues" + apimVer, `"displayName":"shown"`},
		{"service policy", px + "/policies/policy" + apimVer + "&format=rawxml", `"format":"rawxml"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := flexDo(t, env.dash, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}

	t.Run("secrets stay server-side", func(t *testing.T) {
		t.Parallel()

		for _, p := range []string{"/subscriptions", "/subscriptions/k1", "/namedValues", "/namedValues/hush", ""} {
			rec := flexDo(t, env.dash, http.MethodGet, px+p+apimVer, "")
			assert.NotContains(t, rec.Body.String(), env.key, p)
			assert.NotContains(t, rec.Body.String(), env.key2, p)
			assert.NotContains(t, rec.Body.String(), apimNamedVal, p)
			assert.NotContains(t, rec.Body.String(), "primaryKey", p)
		}

		rec := flexDo(t, env.dash, http.MethodGet, px+"/namedValues"+apimVer, "")
		assert.Contains(t, rec.Body.String(), `"isSecret":true`)
		assert.Contains(t, rec.Body.String(), "visible-value")
	})
}

func TestAzureAPIMProxyRejects(t *testing.T) {
	t.Parallel()

	env := newAPIMDashboard(t)
	px := apimPxBase + apimSvcPath

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"list secrets via post", http.MethodPost, px + "/subscriptions/k1/listSecrets" + apimVer, `{}`, 400},
		{"list secrets via get", http.MethodGet, px + "/subscriptions/k1/listSecrets" + apimVer, "", 400},
		{"list value", http.MethodPost, px + "/namedValues/hush/listValue" + apimVer, `{}`, 400},
		{"put service", http.MethodPut, px + apimVer, `{}`, 405},
		{"delete api", http.MethodDelete, px + "/apis/echo" + apimVer, "", 405},
		{"backends", http.MethodGet, px + "/backends" + apimVer, "", 400},
		{
			"other provider",
			http.MethodGet,
			apimPxBase + armBase + "Microsoft.Storage/storageAccounts" + apimVer,
			"",
			400,
		},
		{"traversal", http.MethodGet, px + "/apis/%2e%2e" + apimVer, "", 400},
		{"encoded slash", http.MethodGet, px + "/apis/a%2fb" + apimVer, "", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := flexDo(t, env.dash, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), env.key)
		})
	}
}

func TestAzureAPIMTryIt(t *testing.T) {
	t.Parallel()

	env := newAPIMDashboard(t)
	invoke := apimPxBase + apimSvcPath + "/invoke"

	type out struct {
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
		Status  float64           `json:"status"`
	}

	try := func(t *testing.T, body string) (*httptest.ResponseRecorder, out) {
		t.Helper()

		rec := flexDo(t, env.dash, http.MethodPost, invoke, body)

		var o out

		if rec.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &o))
		}

		return rec, o
	}

	t.Run("subscription key is injected server-side", func(t *testing.T) {
		t.Parallel()

		rec, o := try(t, `{"method":"GET","path":"/echo/items/7","subscription":"k1","api":"echo",`+
			`"headers":[{"name":"Ocp-Apim-Subscription-Key","value":"user-supplied"}]}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.InDelta(t, 200, o.Status, 0, o.Body)
		assert.Contains(t, o.Body, "[redacted]")
		assert.Contains(t, o.Headers["X-Echo-Key"], "[redacted]")
		assert.NotContains(t, rec.Body.String(), env.key)
		assert.NotContains(t, rec.Body.String(), "user-supplied")
	})

	t.Run("without a subscription the gateway refuses", func(t *testing.T) {
		t.Parallel()

		rec, o := try(t, `{"method":"GET","path":"/echo/items/7","api":"echo"}`)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.InDelta(t, 401, o.Status, 0)
	})

	t.Run("http-only api is replayed without tls", func(t *testing.T) {
		t.Parallel()

		_, o := try(t, `{"method":"GET","path":"/plain/x","api":"plain"}`)
		assert.InDelta(t, 200, o.Status, 0, o.Body)
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			path string
			body string
			want int
		}{
			{"unknown subscription", invoke, `{"method":"GET","path":"/echo/x","subscription":"nope"}`, 404},
			{"suspended subscription", invoke, `{"method":"GET","path":"/echo/x","subscription":"off"}`, 409},
			{"unknown service", apimPxBase + strings.Replace(apimSvcPath, "gw1", "other", 1) + "/invoke",
				`{"method":"GET","path":"/echo/x"}`, 404},
			{"bad method", invoke, `{"method":"TRACE","path":"/echo/x"}`, 400},
			{"relative path", invoke, `{"method":"GET","path":"echo/x"}`, 400},
			{"absolute url", invoke, `{"method":"GET","path":"http://evil.example/x"}`, 400},
			{"dot segments", invoke, `{"method":"GET","path":"/echo/../x"}`, 400},
			{"path with query", invoke, `{"method":"GET","path":"/echo/x?a=b"}`, 400},
			{"bad subscription id", invoke, `{"method":"GET","path":"/echo/x","subscription":"../x"}`, 400},
			{
				"oversize body",
				invoke,
				`{"method":"POST","path":"/echo/x","body":"` + strings.Repeat("a", 70000) + `"}`,
				400,
			},
			{"not json", invoke, `nope`, 400},
			{"wrong verb", apimPxBase + apimSvcPath + "/listSecrets", `{}`, 400},
			{"wrong provider", apimPxBase + armBase + "Microsoft.Storage/storageAccounts/x/invoke", `{}`, 400},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				rec := flexDo(t, env.dash, http.MethodPost, tt.path, tt.body)
				assert.Equal(t, tt.want, rec.Code, rec.Body.String())
				assert.NotContains(t, rec.Body.String(), env.key)
			})
		}
	})

	t.Run("unavailable without arm", func(t *testing.T) {
		t.Parallel()

		e := echo.New()
		e.Any("/*", dashboard.NewHandler(dashboard.Config{}).Handler())

		rec := flexDo(t, e, http.MethodPost, invoke, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		_, err := io.Copy(io.Discard, rec.Body)
		require.NoError(t, err)
	})
}
