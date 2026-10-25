package dashboard_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func functionAppZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("hello/function.json")
	require.NoError(t, err)
	_, err = w.Write([]byte(`{"bindings":[{"type":"httpTrigger","authLevel":"anonymous","direction":"in","name":"req"}]}`))
	require.NoError(t, err)
	w, err = zw.Create("tick/function.json")
	require.NoError(t, err)
	_, err = w.Write([]byte(`{"bindings":[{"type":"timerTrigger","schedule":"0 */5 * * * *","name":"t"}]}`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestAzureFunctionsProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/functions"

	cfg := func(t *testing.T) dashboard.Config {
		t.Helper()

		b := azurefunctions.NewInMemoryBackend()
		b.PutApp("app1", azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "NODE|20"})
		require.NoError(t, b.SetSettings("app1", map[string]string{"FOO": "super-secret", "BAR": "x"}))
		_, err := b.Deploy("app1", functionAppZip(t), "zipdeploy", "first")
		require.NoError(t, err)

		return dashboard.Config{AzureFunctionsOps: azurefunctions.NewHandler(b, azurefunctions.Settings{})}
	}

	tests := []struct {
		name      string
		method    string
		path      string
		body      string
		after     string
		wantBody  string
		notWant   string
		wantAfter string
		wantCode  int
		served    bool
	}{
		{
			name: "apps", method: http.MethodGet, path: base + "/apps", served: true,
			wantCode: http.StatusOK, wantBody: `"name":"app1"`,
		},
		{
			name: "settings masked", method: http.MethodGet, path: base + "/apps/app1/settings", served: true,
			wantCode: http.StatusOK, wantBody: `"name":"BAR"`, notWant: "super-secret",
		},
		{
			name: "reveal", method: http.MethodPost, path: base + "/apps/app1/settings/reveal",
			body: `{"name":"FOO"}`, served: true, wantCode: http.StatusOK, wantBody: "super-secret",
		},
		{
			name: "reveal missing", method: http.MethodPost, path: base + "/apps/app1/settings/reveal",
			body: `{"name":"NOPE"}`, served: true, wantCode: http.StatusNotFound, wantBody: "NOT_FOUND",
		},
		{
			name: "reveal bad body", method: http.MethodPost, path: base + "/apps/app1/settings/reveal",
			body: `{`, served: true, wantCode: http.StatusBadRequest, wantBody: "INVALID_ARGUMENT",
		},
		{
			name: "functions", method: http.MethodGet, path: base + "/apps/app1/functions", served: true,
			wantCode: http.StatusOK, wantBody: `"authLevel":"anonymous"`,
		},
		{
			name: "functions invoke url", method: http.MethodGet, path: base + "/apps/app1/functions", served: true,
			wantCode: http.StatusOK, wantBody: `/api/hello`,
		},
		{
			name: "functions timer trigger", method: http.MethodGet, path: base + "/apps/app1/functions",
			served: true, wantCode: http.StatusOK, wantBody: `"triggerType":"timerTrigger"`,
		},
		{
			name: "deployments", method: http.MethodGet, path: base + "/apps/app1/deployments", served: true,
			wantCode: http.StatusOK, wantBody: `"message":"first"`,
		},
		{
			name: "stop", method: http.MethodPost, path: base + "/apps/app1/stop", served: true,
			wantCode: http.StatusNoContent, after: base + "/apps", wantAfter: `"state":"Stopped"`,
		},
		{
			name: "restart", method: http.MethodPost, path: base + "/apps/app1/restart", served: true,
			wantCode: http.StatusNoContent, after: base + "/apps", wantAfter: `"state":"Running"`,
		},
		{
			name: "start", method: http.MethodPost, path: base + "/apps/app1/start", served: true,
			wantCode: http.StatusNoContent, after: base + "/apps", wantAfter: `"state":"Running"`,
		},
		{
			name: "unknown action", method: http.MethodPost, path: base + "/apps/app1/format", served: true,
			wantCode: http.StatusBadRequest, wantBody: "INVALID_ARGUMENT",
		},
		{
			name: "missing app", method: http.MethodGet, path: base + "/apps/nope/functions", served: true,
			wantCode: http.StatusNotFound, wantBody: "NOT_FOUND",
		},
		{
			name: "missing app action", method: http.MethodPost, path: base + "/apps/nope/stop", served: true,
			wantCode: http.StatusNotFound, wantBody: "NOT_FOUND",
		},
		{
			name: "not served", method: http.MethodGet, path: base + "/apps",
			wantCode: http.StatusServiceUnavailable, wantBody: "UNAVAILABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := dashboard.Config{}
			if tt.served {
				conf = cfg(t)
			}

			h := dashboard.NewHandler(conf)
			call := func(method, path, body string) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

				return rec
			}

			rec := call(tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			if tt.notWant != "" {
				assert.NotContains(t, rec.Body.String(), tt.notWant)
			}

			if tt.after != "" {
				assert.Contains(t, call(http.MethodGet, tt.after, "").Body.String(), tt.wantAfter)
			}
		})
	}
}
