package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azureappconfig"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
	"github.com/blackbirdworks/gopherstack/services/gcplogging"
	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

type proxyCase struct {
	name     string
	method   string
	path     string
	body     string
	wantBody string
	wantCode int
}

func runProxyCases(t *testing.T, cfg func() dashboard.Config, tests []proxyCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(cfg())
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureEventHubsProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/eventhubs"

	cfg := func() dashboard.Config {
		b := azureeventhubs.NewInMemoryBackend()
		require.NoError(t, b.EnsureNamespace("ns1"))
		_, err := b.CreateHub(azureeventhubs.HubRef{Namespace: "ns1", Hub: "hub1"},
			azureeventhubs.HubConfig{PartitionCount: 2, Retention: azureeventhubs.DefaultRetention})
		require.NoError(t, err)

		return dashboard.Config{AzureEventHubsOps: azureeventhubs.NewHandler(b)}
	}

	runProxyCases(t, cfg, []proxyCase{
		{name: "namespaces", method: http.MethodGet, path: base + "/namespaces", wantCode: 200, wantBody: `"hub1"`},
		{
			name: "partitions", method: http.MethodGet, path: base + "/namespaces/ns1/hubs/hub1/partitions",
			wantCode: 200, wantBody: `"empty":true`,
		},
		{
			name: "send", method: http.MethodPost, path: base + "/namespaces/ns1/hubs/hub1/events",
			body: `{"body":"hi","partition":1}`, wantCode: 200, wantBody: `"partition":1`,
		},
		{
			name: "send bad partition", method: http.MethodPost, path: base + "/namespaces/ns1/hubs/hub1/events",
			body: `{"body":"hi","partition":9}`, wantCode: 404, wantBody: "NOT_FOUND",
		},
		{
			name: "send bad json", method: http.MethodPost, path: base + "/namespaces/ns1/hubs/hub1/events",
			body: `{`, wantCode: 400, wantBody: "INVALID_ARGUMENT",
		},
		{
			name: "missing hub", method: http.MethodGet, path: base + "/namespaces/ns1/hubs/nope/partitions",
			wantCode: 404, wantBody: "NOT_FOUND",
		},
		{
			name: "bad partition id", method: http.MethodGet,
			path:     base + "/namespaces/ns1/hubs/hub1/partitions/x/events",
			wantCode: 400, wantBody: "invalid partition id",
		},
		{
			name: "empty peek", method: http.MethodGet, path: base + "/namespaces/ns1/hubs/hub1/partitions/0/events",
			wantCode: 200, wantBody: `"events":[]`,
		},
	})
}

func TestAzureEventHubsProxyPeek(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/eventhubs/namespaces/ns1/hubs/hub1"

	b := azureeventhubs.NewInMemoryBackend()
	require.NoError(t, b.EnsureNamespace("ns1"))
	_, err := b.CreateHub(azureeventhubs.HubRef{Namespace: "ns1", Hub: "hub1"},
		azureeventhubs.HubConfig{PartitionCount: 1, Retention: azureeventhubs.DefaultRetention})
	require.NoError(t, err)

	h := dashboard.NewHandler(dashboard.Config{AzureEventHubsOps: azureeventhubs.NewHandler(b)})

	for _, body := range []string{"first", "second"} {
		rec := httptest.NewRecorder()
		payload := `{"body":"` + body + `","partitionKey":"k"}`
		req := httptest.NewRequest(http.MethodPost, base+"/events", strings.NewReader(payload))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, base+"/partitions/0/events?limit=1", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"body":"second"`)
	assert.NotContains(t, rec.Body.String(), "first")
	assert.Contains(t, rec.Body.String(), `"partitionKey":"k"`)
}

func TestAzureEventGridProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/eventgrid"

	cfg := func() dashboard.Config {
		b := azureeventgrid.NewInMemoryBackend()
		_, err := b.PutTopic("topic1", "/subscriptions/s/topics/topic1", "")
		require.NoError(t, err)

		return dashboard.Config{AzureEventGridOps: azureeventgrid.NewHandler(b, azureeventgrid.Settings{})}
	}

	const event = `{"id":"1","subject":"/s","eventType":"T","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1"}`

	runProxyCases(t, cfg, []proxyCase{
		{name: "topics", method: http.MethodGet, path: base + "/topics", wantCode: 200, wantBody: `"topic1"`},
		{
			name: "publish", method: http.MethodPost, path: base + "/topics/topic1/events",
			body: event, wantCode: 200, wantBody: `"published":1`,
		},
		{
			name: "publish array", method: http.MethodPost, path: base + "/topics/topic1/events",
			body: "[" + event + "," + event + "]", wantCode: 200, wantBody: `"published":2`,
		},
		{
			name: "publish invalid", method: http.MethodPost, path: base + "/topics/topic1/events",
			body: `{"id":"1"}`, wantCode: 400, wantBody: "required property",
		},
		{
			name: "publish missing topic", method: http.MethodPost, path: base + "/topics/nope/events",
			body: event, wantCode: 404, wantBody: "NOT_FOUND",
		},
	})
}

func TestAzureAppConfigProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/appconfig"

	cfg := func() dashboard.Config {
		b := azureappconfig.NewInMemoryBackend()
		_, err := b.PutStore("store1")
		require.NoError(t, err)

		v := "v1"
		for _, label := range []string{"", "prod"} {
			_, err = b.SetKeyValue(
				"store1", "app/key", label, azureappconfig.KVInput{Value: &v}, azureappconfig.Precondition{},
			)
			require.NoError(t, err)
		}

		return dashboard.Config{AzureAppConfigOps: azureappconfig.NewHandler(b, azureappconfig.Settings{})}
	}

	runProxyCases(t, cfg, []proxyCase{
		{name: "stores", method: http.MethodGet, path: base + "/stores", wantCode: 200, wantBody: `"store1"`},
		{name: "list", method: http.MethodGet, path: base + "/stores/store1/kv", wantCode: 200, wantBody: `"app/key"`},
		{
			name: "label filter", method: http.MethodGet, path: base + "/stores/store1/kv?label=prod",
			wantCode: 200, wantBody: `"label":"prod"`,
		},
		{
			name: "labels", method: http.MethodGet, path: base + "/stores/store1/labels",
			wantCode: 200, wantBody: `["prod"]`,
		},
		{
			name: "put", method: http.MethodPut, path: base + "/stores/store1/kv",
			body: `{"key":"k2","label":"dev","value":"x"}`, wantCode: 200, wantBody: `"k2"`,
		},
		{
			name: "put invalid key", method: http.MethodPut, path: base + "/stores/store1/kv",
			body: `{"key":"..","value":"x"}`, wantCode: 400, wantBody: "INVALID_ARGUMENT",
		},
		{
			name: "delete", method: http.MethodDelete, path: base + "/stores/store1/kv?key=app/key&label=prod",
			wantCode: 204,
		},
		{
			name: "missing store", method: http.MethodGet, path: base + "/stores/nope/kv",
			wantCode: 404, wantBody: "NOT_FOUND",
		},
	})
}

func TestGCPSQLProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/gcp/sql"

	cfg := func() dashboard.Config {
		return dashboard.Config{GCPSQLOps: gcpsql.NewHandler(gcpsql.NewInMemoryBackend())}
	}

	runProxyCases(t, cfg, []proxyCase{
		{name: "list", method: http.MethodGet, path: base + "/projects/p1/instances", wantCode: 200},
		{name: "missing instance", method: http.MethodGet, path: base + "/projects/p1/instances/nope", wantCode: 404},
		{
			name:     "databases",
			method:   http.MethodGet,
			path:     base + "/projects/p1/instances/nope/databases",
			wantCode: 404,
		},
		{name: "users", method: http.MethodGet, path: base + "/projects/p1/instances/nope/users", wantCode: 404},
		{
			name:     "backups blocked",
			method:   http.MethodGet,
			path:     base + "/projects/p1/instances/i/backupRuns",
			wantCode: 400,
		},
		{name: "tiers blocked", method: http.MethodGet, path: base + "/projects/p1/tiers", wantCode: 400},
		{name: "traversal", method: http.MethodGet, path: base + "/projects/p1/instances/../x", wantCode: 400},
		{name: "bad project", method: http.MethodGet, path: base + "/projects/-bad/instances", wantCode: 400},
		{name: "too deep", method: http.MethodGet, path: base + "/projects/p1/instances/i/users/u/x", wantCode: 400},
		{name: "writes rejected", method: http.MethodPost, path: base + "/projects/p1/instances", wantCode: 405},
	})
}

func TestGCPLoggingProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/gcp/logging/v1"

	cfg := func() dashboard.Config {
		return dashboard.Config{GCPLoggingOps: gcplogging.NewHandler(gcplogging.NewInMemoryBackend())}
	}

	write := `{"logName":"projects/p1/logs/app","resource":{"type":"global"},` +
		`"entries":[{"severity":"ERROR","textPayload":"boom"}]}`
	list := `{"resourceNames":["projects/p1"],"filter":"severity>=ERROR"}`

	runProxyCases(t, cfg, []proxyCase{
		{name: "write", method: http.MethodPost, path: base + "/entries:write", body: write, wantCode: 200},
		{name: "list", method: http.MethodPost, path: base + "/entries:list", body: list, wantCode: 200},
		{
			name:     "list bad filter",
			method:   http.MethodPost,
			path:     base + "/entries:list",
			body:     `{"resourceNames":["projects/p1"],"filter":"((("}`,
			wantCode: 400,
		},
		{name: "list bad body", method: http.MethodPost, path: base + "/entries:list", body: `{`, wantCode: 400},
		{name: "sinks", method: http.MethodGet, path: base + "/projects/p1/sinks", wantCode: 200, wantBody: "_Default"},
		{
			name:     "delete sink unsupported",
			method:   http.MethodDelete,
			path:     base + "/projects/p1/sinks/_Default",
			wantCode: 404,
		},
		{name: "unknown verb", method: http.MethodPost, path: base + "/entries:tail", body: `{}`, wantCode: 404},
		{name: "get entries", method: http.MethodGet, path: base + "/entries:list", wantCode: 404},
	})
}

func TestCloudExtrasProxiesDisabled(t *testing.T) {
	t.Parallel()

	base := "/dashboard/api/v1/"

	for _, path := range []string{
		"azure/eventhubs/namespaces", "azure/eventgrid/topics", "azure/appconfig/stores",
		"gcp/sql/projects/p1/instances", "gcp/logging/v1/projects/p1/sinks",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, base+path, nil), rec)))
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}
}

func TestDashboardRedirectKeepsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "cloud param", url: "/dashboard?cloud=gcp", want: "/dashboard/?cloud=gcp"},
		{name: "no query", url: "/dashboard", want: "/dashboard/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, tt.url, nil), rec)))
			assert.Equal(t, http.StatusMovedPermanently, rec.Code)
			assert.Equal(t, tt.want, rec.Header().Get("Location"))
		})
	}
}
