package dashboard_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

const (
	agwProject = "agw-proj"
	agwBase    = "/dashboard/api/v1/gcp/apigateway/v1/projects/" + agwProject
	agwGlobal  = agwBase + "/locations/global"
	agwRegion  = agwBase + "/locations/us-central1"
	agwGW      = agwRegion + "/gateways/edge"
	agwKey     = "s3cr3t-api-key"
)

const agwSpec = "swagger: \"2.0\"\ninfo:\n  title: t\n  version: \"1.0\"\nx-google-backend:\n  address: %s\n" +
	"securityDefinitions:\n  api_key:\n    type: apiKey\n    name: key\n    in: query\npaths:\n" +
	"  /open:\n    get:\n      responses:\n        \"200\":\n          description: ok\n" +
	"  /locked:\n    get:\n      security:\n        - api_key: []\n      responses:\n        \"200\":\n" +
	"          description: ok\n"

const agwBadSpec = "swagger: \"2.0\"\ninfo:\n  title: t\n  version: \"1.0\"\npaths:\n  /x:\n    get:\n" +
	"      responses:\n        \"200\":\n          description: ok\n"

func newAPIGatewayDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Echo-Query", r.URL.RawQuery)
		_ = json.NewEncoder(w).Encode(map[string]string{"path": r.URL.Path, "query": r.URL.RawQuery})
	}))
	t.Cleanup(be.Close)

	b := gcpapigateway.NewInMemoryBackend()
	b.SetTiming(gcpapigateway.Timing{})

	_, err := b.CreateAPI(&apigatewaypb.CreateApiRequest{
		Parent: "projects/" + agwProject + "/locations/global", ApiId: "orders",
		Api: &apigatewaypb.Api{DisplayName: "Orders"},
	})
	require.NoError(t, err)

	cfg := func(id, doc string) {
		_, cerr := b.CreateAPIConfig(&apigatewaypb.CreateApiConfigRequest{
			Parent: "projects/" + agwProject + "/locations/global/apis/orders", ApiConfigId: id,
			ApiConfig: &apigatewaypb.ApiConfig{OpenapiDocuments: []*apigatewaypb.ApiConfig_OpenApiDocument{{
				Document: &apigatewaypb.ApiConfig_File{Path: "openapi.yaml", Contents: []byte(doc)},
			}}},
		})
		require.NoError(t, cerr)
	}

	cfg("good", strings.Replace(agwSpec, "%s", be.URL, 1))
	cfg("bad", agwBadSpec)

	_, err = b.CreateGateway(&apigatewaypb.CreateGatewayRequest{
		Parent: "projects/" + agwProject + "/locations/us-central1", GatewayId: "edge",
		Gateway: &apigatewaypb.Gateway{
			ApiConfig: "projects/" + agwProject + "/locations/global/apis/orders/configs/good",
		},
	})
	require.NoError(t, err)

	return dashboard.NewHandler(dashboard.Config{GCPAPIGatewayOps: gcpapigateway.NewHandler(b)})
}

func agwDo(t *testing.T, h *dashboard.DashboardHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	return rec
}

func TestGCPAPIGatewayProxy(t *testing.T) {
	t.Parallel()

	t.Run("reads", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, newAPIGatewayDashboard(t), []gcpStep{
			{http.MethodGet, "/dashboard/api/v1/gcp/apigateway/v1/projects", "", agwProject, 200},
			{http.MethodGet, agwGlobal + "/apis", "", `"displayName":"Orders"`, 200},
			{http.MethodGet, agwGlobal + "/apis/orders", "", `"managedService"`, 200},
			{http.MethodGet, agwGlobal + "/apis/orders/configs", "", `configs/bad"`, 200},
			{http.MethodGet, agwGlobal + "/apis/orders/configs/good", "", `"openapiDocuments"`, 200},
			{http.MethodGet, agwRegion + "/gateways", "", `"defaultHostname"`, 200},
			{http.MethodGet, agwGW, "", `"state":"ACTIVE"`, 200},
			{http.MethodGet, agwRegion + "/operations", "", `"operations"`, 200},
			{http.MethodGet, agwGlobal + "/operations", "", `"diagnostics"`, 200},
			{http.MethodGet, agwBase + "/locations/-/gateways", "", `gateways/edge`, 200},
		})
	})

	t.Run("config view carries the document", func(t *testing.T) {
		t.Parallel()

		rec := agwDo(t, newAPIGatewayDashboard(t), http.MethodGet, agwGlobal+"/apis/orders/configs/good", "")
		require.Equal(t, http.StatusOK, rec.Code)

		var cfg struct {
			Docs []struct {
				Document struct {
					Contents string `json:"contents"`
				} `json:"document"`
			} `json:"openapiDocuments"`
		}

		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
		require.Len(t, cfg.Docs, 1)

		raw, err := base64.StdEncoding.DecodeString(cfg.Docs[0].Document.Contents)
		require.NoError(t, err)
		assert.Contains(t, string(raw), "securityDefinitions")
	})

	t.Run("create and delete a gateway", func(t *testing.T) {
		t.Parallel()

		h := newAPIGatewayDashboard(t)
		cfg := "projects/" + agwProject + "/locations/global/apis/orders/configs/good"

		rec := agwDo(
			t,
			h,
			http.MethodPost,
			agwRegion+"/gateways?gatewayId=second",
			`{"displayName":"Second","apiConfig":"`+cfg+`","labels":{"x":"y"},"name":"projects/evil/locations/x/gateways/z"}`,
		)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		got := agwDo(t, h, http.MethodGet, agwRegion+"/gateways/second", "")
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.Contains(t, got.Body.String(), `"displayName":"Second"`)
		assert.NotContains(t, got.Body.String(), `"x":"y"`)

		del := agwDo(t, h, http.MethodDelete, agwRegion+"/gateways/second", "")
		require.Equal(t, http.StatusOK, del.Code, del.Body.String())
		assert.Contains(t, del.Body.String(), "operations/")
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		cfg := "projects/" + agwProject + "/locations/global/apis/orders/configs/good"
		other := "projects/other/locations/global/apis/orders/configs/good"

		runGCPSteps(t, newAPIGatewayDashboard(t), []gcpStep{
			{http.MethodPost, agwGlobal + "/apis", `{}`, "unsupported", 404},
			{http.MethodDelete, agwGlobal + "/apis/orders", "", "unsupported", 404},
			{http.MethodPatch, agwGW, `{}`, "unsupported", 404},
			{http.MethodPost, agwGW, `{}`, "unsupported", 404},
			{http.MethodPost, agwRegion + "/operations", `{}`, "unsupported", 404},
			{http.MethodGet, agwGlobal + "/apis/orders/configs/good:getIamPolicy", "", "unsupported", 404},
			{http.MethodGet, agwGlobal + "/secrets", "", "unsupported", 404},
			{http.MethodGet, agwBase + "/locations/global/apis/%2e%2e", "", "unsupported", 400},
			{http.MethodPost, agwRegion + "/gateways?gatewayId=g1", `{"apiConfig":"` + other + `"}`, "api_config", 400},
			{http.MethodPost, agwRegion + "/gateways", `{"apiConfig":"` + cfg + `"}`, "api_config", 400},
			{
				http.MethodPost,
				agwRegion + "/gateways?gatewayId=g1",
				`{"apiConfig":"` + cfg + `/../x"}`,
				"api_config",
				400,
			},
			{http.MethodPost, agwRegion + "/gateways?gatewayId=g1", `nope`, "invalid", 400},
		})
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, agwGlobal + "/apis", "", "", http.StatusServiceUnavailable},
		})
	})
}

func TestGCPAPIGatewayTryIt(t *testing.T) {
	t.Parallel()

	type out struct {
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
		Status  float64           `json:"status"`
	}

	try := func(t *testing.T, h *dashboard.DashboardHandler, gw, body string) (*httptest.ResponseRecorder, out) {
		t.Helper()

		rec := agwDo(t, h, http.MethodPost, gw+":invoke", body)

		var o out

		if rec.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &o))
		}

		return rec, o
	}

	t.Run("open route", func(t *testing.T) {
		t.Parallel()

		rec, o := try(t, newAPIGatewayDashboard(t), agwGW, `{"method":"GET","path":"/open","query":"a=1"}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.InDelta(t, 200, o.Status, 0)
		assert.Contains(t, o.Body, `"path":"/open"`)
	})

	t.Run("api key reaches the gateway and never comes back", func(t *testing.T) {
		t.Parallel()

		h := newAPIGatewayDashboard(t)

		_, denied := try(t, h, agwGW, `{"method":"GET","path":"/locked"}`)
		assert.InDelta(t, 401, denied.Status, 0)

		rec, o := try(t, h, agwGW, `{"method":"GET","path":"/locked","query":"key=ignored","apiKey":"`+agwKey+`"}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.InDelta(t, 200, o.Status, 0)
		assert.Contains(t, o.Body, "[redacted]")
		assert.NotContains(t, rec.Body.String(), agwKey)
		assert.NotContains(t, rec.Body.String(), "ignored")
	})

	t.Run("hop-by-hop headers are not forwarded", func(t *testing.T) {
		t.Parallel()

		rec, o := try(t, newAPIGatewayDashboard(t), agwGW, `{"method":"GET","path":"/open","headers":[`+
			`{"name":"Connection","value":"close"},{"name":"X-A","value":"1"}]}`)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, o.Headers, "Connection")
		assert.Contains(t, o.Headers, "X-Echo-Query")
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		h := newAPIGatewayDashboard(t)
		tests := []struct {
			name string
			gw   string
			body string
			want int
		}{
			{"unknown gateway", agwRegion + "/gateways/nope", `{"method":"GET","path":"/open"}`, 404},
			{"bad method", agwGW, `{"method":"TRACE","path":"/open"}`, 400},
			{"relative path", agwGW, `{"method":"GET","path":"open"}`, 400},
			{"absolute url", agwGW, `{"method":"GET","path":"http://evil.example/x"}`, 400},
			{"dot segments", agwGW, `{"method":"GET","path":"/a/../x"}`, 400},
			{"path with query", agwGW, `{"method":"GET","path":"/open?a=b"}`, 400},
			{"fragment", agwGW, `{"method":"GET","path":"/open","query":"a#b"}`, 400},
			{"key with newline", agwGW, `{"method":"GET","path":"/open","apiKey":"a\nb"}`, 400},
			{
				"oversize body",
				agwGW,
				`{"method":"POST","path":"/open","body":"` + strings.Repeat("a", 70000) + `"}`,
				400,
			},
			{"not json", agwGW, `nope`, 400},
			{"gateway traversal", agwRegion + "/gateways/%2e%2e", `{"method":"GET","path":"/open"}`, 404},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				rec := agwDo(t, h, http.MethodPost, tt.gw+":invoke", tt.body)
				assert.Equal(t, tt.want, rec.Code, rec.Body.String())
			})
		}
	})

	t.Run("destination is never taken from the request", func(t *testing.T) {
		t.Parallel()

		h := newAPIGatewayDashboard(t)
		rec := agwDo(t, h, http.MethodPost, agwGW+":invoke",
			`{"method":"GET","path":"/open","host":"evil.example","url":"http://evil.example/"}`)
		require.Equal(t, http.StatusOK, rec.Code)

		b, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		assert.NotContains(t, string(b), "evil.example")
	})
}
