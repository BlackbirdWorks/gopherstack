package dashboard_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

const (
	gwComp = "ocid1.compartment.oc1..gw"
	gwBase = "/dashboard/api/v1/oci/apigateway"
	gwAPI  = gwBase + "/api"
)

func gwCert(t *testing.T) (string, string) {
	t.Helper()

	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "api.example.test"},
		DNSNames: []string{
			"api.example.test",
		}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	require.NoError(t, err)

	kb, err := x509.MarshalECPrivateKey(k)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
}

func newGatewayDashboard(t *testing.T) (*dashboard.DashboardHandler, string, string) {
	t.Helper()

	b := ociapigateway.NewInMemoryBackend()
	b.SetTransition(0)

	ops := ociapigateway.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	post := func(path string, body map[string]any) []byte {
		raw, err := json.Marshal(body)
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw))))
		require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())

		return rec.Body.Bytes()
	}

	var gw struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(post("/20190501/gateways", map[string]any{
		"compartmentId": gwComp, "endpointType": "PUBLIC", "displayName": "edge", "subnetId": "ocid1.subnet.oc1..s",
	}), &gw))

	var dep struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(post("/20190501/deployments", map[string]any{
		"compartmentId": gwComp, "gatewayId": gw.ID, "pathPrefix": "/v1", "displayName": "orders",
		"specification": map[string]any{"routes": []map[string]any{{
			"path": "/hello", "methods": []string{"GET"},
			"backend": map[string]any{"type": "STOCK_RESPONSE_BACKEND", "status": 200, "body": "hi there"},
		}}},
	}), &dep))

	certPEM, keyPEM := gwCert(t)
	post("/20190501/certificates", map[string]any{
		"compartmentId": gwComp, "displayName": "tls", "certificate": certPEM, "privateKey": keyPEM,
	})

	return dashboard.NewHandler(dashboard.Config{OCIAPIGatewayOps: ops}), gw.ID, keyPEM
}

func gwTry(t *testing.T, h *dashboard.DashboardHandler, id, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, gwAPI+"/gateways/"+id+"/invoke", strings.NewReader(body))
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	return rec
}

func TestOCIAPIGatewayProxy(t *testing.T) {
	t.Parallel()

	cq := "?compartmentId=" + gwComp

	t.Run("read", func(t *testing.T) {
		t.Parallel()

		h, id, key := newGatewayDashboard(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, gwBase + "/compartments", "", gwComp, 200},
			{http.MethodGet, gwAPI + "/gateways" + cq, "", `"displayName":"edge"`, 200},
			{http.MethodGet, gwAPI + "/gateways/" + id, "", `"hostname"`, 200},
			{http.MethodGet, gwAPI + "/deployments" + cq + "&gatewayId=" + id, "", `"pathPrefix":"/v1"`, 200},
			{http.MethodGet, gwAPI + "/certificates" + cq, "", `"api.example.test"`, 200},
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, gwAPI+"/certificates"+cq, nil)
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.NotContains(t, rec.Body.String(), "PRIVATE KEY")
		assert.NotContains(t, rec.Body.String(), strings.TrimSpace(strings.Split(key, "\n")[1]))
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		h, id, _ := newGatewayDashboard(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, gwAPI + "/gateways", `{}`, "unsupported", 400},
			{http.MethodPost, gwAPI + "/gateways/" + id, `{}`, "unsupported", 400},
			{http.MethodPost, gwAPI + "/gateways/" + id + "/actions/changeCompartment", `{}`, "unsupported", 400},
			{http.MethodGet, gwAPI + "/certificates/c1", "", "unsupported", 400},
			{http.MethodGet, gwAPI + "/workRequests", "", "unsupported", 400},
			{http.MethodGet, gwAPI + "/gateways/%2e%2e", "", "unsupported", 400},
			{http.MethodPost, gwAPI + "/deployments", `{}`, "unsupported", 400},
		})
	})

	t.Run("try it", func(t *testing.T) {
		t.Parallel()

		h, id, _ := newGatewayDashboard(t)

		rec := gwTry(t, h, id, `{"method":"GET","path":"/v1/hello","headers":[{"name":"X-Test","value":"1"},`+
			`{"name":"Host","value":"evil.example"},{"name":"Connection","value":"close"}]}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var out struct {
			Headers  map[string]string `json:"headers"`
			Body     string            `json:"body"`
			Encoding string            `json:"encoding"`
			Status   int               `json:"status"`
		}

		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, 200, out.Status)
		assert.Equal(t, "hi there", out.Body)
		assert.Equal(t, "utf8", out.Encoding)
		assert.NotContains(t, out.Headers, "Connection")

		miss := gwTry(t, h, id, `{"method":"GET","path":"/v1/nope"}`)
		assert.Contains(t, miss.Body.String(), `"status":404`)
	})

	t.Run("try it rejections", func(t *testing.T) {
		t.Parallel()

		h, id, _ := newGatewayDashboard(t)

		tests := []struct {
			name string
			id   string
			body string
			want int
		}{
			{"unknown gateway", "ocid1.apigateway.oc1..nope", `{"method":"GET","path":"/v1/hello"}`, 404},
			{"bad method", id, `{"method":"TRACE","path":"/v1/hello"}`, 400},
			{"relative path", id, `{"method":"GET","path":"v1/hello"}`, 400},
			{"absolute url", id, `{"method":"GET","path":"http://evil.example/x"}`, 400},
			{"dot segments", id, `{"method":"GET","path":"/v1/../x"}`, 400},
			{"encoded slash", id, `{"method":"GET","path":"/v1%2fhello"}`, 400},
			{"path with query", id, `{"method":"GET","path":"/v1/hello?a=b"}`, 400},
			{"fragment", id, `{"method":"GET","path":"/v1/hello","query":"a=b#x"}`, 400},
			{"gateway id traversal", "%2e%2e", `{"method":"GET","path":"/v1/hello"}`, 400},
			{
				"oversize body",
				id,
				`{"method":"POST","path":"/v1/hello","body":"` + strings.Repeat("a", 70000) + `"}`,
				400,
			},
			{"not json", id, `nope`, 400},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.want, gwTry(t, h, tt.id, tt.body).Code)
			})
		}
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, gwAPI + "/gateways", "", "", http.StatusServiceUnavailable},
		})
	})
}
