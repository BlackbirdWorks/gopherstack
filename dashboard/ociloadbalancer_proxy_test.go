package dashboard_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

const (
	lbComp = "ocid1.compartment.oc1..lb"
	lbBase = "/dashboard/api/v1/oci/loadbalancer"
	lbAPI  = lbBase + "/api"

	lbCreateHead = `{"compartmentId":"c","displayName":"d","subnetId":"s"`
)

type lbFixture struct {
	h     *dashboard.DashboardHandler
	id    string
	key   string
	addrs []string
}

func lbCert(t *testing.T) (string, string) {
	t.Helper()

	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "lb.example.test"},
		DNSNames:  []string{"lb.example.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	require.NoError(t, err)

	kb, err := x509.MarshalECPrivateKey(k)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
}

func lbBackendServer(t *testing.T, name string) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", name)
		_, _ = fmt.Fprintf(w, "%s %s host=%s", name, r.URL.Path, r.Host)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	return u.Host
}

func newLBDashboard(t *testing.T) lbFixture {
	t.Helper()

	be := ociloadbalancer.NewInMemoryBackend()
	be.SetTransition(0)
	t.Cleanup(be.Close)

	ops := ociloadbalancer.NewHandler(be)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	certPEM, keyPEM := lbCert(t)

	backends := make([]map[string]any, 0, 2)
	addrs := make([]string, 0, 2)

	for _, n := range []string{"alpha", "beta"} {
		host := lbBackendServer(t, n)
		addrs = append(addrs, host)

		ip, port, _ := strings.Cut(host, ":")

		var p int

		_, err := fmt.Sscanf(port, "%d", &p)
		require.NoError(t, err)

		backends = append(backends, map[string]any{"ipAddress": ip, "port": p})
	}

	raw, err := json.Marshal(map[string]any{
		"compartmentId": lbComp, "displayName": "edge", "shapeName": "flexible", "isPrivate": false,
		"shapeDetails": map[string]any{"minimumBandwidthInMbps": 10, "maximumBandwidthInMbps": 100},
		"subnetIds":    []string{"ocid1.subnet.oc1..s"},
		"certificates": map[string]any{"tls": map[string]any{"publicCertificate": certPEM, "privateKey": keyPEM}},
		"backendSets": map[string]any{"web": map[string]any{
			"policy": "ROUND_ROBIN", "backends": backends,
			"healthChecker": map[string]any{"protocol": "HTTP", "urlPath": "/", "intervalInMillis": 1000},
		}},
		"listeners": map[string]any{
			"http": map[string]any{"defaultBackendSetName": "web", "protocol": "HTTP", "port": 80},
			"tls": map[string]any{
				"defaultBackendSetName": "web", "protocol": "HTTP", "port": 443,
				"sslConfiguration": map[string]any{"certificateName": "tls"},
			},
		},
	})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/20170115/loadBalancers", strings.NewReader(string(raw))))
	require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())

	h := dashboard.NewHandler(dashboard.Config{OCILoadBalancerOps: ops})
	f := lbFixture{h: h, key: keyPEM, addrs: addrs}

	list := lbDo(t, h, http.MethodGet, lbAPI+"/loadBalancers?compartmentId="+lbComp, "")

	var items []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &items), list.Body.String())
	require.Len(t, items, 1)
	f.id = items[0].ID

	return f
}

func lbDo(t *testing.T, h *dashboard.DashboardHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	return rec
}

func lbTry(t *testing.T, f lbFixture, id, listener, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, http.MethodPost, lbAPI+"/loadBalancers/"+id+"/listeners/"+listener+"/invoke", body)
}

func TestOCILoadBalancerProxyRead(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)
	lb := lbAPI + "/loadBalancers/" + f.id

	runGCPSteps(t, f.h, []gcpStep{
		{http.MethodGet, lbBase + "/compartments", "", lbComp, 200},
		{http.MethodGet, lb, "", `"gopherstackAddress":"127.0.0.1:`, 200},
		{http.MethodGet, lb + "/health", "", `"totalBackendSetCount":1`, 200},
		{http.MethodGet, lb + "/workRequests", "", "CreateLoadBalancer", 200},
		{http.MethodGet, lb + "/backendSets/web/health", "", `"totalBackendCount":2`, 200},
		{
			http.MethodGet,
			lb + "/backendSets/web/backends/" + url.PathEscape(f.addrs[0]) + "/health",
			"",
			`"status"`,
			200,
		},
	})

	for _, path := range []string{lb, lb + "/certificates"} {
		rec := lbDo(t, f.h, http.MethodGet, lbAPI+strings.TrimPrefix(path, lbAPI), "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `"lb.example.test"`)
		assert.Contains(t, rec.Body.String(), `"notAfter"`)
		assert.NotContains(t, rec.Body.String(), "PRIVATE KEY")
		assert.NotContains(t, rec.Body.String(), "BEGIN CERTIFICATE")
		assert.NotContains(t, rec.Body.String(), "publicCertificate")
		assert.NotContains(t, rec.Body.String(), strings.TrimSpace(strings.Split(f.key, "\n")[1]))
	}
}

func TestOCILoadBalancerProxyRejects(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)
	lb := lbAPI + "/loadBalancers/" + f.id

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"unknown collection", http.MethodGet, lbAPI + "/shapes", ""},
		{"update load balancer", http.MethodPut, lb, `{"displayName":"x"}`},
		{"change compartment", http.MethodPost, lb + "/changeCompartment", `{}`},
		{"update shape", http.MethodPut, lb + "/updateShape", `{}`},
		{"delete backend set", http.MethodDelete, lb + "/backendSets/web", ""},
		{"create backend set", http.MethodPost, lb + "/backendSets", `{}`},
		{"create listener", http.MethodPost, lb + "/listeners", `{}`},
		{"delete certificate", http.MethodDelete, lb + "/certificates/tls", ""},
		{"create certificate", http.MethodPost, lb + "/certificates", `{}`},
		{"update health checker", http.MethodPut, lb + "/backendSets/web/healthChecker", `{}`},
		{"get one backend", http.MethodGet, lb + "/backendSets/web/backends/a", ""},
		{"dot segments", http.MethodGet, lbAPI + "/loadBalancers/%2e%2e", ""},
		{"encoded slash", http.MethodGet, lbAPI + "/loadBalancers/a%2fb", ""},
		{"bad create body", http.MethodPost, lbAPI + "/loadBalancers", `nope`},
		{"create without name", http.MethodPost, lbAPI + "/loadBalancers", `{"compartmentId":"c","subnetId":"s"}`},
		{
			"create bandwidth too high", http.MethodPost, lbAPI + "/loadBalancers",
			lbCreateHead + `,"minimumBandwidthInMbps":10,"maximumBandwidthInMbps":9000}`,
		},
		{
			"create bad port", http.MethodPost, lbAPI + "/loadBalancers",
			lbCreateHead + `,"listenerPort":70000}`,
		},
		{"backend bad ip", http.MethodPost, lb + "/backendSets/web/backends", `{"ipAddress":"nope","port":80}`},
		{"backend bad port", http.MethodPost, lb + "/backendSets/web/backends", `{"ipAddress":"127.0.0.1","port":0}`},
		{
			"backend zero weight",
			http.MethodPost,
			lb + "/backendSets/web/backends",
			`{"ipAddress":"127.0.0.1","port":80,"weight":0}`,
		},
		{"update missing flags", http.MethodPut, lb + "/backendSets/web/backends/x", `{"weight":2}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestOCILoadBalancerProxyActions(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)
	lb := lbAPI + "/loadBalancers/" + f.id
	set := lb + "/backendSets/web"
	name := url.PathEscape(f.addrs[0])

	add := lbDo(t, f.h, http.MethodPost, set+"/backends",
		`{"ipAddress":"127.0.0.1","port":9,"weight":2,"junk":"ignored","name":"forged"}`)
	require.Less(t, add.Code, http.StatusBadRequest, add.Body.String())

	got := lbDo(t, f.h, http.MethodGet, lb, "")
	assert.Contains(t, got.Body.String(), `"name":"127.0.0.1:9"`)

	upd := lbDo(
		t,
		f.h,
		http.MethodPut,
		set+"/backends/"+name,
		`{"weight":3,"backup":false,"drain":true,"offline":true}`,
	)
	require.Less(t, upd.Code, http.StatusBadRequest, upd.Body.String())

	var detail struct {
		BackendSets map[string]struct {
			Backends []struct {
				Name    string `json:"name"`
				Weight  int    `json:"weight"`
				Drain   bool   `json:"drain"`
				Offline bool   `json:"offline"`
			} `json:"backends"`
		} `json:"backendSets"`
	}

	require.NoError(t, json.Unmarshal(lbDo(t, f.h, http.MethodGet, lb, "").Body.Bytes(), &detail))

	var found bool

	for _, b := range detail.BackendSets["web"].Backends {
		if b.Name == f.addrs[0] {
			found = true

			assert.Equal(t, 3, b.Weight)
			assert.True(t, b.Drain)
			assert.True(t, b.Offline)
		}
	}

	assert.True(t, found)

	del := lbDo(t, f.h, http.MethodDelete, set+"/backends/"+url.PathEscape("127.0.0.1:9"), "")
	require.Less(t, del.Code, http.StatusBadRequest, del.Body.String())
	assert.NotContains(t, lbDo(t, f.h, http.MethodGet, lb, "").Body.String(), `"name":"127.0.0.1:9"`)

	create := lbDo(t, f.h, http.MethodPost, lbAPI+"/loadBalancers",
		`{"compartmentId":"`+lbComp+`","displayName":"second","subnetId":"ocid1.subnet.oc1..s","listenerPort":8080,`+
			`"minimumBandwidthInMbps":10,"maximumBandwidthInMbps":10,"isDeleteProtectionEnabled":true,"shapeName":"100Mbps"}`)
	require.Less(t, create.Code, http.StatusBadRequest, create.Body.String())

	list := lbDo(t, f.h, http.MethodGet, lbAPI+"/loadBalancers?compartmentId="+lbComp+"&displayName=second", "")

	var items []struct {
		ID              string `json:"id"`
		Shape           string `json:"shapeName"`
		DeleteProtected bool   `json:"isDeleteProtectionEnabled"`
	}

	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &items), list.Body.String())
	require.Len(t, items, 1)
	assert.Equal(t, "flexible", items[0].Shape)
	assert.False(t, items[0].DeleteProtected)

	require.Less(
		t,
		lbDo(t, f.h, http.MethodDelete, lbAPI+"/loadBalancers/"+items[0].ID, "").Code,
		http.StatusBadRequest,
	)
}

func TestOCILoadBalancerProxyTryItRoundRobin(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)
	seen := map[string]int{}

	for range 4 {
		rec := lbTry(t, f, f.id, "http", `{"method":"GET","path":"/hello","query":"a=1",`+
			`"headers":[{"name":"Host","value":"evil.example"},{"name":"Connection","value":"close"}]}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var out struct {
			Headers map[string]string `json:"headers"`
			Body    string            `json:"body"`
			Status  int               `json:"status"`
		}

		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, 200, out.Status)
		assert.NotContains(t, out.Headers, "Connection")
		assert.NotContains(t, out.Body, "evil.example")
		seen[out.Headers["X-Backend"]]++
	}

	assert.Equal(t, map[string]int{"alpha": 2, "beta": 2}, seen)
}

func TestOCILoadBalancerProxyTryItVirtualHost(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)
	rec := lbTry(t, f, f.id, "http", `{"method":"GET","path":"/","virtualHost":"app.example.test"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "host=app.example.test")
}

func TestOCILoadBalancerProxyTryItRejects(t *testing.T) {
	t.Parallel()

	f := newLBDashboard(t)

	tests := []struct {
		name     string
		id       string
		listener string
		body     string
		want     int
	}{
		{"unknown load balancer", "ocid1.loadbalancer.oc1..nope", "http", `{"method":"GET","path":"/"}`, 404},
		{"unknown listener", f.id, "nope", `{"method":"GET","path":"/"}`, 404},
		{"tls listener", f.id, "tls", `{"method":"GET","path":"/"}`, 409},
		{"bad method", f.id, "http", `{"method":"TRACE","path":"/"}`, 400},
		{"relative path", f.id, "http", `{"method":"GET","path":"x"}`, 400},
		{"absolute url", f.id, "http", `{"method":"GET","path":"http://evil.example/x"}`, 400},
		{"dot segments", f.id, "http", `{"method":"GET","path":"/a/../x"}`, 400},
		{"encoded slash", f.id, "http", `{"method":"GET","path":"/a%2fb"}`, 400},
		{"path with query", f.id, "http", `{"method":"GET","path":"/a?b=c"}`, 400},
		{"fragment", f.id, "http", `{"method":"GET","path":"/","query":"a=b#x"}`, 400},
		{"host with port", f.id, "http", `{"method":"GET","path":"/","virtualHost":"evil.example:80"}`, 400},
		{"host with slash", f.id, "http", `{"method":"GET","path":"/","virtualHost":"a/b"}`, 400},
		{"id traversal", "%2e%2e", "http", `{"method":"GET","path":"/"}`, 400},
		{
			"oversize body",
			f.id,
			"http",
			`{"method":"POST","path":"/","body":"` + strings.Repeat("a", 70000) + `"}`,
			400,
		},
		{"not json", f.id, "http", `nope`, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, lbTry(t, f, tt.id, tt.listener, tt.body).Code)
		})
	}
}

func TestOCILoadBalancerProxyUnavailable(t *testing.T) {
	t.Parallel()

	runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
		{http.MethodGet, lbAPI + "/loadBalancers?compartmentId=c", "", "", http.StatusServiceUnavailable},
		{http.MethodGet, lbBase + "/compartments", "", "", http.StatusServiceUnavailable},
	})
}
