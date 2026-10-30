package azurearm_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type echoReply struct {
	Header  map[string][]string `json:"header"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   string              `json:"query"`
	Body    string              `json:"body"`
	BodyLen int                 `json:"bodyLen"`
}

func newEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Backend", "yes")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echoReply{
			Method:  r.Method,
			Path:    r.URL.Path,
			Query:   r.URL.RawQuery,
			Header:  r.Header,
			Body:    string(b),
			BodyLen: len(b),
		})
	}))
	t.Cleanup(srv.Close)

	return srv
}

type gwEnv struct {
	*apimStack
	backend *httptest.Server
	key     string
}

// newGWEnv builds a Developer service with API echo (path "echo", backend + /base), operations
// GET /items/{id}, POST /items and GET /static, and an active all-APIs subscription.
func newGWEnv(t *testing.T) *gwEnv {
	t.Helper()

	s := newAPIMStack(t)
	s.devService(t)

	be := newEchoBackend(t)
	s.putAPI(t, "echo", "echo", be.URL+"/base")
	s.putOp(t, "echo", "get-item", "get", "/items/{id}")
	s.putOp(t, "echo", "post-item", "post", "/items")
	s.putOp(t, "echo", "static", "get", "/static")

	_, err := s.f.NewSubscriptionClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "k1",
		armapimanagement.SubscriptionCreateParameters{
			Properties: &armapimanagement.SubscriptionCreateParameterProperties{
				DisplayName: new("k1"), Scope: new("/apis"), State: new(armapimanagement.SubscriptionStateActive),
			},
		}, nil)
	require.NoError(t, err)

	sec, err := s.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, "k1", nil)
	require.NoError(t, err)

	return &gwEnv{apimStack: s, backend: be, key: *sec.PrimaryKey}
}

func (e *gwEnv) call(t *testing.T, method, path, body string, extra map[string]string) (int, string, http.Header) {
	t.Helper()

	hdr := map[string]string{"Ocp-Apim-Subscription-Key": e.key}
	for k, v := range extra {
		if v == "" {
			delete(hdr, k)

			continue
		}

		hdr[k] = v
	}

	return e.gw(t, method, path, body, hdr)
}

func (e *gwEnv) policy(t *testing.T, scope, doc string, wantStatus int) string {
	t.Helper()

	b, err := json.Marshal(map[string]any{"properties": map[string]any{"format": "xml", "value": doc}})
	require.NoError(t, err)

	status, body, _ := e.raw(t, http.MethodPut, e.armPath(scope+"/policies/policy"), string(b), nil)
	require.Equal(t, wantStatus, status, body)

	return body
}

func policyDoc(inbound, backend, outbound string) string {
	if backend == "" {
		backend = "<base />"
	}

	return "<policies><inbound><base />" + inbound + "</inbound><backend>" + backend +
		"</backend><outbound><base />" + outbound + "</outbound><on-error><base /></on-error></policies>"
}

func (r echoReply) keyForwarded(key string) bool {
	return slices.Contains(r.Header["Ocp-Apim-Subscription-Key"], key) ||
		strings.Contains(r.Query, "subscription-key="+key)
}

func decodeEcho(t *testing.T, body string) echoReply {
	t.Helper()

	var r echoReply

	require.NoError(t, json.Unmarshal([]byte(body), &r), body)

	return r
}

const noOperation = "Unable to match incoming request to an operation."

func TestAPIMGateway_SubscriptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		hdr        map[string]string
		wantMsg    string
		wantStatus int
		wantFwd    bool
	}{
		{name: "valid header", method: "GET", path: "/echo/items/7", wantStatus: 200, wantFwd: true},
		{
			name: "valid query", method: "GET", path: "/echo/items/7?subscription-key=KEY",
			hdr: map[string]string{"Ocp-Apim-Subscription-Key": ""}, wantStatus: 200, wantFwd: true,
		},
		{
			name:       "missing",
			method:     "GET",
			path:       "/echo/items/7",
			hdr:        map[string]string{"Ocp-Apim-Subscription-Key": ""},
			wantStatus: 401,
			wantMsg:    "Access denied due to missing subscription key",
		},
		{
			name:       "invalid",
			method:     "GET",
			path:       "/echo/items/7",
			hdr:        map[string]string{"Ocp-Apim-Subscription-Key": "nope"},
			wantStatus: 401,
			wantMsg:    "Access denied due to invalid subscription key",
		},
		{name: "unknown api", method: "GET", path: "/nothing/items/7", wantStatus: 404, wantMsg: noOperation},
		{name: "unknown operation", method: "GET", path: "/echo/other", wantStatus: 404, wantMsg: noOperation},
		{name: "wrong method", method: "DELETE", path: "/echo/items/7", wantStatus: 404, wantMsg: noOperation},
		{name: "post item", method: "POST", path: "/echo/items", wantStatus: 200, wantFwd: true},
		{name: "case insensitive path", method: "GET", path: "/ECHO/Items/9", wantStatus: 200, wantFwd: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)

			path := strings.ReplaceAll(tc.path, "KEY", e.key)
			status, body, hdr := e.call(t, tc.method, path, "", tc.hdr)

			assert.Equal(t, tc.wantStatus, status, body)

			if tc.wantMsg != "" {
				assert.Contains(t, body, tc.wantMsg)
				assert.Equal(t, "application/json", hdr.Get("Content-Type"))
				assert.Contains(t, body, `"statusCode":`)
			}

			if tc.wantStatus == http.StatusOK {
				got := decodeEcho(t, body)
				assert.Equal(t, tc.method, got.Method)
				assert.Equal(t, "yes", hdr.Get("X-Backend"))
				assert.Equal(t, tc.wantFwd, got.keyForwarded(e.key))
			}
		})
	}
}

func TestAPIMGateway_Routing(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	_, body, _ := e.call(t, "GET", "/echo/items/42?x=1&y=2", "", nil)
	got := decodeEcho(t, body)
	assert.Equal(t, "/base/items/42", got.Path)
	assert.Equal(t, "x=1&y=2", got.Query)

	_, body, _ = e.call(t, "POST", "/echo/items", `{"a":1}`, nil)
	got = decodeEcho(t, body)
	assert.JSONEq(t, `{"a":1}`, got.Body)
	assert.NotEmpty(t, got.Header["X-Forwarded-For"])

	status, body, _ := e.raw(t, http.MethodGet, "/echo/items/1", "", map[string]string{
		"Host": "gw1.azure-api.net", "Ocp-Apim-Subscription-Key": e.key,
	})
	assert.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "/base/items/1", decodeEcho(t, body).Path)

	status, _, _ = e.raw(t, http.MethodGet, "/echo/items/1", "", map[string]string{
		"Host": "ghost.azure-api.net", "Ocp-Apim-Subscription-Key": e.key,
	})
	assert.Equal(t, http.StatusNotFound, status)

	status, _, _ = e.raw(t, http.MethodGet, "/_apim/ghost/echo/items/1", "", nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestAPIMGateway_LongestPathAndWildcard(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putAPI(t, "deep", "echo/deep", e.backend.URL+"/deep")
	e.putOp(t, "deep", "all", "get", "/{*rest}")

	status, body, _ := e.call(t, "GET", "/echo/deep/a/b/c", "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "/deep/a/b/c", decodeEcho(t, body).Path)

	status, body, _ = e.call(t, "GET", "/echo/items/3", "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "/base/items/3", decodeEcho(t, body).Path)
}

func TestAPIMGateway_SubscriptionScopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		scope string
		state armapimanagement.SubscriptionState
		link  bool
		want  int
	}{
		{name: "api scope", scope: "/apis/echo", state: "active", want: 200},
		{name: "other api scope", scope: "/apis/other", state: "active", want: 401},
		{name: "product with api", scope: "/products/prod", state: "active", link: true, want: 200},
		{name: "product without api", scope: "/products/prod", state: "active", want: 401},
		{name: "suspended", scope: "/apis/echo", state: "suspended", want: 401},
		{name: "submitted by default", scope: "/apis/echo", state: "", want: 401},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.putAPI(t, "other", "other", "")

			_, err := e.f.NewProductClient().
				CreateOrUpdate(t.Context(), apimRG, apimSvc, "prod", armapimanagement.ProductContract{
					Properties: &armapimanagement.ProductContractProperties{DisplayName: new("Prod")},
				}, nil)
			require.NoError(t, err)

			if tc.link {
				_, err = e.f.NewProductAPIClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "prod", "echo", nil)
				require.NoError(t, err)
			}

			props := &armapimanagement.SubscriptionCreateParameterProperties{
				DisplayName: new("t"), Scope: &tc.scope,
			}
			if tc.state != "" {
				props.State = &tc.state
			}

			_, err = e.f.NewSubscriptionClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "t",
				armapimanagement.SubscriptionCreateParameters{Properties: props}, nil)
			require.NoError(t, err)

			sec, err := e.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, "t", nil)
			require.NoError(t, err)

			status, body, _ := e.gw(t, "GET", "/echo/items/1", "", map[string]string{
				"Ocp-Apim-Subscription-Key": *sec.SecondaryKey,
			})
			assert.Equal(t, tc.want, status, body)
		})
	}
}

func TestAPIMGateway_NoSubscriptionRequired(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	p, err := e.f.NewAPIClient().Update(t.Context(), apimRG, apimSvc, "echo", "*", armapimanagement.APIUpdateContract{
		Properties: &armapimanagement.APIContractUpdateProperties{SubscriptionRequired: new(false)},
	}, nil)
	require.NoError(t, err)
	assert.False(t, *p.Properties.SubscriptionRequired)

	status, _, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Ocp-Apim-Subscription-Key": ""})
	assert.Equal(t, http.StatusOK, status)

	e.putAPI(t, "closed", "closed", e.backend.URL)
	e.putOp(t, "closed", "root", "get", "/")

	_, err = e.f.NewProductClient().
		CreateOrUpdate(t.Context(), apimRG, apimSvc, "open", armapimanagement.ProductContract{
			Properties: &armapimanagement.ProductContractProperties{
				DisplayName: new("Open"), SubscriptionRequired: new(false),
			},
		}, nil)
	require.NoError(t, err)

	status, _, _ = e.call(t, "GET", "/closed", "", map[string]string{"Ocp-Apim-Subscription-Key": ""})
	assert.Equal(t, http.StatusUnauthorized, status)

	_, err = e.f.NewProductAPIClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "open", "closed", nil)
	require.NoError(t, err)

	status, _, _ = e.call(t, "GET", "/closed", "", map[string]string{"Ocp-Apim-Subscription-Key": ""})
	assert.Equal(t, http.StatusOK, status)
}

func TestAPIMGateway_ProtocolsAndCustomKeyNames(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	_, err := e.f.NewAPIClient().Update(t.Context(), apimRG, apimSvc, "echo", "*", armapimanagement.APIUpdateContract{
		Properties: &armapimanagement.APIContractUpdateProperties{
			SubscriptionKeyParameterNames: &armapimanagement.SubscriptionKeyParameterNamesContract{
				Header: new("X-Api-Key"), Query: new("apikey"),
			},
		},
	}, nil)
	require.NoError(t, err)

	status, _, _ := e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, http.StatusUnauthorized, status)

	status, _, _ = e.gw(t, "GET", "/echo/items/1", "", map[string]string{"X-Api-Key": e.key})
	assert.Equal(t, http.StatusOK, status)

	status, _, _ = e.gw(t, "GET", "/echo/items/1?apikey="+e.key, "", nil)
	assert.Equal(t, http.StatusOK, status)

	_, err = e.f.NewAPIClient().Update(t.Context(), apimRG, apimSvc, "echo", "*", armapimanagement.APIUpdateContract{
		Properties: &armapimanagement.APIContractUpdateProperties{
			Protocols: []*armapimanagement.Protocol{new(armapimanagement.ProtocolHTTP)},
		},
	}, nil)
	require.NoError(t, err)

	status, _, _ = e.gw(t, "GET", "/echo/items/1", "", map[string]string{"X-Api-Key": e.key})
	assert.Equal(t, http.StatusNotFound, status)
}

func TestAPIMGateway_ExternalBackendRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{name: "documentation address", url: "http://192.0.2.1:9"},
		{name: "private address", url: "http://10.255.255.1:9"},
		{name: "link local metadata", url: "http://169.254.169.254"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.putAPI(t, "ext", "ext", tc.url)
			e.putOp(t, "ext", "root", "get", "/")

			status, body, _ := e.call(t, "GET", "/ext", "", nil)
			assert.Equal(t, http.StatusBadGateway, status, body)
			assert.Contains(t, body, "outside loopback")
		})
	}

	t.Run("backend entity", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)

		_, err := e.f.NewBackendClient().
			CreateOrUpdate(t.Context(), apimRG, apimSvc, "ext", armapimanagement.BackendContract{
				Properties: &armapimanagement.BackendContractProperties{
					URL: new("http://192.0.2.7"), Protocol: new(armapimanagement.BackendProtocolHTTP),
				},
			}, nil)
		require.NoError(t, err)

		e.policy(t, "/apis/echo", policyDoc(`<set-backend-service backend-id="ext" />`, "", ""), http.StatusCreated)

		status, body, _ := e.call(t, "GET", "/echo/items/1", "", nil)
		assert.Equal(t, http.StatusBadGateway, status, body)
		assert.Contains(t, body, "outside loopback")
	})

	t.Run("opt in permits non-loopback", func(t *testing.T) {
		t.Parallel()

		ip := nonLoopbackIP(t)
		l, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(t, err)

		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("external-ok"))
		}))
		srv.Listener = l
		srv.Start()
		t.Cleanup(srv.Close)

		_, port, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)

		e := newGWEnv(t)
		e.putAPI(t, "ext", "ext", "http://"+net.JoinHostPort(ip, port))
		e.putOp(t, "ext", "root", "get", "/")

		status, body, _ := e.call(t, "GET", "/ext", "", nil)
		assert.Equal(t, http.StatusBadGateway, status, body)

		e.prov.SetAllowExternal(true)

		status, body, _ = e.call(t, "GET", "/ext", "", nil)
		assert.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, "external-ok", body)
	})
}

func nonLoopbackIP(t *testing.T) string {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	require.NoError(t, err)

	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil && !ipn.IP.IsLoopback() {
			return ipn.IP.String()
		}
	}

	t.Skip("no non-loopback IPv4 address on this host")

	return ""
}

func TestAPIMGateway_Persistence(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(
		t,
		"/apis/echo",
		policyDoc(`<set-header name="X-Persisted" exists-action="override"><value>yes</value></set-header>`, "", ""),
		http.StatusCreated,
	)

	data := e.handler.Snapshot(context.Background())
	require.NotEmpty(t, data)

	fresh := newAPIMStack(t)
	require.NoError(t, fresh.handler.Restore(context.Background(), data))
	fresh.clock.Advance(2 * time.Hour)

	got, err := fresh.f.NewServiceClient().Get(t.Context(), apimRG, apimSvc, nil)
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", *got.Properties.ProvisioningState)

	sec, err := fresh.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, "k1", nil)
	require.NoError(t, err)
	assert.Equal(t, e.key, *sec.PrimaryKey)

	status, body, _ := fresh.gw(t, "GET", "/echo/items/5", "", map[string]string{"Ocp-Apim-Subscription-Key": e.key})
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "/base/items/5", decodeEcho(t, body).Path)

	ops := 0

	pager := fresh.f.NewAPIOperationClient().NewListByAPIPager(apimRG, apimSvc, "echo", nil)
	for pager.More() {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		ops += len(page.Value)
	}

	assert.Equal(t, 3, ops)

	empty := newAPIMStack(t)
	require.NoError(t, fresh.handler.Restore(context.Background(), empty.handler.Snapshot(context.Background())))

	_, err = fresh.f.NewServiceClient().Get(t.Context(), apimRG, apimSvc, nil)
	require.Error(t, err)
}
