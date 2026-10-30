package gcpapigateway_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func op(method, extra string) string {
	return "    " + method + ":\n" + extra + okResponses
}

func TestRoutingAndPathTranslation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		root      string
		top       string
		paths     string
		method    string
		target    string
		wantPath  string
		wantQuery string
		wantMsg   string
		wantCode  int
	}{
		{
			name: "append is the root default", root: "BE/base",
			paths:  "  /hello/{name}:\n" + op("get", ""),
			method: "GET", target: "/hello/world?x=1", wantPath: "/base/hello/world", wantQuery: "x=1", wantCode: 200,
		},
		{
			name:   "constant address moves path params to the query",
			root:   "BE/fn\n  path_translation: CONSTANT_ADDRESS",
			paths:  "  /hello/{name}:\n" + op("get", ""),
			method: "GET", target: "/hello/world", wantPath: "/fn", wantQuery: "name=world", wantCode: 200,
		},
		{
			name: "operation backend defaults to constant address", root: "BE/root",
			paths:  "  /a/{id}:\n" + op("get", "      x-google-backend:\n        address: BE/op\n"),
			method: "GET", target: "/a/7?q=1", wantPath: "/op", wantQuery: "id=7&q=1", wantCode: 200,
		},
		{
			name: "operation backend can append", root: "BE/root",
			paths: "  /a/{id}:\n" + op("get", "      x-google-backend:\n        address: BE/op\n"+
				"        path_translation: APPEND_PATH_TO_ADDRESS\n"),
			method: "GET", target: "/a/7", wantPath: "/op/a/7", wantCode: 200,
		},
		{
			name: "base path is part of the appended path", root: "BE/base", top: "basePath: /v1\n",
			paths:  "  /hello:\n" + op("get", ""),
			method: "GET", target: "/v1/hello", wantPath: "/base/v1/hello", wantCode: 200,
		},
		{
			name: "literal beats template", root: "BE", top: "",
			paths:  "  /items/{id}:\n" + op("get", "") + "  /items/special:\n" + op("get", ""),
			method: "GET", target: "/items/special", wantPath: "/items/special", wantCode: 200,
		},
		{
			name: "template matches other values", root: "BE", top: "",
			paths:  "  /items/{id}:\n" + op("get", "") + "  /items/special:\n" + op("get", ""),
			method: "GET", target: "/items/42", wantPath: "/items/42", wantCode: 200,
		},
		{
			name: "undefined path", root: "BE", paths: "  /hello:\n" + op("get", ""),
			method: "GET", target: "/nope", wantCode: 404,
			wantMsg: "The current request is not defined by this API.",
		},
		{
			name:     "undefined method",
			root:     "BE",
			paths:    "  /hello:\n" + op("get", ""),
			method:   "POST",
			target:   "/hello",
			wantCode: 405,
			wantMsg:  `The current request is matched to the defined url template "/hello" but its http method is not allowed`,
		},
		{
			name: "undefined method keeps the base path in the template", root: "BE", top: "basePath: /v1\n",
			paths:  "  /items/{id}:\n" + op("get", ""),
			method: "DELETE", target: "/v1/items/3", wantCode: 405,
			wantMsg: `The current request is matched to the defined url template "/v1/items/{id}" ` +
				`but its http method is not allowed`,
		},
		{
			name: "base path mismatch", root: "BE", top: "basePath: /v1\n", paths: "  /hello:\n" + op("get", ""),
			method: "GET", target: "/hello", wantCode: 404,
			wantMsg: "The current request is not defined by this API.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := echoBackend(t)
			e := newEnv(t)
			gw := e.deploy(t, strings.ReplaceAll(opSpec(tt.root, tt.top, tt.paths), "BE", be.URL))

			got := e.call(t, gw, tt.method, tt.target, nil)
			require.Equal(t, tt.wantCode, got.status, got.raw)

			if tt.wantCode != http.StatusOK {
				assert.EqualValues(t, tt.wantCode, got.body["code"])
				assert.Equal(t, tt.wantMsg, got.body["message"])

				return
			}

			assert.Equal(t, tt.wantPath, got.body["path"])
			assert.Equal(t, tt.wantQuery, got.body["query"])
		})
	}
}

func TestPathStyleURLAndGatewayResolution(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	e := newEnv(t)
	_ = e.deploy(t, opSpec(be.URL, "", "  /hello:\n"+op("get", "")))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+e.handler.Addr()+"/_apigateway/"+project+"/"+region+"/my-gw/hello", nil)
	require.NoError(t, err)

	got := e.do(t, req)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "/hello", got.body["path"])

	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+e.handler.Addr()+"/_apigateway/"+project+"/"+region+"/ghost/hello", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, e.do(t, req).status)

	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+e.handler.Addr()+"/hello", nil)
	require.NoError(t, err)
	req.Host = "other-abc.uc.gateway.dev"
	assert.Equal(t, http.StatusNotFound, e.do(t, req).status, "control-plane REST handles unknown hosts")
}

func TestForwardedHeadersAndBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backendOpts string
		wantAuth    string
		wantFwdAuth string
	}{
		{"authorization is replaced", "", "", "Bearer user-token"},
		{"disable auth keeps authorization", "  disable_auth: true\n", "Bearer user-token", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := echoBackend(t)
			e := newEnv(t)
			gw := e.deploy(t, opSpec(be.URL+"\n"+strings.TrimRight(tt.backendOpts, "\n"), "",
				"  /echo:\n"+op("post", "")))

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+e.handler.Addr()+"/echo",
				strings.NewReader("payload"))
			require.NoError(t, err)

			req.Host = gw.GetDefaultHostname()
			req.Header.Set("Authorization", "Bearer user-token")
			req.Header.Set("X-Apigateway-Api-Userinfo", "forged")

			got := e.do(t, req)
			require.Equal(t, http.StatusOK, got.status, got.raw)

			hdr, ok := got.body["headers"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "payload", got.body["body"])
			assert.Equal(t, tt.wantAuth, hdr["Authorization"])
			assert.Equal(t, tt.wantFwdAuth, hdr["X-Forwarded-Authorization"])
			assert.Empty(t, hdr["X-Apigateway-Api-Userinfo"], "client supplied userinfo is dropped")
		})
	}
}

func TestCloudRunLocalhostBackend(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	port := be.URL[strings.LastIndex(be.URL, ":")+1:]

	e := newEnv(t)
	gw := e.deploy(t, opSpec("http://svc-abc123-uc.localhost:"+port, "", "  /hello:\n"+op("get", "")))

	got := e.call(t, gw, http.MethodGet, "/hello", nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "/hello", got.body["path"])
}

func TestExternalBackendRefusedByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCodes []int
		allow     bool
	}{
		{name: "refused by default", allow: false, wantCodes: []int{http.StatusBadGateway}},
		{
			name: "opt in dials out", allow: true,
			wantCodes: []int{http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetAllowExternal(tt.allow)

			gw := e.deploy(t, opSpec("http://192.0.2.1:9\n  deadline: 0.2", "", "  /hello:\n"+op("get", "")))

			got := e.call(t, gw, http.MethodGet, "/hello", nil)
			assert.Contains(t, tt.wantCodes, got.status, got.raw)
		})
	}
}

func TestBackendTimeout(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	gw := e.deploy(t, opSpec("http://127.0.0.1:1\n  deadline: 0.5", "", "  /hello:\n"+op("get", "")))

	got := e.call(t, gw, http.MethodGet, "/hello", nil)
	assert.Equal(t, http.StatusServiceUnavailable, got.status, "connection refused is a connect error")
}

func TestGatewayNotServing(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, "", "  /hello:\n"+op("get", "")))

	require.Equal(t, http.StatusOK, e.call(t, gw, http.MethodGet, "/hello", nil).status)

	delOp, err := e.client.DeleteGateway(t.Context(), deleteGatewayRequest(gw.GetName()))
	require.NoError(t, err)
	require.NoError(t, delOp.Wait(t.Context()))

	assert.Equal(t, http.StatusNotFound, e.call(t, gw, http.MethodGet, "/hello", nil).status)
}
