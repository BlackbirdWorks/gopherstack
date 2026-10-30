package gcpapigateway_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const corsHost = "api.endpoints.proj.cloud.goog"

func corsTop(allow string) string {
	return "host: " + corsHost + "\nx-google-endpoints:\n- name: " + corsHost + "\n  allowCors: " + allow + "\n"
}

func TestAllowCorsOpenAPI2(t *testing.T) {
	t.Parallel()

	const keyDef = "securityDefinitions:\n  api_key:\n    type: apiKey\n    name: key\n    in: query\n"

	locked := "  /items/{id}:\n" + op("get", "      security:\n        - api_key: []\n")

	tests := []struct {
		name       string
		top        string
		paths      string
		method     string
		target     string
		wantMethod string
		wantStatus int
	}{
		{
			name: "allowCors proxies preflight to the backend", top: corsTop("True"), paths: locked,
			method: http.MethodOptions, target: "/items/7", wantStatus: 200, wantMethod: http.MethodOptions,
		},
		{
			name: "preflight skips the API key requirement", top: keyDef + corsTop("true"), paths: locked,
			method: http.MethodOptions, target: "/items/7", wantStatus: 200, wantMethod: http.MethodOptions,
		},
		{
			name: "allowCors false rejects OPTIONS", top: corsTop("false"), paths: locked,
			method: http.MethodOptions, target: "/items/7", wantStatus: 405,
		},
		{
			name: "no extension rejects OPTIONS", top: "", paths: locked,
			method: http.MethodOptions, target: "/items/7", wantStatus: 405,
		},
		{
			name:   "endpoint name that is not the host does not enable CORS",
			top:    "host: " + corsHost + "\nx-google-endpoints:\n- name: other.cloud.goog\n  allowCors: true\n",
			paths:  locked,
			method: http.MethodOptions, target: "/items/7", wantStatus: 405,
		},
		{
			name: "undefined path stays 404", top: corsTop("true"), paths: locked,
			method: http.MethodOptions, target: "/nope", wantStatus: 404,
		},
		{
			name: "a declared OPTIONS operation wins", top: corsTop("true"),
			paths:  "  /items:\n" + op("get", "") + op("options", "      security:\n        - api_key: []\n"),
			method: http.MethodOptions, target: "/items?key=k", wantStatus: 200, wantMethod: http.MethodOptions,
		},
		{
			name: "other methods are unaffected", top: corsTop("true"), paths: locked,
			method: http.MethodDelete, target: "/items/7", wantStatus: 405,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if strings.Contains(tt.paths, "api_key") && !strings.Contains(tt.top, "securityDefinitions") {
				tt.top = keyDef + tt.top
			}

			be := echoBackend(t)
			e := newEnv(t)
			gw := e.deploy(t, opSpec(be.URL, tt.top, tt.paths))

			got := e.call(t, gw, tt.method, tt.target, nil)
			require.Equal(t, tt.wantStatus, got.status, got.raw)

			if tt.wantMethod != "" {
				assert.Equal(t, tt.wantMethod, got.body["method"])
			}
		})
	}
}

func TestAllowCorsOpenAPI3(t *testing.T) {
	t.Parallel()

	const server = "servers:\n- url: https://api.example.com\n  x-google-endpoint:"

	tests := []struct {
		name       string
		servers    string
		wantStatus int
	}{
		{"allowCors true", server + "\n    allowCors: true\n", 200},
		{"allowCors false", server + "\n    allowCors: false\n", 405},
		{"default is false", server + " {}\n", 405},
		{"no servers entry", "", 405},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := echoBackend(t)
			e := newEnv(t)
			gw := e.deploy(t, spec3(mgmt3(be.URL, ""), tt.servers+"x-google-backend: be\n",
				"  /hello:\n"+op("get", "")))

			got := e.call(t, gw, http.MethodOptions, "/hello", nil)
			require.Equal(t, tt.wantStatus, got.status, got.raw)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, http.MethodOptions, got.body["method"])
			}
		})
	}
}

func TestDefaultHostnameShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	gw := e.deploy(t, opSpec("http://127.0.0.1:1", "", "  /h:\n"+op("get", "")))

	assert.Regexp(t, `^my-gw-[a-z0-9]{18}\.uc\.gateway\.dev$`, gw.GetDefaultHostname())
}
