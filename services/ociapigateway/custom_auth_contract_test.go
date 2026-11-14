package ociapigateway_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

func TestCustomAuthenticatorContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		path string
		resp ociapigateway.FunctionResponse
		want int
	}{
		{
			name: "scope string", path: "/scoped",
			resp: ociapigateway.FunctionResponse{Status: 200, Body: []byte(`{"active":true,"scope":"read admin"}`)},
			want: 200,
		},
		{
			name: "scope array", path: "/scoped",
			resp: ociapigateway.FunctionResponse{Status: 200, Body: []byte(`{"active":true,"scope":["admin"]}`)},
			want: 200,
		},
		{
			name: "wrong scope", path: "/scoped",
			resp: ociapigateway.FunctionResponse{Status: 200, Body: []byte(`{"active":true,"scope":"read"}`)},
			want: 403,
		},
		{
			name: "function 5xx", path: "/x",
			resp: ociapigateway.FunctionResponse{Status: 500, Body: []byte(`{"active":true}`)},
			want: http.StatusBadGateway,
		},
		{name: "invoke error", path: "/x", err: errBoom, want: http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetFunctionInvoker(&fakeInvoker{
				fn: func(string, ociapigateway.FunctionRequest) (ociapigateway.FunctionResponse, error) {
					return tt.resp, tt.err
				},
			})

			gw := e.mkGateway(t, "custom-contract")
			e.mkDeployment(t, *gw.Id, "/a",
				`{"requestPolicies":{"authentication":{"type":"CUSTOM_AUTHENTICATION","functionId":"`+testFunction+
					`","tokenHeader":"Authorization"}},"routes":[`+stock("/x", `"GET"`, "ok")+`,
		{"path":"/scoped","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200},
		 "requestPolicies":{"authorization":{"type":"ANY_OF","allowedScope":["admin"]}}}]}`)

			got := e.call(t, *gw.Id, http.MethodGet, "/a"+tt.path, map[string]string{"Authorization": "t"}, "")
			assert.Equal(t, tt.want, got.status)
		})
	}
}

func TestValidationFailurePolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetFunctionInvoker(&fakeInvoker{
		fn: func(string, ociapigateway.FunctionRequest) (ociapigateway.FunctionResponse, error) {
			return ociapigateway.FunctionResponse{Status: 200, Body: []byte(`{"active":false}`)}, nil
		},
	})

	gw := e.mkGateway(t, "failure-policy")
	e.mkDeployment(t, *gw.Id, "/a",
		`{"requestPolicies":{"authentication":{"type":"CUSTOM_AUTHENTICATION","functionId":"`+testFunction+
			`","tokenHeader":"Authorization","validationFailurePolicy":{"type":"MODIFY_RESPONSE",`+
			`"responseCode":"418","responseMessage":"nope","responseHeaderTransformations":`+
			`{"setHeaders":{"items":[{"name":"X-Denied","values":["yes"]}]}}}}},"routes":[`+
			stock("/x", `"GET"`, "ok")+`]}`)

	got := e.call(t, *gw.Id, http.MethodGet, "/a/x", map[string]string{"Authorization": "t"}, "")
	assert.Equal(t, http.StatusTeapot, got.status)
	assert.Equal(t, "yes", got.header.Get("X-Denied"))
	assert.Contains(t, got.body, "nope")
}

func TestValidationFailurePolicyRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{"oauth2", `{"type":"OAUTH2"}`},
		{"bad code", `{"type":"MODIFY_RESPONSE","responseCode":"abc"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			gw := e.mkGateway(t, "failure-reject")
			_, err := e.createDeployment(t, *gw.Id, "/a", specFromJSON(t,
				`{"requestPolicies":{"authentication":{"type":"CUSTOM_AUTHENTICATION","functionId":"`+testFunction+
					`","tokenHeader":"Authorization","validationFailurePolicy":`+tt.policy+`}},"routes":[`+
					stock("/x", `"GET"`, "ok")+`]}`))
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
