package gcpapigateway_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	const (
		apiPath = "/v1/projects/test-project/locations/global/apis/rest-api"
		gwPath  = "/v1/projects/test-project/locations/us-central1/gateways/rest-gw"
		spec    = "c3dhZ2dlcjogIjIuMCIKaW5mbzoKICB0aXRsZTogdAogIHZlcnNpb246ICIxIgp4LWdvb2dsZS1iYWNrZW5kOgogIG" +
			"FkZHJlc3M6IGh0dHBzOi8vYi5leGFtcGxlCnBhdGhzOgogIC9oZWxsbzoKICAgIGdldDoKICAgICAgcmVzcG9uc2Vz" +
			"OgogICAgICAgICIyMDAiOgogICAgICAgICAgZGVzY3JpcHRpb246IG9rCg=="
	)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create api", Method: http.MethodPost,
			Path: "/v1/projects/test-project/locations/global/apis?apiId=rest-api", Body: `{"displayName":"REST"}`,
			Status: 200, Contains: []string{`"done":true`, `"managedService"`, `OperationMetadata`},
		},
		{
			Name: "get api", Method: http.MethodGet, Path: apiPath,
			Status: 200, Contains: []string{`"displayName":"REST"`, `"state":"ACTIVE"`},
		},
		{
			Name: "create config", Method: http.MethodPost, Path: apiPath + "/configs?apiConfigId=rest-cfg",
			Body:   `{"openapiDocuments":[{"document":{"path":"openapi.yaml","contents":"` + spec + `"}}]}`,
			Status: 200, Contains: []string{`"done":true`, `rest-cfg`},
		},
		{
			Name: "create gateway", Method: http.MethodPost,
			Path:   "/v1/projects/test-project/locations/us-central1/gateways?gatewayId=rest-gw",
			Body:   `{"apiConfig":"projects/test-project/locations/global/apis/rest-api/configs/rest-cfg"}`,
			Status: 200, Contains: []string{`"done":true`, `.uc.gateway.dev`},
		},
		{
			Name: "full config view", Method: http.MethodGet, Path: apiPath + "/configs/rest-cfg?view=FULL",
			Status: 200, Contains: []string{`"openapiDocuments"`, spec},
		},
		{
			Name: "patch gateway", Method: http.MethodPatch, Path: gwPath + "?updateMask=labels",
			Body:   `{"labels":{"a":"b"}}`,
			Status: 200, Contains: []string{`"done":true`, `"a":"b"`},
		},
		{
			Name: "set iam", Method: http.MethodPost, Path: gwPath + ":setIamPolicy",
			Body:   `{"policy":{"bindings":[{"role":"roles/apigateway.viewer","members":["user:a@example.com"]}]}}`,
			Status: 200, Contains: []string{`roles/apigateway.viewer`},
		},
		{
			Name: "get iam", Method: http.MethodPost, Path: gwPath + ":getIamPolicy", Body: `{}`,
			Status: 200, Contains: []string{`user:a@example.com`},
		},
		{
			Name:     "test iam",
			Method:   http.MethodPost,
			Path:     gwPath + ":testIamPermissions",
			Body:     `{"permissions":["apigateway.gateways.get"]}`,
			Status:   200,
			Contains: []string{`apigateway.gateways.get`},
		},
		{
			Name:     "list operations",
			Method:   http.MethodGet,
			Path:     "/v1/projects/test-project/locations/global/operations",
			Status:   200,
			Contains: []string{`"operations"`, `operation-`},
		},
		{
			Name: "list locations", Method: http.MethodGet, Path: "/v1/projects/test-project/locations",
			Status: 200, Contains: []string{`"locationId":"global"`, `"locationId":"us-central1"`},
		},
		{
			Name: "missing api", Method: http.MethodGet, Path: apiPath + "-ghost",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "invalid api id", Method: http.MethodPost,
			Path: "/v1/projects/test-project/locations/global/apis?apiId=Bad_Id", Body: `{}`,
			Status: 400, Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "config in use", Method: http.MethodDelete, Path: apiPath + "/configs/rest-cfg",
			Status: 400, Contains: []string{`"status":"FAILED_PRECONDITION"`},
		},
		{
			Name: "delete gateway", Method: http.MethodDelete, Path: gwPath,
			Status: 200, Contains: []string{`"done":true`},
		},
	})
}
