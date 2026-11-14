package gcprun_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "list services", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/services", Status: 200,
			Contains: []string{`{`},
		},
		{
			Name: "list worker pools", Method: http.MethodGet,
			Path: "/v2/projects/p/locations/l/workerPools", Status: 200,
			Contains: []string{`{`},
		},
		{
			Name: "list instances", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/instances", Status: 200,
			Contains: []string{`{`},
		},
		{
			Name: "get missing worker pool", Method: http.MethodGet,
			Path:   "/v2/projects/p/locations/l/workerPools/nope",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "list locations", Method: http.MethodGet, Path: "/v1/projects/p/locations", Status: 200,
			Contains: []string{`"locationId":"us-central1"`},
		},
		{
			Name: "submit build without cloud build", Method: http.MethodPost,
			Path: "/v2/projects/p/locations/l/builds:submit", Body: `{"storageSource":{"bucket":"b","object":"o"},` +
				`"imageUri":"r/i","dockerBuild":{}}`,
			Status: 400, Contains: []string{`FAILED_PRECONDITION`},
		},
		{
			Name: "get missing service", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/services/nope",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
