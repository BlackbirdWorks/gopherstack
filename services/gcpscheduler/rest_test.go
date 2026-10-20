package gcpscheduler_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create job", Method: http.MethodPost, Path: "/v1/projects/p/locations/l/jobs",
			Body: `{"name":"projects/p/locations/l/jobs/j","schedule":"* * * * *",` +
				`"httpTarget":{"uri":"http://example.invalid/","httpMethod":"GET"}}`,
			Status: 200, Contains: []string{`"name":"projects/p/locations/l/jobs/j"`},
		},
		{
			Name: "pause job", Method: http.MethodPost, Path: "/v1/projects/p/locations/l/jobs/j:pause", Body: `{}`,
			Status: 200, Contains: []string{`"state":"PAUSED"`},
		},
		{
			Name: "get missing job", Method: http.MethodGet, Path: "/v1/projects/p/locations/l/jobs/nope",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
