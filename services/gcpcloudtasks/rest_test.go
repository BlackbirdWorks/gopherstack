package gcpcloudtasks_test

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
			Name: "create queue", Method: http.MethodPost, Path: "/v2/projects/p/locations/l/queues",
			Body: `{"name":"projects/p/locations/l/queues/q"}`, Status: 200,
			Contains: []string{`"name":"projects/p/locations/l/queues/q"`},
		},
		{
			Name: "get queue", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/queues/q", Status: 200,
			Contains: []string{`"state":"RUNNING"`},
		},
		{
			Name: "get missing queue", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/queues/nope",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
