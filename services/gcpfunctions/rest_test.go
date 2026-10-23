package gcpfunctions_test

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
			Name: "list functions", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/functions", Status: 200,
			Contains: []string{`{`},
		},
		{
			Name: "get missing function", Method: http.MethodGet, Path: "/v2/projects/p/locations/l/functions/nope",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
