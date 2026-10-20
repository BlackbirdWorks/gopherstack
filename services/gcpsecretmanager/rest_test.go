package gcpsecretmanager_test

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
			Name: "create secret", Method: http.MethodPost, Path: "/v1/projects/p/secrets?secretId=s1",
			Body: `{"replication":{"automatic":{}}}`, Status: 200, Contains: []string{`"name":"projects/p/secrets/s1"`},
		},
		{
			Name: "add version", Method: http.MethodPost, Path: "/v1/projects/p/secrets/s1:addVersion",
			Body: `{"payload":{"data":"aGVsbG8="}}`, Status: 200, Contains: []string{`secrets/s1/versions/1`},
		},
		{
			Name: "access version", Method: http.MethodGet, Path: "/v1/projects/p/secrets/s1/versions/latest:access",
			Status: 200, Contains: []string{`"data":"aGVsbG8="`},
		},
		{Name: "list", Method: http.MethodGet, Path: "/v1/projects/p/secrets", Status: 200, Contains: []string{`s1`}},
		{
			Name: "missing", Method: http.MethodGet, Path: "/v1/projects/p/secrets/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`, `"code":404`},
		},
		{
			Name:     "delete",
			Method:   http.MethodDelete,
			Path:     "/v1/projects/p/secrets/s1",
			Status:   200,
			Contains: []string{`{}`},
		},
	})
}
