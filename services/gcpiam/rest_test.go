package gcpiam_test

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
			Name: "create service account", Method: http.MethodPost, Path: "/v1/projects/p/serviceAccounts",
			Body: `{"accountId":"tester1","serviceAccount":{"displayName":"Test"}}`, Status: 200,
			Contains: []string{`"email":"tester1@p.iam.gserviceaccount.com"`},
		},
		{
			Name: "get service account", Method: http.MethodGet,
			Path: "/v1/projects/p/serviceAccounts/tester1@p.iam.gserviceaccount.com", Status: 200,
			Contains: []string{`"displayName":"Test"`},
		},
		{
			Name: "get missing", Method: http.MethodGet,
			Path: "/v1/projects/p/serviceAccounts/nope@p.iam.gserviceaccount.com", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
