package gcpfirestore_test

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
			Name: "create document", Method: http.MethodPost,
			Path: "/v1/projects/p/databases/(default)/documents/col?documentId=d1",
			Body: `{"fields":{"a":{"stringValue":"x"}}}`, Status: 200, Contains: []string{`"stringValue":"x"`},
		},
		{
			Name: "get document", Method: http.MethodGet, Path: "/v1/projects/p/databases/(default)/documents/col/d1",
			Status: 200, Contains: []string{`"stringValue":"x"`},
		},
		{
			Name: "run query streams an array", Method: http.MethodPost,
			Path: "/v1/projects/p/databases/(default)/documents:runQuery",
			Body: `{"structuredQuery":{"from":[{"collectionId":"col"}]}}`, Status: 200,
			Contains: []string{`"stringValue":"x"`, `[`},
		},
		{
			Name: "get missing document", Method: http.MethodGet,
			Path: "/v1/projects/p/databases/(default)/documents/col/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
