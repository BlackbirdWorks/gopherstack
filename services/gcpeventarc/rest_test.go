package gcpeventarc_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	const trig = "/v1/projects/test-project/locations/us-central1/triggers/rest-trigger"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create trigger", Method: http.MethodPost,
			Path: "/v1/projects/test-project/locations/us-central1/triggers?triggerId=rest-trigger",
			Body: `{"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
				`"destination":{"cloudRun":{"service":"receiver","region":"us-central1","path":"/rest"}}}`,
			Status: 200, Contains: []string{`"done":true`, `rest-trigger`, `OperationMetadata`},
		},
		{
			Name: "get trigger", Method: http.MethodGet, Path: trig,
			Status: 200, Contains: []string{`"service":"receiver"`, `"path":"/rest"`, `"subscription"`},
		},
		{
			Name: "list providers", Method: http.MethodGet,
			Path:   "/v1/projects/test-project/locations/us-central1/providers",
			Status: 200, Contains: []string{`pubsub.googleapis.com`, `storage.googleapis.com`, `"pathPatternSupported":true`},
		},
		{
			Name: "patch trigger", Method: http.MethodPatch, Path: trig + "?updateMask=labels",
			Body: `{"labels":{"a":"b"}}`, Status: 200, Contains: []string{`"done":true`, `"a":"b"`},
		},
		{
			Name: "missing trigger", Method: http.MethodGet,
			Path:   "/v1/projects/test-project/locations/us-central1/triggers/ghost-trigger",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "list locations", Method: http.MethodGet, Path: "/v1/projects/test-project/locations",
			Status: 200, Contains: []string{`"locationId":"us-central1"`},
		},
		{
			Name: "delete trigger", Method: http.MethodDelete, Path: trig,
			Status: 200, Contains: []string{`"done":true`},
		},
	})
}
