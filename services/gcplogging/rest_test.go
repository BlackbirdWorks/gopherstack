package gcplogging_test

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
			Name: "write entries", Method: http.MethodPost, Path: "/v2/entries:write",
			Body: `{"logName":"projects/p/logs/rest","resource":{"type":"global"},"entries":[` +
				`{"textPayload":"hello","severity":"ERROR"},{"jsonPayload":{"k":"v"}}]}`,
			Status: 200, Contains: []string{`{}`},
		},
		{
			Name: "list entries", Method: http.MethodPost, Path: "/v2/entries:list",
			Body:   `{"resourceNames":["projects/p"],"filter":"severity>=ERROR","orderBy":"timestamp desc"}`,
			Status: 200, Contains: []string{`"textPayload":"hello"`, `"severity":"ERROR"`},
		},
		{
			Name: "create sink", Method: http.MethodPost, Path: "/v2/projects/p/sinks?uniqueWriterIdentity=true",
			Body:   `{"name":"s1","destination":"storage.googleapis.com/b","filter":"severity>=ERROR"}`,
			Status: 200, Contains: []string{`"name":"s1"`, `gcp-sa-logging.iam.gserviceaccount.com`},
		},
		{
			Name: "patch sink", Method: http.MethodPatch, Path: "/v2/projects/p/sinks/s1?updateMask=filter",
			Body: `{"filter":"severity>=WARNING"}`, Status: 200, Contains: []string{`"filter":"severity>=WARNING"`},
		},
		{
			Name:     "get sink",
			Method:   http.MethodGet,
			Path:     "/v2/projects/p/sinks/s1",
			Status:   200,
			Contains: []string{`"destination"`},
		},
		{
			Name: "list sinks", Method: http.MethodGet, Path: "/v2/projects/p/sinks", Status: 200,
			Contains: []string{`_Default`, `s1`},
		},
		{
			Name: "create metric", Method: http.MethodPost, Path: "/v2/projects/p/metrics",
			Body: `{"name":"m1","filter":"severity>=ERROR",` +
				`"metricDescriptor":{"metricKind":"DELTA","valueType":"INT64","unit":"1"}}`,
			Status: 200, Contains: []string{`"name":"m1"`, `"valueType":"INT64"`},
		},
		{
			Name: "create exclusion", Method: http.MethodPost, Path: "/v2/projects/p/exclusions",
			Body: `{"name":"x1","filter":"severity<=DEBUG"}`, Status: 200, Contains: []string{`"name":"x1"`},
		},
		{
			Name:     "list buckets",
			Method:   http.MethodGet,
			Path:     "/v2/projects/p/locations/global/buckets",
			Status:   200,
			Contains: []string{`_Default`},
		},
		{
			Name: "list logs", Method: http.MethodGet, Path: "/v2/projects/p/logs", Status: 200,
			Contains: []string{`projects/p/logs/rest`},
		},
		{
			Name: "resource descriptors", Method: http.MethodGet, Path: "/v2/monitoredResourceDescriptors",
			Status: 200, Contains: []string{`gce_instance`},
		},
		{
			Name: "missing sink", Method: http.MethodGet, Path: "/v2/projects/p/sinks/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name:     "delete sink",
			Method:   http.MethodDelete,
			Path:     "/v2/projects/p/sinks/s1",
			Status:   200,
			Contains: []string{`{}`},
		},
		{
			Name: "delete log", Method: http.MethodDelete, Path: "/v2/projects/p/logs/rest", Status: 200,
			Contains: []string{`{}`},
		},
		{
			Name: "bad filter", Method: http.MethodPost, Path: "/v2/entries:list",
			Body: `{"resourceNames":["projects/p"],"filter":"a="}`, Status: 400, Contains: []string{`INVALID_ARGUMENT`},
		},
	})
}
