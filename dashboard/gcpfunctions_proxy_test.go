package dashboard_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

func TestGCPFunctionsProxy(t *testing.T) {
	t.Parallel()

	const (
		loc = "/dashboard/api/v1/gcp/functions/v1/projects/proj-local/locations/us-central1"
		fn  = loc + "/functions/hello"
		bad = loc + "/functions:upload?uploadUrl=http://x/other"
		got = loc + "/functions:upload?uploadUrl=http://x/_gcf/upload/b/o.zip"
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"reads", []gcpStep{
			{http.MethodGet, loc + "/functions", "", "{}", http.StatusOK},
			{http.MethodGet, loc + "/runtimes", "", "nodejs", http.StatusOK},
			{http.MethodGet, fn, "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodDelete, fn, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"writes", []gcpStep{
			{
				http.MethodPost,
				loc + "/functions?functionId=hello",
				`{}`,
				"build_config",
				http.StatusBadRequest,
			},
			{
				http.MethodPost,
				loc + "/functions?functionId=hello",
				`{bad`,
				"invalid request body",
				http.StatusBadRequest,
			},
			{
				http.MethodPatch,
				fn + "?updateMask=description",
				`{}`,
				"NOT_FOUND",
				http.StatusNotFound,
			},
			{
				http.MethodPost,
				loc + "/functions:generateUploadUrl",
				`{}`,
				"ingress",
				http.StatusBadRequest,
			},
			{
				http.MethodPut,
				bad,
				"zip",
				"Invalid upload URL",
				http.StatusBadRequest,
			},
			{
				http.MethodPut,
				got,
				"zip",
				"Upload rejected",
				http.StatusBadRequest,
			},
		}},
		{"invoke", []gcpStep{
			{http.MethodPost, fn + ":invoke", `{"method":"GET","path":"/"}`, "NOT_FOUND", http.StatusNotFound},
			{http.MethodPost, fn + ":invoke", `{"method":"TRACE"}`, "unsupported method", http.StatusBadRequest},
			{http.MethodPost, fn + ":invoke", `{bad`, "invalid request body", http.StatusBadRequest},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, fn + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodPut, fn, "{}", "unsupported", http.StatusNotFound},
			{http.MethodGet, loc + "/functions:upload", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, loc + "/runtimes:x", "", "unsupported", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fh := gcpfunctions.NewHandler(gcpfunctions.NewInMemoryBackend())
			t.Cleanup(func() { fh.Shutdown(context.Background()) })

			h := dashboard.NewHandler(dashboard.Config{GCPFunctionsOps: fh})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPFunctionsProxyUnserved(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	runGCPSteps(t, h, []gcpStep{
		{
			http.MethodGet,
			"/dashboard/api/v1/gcp/functions/v1/projects/p/locations/l/functions",
			"",
			"",
			http.StatusServiceUnavailable,
		},
	})
}
