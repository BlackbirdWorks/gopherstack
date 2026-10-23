package dashboard_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcprun"
)

func TestGCPRunProxy(t *testing.T) {
	t.Parallel()

	const (
		loc     = "/dashboard/api/v1/gcp/run/v1/projects/proj-local/locations/us-central1"
		svc     = loc + "/services/web"
		job     = loc + "/jobs/batch"
		svcBody = `{"template":{"containers":[{"image":"example/web:1","ports":[{"containerPort":8080}]}]}}`
		jobBody = `{"template":{"taskCount":2,"template":{"containers":[{"image":"example/job:1"}]}}}`
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{
			"service lifecycle",
			[]gcpStep{
				{http.MethodPost, loc + "/services?serviceId=web", svcBody, "operations", http.StatusOK},
				{http.MethodPost, loc + "/services?serviceId=web", svcBody, "ALREADY_EXISTS", http.StatusConflict},
				{http.MethodGet, loc + "/services", "", `"services"`, http.StatusOK},
				{http.MethodGet, svc, "", "example/web:1", http.StatusOK},
				{http.MethodGet, svc + "/revisions", "", `"revisions"`, http.StatusOK},
				{
					http.MethodPatch,
					svc + "?updateMask=traffic",
					`{"traffic":[{"type":"TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST","percent":100}]}`,
					"operations",
					http.StatusOK,
				},
				{http.MethodDelete, svc, "", "operations", http.StatusOK},
				{http.MethodGet, svc, "", "NOT_FOUND", http.StatusNotFound},
			},
		},
		{"job lifecycle", []gcpStep{
			{http.MethodPost, loc + "/jobs?jobId=batch", jobBody, "operations", http.StatusOK},
			{http.MethodGet, loc + "/jobs", "", `"jobs"`, http.StatusOK},
			{http.MethodGet, job, "", "example/job:1", http.StatusOK},
			{http.MethodPost, job + ":run", "", "operations", http.StatusOK},
			{http.MethodGet, job + "/executions", "", `"executions"`, http.StatusOK},
			{http.MethodDelete, job, "", "operations", http.StatusOK},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, svc + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodPut, svc, "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, loc + "/services?serviceId=x", `{bad`, "invalid request body", http.StatusBadRequest},
			{http.MethodPost, job + "/executions/e1:run", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, loc + "/jobs/nope/executions/e1/tasks", "", "NOT_FOUND", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rh := gcprun.NewHandler(gcprun.NewInMemoryBackend())
			t.Cleanup(func() { rh.Shutdown(context.Background()) })

			h := dashboard.NewHandler(dashboard.Config{GCPRunOps: rh})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPRunProxyUnserved(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	runGCPSteps(t, h, []gcpStep{
		{
			http.MethodGet,
			"/dashboard/api/v1/gcp/run/v1/projects/p/locations/l/services",
			"",
			"",
			http.StatusServiceUnavailable,
		},
	})
}
