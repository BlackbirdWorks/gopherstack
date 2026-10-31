package dashboard_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

const (
	batchProject = "batch-proj"
	batchBase    = "/dashboard/api/v1/gcp/batch/v1/projects/" + batchProject + "/locations/us-central1"
	batchJob     = batchBase + "/jobs/hello"
	batchBody    = `{"labels":{"team":"data"},"taskGroups":[{"taskCount":"3","parallelism":"2","taskSpec":{` +
		`"runnables":[{"container":{"imageUri":"alpine:3","commands":["echo","hi"]}}],` +
		`"environment":{"variables":{"A":"b"},"secretVariables":{"S":"projects/p/secrets/s/versions/1"}}}}],` +
		`"name":"projects/evil/locations/x/jobs/z",` +
		`"allocationPolicy":{"serviceAccount":{"email":"x@y.iam.gserviceaccount.com"}}}`
)

func newBatchDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	b := gcpbatch.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return dashboard.NewHandler(dashboard.Config{GCPBatchOps: gcpbatch.NewHandler(b)})
}

func TestGCPBatchProxy(t *testing.T) {
	t.Parallel()

	t.Run("create read cancel delete", func(t *testing.T) {
		t.Parallel()

		h := newBatchDashboard(t)

		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, batchBase + "/jobs?jobId=hello", batchBody, `/jobs/hello"`, 200},
			{http.MethodGet, "/dashboard/api/v1/gcp/batch/v1/projects", "", batchProject, 200},
			{http.MethodGet, batchBase + "/jobs", "", `jobs/hello`, 200},
			{http.MethodGet, batchBase + "/jobs/hello", "", `"taskCount":"3"`, 200},
			{http.MethodGet, batchBase + "/jobs/hello/taskGroups/group0/tasks", "", `"tasks"`, 200},
			{http.MethodGet, batchBase + "/jobs/hello/taskGroups/other/tasks", "", "unsupported", 404},
			{http.MethodPost, batchBase + "/jobs/hello:cancel", "", "operations/", 200},
			{http.MethodPost, batchBase + "/jobs/hello:cancel", "", "cannot be cancelled", 400},
			{http.MethodDelete, batchBase + "/jobs/hello", "", "operations/", 200},
		})
	})

	t.Run("body is rebuilt from fixed fields", func(t *testing.T) {
		t.Parallel()

		h := newBatchDashboard(t)
		rec := agwDo(t, h, http.MethodPost, batchBase+"/jobs?jobId=hello", batchBody)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		got := agwDo(t, h, http.MethodGet, batchJob, "")
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.Contains(t, got.Body.String(), `"A":"b"`)
		assert.Contains(t, got.Body.String(), `"team":"data"`)
		assert.NotContains(t, got.Body.String(), "secretVariables")
		assert.NotContains(t, got.Body.String(), "serviceAccount")
		assert.NotContains(t, got.Body.String(), "evil")
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		script := `{"taskGroups":[{"taskSpec":{"runnables":[{"script":{"text":"echo"}}]}}]}`

		runGCPSteps(t, newBatchDashboard(t), []gcpStep{
			{http.MethodPost, batchBase + "/jobs", batchBody, "jobId", 400},
			{http.MethodPost, batchBase + "/jobs?jobId=x", `{}`, "task group", 400},
			{http.MethodPost, batchBase + "/jobs?jobId=x", script, "container runnable", 400},
			{http.MethodPost, batchBase + "/jobs?jobId=x", `nope`, "invalid", 400},
			{http.MethodPatch, batchJob, `{}`, "unsupported", 404},
			{http.MethodPut, batchBase + "/jobs", `{}`, "unsupported", 404},
			{http.MethodGet, batchBase + "/operations", "", "unsupported", 404},
			{http.MethodGet, batchJob + ":getIamPolicy", "", "unsupported", 404},
			{http.MethodGet, batchBase + "/jobs/%2e%2e", "", "unsupported", 400},
			{http.MethodGet, batchBase + "/jobs/ne", "", "not found", 404},
		})
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, batchBase + "/jobs", "", "", http.StatusServiceUnavailable},
		})
	})
}
