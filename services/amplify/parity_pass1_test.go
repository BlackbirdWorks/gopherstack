package amplify_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// seedApp creates an app and fails the test if it errors.
func seedApp(t *testing.T, b *amplify.InMemoryBackend, name string) *amplify.App {
	t.Helper()

	app, err := b.CreateApp(name, "", "", "WEB", nil)
	require.NoError(t, err)

	return app
}

// seedMainBranch creates a "main" branch and fails the test if it errors.
func seedMainBranch(t *testing.T, b *amplify.InMemoryBackend, appID string) *amplify.Branch {
	t.Helper()

	br, err := b.CreateBranch(appID, "main", "", "", false, nil)
	require.NoError(t, err)

	return br
}

// TestParity_UpdateApp verifies POST /apps/{appId} returns the updated app.
func TestParity_UpdateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "updates_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				return seedApp(t, b, "OldName").AppID
			},
			body:       map[string]any{"name": "NewName"},
			wantStatus: http.StatusOK,
			wantName:   "NewName",
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"name": "X"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				app := resp["app"].(map[string]any)
				assert.Equal(t, tt.wantName, app["name"])
			}
		})
	}
}

// TestParity_UpdateBranch verifies POST /apps/{appId}/branches/{branchName} returns updated branch.
func TestParity_UpdateBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) (string, string)
		body       any
		name       string
		wantDesc   string
		wantStatus int
	}{
		{
			name: "updates_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app := seedApp(t, b, "App1")
				seedMainBranch(t, b, app.AppID)

				return app.AppID, "main"
			},
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			wantDesc:   "updated",
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				return seedApp(t, b, "App2").AppID, "nonexistent"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID+"/branches/"+branchName, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDesc != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				br := resp["branch"].(map[string]any)
				assert.Equal(t, tt.wantDesc, br["description"])
			}
		})
	}
}

// TestParity_WebhookCRUD verifies create/get/list/update/delete webhook lifecycle.
func TestParity_WebhookCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "full_webhook_lifecycle", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "WebhookApp")
			seedMainBranch(t, b, app.AppID)

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/webhooks",
				map[string]any{"branchName": "main", "description": "test hook"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			wh := createResp["webhook"].(map[string]any)
			webhookID := wh["webhookId"].(string)
			assert.NotEmpty(t, webhookID)

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/webhooks/"+webhookID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/webhooks", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["webhooks"].([]any), 1)

			// Update.
			rec = doRequest(t, h, http.MethodPost, "/webhooks/"+webhookID,
				map[string]any{"branchName": "main", "description": "updated"})
			require.Equal(t, tt.wantStatus, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			updated := updateResp["webhook"].(map[string]any)
			assert.Equal(t, "updated", updated["description"])

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/webhooks/"+webhookID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/webhooks/"+webhookID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_BackendEnvironmentCRUD verifies backend environment lifecycle via the HTTP handler.
func TestParity_BackendEnvironmentCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_backend_env_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "BEApp")

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/backendenvironments",
				map[string]any{"environmentName": "prod", "stackName": "MyStack"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			be := createResp["backendEnvironment"].(map[string]any)
			assert.Equal(t, "prod", be["environmentName"])

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["backendEnvironments"].([]any), 1)

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_DomainAssociationCRUD verifies domain association lifecycle via the HTTP handler.
func TestParity_DomainAssociationCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_domain_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "DomainApp")

			subDomains := []map[string]any{{"prefix": "www", "branchName": "main"}}

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains",
				map[string]any{"domainName": "example.com", "subDomainSettings": subDomains})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			da := createResp["domainAssociation"].(map[string]any)
			assert.Equal(t, "example.com", da["domainName"])

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains/example.com", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["domainAssociations"].([]any), 1)

			// Update.
			rec = doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains/example.com",
				map[string]any{"subDomainSettings": subDomains, "enableAutoSubDomain": true})
			require.Equal(t, http.StatusOK, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			updated := updateResp["domainAssociation"].(map[string]any)
			assert.Equal(t, true, updated["enableAutoSubDomain"])

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/apps/"+app.AppID+"/domains/example.com", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains/example.com", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_JobLifecycle verifies start/list/get/stop/delete job via the HTTP handler.
func TestParity_JobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_job_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "JobApp")
			seedMainBranch(t, b, app.AppID)

			basePath := "/apps/" + app.AppID + "/branches/main/jobs"

			// Start job.
			rec := doRequest(t, h, http.MethodPost, basePath,
				map[string]any{"jobType": "RELEASE", "commitId": "abc123"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			summary := startResp["jobSummary"].(map[string]any)
			jobID := summary["jobId"].(string)
			assert.NotEmpty(t, jobID)

			// List jobs.
			rec = doRequest(t, h, http.MethodGet, basePath, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["jobSummaries"].([]any), 1)

			// Get job.
			rec = doRequest(t, h, http.MethodGet, basePath+"/"+jobID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			job := getResp["job"].(map[string]any)
			assert.NotNil(t, job["summary"])
			assert.NotNil(t, job["steps"])

			// Stop job.
			rec = doRequest(t, h, http.MethodDelete, basePath+"/"+jobID+"/stop", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete job.
			rec = doRequest(t, h, http.MethodDelete, basePath+"/"+jobID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, basePath+"/"+jobID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_ListJobs_Pagination verifies nextToken pagination for ListJobs.
func TestParity_ListJobs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryString   string
		wantCount     int
		wantNextToken bool
	}{
		{name: "no_limit_returns_all", queryString: "", wantCount: 3},
		{name: "first_page", queryString: "?maxResults=2", wantCount: 2, wantNextToken: true},
		{name: "second_page", queryString: "?maxResults=2&nextToken=2", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "PagApp")
			seedMainBranch(t, b, app.AppID)

			for range 3 {
				_, err := b.StartJob(app.AppID, "main", "RELEASE", "", "")
				require.NoError(t, err)
			}

			path := "/apps/" + app.AppID + "/branches/main/jobs" + tt.queryString
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["jobSummaries"].([]any), tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextToken"])
			} else {
				assert.Empty(t, resp["nextToken"])
			}
		})
	}
}

// TestParity_DeploymentOps verifies CreateDeployment and StartDeployment via the HTTP handler.
func TestParity_DeploymentOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_and_start_deployment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "DeployApp")
			seedMainBranch(t, b, app.AppID)

			basePath := "/apps/" + app.AppID + "/branches/main/deployments"

			// Create deployment.
			rec := doRequest(t, h, http.MethodPost, basePath, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			jobID := createResp["jobId"].(string)
			assert.NotEmpty(t, jobID)
			assert.NotNil(t, createResp["fileUploadUrls"])

			// Start deployment.
			rec = doRequest(t, h, http.MethodPost, basePath+"/start",
				map[string]any{"jobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			assert.NotNil(t, startResp["jobSummary"])
		})
	}
}

// TestParity_GenerateAccessLogs verifies POST /apps/{appId}/accesslogs returns a log URL.
func TestParity_GenerateAccessLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "returns_log_url", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "LogApp")

			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/accesslogs",
				map[string]any{"domainName": "example.com"})
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotNil(t, resp["logUrl"])
		})
	}
}

// TestParity_ExtractOperation_Extended verifies operation name extraction for all new routes.
func TestParity_ExtractOperation_Extended(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "update_app", method: http.MethodPost, path: "/apps/abc", wantOp: "UpdateApp"},
		{name: "update_branch", method: http.MethodPost, path: "/apps/abc/branches/main", wantOp: "UpdateBranch"},
		{name: "create_webhook", method: http.MethodPost, path: "/apps/abc/webhooks", wantOp: "CreateWebhook"},
		{name: "list_webhooks", method: http.MethodGet, path: "/apps/abc/webhooks", wantOp: "ListWebhooks"},
		{name: "get_webhook", method: http.MethodGet, path: "/webhooks/wh1", wantOp: "GetWebhook"},
		{name: "update_webhook", method: http.MethodPost, path: "/webhooks/wh1", wantOp: "UpdateWebhook"},
		{name: "delete_webhook", method: http.MethodDelete, path: "/webhooks/wh1", wantOp: "DeleteWebhook"},
		{name: "create_be", method: http.MethodPost,
			path: "/apps/abc/backendenvironments", wantOp: "CreateBackendEnvironment"},
		{name: "list_be", method: http.MethodGet,
			path: "/apps/abc/backendenvironments", wantOp: "ListBackendEnvironments"},
		{name: "get_be", method: http.MethodGet,
			path: "/apps/abc/backendenvironments/prod", wantOp: "GetBackendEnvironment"},
		{name: "delete_be", method: http.MethodDelete,
			path: "/apps/abc/backendenvironments/prod", wantOp: "DeleteBackendEnvironment"},
		{name: "create_domain", method: http.MethodPost,
			path: "/apps/abc/domains", wantOp: "CreateDomainAssociation"},
		{name: "list_domains", method: http.MethodGet,
			path: "/apps/abc/domains", wantOp: "ListDomainAssociations"},
		{name: "get_domain", method: http.MethodGet,
			path: "/apps/abc/domains/example.com", wantOp: "GetDomainAssociation"},
		{name: "delete_domain", method: http.MethodDelete,
			path: "/apps/abc/domains/example.com", wantOp: "DeleteDomainAssociation"},
		{name: "update_domain", method: http.MethodPost,
			path: "/apps/abc/domains/example.com", wantOp: "UpdateDomainAssociation"},
		{name: "start_job", method: http.MethodPost,
			path: "/apps/abc/branches/main/jobs", wantOp: "StartJob"},
		{name: "list_jobs", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs", wantOp: "ListJobs"},
		{name: "get_job", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs/j1", wantOp: "GetJob"},
		{name: "delete_job", method: http.MethodDelete,
			path: "/apps/abc/branches/main/jobs/j1", wantOp: "DeleteJob"},
		{name: "stop_job", method: http.MethodDelete,
			path: "/apps/abc/branches/main/jobs/j1/stop", wantOp: "StopJob"},
		{name: "create_deployment", method: http.MethodPost,
			path: "/apps/abc/branches/main/deployments", wantOp: "CreateDeployment"},
		{name: "start_deployment", method: http.MethodPost,
			path: "/apps/abc/branches/main/deployments/start", wantOp: "StartDeployment"},
		{name: "generate_access_logs", method: http.MethodPost,
			path: "/apps/abc/accesslogs", wantOp: "GenerateAccessLogs"},
		{name: "get_artifact_url", method: http.MethodGet, path: "/artifacts/art1", wantOp: "GetArtifactUrl"},
		{name: "list_artifacts", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs/j1/artifacts", wantOp: "ListArtifacts"},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}
