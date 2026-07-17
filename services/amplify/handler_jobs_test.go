package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_JobLifecycle verifies start/list/get/stop/delete job via the HTTP handler.
func TestHandler_JobLifecycle(t *testing.T) {
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

// TestHandler_ListJobs_Pagination verifies nextToken pagination for ListJobs.
func TestHandler_ListJobs_Pagination(t *testing.T) {
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

// TestHandler_StartJob_ResponseShape verifies StartJob's response wraps the
// job summary under "jobSummary" and includes "jobArn" -- a required field
// on types.JobSummary in the real SDK (see JobSummary in
// aws-sdk-go-v2/service/amplify/types) that a caller may dereference
// unconditionally.
func TestHandler_StartJob_ResponseShape(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateBranch(app.AppID, "main", "", "", false, nil)
	require.NoError(t, err)

	body := map[string]any{"jobType": "RELEASE"}
	rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/branches/main/jobs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var payload struct {
		JobSummary struct {
			JobID  string `json:"jobId"`
			JobARN string `json:"jobArn"`
			Status string `json:"status"`
		} `json:"jobSummary"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	assert.NotEmpty(t, payload.JobSummary.JobID)
	assert.NotEmpty(t, payload.JobSummary.JobARN)
	assert.Contains(t, payload.JobSummary.JobARN, "arn:aws:amplify:")
	assert.Equal(t, "RUNNING", payload.JobSummary.Status)
}
