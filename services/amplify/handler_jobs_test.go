package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
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
				_, err := b.StartJob(app.AppID, "main", "RELEASE", "", "", "", time.Time{})
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

// TestHandler_GetJob_Steps verifies GetJob's steps list (previously always
// []any{} -- see PARITY.md) carries one real, non-fabricated step per job:
// its status/timestamps are derived from the job's own state, and it
// transitions from RUNNING to SUCCEED (with a real EndTime) once the janitor
// advances the underlying job, exactly like the job's own summary does.
func TestHandler_GetJob_Steps(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	app := seedApp(t, b, "StepsApp")
	seedMainBranch(t, b, app.AppID)

	basePath := "/apps/" + app.AppID + "/branches/main/jobs"

	rec := doRequest(t, h, http.MethodPost, basePath, map[string]any{"jobType": "RELEASE"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID := startResp["jobSummary"].(map[string]any)["jobId"].(string)

	type step struct {
		StepName  string  `json:"stepName"`
		Status    string  `json:"status"`
		StartTime float64 `json:"startTime"`
		EndTime   float64 `json:"endTime"`
	}

	getSteps := func() []step {
		t.Helper()

		stepsRec := doRequest(t, h, http.MethodGet, basePath+"/"+jobID, nil)
		require.Equal(t, http.StatusOK, stepsRec.Code)

		var payload struct {
			Job struct {
				Steps []step `json:"steps"`
			} `json:"job"`
		}
		require.NoError(t, json.Unmarshal(stepsRec.Body.Bytes(), &payload))

		return payload.Job.Steps
	}

	running := getSteps()
	require.Len(t, running, 1)
	assert.Equal(t, "BUILD", running[0].StepName)
	assert.Equal(t, "RUNNING", running[0].Status)
	assert.Positive(t, running[0].StartTime)
	assert.InDelta(t, running[0].StartTime, running[0].EndTime, 0,
		"an in-progress step reports its StartTime as a provisional EndTime, not zero")

	// Advance the job to SUCCEED via the janitor, same as real Amplify
	// eventually finishing the build on its own.
	j := amplify.NewJanitor(b, time.Second)
	j.SweepOnce(t.Context())

	done := getSteps()
	require.Len(t, done, 1)
	assert.Equal(t, "SUCCEED", done[0].Status)
	assert.GreaterOrEqual(t, done[0].EndTime, done[0].StartTime)
}
