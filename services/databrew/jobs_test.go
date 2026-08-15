package databrew_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ---- Job backend ----

func TestCreateJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds1",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r1", "", nil, nil)
	require.NoError(t, err)
	outputs := []databrew.Output{
		{Location: databrew.S3Location{Bucket: "out-bkt", Key: "out/"}, Format: "CSV"},
	}
	j, err := b.CreateJob(
		context.Background(),
		"my-job",
		"RECIPE",
		"ds1",
		"",
		"r1",
		"arn:aws:iam::123456789012:role/Role",
		outputs,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-job", j.Name)
	assert.Equal(t, "RECIPE", j.Type)
	assert.NotEmpty(t, j.Arn)
	assert.Len(t, j.Outputs, 1)
}

func TestCreateJob_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(
		context.Background(),
		"",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.Error(t, err)
}

func TestCreateJob_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.Error(t, err)
}

func TestDescribeJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"j1",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		map[string]string{"x": "y"},
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	j, err := b.DescribeJob(context.Background(), "j1")
	require.NoError(t, err)
	assert.Equal(t, "j1", j.Name)
	assert.Equal(t, "y", j.Tags["x"])
}

func TestDescribeJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeJob(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListJobs(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"j1",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"j2",
		"RECIPE",
		"ds",
		"",
		"r",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	list, _ := b.ListJobs(context.Background(), 100, "", "", "")
	assert.Len(t, list, 2)
}

func TestUpdateJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"upd-j",
		"PROFILE",
		"ds",
		"",
		"",
		"old-role",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	outputs := []databrew.Output{{Location: databrew.S3Location{Bucket: "b"}}}
	err = b.UpdateJob(
		context.Background(),
		"upd-j",
		"new-role",
		outputs,
		5,
		2,
		60,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	j, err := b.DescribeJob(context.Background(), "upd-j")
	require.NoError(t, err)
	assert.Equal(t, "new-role", j.RoleArn)
	assert.Equal(t, 5, j.MaxCapacity)
	assert.Equal(t, 2, j.MaxRetries)
	assert.Equal(t, 60, j.Timeout)
}

func TestUpdateJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateJob(context.Background(), "no-such", "", nil, 0, 0, 0, databrew.JobExtras{})
	require.Error(t, err)
}

func TestDeleteJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"del-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	err = b.DeleteJob(context.Background(), "del-j")
	require.NoError(t, err)
	_, err = b.DescribeJob(context.Background(), "del-j")
	require.Error(t, err)
}

func TestDeleteJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteJob(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Job run backend ----

func TestStartJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"run-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	run, err := b.StartJobRun(context.Background(), "run-j")
	require.NoError(t, err)
	assert.Equal(t, "run-j", run.JobName)
	assert.Equal(t, "STARTING", run.State)
	assert.NotEmpty(t, run.RunID)
}

func TestStartJobRun_TransitionsToSucceeded(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"run-j2",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "run-j2")
	require.NoError(t, err)

	// Poll for async state transition instead of fixed sleep.
	require.Eventually(t, func() bool {
		runs, _, listErr := b.ListJobRuns(context.Background(), "run-j2", 100, "")

		return listErr == nil && len(runs) == 1 && runs[0].State == "SUCCEEDED"
	}, 3*time.Second, 25*time.Millisecond)
}

func TestStartJobRun_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.StartJobRun(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListJobRuns_Empty(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"empty-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns(context.Background(), "empty-j", 100, "")
	require.NoError(t, err)
	assert.Empty(t, runs)
}

func TestListJobRuns_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, _, err := b.ListJobRuns(context.Background(), "no-such", 100, "")
	require.Error(t, err)
}

func TestListJobRuns_MultipleRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"multi-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "multi-j")
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "multi-j")
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns(context.Background(), "multi-j", 100, "")
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}

func TestListJobRuns_Pagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"pag-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	for range 5 {
		_, err = b.StartJobRun(context.Background(), "pag-j")
		require.NoError(t, err)
	}
	page1, next, err := b.ListJobRuns(context.Background(), "pag-j", 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next)

	page2, _, err := b.ListJobRuns(context.Background(), "pag-j", 2, next)
	require.NoError(t, err)
	assert.NotEmpty(t, page2)
}

func TestStopJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"stop-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	run, err := b.StartJobRun(context.Background(), "stop-j")
	require.NoError(t, err)
	stopped, err := b.StopJobRun(context.Background(), "stop-j", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", stopped.State)
}

func TestStopJobRun_AlreadySucceeded(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"stop-j2",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	run, err := b.StartJobRun(context.Background(), "stop-j2")
	require.NoError(t, err)
	// Wait for the async transition.
	require.Eventually(t, func() bool {
		runs, _, listErr := b.ListJobRuns(context.Background(), "stop-j2", 100, "")

		return listErr == nil && len(runs) == 1 && runs[0].State == "SUCCEEDED"
	}, 3*time.Second, 25*time.Millisecond)
	// Stopping a SUCCEEDED run should be a no-op (returns the run).
	stopped, err := b.StopJobRun(context.Background(), "stop-j2", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", stopped.State)
}

func TestStopJobRun_NotFound_NoRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.StopJobRun(context.Background(), "no-such-job", "any-run-id")
	require.Error(t, err)
}

func TestStopJobRun_RunIDNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"stop-j3",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "stop-j3")
	require.NoError(t, err)
	_, err = b.StopJobRun(context.Background(), "stop-j3", "nonexistent-run-id")
	require.Error(t, err)
}

func TestDescribeJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"desc-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	run, err := b.StartJobRun(context.Background(), "desc-j")
	require.NoError(t, err)
	got, err := b.DescribeJobRun(context.Background(), "desc-j", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, run.RunID, got.RunID)
	assert.Equal(t, "desc-j", got.JobName)
}

func TestDescribeJobRun_NotFound_NoRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeJobRun(context.Background(), "no-such-job", "any-run-id")
	require.Error(t, err)
}

func TestDescribeJobRun_RunIDNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"desc-j2",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "desc-j2")
	require.NoError(t, err)
	_, err = b.DescribeJobRun(context.Background(), "desc-j2", "no-such-run")
	require.Error(t, err)
}

// ---- Job handler ----

func TestHandlerCreateProfileJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "ds1",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "profile-job", "DatasetName": "ds1",
		"RoleArn":        "arn:aws:iam::123456789012:role/Role",
		"OutputLocation": map[string]any{"Bucket": "out"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerCreateRecipeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name": "recipe-job", "RecipeName": "r1",
		"RoleArn": "arn:aws:iam::123456789012:role/Role",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "ds1",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "j1", "DatasetName": "ds1",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/j1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListJobs(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateProfileJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "upd-profile-j",
	})
	rec := databrewReq(
		t,
		h,
		http.MethodPut,
		"/databrew/v1/profileJobs/upd-profile-j",
		map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/NewRole",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "upd-profile-j", resp["Name"])
}

func TestHandlerUpdateRecipeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name": "upd-recipe-j",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/recipeJobs/upd-recipe-j", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/NewRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateJob_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/profileJobs/no-such", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerDeleteJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "del-job",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/jobs/del-job", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteJob_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/jobs/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerStartJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "run-job",
	})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/run-job/startJobRun", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListJobRuns(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "list-job",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/list-job/jobRuns", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(
		t,
		h,
		http.MethodPost,
		"/databrew/v1/profileJobs",
		map[string]any{"Name": "djr-job"},
	)
	runRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/djr-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, runRec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &startResp))
	runID, _ := startResp["RunId"].(string)
	require.NotEmpty(t, runID)

	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/djr-job/jobRun/"+runID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerStopJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(
		t,
		h,
		http.MethodPost,
		"/databrew/v1/profileJobs",
		map[string]any{"Name": "sjr-job"},
	)
	runRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/sjr-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, runRec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &startResp))
	runID, _ := startResp["RunId"].(string)
	require.NotEmpty(t, runID)

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/sjr-job/jobRun/"+runID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Job wire-shape / routing regression coverage ----

// TestStartJobRun_RunId verifies StartJobRun response uses "RunId" (not "RunID").
func TestStartJobRun_RunId(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "pj1"})

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/pj1/startJobRun", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["RunID"], "RunID (uppercase D) must not appear — AWS SDK uses RunId")
	runID, ok := resp["RunId"].(string)
	assert.True(t, ok, "response must have RunId string field")
	assert.NotEmpty(t, runID)
}

// TestListJobs_Filters verifies ListJobs datasetName and projectName filtering.
func TestListJobs_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "ds-a",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "ds-b",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "profile-ds-a", "DatasetName": "ds-a"})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "profile-ds-b", "DatasetName": "ds-b"})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
		map[string]any{"Name": "r1", "Steps": []any{}})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "proj-a", "DatasetName": "ds-a", "RecipeName": "r1",
		"RoleArn": "arn:aws:iam::123456789012:role/r",
	})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name":            "recipe-proj-a",
		"ProjectName":     "proj-a",
		"RecipeReference": map[string]any{"Name": "r1"},
		"RoleArn":         "arn:aws:iam::123456789012:role/r",
	})

	tests := []struct {
		queryParams url.Values
		name        string
		wantCount   int
	}{
		{
			name:        "no filter returns all",
			queryParams: url.Values{},
			wantCount:   3,
		},
		{
			name:        "filter by datasetName=ds-a",
			queryParams: url.Values{"datasetName": {"ds-a"}},
			wantCount:   1,
		},
		{
			name:        "filter by datasetName=ds-b",
			queryParams: url.Values{"datasetName": {"ds-b"}},
			wantCount:   1,
		},
		{
			name:        "filter by projectName=proj-a",
			queryParams: url.Values{"projectName": {"proj-a"}},
			wantCount:   1,
		},
		{
			name:        "filter by nonexistent datasetName",
			queryParams: url.Values{"datasetName": {"ds-none"}},
			wantCount:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := "/databrew/v1/jobs"
			if len(tc.queryParams) > 0 {
				path = path + "?" + tc.queryParams.Encode()
			}
			rec := databrewReq(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["Jobs"].([]any)
			assert.Len(t, jobs, tc.wantCount, "query=%v", tc.queryParams)
		})
	}
}

// TestJobRunIdField_RoundTrip verifies RunId field survives start→describe→stop.
func TestJobRunIdField_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "rt-job"})

	startRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/rt-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	runID, ok := startResp["RunId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, runID)

	// Wait for transition so DescribeJobRun is non-empty.
	time.Sleep(200 * time.Millisecond)

	descRec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/rt-job/jobRun/"+runID, nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, runID, descResp["RunId"])

	stopRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/rt-job/jobRun/"+runID, nil)
	require.Equal(t, http.StatusOK, stopRec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopResp))
	assert.Equal(t, runID, stopResp["RunId"])
}

// ---- Job extras: ProfileConfiguration/JobSample/ValidationConfigurations,
// DataCatalogOutputs/DatabaseOutputs/Encryption*/LogSubscription ----

// TestCreateJob_ProfileExtras verifies CreateJob threads ProfileConfiguration,
// JobSample, and ValidationConfigurations through to the stored Job -- these
// fields previously had matching JSON tags on the Job entity but nothing
// ever populated them (see PARITY.md gaps).
func TestCreateJob_ProfileExtras(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	extra := databrew.JobExtras{
		ProfileConfiguration: map[string]any{"DatasetStatisticsConfiguration": map[string]any{}},
		JobSample:            &databrew.JobSample{Mode: "FULL_DATASET"},
		ValidationConfigurations: []map[string]any{
			{"RulesetArn": "arn:aws:databrew:us-east-1:123456789012:ruleset/r1"},
		},
	}
	j, err := b.CreateJob(
		context.Background(), "profile-extras-j", "PROFILE", "ds", "", "", "", nil, nil, extra,
	)
	require.NoError(t, err)
	require.NotNil(t, j.JobSample)
	assert.Equal(t, "FULL_DATASET", j.JobSample.Mode)
	assert.NotNil(t, j.ProfileConfiguration)
	require.Len(t, j.ValidationConfigurations, 1)

	described, err := b.DescribeJob(context.Background(), "profile-extras-j")
	require.NoError(t, err)
	require.NotNil(t, described.JobSample)
	assert.Equal(t, "FULL_DATASET", described.JobSample.Mode)
}

// TestCreateJob_RecipeExtras verifies CreateJob threads
// DataCatalogOutputs/DatabaseOutputs/EncryptionMode/EncryptionKeyArn/
// LogSubscription through to the stored Job.
func TestCreateJob_RecipeExtras(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	extra := databrew.JobExtras{
		EncryptionMode:     "SSE-KMS",
		EncryptionKeyArn:   "arn:aws:kms:us-east-1:123456789012:key/abc",
		LogSubscription:    "ENABLE",
		DataCatalogOutputs: []databrew.DataCatalogOutput{{DatabaseName: "db1", TableName: "t1"}},
		DatabaseOutputs: []databrew.DatabaseOutput{{
			GlueConnectionName: "conn1",
			DatabaseOptions:    &databrew.DatabaseTableOutputOptions{TableName: "t1"},
		}},
	}
	j, err := b.CreateJob(
		context.Background(), "recipe-extras-j", "RECIPE", "ds", "", "r", "", nil, nil, extra,
	)
	require.NoError(t, err)
	assert.Equal(t, "SSE-KMS", j.EncryptionMode)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc", j.EncryptionKeyArn)
	assert.Equal(t, "ENABLE", j.LogSubscription)
	require.Len(t, j.DataCatalogOutputs, 1)
	require.Len(t, j.DatabaseOutputs, 1)
	assert.Equal(t, "db1", j.DataCatalogOutputs[0].DatabaseName)
}

// TestCreateJob_ExtrasValidation proves CreateJob rejects extras values the
// real service would reject (per botocore databrew/2017-07-25's
// EncryptionMode/LogSubscription/SampleMode enums and DataCatalogOutput/
// DatabaseOutput "required" lists) instead of storing them.
func TestCreateJob_ExtrasValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		extra databrew.JobExtras
	}{
		{
			name:  "invalid encryption mode",
			extra: databrew.JobExtras{EncryptionMode: "SSE-BOGUS"},
		},
		{
			name:  "invalid log subscription",
			extra: databrew.JobExtras{LogSubscription: "MAYBE"},
		},
		{
			name:  "invalid job sample mode",
			extra: databrew.JobExtras{JobSample: &databrew.JobSample{Mode: "SOME_ROWS"}},
		},
		{
			name: "data catalog output missing table name",
			extra: databrew.JobExtras{
				DataCatalogOutputs: []databrew.DataCatalogOutput{{DatabaseName: "db1"}},
			},
		},
		{
			name: "data catalog output overwrite with database options",
			extra: databrew.JobExtras{
				DataCatalogOutputs: []databrew.DataCatalogOutput{{
					DatabaseName: "db1", TableName: "t1", Overwrite: true,
					DatabaseOptions: &databrew.DatabaseTableOutputOptions{TableName: "t1"},
				}},
			},
		},
		{
			name: "database output missing database options",
			extra: databrew.JobExtras{
				DatabaseOutputs: []databrew.DatabaseOutput{{GlueConnectionName: "conn1"}},
			},
		},
		{
			name: "database output invalid mode",
			extra: databrew.JobExtras{
				DatabaseOutputs: []databrew.DatabaseOutput{{
					GlueConnectionName: "conn1",
					DatabaseOptions:    &databrew.DatabaseTableOutputOptions{TableName: "t1"},
					DatabaseOutputMode: "REPLACE",
				}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateDataset(
				context.Background(),
				"ds",
				"CSV",
				s3Input("b", ""),
				databrew.DatasetFormatOptions{},
				nil,
				nil,
			)
			require.NoError(t, err)
			_, err = b.CreateJob(
				context.Background(),
				"extras-validation-j",
				"RECIPE",
				"ds",
				"",
				"",
				"",
				nil,
				nil,
				tc.extra,
			)
			require.ErrorIs(t, err, databrew.ErrValidation)

			_, describeErr := b.DescribeJob(context.Background(), "extras-validation-j")
			require.ErrorIs(
				t,
				describeErr,
				databrew.ErrNotFound,
				"rejected CreateJob must not store partial state",
			)
		})
	}
}

// TestCreateJob_ResourceRefsValidation proves CreateJob rejects a
// DatasetName/ProjectName/RecipeReference naming a dataset, project, or
// recipe that was never created (per CreateProfileJob/CreateRecipeJob's
// documented ResourceNotFoundException) instead of storing a job that points
// at nothing, while leaving an unset reference accepted (CreateRecipeJob
// takes ProjectName as an alternative to DatasetName+RecipeReference).
func TestCreateJob_ResourceRefsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobType     string
		datasetName string
		projectName string
		recipeName  string
		seedDataset bool
		seedProject bool
		seedRecipe  bool
		wantErr     bool
	}{
		{
			name:        "profile job missing dataset rejects",
			jobType:     "PROFILE",
			datasetName: "ds",
			wantErr:     true,
		},
		{
			name:        "profile job existing dataset succeeds",
			jobType:     "PROFILE",
			datasetName: "ds",
			seedDataset: true,
		},
		{
			name:        "recipe job missing dataset rejects",
			jobType:     "RECIPE",
			datasetName: "ds",
			wantErr:     true,
		},
		{
			name:        "recipe job missing project rejects",
			jobType:     "RECIPE",
			projectName: "proj",
			wantErr:     true,
		},
		{
			name:       "recipe job missing recipe rejects",
			jobType:    "RECIPE",
			recipeName: "rcp",
			wantErr:    true,
		},
		{
			name:        "recipe job all refs present succeeds",
			jobType:     "RECIPE",
			datasetName: "ds",
			projectName: "proj",
			recipeName:  "rcp",
			seedDataset: true,
			seedProject: true,
			seedRecipe:  true,
		},
		{
			name:    "recipe job with no refs is not an error",
			jobType: "RECIPE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			if tc.seedDataset {
				_, err := b.CreateDataset(
					context.Background(), tc.datasetName, "CSV", s3Input("b", ""),
					databrew.DatasetFormatOptions{}, nil, nil,
				)
				require.NoError(t, err)
			}
			if tc.seedRecipe {
				_, err := b.CreateRecipe(context.Background(), tc.recipeName, "", nil, nil)
				require.NoError(t, err)
			}
			if tc.seedProject {
				_, err := b.CreateProject(
					context.Background(),
					tc.projectName,
					tc.datasetName,
					tc.recipeName,
					"",
					databrew.Sample{},
					nil,
				)
				require.NoError(t, err)
			}

			_, err := b.CreateJob(
				context.Background(),
				"refs-j",
				tc.jobType,
				tc.datasetName,
				tc.projectName,
				tc.recipeName,
				"",
				nil,
				nil,
				databrew.JobExtras{},
			)

			if tc.wantErr {
				require.ErrorIs(t, err, databrew.ErrNotFound)
				_, describeErr := b.DescribeJob(context.Background(), "refs-j")
				require.ErrorIs(
					t,
					describeErr,
					databrew.ErrNotFound,
					"rejected CreateJob must not store partial state",
				)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestUpdateJob_ExtrasValidation proves UpdateJob validates extras before
// mutating the stored job -- an invalid extras value must leave the job's
// other already-applied fields (RoleArn here) untouched.
func TestUpdateJob_ExtrasValidation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(),
		"upd-validation-j",
		"RECIPE",
		"ds",
		"",
		"",
		"arn:aws:iam::123456789012:role/orig",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)

	err = b.UpdateJob(
		context.Background(),
		"upd-validation-j",
		"arn:aws:iam::123456789012:role/new",
		nil,
		0,
		0,
		0,
		databrew.JobExtras{EncryptionMode: "SSE-BOGUS"},
	)
	require.ErrorIs(t, err, databrew.ErrValidation)

	j, err := b.DescribeJob(context.Background(), "upd-validation-j")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/orig", j.RoleArn,
		"RoleArn must not be applied when extras validation rejects the request")
}

// TestUpdateJob_Extras verifies UpdateJob overwrites extras fields when
// provided and leaves them unchanged when omitted (zero-valued JobExtras).
func TestUpdateJob_Extras(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(
		context.Background(), "upd-extras-j", "RECIPE", "ds", "", "r", "", nil, nil,
		databrew.JobExtras{EncryptionMode: "SSE-S3"},
	)
	require.NoError(t, err)

	err = b.UpdateJob(
		context.Background(), "upd-extras-j", "", nil, 0, 0, 0,
		databrew.JobExtras{EncryptionKeyArn: "arn:aws:kms:us-east-1:123456789012:key/xyz"},
	)
	require.NoError(t, err)

	j, err := b.DescribeJob(context.Background(), "upd-extras-j")
	require.NoError(t, err)
	assert.Equal(
		t,
		"SSE-S3",
		j.EncryptionMode,
		"unset extras field on Update must not clobber the existing value",
	)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/xyz", j.EncryptionKeyArn)
}

// TestHandlerCreateProfileJob_Extras verifies the profile-job extras wire
// shape round-trips through the HTTP handler layer.
func TestHandlerCreateProfileJob_Extras(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "pj-extras-ds",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name":        "pj-extras",
		"DatasetName": "pj-extras-ds",
		"RoleArn":     "arn:aws:iam::123456789012:role/r",
		"Configuration": map[string]any{
			"ProfileColumns": []any{map[string]any{"Name": "col1"}},
		},
		"JobSample": map[string]any{"Mode": "CUSTOM_ROWS", "Size": 100},
	})

	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/pj-extras", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var job map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &job))
	jobSample, ok := job["JobSample"].(map[string]any)
	require.True(t, ok, "JobSample must round-trip as an object")
	assert.Equal(t, "CUSTOM_ROWS", jobSample["Mode"])
	assert.NotNil(t, job["ProfileConfiguration"])
}

// TestHandlerCreateRecipeJob_Extras verifies the recipe-job extras wire
// shape (DataCatalogOutputs/EncryptionMode/LogSubscription) round-trips
// through the HTTP handler layer.
func TestHandlerCreateRecipeJob_Extras(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(
		t,
		h,
		http.MethodPost,
		"/databrew/v1/recipes",
		map[string]any{"Name": "rj-extras-r"},
	)
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name":            "rj-extras",
		"RecipeReference": map[string]any{"Name": "rj-extras-r"},
		"RoleArn":         "arn:aws:iam::123456789012:role/r",
		"EncryptionMode":  "SSE-KMS",
		"LogSubscription": "ENABLE",
		"DataCatalogOutputs": []any{
			map[string]any{"DatabaseName": "db1", "TableName": "t1"},
		},
	})

	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/rj-extras", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var job map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &job))
	assert.Equal(t, "SSE-KMS", job["EncryptionMode"])
	assert.Equal(t, "ENABLE", job["LogSubscription"])
	outputs, ok := job["DataCatalogOutputs"].([]any)
	require.True(t, ok)
	require.Len(t, outputs, 1)
}
