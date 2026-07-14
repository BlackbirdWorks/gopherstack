package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_EdgePackagingJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
		"ModelName":            "my-model",
		"ModelVersion":         "1.0",
		"RoleArn":              "arn:aws:iam::000000000000:role/TestRole",
		"CompilationJobName":   "my-comp-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["EdgePackagingJobArn"], "my-edge-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-edge-job", descResp["EdgePackagingJobName"])
	assert.Equal(t, "my-model", descResp["ModelName"])
	assert.Equal(t, "1.0", descResp["ModelVersion"])
	assert.Equal(t, "STARTING", descResp["EdgePackagingJobStatus"])

	// List
	rec = doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Stop
	rec = doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stopped
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["EdgePackagingJobStatus"])
}

func TestHandler_EdgePackagingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgePackagingJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"EdgePackagingJobName": "dup-job"}
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)

	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListEdgePackagingJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-b"})
	doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})

	rec := doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{
		"StatusEquals": "STARTING",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob tests
// ---------------------------------------------------------------------------
