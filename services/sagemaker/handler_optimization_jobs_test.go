package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["OptimizationJobArn"], "my-opt-job")
}

func TestHandler_DescribeOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	rec := doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "opt-1", resp["OptimizationJobName"])
}

func TestHandler_StopOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	rec := doSageMakerRequest(t, h, "StopOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	rec := doSageMakerRequest(t, h, "DeleteOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

func TestHandler_ListOptimizationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["OptimizationJobSummaries"])

	// Create and list
	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
		"RoleArn":             "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["OptimizationJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-opt-job", s["OptimizationJobName"])
}
