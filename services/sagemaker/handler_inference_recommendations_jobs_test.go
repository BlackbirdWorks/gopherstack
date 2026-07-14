package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_InferenceRecommendationsJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateInferenceRecommendationsJob", map[string]any{
		"JobName":        "my-rec-job",
		"JobType":        "Default",
		"JobDescription": "Test recommendation job",
		"RoleArn":        "arn:aws:iam::000000000000:role/TestRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["JobArn"], "my-rec-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-rec-job", descResp["JobName"])
	assert.Equal(t, "IN_PROGRESS", descResp["Status"])
	assert.Equal(t, "Default", descResp["JobType"])
	recs := descResp["InferenceRecommendations"].([]any)
	assert.Empty(t, recs)

	// List
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["InferenceRecommendationsJobs"].([]any)
	assert.Len(t, summaries, 1)

	// List steps (always empty)
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var stepsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stepsResp))
	steps := stepsResp["Steps"].([]any)
	assert.Empty(t, steps)

	// Stop
	rec = doSageMakerRequest(t, h, "StopInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["Status"])
}

func TestHandler_InferenceRecommendationsJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListInferenceRecommendationsJobSteps_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ListMlflowTrackingServers tests
// ---------------------------------------------------------------------------
