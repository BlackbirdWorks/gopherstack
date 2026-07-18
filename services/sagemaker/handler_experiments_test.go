package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ExperimentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create experiment.
	recCreate := doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["ExperimentArn"])

	// Describe experiment.
	recDesc := doSageMakerRequest(t, h, "DescribeExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List experiments.
	recList := doSageMakerRequest(t, h, "ListExperiments", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ExperimentSummaries"].([]any), 1)

	// Delete experiment.
	recDelete := doSageMakerRequest(t, h, "DeleteExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Experiment_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeExperiment", "DeleteExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"ExperimentName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UpdateExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "my-exp",
	})

	rec := doSageMakerRequest(t, h, "UpdateExperiment", map[string]any{
		"ExperimentName": "my-exp",
		"DisplayName":    "My Experiment",
		"Description":    "A test experiment",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["ExperimentArn"])

	// Describe returns updated fields
	rec = doSageMakerRequest(t, h, "DescribeExperiment", map[string]any{
		"ExperimentName": "my-exp",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "My Experiment", descResp["DisplayName"])
	assert.Equal(t, "A test experiment", descResp["Description"])
}

func TestHandler_UpdateExperiment_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateExperiment", map[string]any{
		"ExperimentName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Tags_Experiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "tagged-exp",
		"Tags": []any{
			map[string]any{"Key": "project", "Value": "ml"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	expARN := createResp["ExperimentArn"]
	require.NotEmpty(t, expARN)

	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": expARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "project", tags[0].(map[string]any)["Key"])
}
