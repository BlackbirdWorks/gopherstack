package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TrialLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create experiment first.
	doSageMakerRequest(
		t,
		h,
		"CreateExperiment",
		map[string]any{"ExperimentName": "trial-experiment"},
	)

	// Create trial.
	recCreate := doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe trial.
	recDesc := doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List trials.
	recList := doSageMakerRequest(t, h, "ListTrials", map[string]any{
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["TrialSummaries"].([]any), 1)

	// Delete trial.
	recDelete := doSageMakerRequest(t, h, "DeleteTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_UpdateTrial(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "exp",
	})

	rec := doSageMakerRequest(t, h, "UpdateTrial", map[string]any{
		"TrialName":   "my-trial",
		"DisplayName": "Trial Display",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["TrialArn"])

	// Describe returns updated DisplayName
	rec = doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Trial Display", descResp["DisplayName"])
}
