package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTrainingJobsForHyperParameterTuningJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hp-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
		},
	})

	rec := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hp-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrainingJobSummaries"].([]any)
	assert.Empty(t, summaries)
}

func TestHandler_ListTrainingJobsForHyperParameterTuningJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_HyperParameterTuningJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create HPT job.
	recCreate := doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
			"ResourceLimits": map[string]any{
				"MaxNumberOfTrainingJobs": 10,
				"MaxParallelTrainingJobs": 2,
			},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["HyperParameterTuningJobArn"])

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List.
	recList := doSageMakerRequest(t, h, "ListHyperParameterTuningJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["HyperParameterTuningJobSummaries"].([]any), 1)

	// Stop.
	recStop := doSageMakerRequest(t, h, "StopHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}
