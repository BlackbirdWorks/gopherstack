package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TrainingJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create training job.
	recCreate := doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
		"TrainingJobName":        "my-training-job",
		"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/output"},
		"ResourceConfig": map[string]any{
			"InstanceType":   "ml.m5.large",
			"InstanceCount":  1,
			"VolumeSizeInGB": 20,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["TrainingJobArn"])

	// Describe training job.
	recDesc := doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List training jobs.
	recList := doSageMakerRequest(t, h, "ListTrainingJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["TrainingJobSummaries"].([]any), 1)

	// StopTrainingJob.
	recStop := doSageMakerRequest(t, h, "StopTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// UpdateTrainingJob.
	recUpdate := doSageMakerRequest(t, h, "UpdateTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// DeleteTrainingJob.
	recDelete := doSageMakerRequest(t, h, "DeleteTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

// ---------------------------------------------------------------------------
// Notebook Instance lifecycle
// ---------------------------------------------------------------------------
