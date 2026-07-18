package sagemaker_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeleteProcessingJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProcessingJob", map[string]any{
		"ProcessingJobName": "del-pj",
		"AppSpecification":  map[string]any{"ImageUri": "img:latest"},
		"ProcessingResources": map[string]any{
			"ClusterConfig": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 10},
		},
	})

	// Cannot delete while still InProgress.
	recEarly := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recEarly.Code)

	// Wait for the simulated job to reach a terminal state.
	time.Sleep(400 * time.Millisecond)

	recDelete := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recDescribe.Code)
}

func TestHandler_DeleteProcessingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
