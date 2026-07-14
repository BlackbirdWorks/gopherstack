package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateTrainingPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName": "my-plan",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrainingPlanArn"], "my-plan")
}

func TestHandler_DescribeTrainingPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{"TrainingPlanName": "plan-1"})
	rec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{"TrainingPlanName": "plan-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "plan-1", resp["TrainingPlanName"])
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer — CreatePresignedMlflowTrackingServerUrl
// ---------------------------------------------------------------------------
