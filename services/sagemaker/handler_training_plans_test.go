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
		"TrainingPlanName":       "my-plan",
		"TrainingPlanOfferingId": "tpo-p5-48xlarge-30d",
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrainingPlanArn"], "my-plan")
}

func TestHandler_DescribeTrainingPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName":       "plan-1",
		"TrainingPlanOfferingId": "tpo-p5-48xlarge-30d",
	})
	rec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{"TrainingPlanName": "plan-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "plan-1", resp["TrainingPlanName"])

	// TargetResources/TotalInstanceCount/UpfrontFee were previously tagged
	// json:"-" on TrainingPlan and so never emitted by Describe (though
	// already present in ListTrainingPlans' summaries) -- assert all three
	// now round-trip.
	assert.Equal(t, []any{"training-job"}, resp["TargetResources"])
	assert.InEpsilon(t, float64(8), resp["TotalInstanceCount"], 0)
	assert.Equal(t, "184320.00", resp["UpfrontFee"])
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer — CreatePresignedMlflowTrackingServerUrl
// ---------------------------------------------------------------------------
