package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TrainingPlan_OfferingLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	searchRec := doSageMakerRequest(t, h, "SearchTrainingPlanOfferings", map[string]any{
		"InstanceType": "ml.p5.48xlarge",
	})
	assert.Equal(t, http.StatusOK, searchRec.Code)

	var searchResp map[string]any
	require.NoError(t, json.Unmarshal(searchRec.Body.Bytes(), &searchResp))
	offerings, ok := searchResp["TrainingPlanOfferings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, offerings)

	offering, ok := offerings[0].(map[string]any)
	require.True(t, ok)
	offeringID, _ := offering["TrainingPlanOfferingId"].(string)
	require.NotEmpty(t, offeringID)

	createRec := doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName":       "gpu-plan",
		"TrainingPlanOfferingId": offeringID,
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{
		"TrainingPlanName": "gpu-plan",
	})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "Scheduled", describeResp["Status"])
	assert.InEpsilon(t, float64(720), describeResp["DurationHours"], 0.001)

	rcSummaries, ok := describeResp["ReservedCapacitySummaries"].([]any)
	require.True(t, ok)
	require.Len(t, rcSummaries, 1)
	rcSummary, ok := rcSummaries[0].(map[string]any)
	require.True(t, ok)
	rcArn, _ := rcSummary["ReservedCapacityArn"].(string)
	require.NotEmpty(t, rcArn)

	planArn, _ := describeResp["TrainingPlanArn"].(string)
	require.NotEmpty(t, planArn)

	// DescribeReservedCapacity
	rcRec := doSageMakerRequest(t, h, "DescribeReservedCapacity", map[string]any{
		"ReservedCapacityArn": rcArn,
	})
	assert.Equal(t, http.StatusOK, rcRec.Code)

	var rcResp map[string]any
	require.NoError(t, json.Unmarshal(rcRec.Body.Bytes(), &rcResp))
	assert.Equal(t, "ml.p5.48xlarge", rcResp["InstanceType"])
	assert.InEpsilon(t, float64(8), rcResp["TotalInstanceCount"], 0.001)

	// ListTrainingPlans
	listRec := doSageMakerRequest(t, h, "ListTrainingPlans", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["TrainingPlanSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	// Extension lifecycle: search extension offerings tied to this plan.
	extSearchRec := doSageMakerRequest(t, h, "SearchTrainingPlanOfferings", map[string]any{
		"TrainingPlanArn": planArn,
	})
	assert.Equal(t, http.StatusOK, extSearchRec.Code)

	var extSearchResp map[string]any
	require.NoError(t, json.Unmarshal(extSearchRec.Body.Bytes(), &extSearchResp))
	extOfferings, ok := extSearchResp["TrainingPlanExtensionOfferings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, extOfferings)

	extOffering, ok := extOfferings[0].(map[string]any)
	require.True(t, ok)
	extOfferingID, _ := extOffering["TrainingPlanExtensionOfferingId"].(string)
	require.NotEmpty(t, extOfferingID)

	extendRec := doSageMakerRequest(t, h, "ExtendTrainingPlan", map[string]any{
		"TrainingPlanExtensionOfferingId": extOfferingID,
	})
	assert.Equal(t, http.StatusOK, extendRec.Code)

	var extendResp map[string]any
	require.NoError(t, json.Unmarshal(extendRec.Body.Bytes(), &extendResp))
	extensions, ok := extendResp["TrainingPlanExtensions"].([]any)
	require.True(t, ok)
	assert.Len(t, extensions, 1)

	historyRec := doSageMakerRequest(t, h, "DescribeTrainingPlanExtensionHistory", map[string]any{
		"TrainingPlanArn": planArn,
	})
	assert.Equal(t, http.StatusOK, historyRec.Code)

	var historyResp map[string]any
	require.NoError(t, json.Unmarshal(historyRec.Body.Bytes(), &historyResp))
	historyExtensions, ok := historyResp["TrainingPlanExtensions"].([]any)
	require.True(t, ok)
	assert.Len(t, historyExtensions, 1)

	// Duration grew by the consumed extension offering's duration.
	describeAfterRec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{
		"TrainingPlanName": "gpu-plan",
	})

	var describeAfterResp map[string]any
	require.NoError(t, json.Unmarshal(describeAfterRec.Body.Bytes(), &describeAfterResp))
	assert.Greater(t, describeAfterResp["DurationHours"], describeResp["DurationHours"])
}

func TestHandler_TrainingPlan_UltraServerOffering(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	searchRec := doSageMakerRequest(t, h, "SearchTrainingPlanOfferings", map[string]any{
		"UltraServerType": "ml.u-p6e-gb200x72",
	})

	var searchResp map[string]any
	require.NoError(t, json.Unmarshal(searchRec.Body.Bytes(), &searchResp))
	offerings, ok := searchResp["TrainingPlanOfferings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, offerings)

	offering, ok := offerings[0].(map[string]any)
	require.True(t, ok)
	offeringID, _ := offering["TrainingPlanOfferingId"].(string)

	doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName":       "ultraserver-plan",
		"TrainingPlanOfferingId": offeringID,
	})

	describeRec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{
		"TrainingPlanName": "ultraserver-plan",
	})

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	rcSummaries, ok := describeResp["ReservedCapacitySummaries"].([]any)
	require.True(t, ok)
	require.Len(t, rcSummaries, 1)

	rcSummary, ok := rcSummaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UltraServer", rcSummary["ReservedCapacityType"])
	rcArn, _ := rcSummary["ReservedCapacityArn"].(string)

	ultraRec := doSageMakerRequest(t, h, "ListUltraServersByReservedCapacity", map[string]any{
		"ReservedCapacityArn": rcArn,
	})
	assert.Equal(t, http.StatusOK, ultraRec.Code)

	var ultraResp map[string]any
	require.NoError(t, json.Unmarshal(ultraRec.Body.Bytes(), &ultraResp))
	servers, ok := ultraResp["UltraServers"].([]any)
	require.True(t, ok)
	assert.Len(t, servers, 1)
}

func TestHandler_DescribeReservedCapacity_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeReservedCapacity", map[string]any{
		"ReservedCapacityArn": "arn:aws:sagemaker:us-east-1:000000000000:reserved-capacity/nope",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateTrainingPlan_WithoutOffering_StaysMinimal(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName": "no-offering-plan",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{
		"TrainingPlanName": "no-offering-plan",
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "Active", resp["Status"])
	assert.Nil(t, resp["ReservedCapacitySummaries"])
}
