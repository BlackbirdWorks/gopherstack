package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Family 1 — Training Plans / Reserved Capacity
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Family 2 — AutoML candidates / Search / model metadata
// ---------------------------------------------------------------------------

func TestHandler_ListCandidatesForAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "my-automl-job",
		"RoleArn":       "arn:test",
	})

	rec := doSageMakerRequest(t, h, "ListCandidatesForAutoMLJob", map[string]any{
		"AutoMLJobName": "my-automl-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	candidates, ok := resp["Candidates"].([]any)
	require.True(t, ok)
	assert.Len(t, candidates, 3)

	first, ok := candidates[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Completed", first["CandidateStatus"])
	assert.NotNil(t, first["FinalAutoMLJobObjectiveMetric"])
}

func TestHandler_ListCandidatesForAutoMLJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListCandidatesForAutoMLJob", map[string]any{
		"AutoMLJobName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Search_TrainingJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
		"TrainingJobName":        "search-job",
		"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/output"},
		"ResourceConfig": map[string]any{
			"InstanceType":   "ml.m5.large",
			"InstanceCount":  1,
			"VolumeSizeInGB": 20,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	})

	rec := doSageMakerRequest(t, h, "Search", map[string]any{
		"Resource": "TrainingJob",
		"SearchExpression": map[string]any{
			"Filters": []map[string]any{
				{"Name": "TrainingJobName", "Operator": "Equals", "Value": "search-job"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, ok := resp["Results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 1)

	result, ok := results[0].(map[string]any)
	require.True(t, ok)
	tj, ok := result["TrainingJob"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "search-job", tj["TrainingJobName"])

	// A non-matching filter yields no results.
	noMatchRec := doSageMakerRequest(t, h, "Search", map[string]any{
		"Resource": "TrainingJob",
		"SearchExpression": map[string]any{
			"Filters": []map[string]any{
				{"Name": "TrainingJobName", "Operator": "Equals", "Value": "nope"},
			},
		},
	})

	var noMatchResp map[string]any
	require.NoError(t, json.Unmarshal(noMatchRec.Body.Bytes(), &noMatchResp))
	assert.Empty(t, noMatchResp["Results"])
}

func TestHandler_Search_InvalidResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "Search", map[string]any{
		"Resource": "NotARealResource",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListModelMetadata", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["ModelMetadataSummaries"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, summaries)

	filteredRec := doSageMakerRequest(t, h, "ListModelMetadata", map[string]any{
		"SearchExpression": map[string]any{
			"Filters": []map[string]any{{"Name": "Framework", "Value": "XGBOOST"}},
		},
	})

	var filteredResp map[string]any
	require.NoError(t, json.Unmarshal(filteredRec.Body.Bytes(), &filteredResp))
	filtered, ok := filteredResp["ModelMetadataSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, filtered, 1)
	entry, ok := filtered[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "XGBOOST", entry["Framework"])
}

func TestHandler_GetSearchSuggestions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "GetSearchSuggestions", map[string]any{
		"Resource": "TrainingJob",
		"SuggestionQuery": map[string]any{
			"PropertyNameQuery": map[string]any{"PropertyNameHint": "TrainingJobS"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	suggestions, ok := resp["PropertyNameSuggestions"].([]any)
	require.True(t, ok)
	require.Len(t, suggestions, 1)
	s, ok := suggestions[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TrainingJobStatus", s["PropertyName"])
}

func TestHandler_GetScalingConfigurationRecommendation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
		"JobType": "Default",
		"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
	})

	rec := doSageMakerRequest(t, h, "GetScalingConfigurationRecommendation", map[string]any{
		"InferenceRecommendationsJobName": "my-rec-job",
		"TargetCpuUtilizationPerCore":     60,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InEpsilon(t, float64(60), resp["TargetCpuUtilizationPerCore"], 0.001)
	dyn, ok := resp["DynamicScalingConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.NotNil(t, dyn["MinCapacity"])
}

func TestHandler_GetScalingConfigurationRecommendation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "GetScalingConfigurationRecommendation", map[string]any{
		"InferenceRecommendationsJobName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Family 3 — ModelCard export + Image/ImageVersion updates
// ---------------------------------------------------------------------------

func TestHandler_ModelCardExportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	createRec := doSageMakerRequest(t, h, "CreateModelCardExportJob", map[string]any{
		"ModelCardExportJobName": "export-1",
		"ModelCardName":          "my-card",
		"OutputConfig":           map[string]any{"S3OutputPath": "s3://bucket/exports"},
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobArn, _ := createResp["ModelCardExportJobArn"].(string)
	require.NotEmpty(t, jobArn)

	describeRec := doSageMakerRequest(t, h, "DescribeModelCardExportJob", map[string]any{
		"ModelCardExportJobArn": jobArn,
	})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "Completed", describeResp["Status"])
	assert.Equal(t, "export-1", describeResp["ModelCardExportJobName"])
	assert.Equal(t, "my-card", describeResp["ModelCardName"])

	listRec := doSageMakerRequest(t, h, "ListModelCardExportJobs", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["ModelCardExportJobSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)
}

func TestHandler_CreateModelCardExportJob_ModelCardNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCardExportJob", map[string]any{
		"ModelCardExportJobName": "export-1",
		"ModelCardName":          "does-not-exist",
		"OutputConfig":           map[string]any{"S3OutputPath": "s3://bucket/exports"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeModelCardExportJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeModelCardExportJob", map[string]any{
		"ModelCardExportJobArn": "arn:aws:sagemaker:us-east-1:000000000000:model-card/x/export-job/y",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName":   "my-image",
		"RoleArn":     "arn:test",
		"Description": "original",
	})

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":   "my-image",
		"DisplayName": "My Image",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["Description"])
	assert.Equal(t, "My Image", resp["DisplayName"])

	// DeleteProperties clears Description.
	doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":        "my-image",
		"DeleteProperties": []string{"Description"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	assert.Empty(t, resp2["Description"])
}

func TestHandler_UpdateImage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "my-image",
	})

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":    "my-image",
		"Version":      1,
		"MLFramework":  "PyTorch 2.0",
		"AliasesToAdd": []string{"latest", "stable"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "PyTorch 2.0", resp["MLFramework"])
	aliases, ok := resp["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	// Remove one alias.
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":       "my-image",
		"Version":         1,
		"AliasesToDelete": []string{"latest"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	aliases2, ok := resp2["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases2, 1)
	assert.Equal(t, "stable", aliases2[0])
}

func TestHandler_UpdateImageVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "does-not-exist",
		"Version":   1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
