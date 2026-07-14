package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
