package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListCandidatesForAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("my-automl-job"))

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
		"JobName":     "my-rec-job",
		"JobType":     "Default",
		"RoleArn":     "arn:aws:iam::000000000000:role/TestRole",
		"InputConfig": map[string]any{"ModelName": "my-model"},
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

// TestHandler_ListCandidatesForAutoMLJob_SortByStatus asserts SortBy=Status
// -- previously entirely absent from decode, so it had no effect at all.
func TestHandler_ListCandidatesForAutoMLJob_SortByStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("sort-job"))

	rec := doSageMakerRequest(t, h, "ListCandidatesForAutoMLJob", map[string]any{
		"AutoMLJobName": "sort-job",
		"SortBy":        "Status",
		"SortOrder":     "Descending",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	candidates := resp["Candidates"].([]any)
	require.Len(t, candidates, 3)

	first := candidates[0].(map[string]any)
	assert.Equal(t, "InProgress", first["CandidateStatus"])
}

// TestHandler_Search_SortByAndCrossAccount_RealClient asserts SortBy/SortOrder
// -- previously decoded by the handler and then dropped before reaching the
// backend, so a real request specifying either had no effect at all -- and
// CrossAccountFilterOption=CrossAccount, which this single-tenant backend
// correctly answers with zero matches since it models no other account's
// resources.
func TestHandler_Search_SortByAndCrossAccount_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"aaa-job", "zzz-job"} {
		_, err := client.CreateTrainingJob(t.Context(), &sagemakersdk.CreateTrainingJobInput{
			TrainingJobName: aws.String(name),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/TestRole"),
			AlgorithmSpecification: &smtypes.AlgorithmSpecification{
				TrainingInputMode: smtypes.TrainingInputModeFile,
			},
			OutputDataConfig: &smtypes.OutputDataConfig{
				S3OutputPath: aws.String("s3://bucket/output"),
			},
			ResourceConfig: &smtypes.ResourceConfig{
				InstanceType:   smtypes.TrainingInstanceTypeMlM5Large,
				InstanceCount:  aws.Int32(1),
				VolumeSizeInGB: aws.Int32(20),
			},
			StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		})
		require.NoError(t, err)
	}

	out, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource:  smtypes.ResourceTypeTrainingJob,
		SortBy:    aws.String("TrainingJobName"),
		SortOrder: smtypes.SearchSortOrderDescending,
	})
	require.NoError(t, err)
	require.Len(t, out.Results, 2)
	require.NotNil(t, out.Results[0].TrainingJob)
	assert.Equal(t, "zzz-job", aws.ToString(out.Results[0].TrainingJob.TrainingJobName))
	require.NotNil(t, out.TotalHits)
	assert.Equal(t, int64(2), aws.ToInt64(out.TotalHits.Value))

	crossOut, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource:                 smtypes.ResourceTypeTrainingJob,
		CrossAccountFilterOption: smtypes.CrossAccountFilterOptionCrossAccount,
	})
	require.NoError(t, err)
	assert.Empty(t, crossOut.Results)
}

// TestHandler_GetScalingConfigurationRecommendation_ScalingPolicyObjective_RealClient
// asserts ScalingPolicyObjective is echoed back -- previously entirely
// absent from decode, an accept-and-drop gap since the real response
// echoes it back verbatim.
func TestHandler_GetScalingConfigurationRecommendation_ScalingPolicyObjective_RealClient(
	t *testing.T,
) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateInferenceRecommendationsJob(
		t.Context(),
		&sagemakersdk.CreateInferenceRecommendationsJobInput{
			JobName: aws.String("rec-job-spo"),
			JobType: smtypes.RecommendationJobTypeDefault,
			RoleArn: aws.String("arn:aws:iam::000000000000:role/TestRole"),
			InputConfig: &smtypes.RecommendationJobInputConfig{
				ModelName: aws.String("my-model"),
			},
		},
	)
	require.NoError(t, err)

	out, err := client.GetScalingConfigurationRecommendation(
		t.Context(), &sagemakersdk.GetScalingConfigurationRecommendationInput{
			InferenceRecommendationsJobName: aws.String("rec-job-spo"),
			ScalingPolicyObjective: &smtypes.ScalingPolicyObjective{
				MinInvocationsPerMinute: aws.Int32(10),
				MaxInvocationsPerMinute: aws.Int32(100),
			},
		},
	)
	require.NoError(t, err)

	require.NotNil(t, out.ScalingPolicyObjective)
	assert.Equal(t, int32(10), aws.ToInt32(out.ScalingPolicyObjective.MinInvocationsPerMinute))
	assert.Equal(t, int32(100), aws.ToInt32(out.ScalingPolicyObjective.MaxInvocationsPerMinute))
	require.NotNil(t, out.Metric)
}

// searchTrainingJobInput builds a minimal valid CreateTrainingJobInput for
// the Search-family real-client tests below.
func searchTrainingJobInput(name string) *sagemakersdk.CreateTrainingJobInput {
	return &sagemakersdk.CreateTrainingJobInput{
		TrainingJobName: aws.String(name),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/TestRole"),
		AlgorithmSpecification: &smtypes.AlgorithmSpecification{
			TrainingInputMode: smtypes.TrainingInputModeFile,
		},
		OutputDataConfig: &smtypes.OutputDataConfig{
			S3OutputPath: aws.String("s3://bucket/output"),
		},
		ResourceConfig: &smtypes.ResourceConfig{
			InstanceType:   smtypes.TrainingInstanceTypeMlM5Large,
			InstanceCount:  aws.Int32(1),
			VolumeSizeInGB: aws.Int32(20),
		},
		StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
	}
}

// TestHandler_Search_NestedFilters_RealClient asserts NestedFilters -
// previously not decoded from the wire at all, so SearchExpression.Filters
// being empty made matchesSearchExpression match every resource. Per
// types.NestedFilters' doc and the SDK API_NestedFilters.html worked
// example, a NestedFilters entry is satisfied only if a SINGLE object in
// the NestedPropertyName list satisfies every one of its Filters, whose
// Name carries the full dotted path including the NestedPropertyName
// prefix (e.g. "InputDataConfig.DataSource.S3DataSource.S3Uri").
func TestHandler_Search_NestedFilters_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	create := func(name, channelName, s3uri string) {
		in := searchTrainingJobInput(name)
		in.InputDataConfig = []smtypes.Channel{
			{
				ChannelName: aws.String(channelName),
				DataSource: &smtypes.DataSource{
					S3DataSource: &smtypes.S3DataSource{
						S3Uri: aws.String(s3uri), S3DataType: smtypes.S3DataTypeS3Prefix,
					},
				},
			},
		}
		_, err := client.CreateTrainingJob(t.Context(), in)
		require.NoError(t, err)
	}

	create("nested-match", "train", "s3://mybucket/catdata/part1")
	create("nested-wrong-channel", "validation", "s3://mybucket/catdata/part1")
	create("nested-wrong-uri", "train", "s3://otherbucket/dogdata")

	out, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource: smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{
			NestedFilters: []smtypes.NestedFilters{
				{
					NestedPropertyName: aws.String("InputDataConfig"),
					Filters: []smtypes.Filter{
						{
							Name: aws.String("InputDataConfig.ChannelName"), Operator: smtypes.OperatorEquals,
							Value: aws.String("train"),
						},
						{
							Name:     aws.String("InputDataConfig.DataSource.S3DataSource.S3Uri"),
							Operator: smtypes.OperatorContains, Value: aws.String("mybucket/catdata"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Results, 1)
	require.NotNil(t, out.Results[0].TrainingJob)
	assert.Equal(t, "nested-match", aws.ToString(out.Results[0].TrainingJob.TrainingJobName))
}

// TestHandler_Search_SubExpressions_RealClient asserts SubExpressions are
// decoded and recursively evaluated, combined with sibling Filters by the
// SAME single Operator (types.SearchExpression's doc: "every conditional
// statement in all lists"). Previously SubExpressions were entirely absent
// from decode, so an Or across a top-level Filter and a SubExpression had
// no effect on the SubExpression arm at all.
func TestHandler_Search_SubExpressions_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"sub-a", "sub-b", "sub-c"} {
		_, err := client.CreateTrainingJob(t.Context(), searchTrainingJobInput(name))
		require.NoError(t, err)
	}

	out, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource: smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{
			Operator: smtypes.BooleanOperatorOr,
			Filters: []smtypes.Filter{
				{Name: aws.String("TrainingJobName"), Operator: smtypes.OperatorEquals, Value: aws.String("sub-a")},
			},
			SubExpressions: []smtypes.SearchExpression{
				{
					Filters: []smtypes.Filter{
						{
							Name: aws.String("TrainingJobName"), Operator: smtypes.OperatorEquals,
							Value: aws.String("sub-b"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Results, 2)

	names := make([]string, len(out.Results))
	for i, r := range out.Results {
		require.NotNil(t, r.TrainingJob)
		names[i] = aws.ToString(r.TrainingJob.TrainingJobName)
	}
	assert.ElementsMatch(t, []string{"sub-a", "sub-b"}, names)
}

// TestHandler_Search_DefaultOperatorIsAnd_RealClient asserts
// SearchExpression's documented default Operator ("If you want every
// conditional statement in all lists to be satisfied... specify And. ...
// The default value is And.") is honoured when Operator is omitted.
func TestHandler_Search_DefaultOperatorIsAnd_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateTrainingJob(t.Context(), searchTrainingJobInput("and-job"))
	require.NoError(t, err)

	filters := []smtypes.Filter{
		{Name: aws.String("TrainingJobName"), Operator: smtypes.OperatorEquals, Value: aws.String("and-job")},
		{Name: aws.String("TrainingJobStatus"), Operator: smtypes.OperatorEquals, Value: aws.String("Completed")},
	}

	andOut, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource:         smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{Filters: filters},
	})
	require.NoError(t, err)
	assert.Empty(t, andOut.Results)

	orOut, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource: smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{
			Operator: smtypes.BooleanOperatorOr, Filters: filters,
		},
	})
	require.NoError(t, err)
	require.Len(t, orOut.Results, 1)
}

// TestHandler_Search_RangeOperators_TimestampConsistency_RealClient asserts
// GreaterThan/LessThan (previously unimplemented and falling through to an
// unconditional match) and that they compare CreationTime correctly despite
// the response emitting it as an epoch-seconds number while types.Filter's
// doc states timestamp Values are ISO 8601 strings
// (YYYY-mm-dd'T'HH:MM:SS) -- a filter built in the documented format must
// still be able to match this API's own emitted CreationTime.
func TestHandler_Search_RangeOperators_TimestampConsistency_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateTrainingJob(t.Context(), searchTrainingJobInput("range-job"))
	require.NoError(t, err)

	search := func(op smtypes.Operator, value string) []smtypes.SearchRecord {
		out, searchErr := client.Search(t.Context(), &sagemakersdk.SearchInput{
			Resource: smtypes.ResourceTypeTrainingJob,
			SearchExpression: &smtypes.SearchExpression{
				Filters: []smtypes.Filter{
					{Name: aws.String("CreationTime"), Operator: op, Value: aws.String(value)},
				},
			},
		})
		require.NoError(t, searchErr)

		return out.Results
	}

	require.Len(t, search(smtypes.OperatorGreaterThan, "2000-01-01T00:00:00"), 1)
	assert.Empty(t, search(smtypes.OperatorGreaterThan, "2100-01-01T00:00:00"))
	require.Len(t, search(smtypes.OperatorLessThan, "2100-01-01T00:00:00"), 1)
	assert.Empty(t, search(smtypes.OperatorLessThan, "2000-01-01T00:00:00"))
}

// TestHandler_Search_InOperator_RealClient asserts the In operator
// ("the value of Name is one of the comma delimited strings in Value"),
// previously unimplemented and falling through to an unconditional match.
func TestHandler_Search_InOperator_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateTrainingJob(t.Context(), searchTrainingJobInput("in-job"))
	require.NoError(t, err)

	search := func(value string) []smtypes.SearchRecord {
		out, searchErr := client.Search(t.Context(), &sagemakersdk.SearchInput{
			Resource: smtypes.ResourceTypeTrainingJob,
			SearchExpression: &smtypes.SearchExpression{
				Filters: []smtypes.Filter{
					{Name: aws.String("TrainingJobStatus"), Operator: smtypes.OperatorIn, Value: aws.String(value)},
				},
			},
		})
		require.NoError(t, searchErr)

		return out.Results
	}

	require.Len(t, search("Failed,Completed,InProgress"), 1)
	assert.Empty(t, search("Failed,Completed"))
}

// TestHandler_Search_UnsupportedOperator_RealClient asserts an Operator
// value outside types.Operator's documented enum is REJECTED (matches
// nothing) rather than matching every resource -- the over-accept shape
// this class has found elsewhere in this campaign (e.g. an SNS numeric
// operator outside its documented set).
func TestHandler_Search_UnsupportedOperator_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateTrainingJob(t.Context(), searchTrainingJobInput("op-job"))
	require.NoError(t, err)

	out, err := client.Search(t.Context(), &sagemakersdk.SearchInput{
		Resource: smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{
			Filters: []smtypes.Filter{
				{
					Name:     aws.String("TrainingJobName"),
					Operator: smtypes.Operator("Between"),
					Value:    aws.String("op-job"),
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Results)
}

// ---------------------------------------------------------------------------
