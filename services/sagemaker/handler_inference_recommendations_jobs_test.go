package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_InferenceRecommendationsJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateInferenceRecommendationsJob", map[string]any{
		"JobName":        "my-rec-job",
		"JobType":        "Default",
		"JobDescription": "Test recommendation job",
		"RoleArn":        "arn:aws:iam::000000000000:role/TestRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["JobArn"], "my-rec-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-rec-job", descResp["JobName"])
	assert.Equal(t, "IN_PROGRESS", descResp["Status"])
	assert.Equal(t, "Default", descResp["JobType"])
	recs := descResp["InferenceRecommendations"].([]any)
	assert.Empty(t, recs)

	// List
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["InferenceRecommendationsJobs"].([]any)
	assert.Len(t, summaries, 1)

	// List steps (always empty)
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var stepsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stepsResp))
	steps := stepsResp["Steps"].([]any)
	assert.Empty(t, steps)

	// Stop
	rec = doSageMakerRequest(t, h, "StopInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["Status"])
}

func TestHandler_CreateInferenceRecommendationsJob_InputConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateInferenceRecommendationsJob", map[string]any{
		"JobName": "rec-job-input-config",
		"JobType": "Default",
		"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
		"InputConfig": map[string]any{
			"ModelName": "my-model",
			"Endpoints": []any{
				map[string]any{"EndpointName": "my-endpoint"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "rec-job-input-config",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	inputConfig, ok := descResp["InputConfig"].(map[string]any)
	require.True(t, ok, "DescribeInferenceRecommendationsJob must return the accepted InputConfig")
	assert.Equal(t, "my-model", inputConfig["ModelName"])
}

func TestHandler_InferenceRecommendationsJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListInferenceRecommendationsJobSteps_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ListMlflowTrackingServers tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AIRecommendationJob
// ---------------------------------------------------------------------------

func TestHandler_AIRecommendationJobLifecycle(t *testing.T) {
	t.Parallel()

	createBody := func(name, workloadConfig string) map[string]any {
		return map[string]any{
			"AIRecommendationJobName":    name,
			"AIWorkloadConfigIdentifier": workloadConfig,
			"RoleArn":                    "arn:aws:iam::000000000000:role/TestRole",
			"ModelSource":                map[string]any{"S3": map[string]any{"S3Uri": "s3://bucket/model/"}},
			"OutputConfig":               map[string]any{"S3OutputLocation": "s3://bucket/out/"},
			"PerformanceTarget":          map[string]any{"MetricName": "ttft-ms", "Threshold": 100},
		}
	}

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create rejects an unknown AIWorkloadConfigIdentifier",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "CreateAIRecommendationJob", createBody("rec-1", "nonexistent-config"))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create then describe starts InProgress with empty Recommendations",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "wc-rec"})

				rec := doSageMakerRequest(t, h, "CreateAIRecommendationJob", createBody("rec-2", "wc-rec"))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				assert.Contains(t, createResp["AIRecommendationJobArn"], "rec-2")

				rec = doSageMakerRequest(t, h, "DescribeAIRecommendationJob", map[string]any{
					"AIRecommendationJobName": "rec-2",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "InProgress", descResp["AIRecommendationJobStatus"])
				assert.Empty(t, descResp["Recommendations"])
			},
		},
		{
			name: "stop transitions the job to Stopping",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "wc-stop"})
				doSageMakerRequest(t, h, "CreateAIRecommendationJob", createBody("rec-stop", "wc-stop"))

				rec := doSageMakerRequest(t, h, "StopAIRecommendationJob", map[string]any{
					"AIRecommendationJobName": "rec-stop",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeAIRecommendationJob", map[string]any{
					"AIRecommendationJobName": "rec-stop",
				})
				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "Stopping", descResp["AIRecommendationJobStatus"])
			},
		},
		{
			name: "delete removes the job",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "wc-del"})
				doSageMakerRequest(t, h, "CreateAIRecommendationJob", createBody("rec-del", "wc-del"))

				rec := doSageMakerRequest(t, h, "DeleteAIRecommendationJob", map[string]any{
					"AIRecommendationJobName": "rec-del",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeAIRecommendationJob", map[string]any{
					"AIRecommendationJobName": "rec-del",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list returns created jobs",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "wc-list"})
				doSageMakerRequest(t, h, "CreateAIRecommendationJob", createBody("rec-list", "wc-list"))

				rec := doSageMakerRequest(t, h, "ListAIRecommendationJobs", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items := resp["AIRecommendationJobs"].([]any)
				require.Len(t, items, 1)
				assert.Equal(t, "rec-list", items[0].(map[string]any)["AIRecommendationJobName"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}
