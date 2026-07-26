package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["OptimizationJobArn"], "my-opt-job")
}

func TestHandler_DescribeOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	rec := doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "opt-1", resp["OptimizationJobName"])
}

func TestHandler_StopOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	rec := doSageMakerRequest(t, h, "StopOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	rec := doSageMakerRequest(t, h, "DeleteOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

func TestHandler_ListOptimizationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["OptimizationJobSummaries"])

	// Create and list
	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
		"RoleArn":             "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["OptimizationJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-opt-job", s["OptimizationJobName"])
}

// ---------------------------------------------------------------------------
// AIWorkloadConfig
// ---------------------------------------------------------------------------

func TestHandler_AIWorkloadConfigLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create then describe returns the stored config",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-1",
					"AIWorkloadConfigs":    map[string]any{"WorkloadSpec": map[string]any{"Yaml": "spec: v1"}},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				assert.Contains(t, createResp["AIWorkloadConfigArn"], "cfg-1")

				rec = doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-1",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "cfg-1", descResp["AIWorkloadConfigName"])
				assert.NotEmpty(t, descResp["CreationTime"])
			},
		},
		{
			name: "duplicate name is rejected",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				body := map[string]any{"AIWorkloadConfigName": "cfg-dup"}
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", body)

				rec := doSageMakerRequest(t, h, "CreateAIWorkloadConfig", body)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe missing config returns not found",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ResourceNotFound", out["__type"])
			},
		},
		{
			name: "delete then describe returns not found",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "cfg-del"})

				rec := doSageMakerRequest(t, h, "DeleteAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-del",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-del",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list returns created configs",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "cfg-list"})

				rec := doSageMakerRequest(t, h, "ListAIWorkloadConfigs", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items := resp["AIWorkloadConfigs"].([]any)
				assert.Len(t, items, 1)
				assert.Equal(t, "cfg-list", items[0].(map[string]any)["AIWorkloadConfigName"])
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
