package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreatePipeline_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":        "my-pipeline",
		"PipelineDisplayName": "My Pipeline",
		"PipelineDescription": "A test pipeline",
		"RoleArn":             "arn:aws:iam::000000000000:role/SageMakerRole",
		"ParallelismConfiguration": map[string]any{
			"MaxParallelExecutionSteps": 5,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe returns new fields
	rec = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "My Pipeline", descResp["PipelineDisplayName"])
	assert.Equal(t, "A test pipeline", descResp["PipelineDescription"])
	assert.NotNil(t, descResp["ParallelismConfiguration"])
}

func TestHandler_UpdatePipeline_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "my-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName":        "my-pipeline",
		"PipelineDisplayName": "Updated Display",
		"PipelineDescription": "Updated desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "my-pipeline",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Updated Display", descResp["PipelineDisplayName"])
	assert.Equal(t, "Updated desc", descResp["PipelineDescription"])
}

func TestHandler_StartPipelineExecution_WithParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "param-pipeline",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName":                 "param-pipeline",
		"PipelineExecutionDisplayName": "Run 1",
		"PipelineExecutionDescription": "First run",
		"PipelineParameters": []any{
			map[string]any{"Name": "learning_rate", "Value": "0.001"},
			map[string]any{"Name": "epochs", "Value": "10"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["PipelineExecutionArn"]
	assert.NotEmpty(t, execArn)

	// Describe returns parameters
	rec = doSageMakerRequest(t, h, "DescribePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Run 1", descResp["PipelineExecutionDisplayName"])
	assert.Equal(t, "First run", descResp["PipelineExecutionDescription"])
	params := descResp["PipelineParameters"].([]any)
	assert.Len(t, params, 2)
}

// ---------------------------------------------------------------------------
// DescribeEndpoint — ProductionVariants + FailureReason (gap #9)
// ---------------------------------------------------------------------------

func TestHandler_ListPipelineParametersForExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline and start execution with parameters.
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "param-pipe",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
		"RoleArn":            "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName":                 "param-pipe",
		"PipelineExecutionDisplayName": "run-1",
		"PipelineParameters": []map[string]string{
			{"Name": "LearningRate", "Value": "0.01"},
			{"Name": "BatchSize", "Value": "32"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["PipelineExecutionArn"]
	require.NotEmpty(t, execArn)

	// ListPipelineParametersForExecution returns the stored parameters.
	rec2 := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp struct {
		PipelineParameters []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"PipelineParameters"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp))
	assert.Len(t, listResp.PipelineParameters, 2)

	names := make(map[string]string)
	for _, p := range listResp.PipelineParameters {
		names[p.Name] = p.Value
	}
	assert.Equal(t, "0.01", names["LearningRate"])
	assert.Equal(t, "32", names["BatchSize"])
}

func TestHandler_ListPipelineParametersForExecution_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "empty-param-pipe",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
		"RoleArn":            "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "empty-param-pipe",
	})
	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))

	rec2 := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": startResp["PipelineExecutionArn"],
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp struct {
		PipelineParameters []any `json:"PipelineParameters"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp))
	assert.NotNil(t, listResp.PipelineParameters)
	assert.Empty(t, listResp.PipelineParameters)
}

func TestHandler_ListPipelineParametersForExecution_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": "arn:aws:sagemaker:us-east-1:000000000000:pipeline/nonexistent/execution/abc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
