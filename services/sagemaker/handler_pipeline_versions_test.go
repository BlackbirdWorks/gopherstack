package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PipelineVersions_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "ver-pipeline", "PipelineDefinition": `{"Version":"v1"}`,
	})

	recList := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	versions, _ := listOut["PipelineVersionSummaries"].([]any)
	require.Len(t, versions, 1)
	assert.InDelta(t, float64(1), versions[0].(map[string]any)["PipelineVersionId"], 0)

	doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName": "ver-pipeline", "PipelineDefinition": `{"Version":"v2"}`,
	})

	recList2 := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recList2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	versions2, _ := listOut2["PipelineVersionSummaries"].([]any)
	require.Len(t, versions2, 2)
	// Newest first.
	assert.InDelta(t, float64(2), versions2[0].(map[string]any)["PipelineVersionId"], 0)

	// SortOrder=Ascending reverses the default newest-first order.
	recAsc := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{
		"PipelineName": "ver-pipeline", "SortOrder": "Ascending",
	})
	require.Equal(t, http.StatusOK, recAsc.Code)

	var ascOut map[string]any
	require.NoError(t, json.Unmarshal(recAsc.Body.Bytes(), &ascOut))
	ascVersions, _ := ascOut["PipelineVersionSummaries"].([]any)
	require.Len(t, ascVersions, 2)
	assert.InDelta(t, float64(1), ascVersions[0].(map[string]any)["PipelineVersionId"], 0)
	assert.InDelta(t, float64(2), ascVersions[1].(map[string]any)["PipelineVersionId"], 0)

	// CreatedAfter/CreatedBefore actually filter, not just parse-and-drop.
	recFuture := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{
		"PipelineName": "ver-pipeline",
		"CreatedAfter": float64(time.Now().Add(time.Hour).Unix()),
	})
	require.Equal(t, http.StatusOK, recFuture.Code)

	var futureOut map[string]any
	require.NoError(t, json.Unmarshal(recFuture.Body.Bytes(), &futureOut))
	futureVersions, _ := futureOut["PipelineVersionSummaries"].([]any)
	assert.Empty(t, futureVersions, "CreatedAfter in the future must exclude every version")

	recPast := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{
		"PipelineName":  "ver-pipeline",
		"CreatedBefore": float64(time.Now().Add(-time.Hour).Unix()),
	})
	require.Equal(t, http.StatusOK, recPast.Code)

	var pastOut map[string]any
	require.NoError(t, json.Unmarshal(recPast.Body.Bytes(), &pastOut))
	pastVersions, _ := pastOut["PipelineVersionSummaries"].([]any)
	assert.Empty(t, pastVersions, "CreatedBefore in the past must exclude every version")

	recDescribe := doSageMakerRequest(t, h, "DescribePipeline", map[string]any{"PipelineName": "ver-pipeline"})
	var pipelineOut map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &pipelineOut))
	pipelineArn, _ := pipelineOut["PipelineArn"].(string)
	require.NotEmpty(t, pipelineArn)

	recUpdateVersion := doSageMakerRequest(t, h, "UpdatePipelineVersion", map[string]any{
		"PipelineArn": pipelineArn, "PipelineVersionId": 1, "PipelineVersionDescription": "first cut",
	})
	require.Equal(t, http.StatusOK, recUpdateVersion.Code)

	recStart := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recStart.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(recStart.Body.Bytes(), &startOut))
	execArn, _ := startOut["PipelineExecutionArn"].(string)
	require.NotEmpty(t, execArn)

	recDefinition := doSageMakerRequest(t, h, "DescribePipelineDefinitionForExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	require.Equal(t, http.StatusOK, recDefinition.Code)

	var defOut map[string]any
	require.NoError(t, json.Unmarshal(recDefinition.Body.Bytes(), &defOut))
	assert.JSONEq(t, `{"Version":"v2"}`, defOut["PipelineDefinition"].(string))

	recUpdateExec := doSageMakerRequest(t, h, "UpdatePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn, "PipelineExecutionDescription": "manual re-run",
	})
	require.Equal(t, http.StatusOK, recUpdateExec.Code)

	var updateExecOut map[string]any
	require.NoError(t, json.Unmarshal(recUpdateExec.Body.Bytes(), &updateExecOut))
	assert.Equal(t, execArn, updateExecOut["PipelineExecutionArn"])
}

func TestHandler_PipelineVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "ListPipelineVersions", body: map[string]any{"PipelineName": "no-such-pipeline"}},
		{
			op: "UpdatePipelineVersion",
			body: map[string]any{
				"PipelineArn":       "arn:aws:sagemaker:us-east-1:000000000000:pipeline/no-such-pipeline",
				"PipelineVersionId": 1,
			},
		},
		{
			op:   "DescribePipelineDefinitionForExecution",
			body: map[string]any{"PipelineExecutionArn": "no-such-execution"},
		},
		{op: "UpdatePipelineExecution", body: map[string]any{"PipelineExecutionArn": "no-such-execution"}},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// InferenceExperiment Start/Update
// ---------------------------------------------------------------------------
