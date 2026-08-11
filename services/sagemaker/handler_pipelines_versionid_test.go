package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribePipeline_PipelineVersionId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "versioned-pipeline",
		"PipelineDefinition": `{"Version":"v1"}`,
		"RoleArn":            "arn:aws:iam::000000000000:role/Role",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	updateRec := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName":       "versioned-pipeline",
		"PipelineDefinition": `{"Version":"v2"}`,
	})
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())

	tests := []struct {
		versionID    any
		wantDef      string
		name         string
		wantNotFound bool
	}{
		{name: "unspecified returns current version", versionID: nil, wantDef: `{"Version":"v2"}`},
		{name: "version 1 returns original definition", versionID: 1, wantDef: `{"Version":"v1"}`},
		{name: "version 2 returns updated definition", versionID: 2, wantDef: `{"Version":"v2"}`},
		{name: "unknown version is not found", versionID: 99, wantNotFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"PipelineName": "versioned-pipeline"}
			if tt.versionID != nil {
				body["PipelineVersionId"] = tt.versionID
			}

			rec := doSageMakerRequest(t, h, "DescribePipeline", body)

			if tt.wantNotFound {
				assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

				return
			}

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantDef, out["PipelineDefinition"])
		})
	}
}

func TestHandler_DescribePipeline_LastRunTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "run-time-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
	})

	beforeRec := doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "run-time-pipeline",
	})
	require.Equal(t, http.StatusOK, beforeRec.Code, beforeRec.Body.String())

	var beforeOut map[string]any
	require.NoError(t, json.Unmarshal(beforeRec.Body.Bytes(), &beforeOut))
	_, hasLastRunTime := beforeOut["LastRunTime"]
	assert.False(t, hasLastRunTime, "a pipeline that has never run must not emit LastRunTime")

	startRec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "run-time-pipeline",
	})
	require.Equal(t, http.StatusOK, startRec.Code, startRec.Body.String())

	afterRec := doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "run-time-pipeline",
	})
	require.Equal(t, http.StatusOK, afterRec.Code, afterRec.Body.String())

	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))

	lastRunTime, ok := afterOut["LastRunTime"].(float64)
	require.True(t, ok, "a pipeline that has run must emit LastRunTime")
	assert.Positive(t, lastRunTime)
}
