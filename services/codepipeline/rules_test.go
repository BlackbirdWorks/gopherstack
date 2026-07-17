package codepipeline_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestListRuleTypes_ReturnsCatalog(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	types := b.ListRuleTypes()

	require.NotEmpty(t, types)

	for _, rt := range types {
		id, ok := rt["id"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Rule", id["category"])
		assert.Equal(t, "AWS", id["owner"])
		assert.NotEmpty(t, id["provider"])
		assert.NotEmpty(t, id["version"])
	}
}

// TestListRuleExecutions_KnownAndUnknownPipeline verifies error handling.

func TestListRuleExecutions_KnownAndUnknownPipeline(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.ListRuleExecutions(context.Background(), "missing")
	require.Error(t, err)

	_, err = b.CreatePipeline(context.Background(), samplePipeline("re-pipeline"), nil)
	require.NoError(t, err)

	items, err := b.ListRuleExecutions(context.Background(), "re-pipeline")
	require.NoError(t, err)
	assert.Empty(t, items)
}

// TestListDeployActionExecutionTargets_KnownAndUnknown verifies error handling.

func TestCPBounds_ListRuleExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("re-bounds-pipe"),
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		name       string
		maxResults int32
		wantError  bool
	}{
		{"0 uses cap", 0, false},
		{"1 is valid", 1, false},
		{"100 is valid cap", 100, false},
		{"101 exceeds cap", 101, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListRuleExecutions", map[string]any{
				"pipelineName": "re-bounds-pipe",
				"maxResults":   tc.maxResults,
			})

			if tc.wantError {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestHandler_ListExecutionsAndRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("el-pipeline"),
	})
	doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "el-pipeline"})

	// List action executions
	rec := doRequest(t, h, "ListActionExecutions", map[string]any{
		"pipelineName": "el-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "ListActionExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List rule executions
	rec = doRequest(t, h, "ListRuleExecutions", map[string]any{
		"pipelineName": "el-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "ListRuleExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List rule types
	rec = doRequest(t, h, "ListRuleTypes", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// List deploy action execution targets
	rec = doRequest(t, h, "ListDeployActionExecutionTargets", map[string]any{
		"pipelineName":        "el-pipeline",
		"pipelineExecutionId": "some-exec-id",
	})
	require.Equal(t, 200, rec.Code)
}

// ---- Persistence String coverage ----
