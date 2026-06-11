package codepipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestListActionExecutions_TracksExecutions verifies that StartPipelineExecution
// records an action execution per action and that ListActionExecutions returns
// them, optionally filtered by pipelineExecutionId.
func TestListActionExecutions_TracksExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filterFn    func(execID string) string
		name        string
		wantCount   int
		wantErr     bool
		unknownPipe bool
	}{
		{
			name:      "all_executions",
			filterFn:  func(string) string { return "" },
			wantCount: 2, // two StartPipelineExecution calls, one action each
		},
		{
			name:      "filter_by_execution_id",
			filterFn:  func(execID string) string { return execID },
			wantCount: 1,
		},
		{
			name:      "filter_unknown_execution_id",
			filterFn:  func(string) string { return "does-not-exist" },
			wantCount: 0,
		},
		{
			name:        "unknown_pipeline",
			unknownPipe: true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

			if tt.unknownPipe {
				_, err := b.ListActionExecutions(context.Background(), "missing", "")
				require.Error(t, err)

				return
			}

			_, err := b.CreatePipeline(context.Background(), samplePipeline("ae-pipeline"), nil)
			require.NoError(t, err)

			exec1, err := b.StartPipelineExecution(context.Background(), "ae-pipeline")
			require.NoError(t, err)
			_, err = b.StartPipelineExecution(context.Background(), "ae-pipeline")
			require.NoError(t, err)

			items, err := b.ListActionExecutions(
				context.Background(),
				"ae-pipeline",
				tt.filterFn(exec1.PipelineExecutionID),
			)
			require.NoError(t, err)
			assert.Len(t, items, tt.wantCount)

			for _, it := range items {
				assert.Equal(t, "SourceAction", it["actionName"])
				assert.Equal(t, "Source", it["stageName"])
				assert.Equal(t, "Succeeded", it["status"])
				assert.NotEmpty(t, it["actionExecutionId"])
			}
		})
	}
}

// TestListRuleTypes_ReturnsCatalog verifies that ListRuleTypes returns the
// AWS-managed rule type catalog with well-formed identifiers.
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
func TestListDeployActionExecutionTargets_KnownAndUnknown(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.ListDeployActionExecutionTargets(context.Background(), "missing", "exec-1")
	require.Error(t, err)

	_, err = b.CreatePipeline(context.Background(), samplePipeline("dt-pipeline"), nil)
	require.NoError(t, err)

	items, err := b.ListDeployActionExecutionTargets(context.Background(), "dt-pipeline", "exec-1")
	require.NoError(t, err)
	assert.Empty(t, items)
}
