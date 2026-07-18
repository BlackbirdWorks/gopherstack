package codepipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestInMemoryBackend_Restore_DefensiveCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn func(t *testing.T)
		name    string
	}{
		{
			name: "Restore followed by mutation does not corrupt backend",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("snap-pl"), nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, b2.Restore(t.Context(), snap))

				// Snap is now parsed and owned by b2; zero it out to detect aliasing.
				for i := range snap {
					snap[i] = 0
				}

				// b2 should still have the pipeline.
				p, err := b2.GetPipeline(context.Background(), "snap-pl")
				require.NoError(t, err)
				assert.Equal(t, "snap-pl", p.Declaration.Name)
			},
		},
		{
			name: "Restore clears prior state",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("old-pl"), nil)
				require.NoError(t, err)

				// Take snapshot of empty state.
				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				emptySnap := b2.Snapshot(t.Context())

				// Restore empty snapshot onto b which has "old-pl".
				require.NoError(t, b.Restore(t.Context(), emptySnap))

				assert.Equal(t, 0, b.PipelineCount())
			},
		},
		{
			name: "Snapshot+Restore round-trips executions",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("exec-snap"), nil)
				require.NoError(t, err)

				exec, err := b.StartPipelineExecution(context.Background(), "exec-snap")
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())

				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, b2.Restore(t.Context(), snap))

				execs, err := b2.ListPipelineExecutions(context.Background(), "exec-snap")
				require.NoError(t, err)
				require.Len(t, execs, 1)
				assert.Equal(t, exec.PipelineExecutionID, execs[0].PipelineExecutionID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.checkFn(t)
		})
	}
}

// --------------------------------------------------------------------------
// #29 UpdatePipeline version check (ConflictException)
// --------------------------------------------------------------------------

func TestCPBackend_PersistenceString(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreatePipeline(context.Background(), samplePipeline("snap-pipe"), nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)
	assert.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)
	assert.Equal(t, 1, b2.PipelineCount())
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("123456789012", "eu-west-1")

	// Populate state.
	b.AddPipelineInternal(samplePipeline("persist-pl"), map[string]string{"tier": "prod"})
	b.AddCustomActionTypeInternal(&codepipeline.CustomActionType{
		Category: "Deploy", Provider: "MyDeploy", Version: "2",
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	})
	b.AddJobInternal(&codepipeline.Job{ID: "persist-job", Nonce: "nonce-42", Status: "Created"})
	b.AddWebhookInternal(&codepipeline.Webhook{Name: "persist-wh", TargetPipeline: "persist-pl"})

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify pipeline.
	p, err := b2.GetPipeline(context.Background(), "persist-pl")
	require.NoError(t, err)
	assert.Equal(t, "persist-pl", p.Declaration.Name)

	// Verify custom action type.
	cat, err := b2.GetActionType(context.Background(), "Deploy", "Custom", "MyDeploy", "2")
	require.NoError(t, err)
	assert.Equal(t, "Deploy", cat.Category)

	// Verify job.
	job, err := b2.GetJobDetails(context.Background(), "persist-job")
	require.NoError(t, err)
	assert.Equal(t, "persist-job", job.ID)

	// Verify region/account carried through.
	assert.Equal(t, "eu-west-1", b2.Region())
}

func TestPersistenceWithStageTransitions(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddPipelineInternal(samplePipeline("trans-pl"), nil)

	err := b.DisableStageTransition(context.Background(), "trans-pl", "Source", "Inbound", "test reason")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StageTransitionCount())

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify stage transition was restored.
	assert.Equal(t, 1, b2.StageTransitionCount())
	state := b2.GetStageTransitionState(context.Background(), "trans-pl", "Source", "Inbound")
	require.NotNil(t, state)
	assert.Equal(t, "test reason", state.Reason)
	assert.True(t, state.Disabled)
}
