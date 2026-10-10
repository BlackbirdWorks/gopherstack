package codepipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func customJobPipeline(name string) codepipeline.PipelineDeclaration {
	p := samplePipeline(name)
	p.Stages = append(p.Stages, codepipeline.Stage{
		Name: "Test",
		Actions: []codepipeline.Action{{
			Name: "RunTests",
			ActionTypeID: codepipeline.ActionTypeID{
				Category: "Test", Owner: "Custom", Provider: "MyTester", Version: "1",
			},
			Configuration: map[string]string{"Suite": "smoke"},
		}},
	})

	return p
}

func TestCustomActionJobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		run        func(t *testing.T, b *codepipeline.InMemoryBackend, jobID string)
		wantStatus string
		wantOutput bool
		wantErr    bool
	}{
		{
			name: "success_with_result",
			run: func(t *testing.T, b *codepipeline.InMemoryBackend, jobID string) {
				t.Helper()
				require.NoError(t, b.PutJobSuccessResultWith(context.Background(), jobID, codepipeline.JobSuccess{
					ExecutionDetails: &codepipeline.JobExecutionDetails{ExternalExecutionID: "ext-1", Summary: "ok"},
					OutputVariables:  map[string]string{"Passed": "12"},
				}))
			},
			wantStatus: "Succeeded",
			wantOutput: true,
		},
		{
			name: "failure",
			run: func(t *testing.T, b *codepipeline.InMemoryBackend, jobID string) {
				t.Helper()
				require.NoError(t, b.PutJobFailureResultWith(context.Background(), jobID, codepipeline.JobFailure{
					Message: "boom", Type: "JobFailed", ExternalExecutionID: "ext-2",
				}))
			},
			wantStatus: "Failed",
			wantOutput: true,
		},
		{
			name: "continuation_keeps_in_progress",
			run: func(t *testing.T, b *codepipeline.InMemoryBackend, jobID string) {
				t.Helper()
				require.NoError(t, b.PutJobSuccessResultWith(context.Background(), jobID, codepipeline.JobSuccess{
					ContinuationToken: "tok-1",
				}))
			},
			wantStatus: "InProgress",
		},
		{
			name: "second_result_rejected",
			run: func(t *testing.T, b *codepipeline.InMemoryBackend, jobID string) {
				t.Helper()
				require.NoError(t, b.PutJobSuccessResult(context.Background(), jobID))
				require.ErrorIs(t, b.PutJobFailureResult(context.Background(), jobID, "late", "JobFailed"),
					codepipeline.ErrInvalidJobState)
			},
			wantStatus: "Succeeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
			p := customJobPipeline("job-" + tt.name)
			_, err := b.CreatePipeline(ctx, p, nil)
			require.NoError(t, err)

			exec, err := b.StartPipelineExecution(ctx, p.Name)
			require.NoError(t, err)
			require.Equal(t, "InProgress", exec.Status)

			miss, err := b.PollForJobsQuery(ctx, "Test", "Custom", "MyTester", "1", map[string]string{"Suite": "full"})
			require.NoError(t, err)
			assert.Empty(t, miss, "queryParam must filter on the action configuration")

			jobs, err := b.PollForJobsQuery(ctx, "Test", "Custom", "MyTester", "1", map[string]string{"Suite": "smoke"})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			assert.Equal(t, exec.PipelineExecutionID, jobs[0].ExecutionID)

			tt.run(t, b, jobs[0].ID)

			got, err := b.GetPipelineExecution(ctx, p.Name, exec.PipelineExecutionID)
			require.NoError(t, err)

			wantExec := tt.wantStatus
			assert.Equal(t, wantExec, got.Status)

			acts, err := b.ListActionExecutions(ctx, p.Name, exec.PipelineExecutionID)
			require.NoError(t, err)

			var test map[string]any

			for _, a := range acts {
				if a["actionName"] == "RunTests" {
					test = a
				}
			}

			require.NotNil(t, test)
			assert.Equal(t, tt.wantStatus, test["status"])

			if tt.wantOutput {
				assert.Contains(t, test, "output")
			}

			if tt.name == "continuation_keeps_in_progress" {
				next, pollErr := b.PollForJobs(ctx, "Test", "Custom", "MyTester", "1")
				require.NoError(t, pollErr)
				require.Len(t, next, 1)
				assert.Equal(t, "tok-1", next[0].ContinuationToken)
			}
		})
	}
}
