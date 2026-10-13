package sagemaker //nolint:testpackage // needs startTransitionDelay

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelineExecution_StepsFollowDefinition(t *testing.T) {
	t.Parallel()

	const def = `{"Version":"2020-12-01","Steps":[{"Name":"prep","Type":"Processing"},{"Name":"train","Type":"Training"}]}`

	tests := []struct {
		start func(b *InMemoryBackend) (*PipelineExecution, error)
		name  string
	}{
		{name: "plain", start: func(b *InMemoryBackend) (*PipelineExecution, error) {
			return b.StartPipelineExecution(context.Background(), "p")
		}},
		{name: "full", start: func(b *InMemoryBackend) (*PipelineExecution, error) {
			return b.StartPipelineExecutionFull(context.Background(), StartPipelineExecutionOptions{PipelineName: "p"})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Shutdown(context.Background())

				_, err := b.CreatePipeline(context.Background(), "p", def, "role", nil)
				require.NoError(t, err)

				exec, err := tt.start(b)
				require.NoError(t, err)

				list := func() map[string]string {
					steps, _ := b.ListPipelineExecutionSteps(context.Background(),
						ListPipelineExecutionStepsParams{ExecutionArn: exec.PipelineExecutionArn})
					out := map[string]string{}
					for _, s := range steps {
						out[s.StepName] = s.StepStatus
					}

					return out
				}

				assert.Equal(t, map[string]string{"prep": "Executing", "train": "Executing"}, list())

				time.Sleep(startTransitionDelay + time.Millisecond)
				synctest.Wait()

				assert.Equal(t, map[string]string{"prep": "Succeeded", "train": "Succeeded"}, list())
			})
		})
	}
}
