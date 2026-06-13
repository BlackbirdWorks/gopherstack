package pipes_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func TestPipes_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *pipes.InMemoryBackend)
		verify func(t *testing.T, b *pipes.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *pipes.InMemoryBackend) {},
			verify: func(t *testing.T, b *pipes.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.ListPipesAll())
			},
		},
		{
			name: "pipe_preserved",
			setup: func(b *pipes.InMemoryBackend) {
				_, _ = b.CreatePipeSimple("my-pipe", "arn:aws:iam::123:role/r",
					"arn:aws:sqs:us-east-1:123:src",
					"arn:aws:lambda:us-east-1:123:function:fn",
					"desc", "RUNNING", nil)
			},
			verify: func(t *testing.T, b *pipes.InMemoryBackend) {
				t.Helper()
				ps := b.ListPipesAll()
				require.Len(t, ps, 1)
				assert.Equal(t, "my-pipe", ps[0].Name)
				err := b.TagResource(context.Background(), ps[0].ARN, map[string]string{"env": "test"})
				require.NoError(t, err)
			},
		},
		{
			name: "source_parameters_preserved",
			setup: func(b *pipes.InMemoryBackend) {
				_, _ = b.CreatePipe(context.Background(), pipes.CreatePipeInput{
					Name:    "param-pipe",
					RoleARN: "arn:aws:iam::123:role/r",
					Source:  "arn:aws:sqs:us-east-1:123:src",
					Target:  "arn:aws:lambda:us-east-1:123:function:fn",
					SourceParameters: &pipes.SourceParameters{
						SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 5},
					},
				})
			},
			verify: func(t *testing.T, b *pipes.InMemoryBackend) {
				t.Helper()
				ps := b.ListPipesAll()
				require.Len(t, ps, 1)
				require.NotNil(t, ps[0].SourceParameters)
				require.NotNil(t, ps[0].SourceParameters.SqsQueueParameters)
				assert.Equal(t, 5, ps[0].SourceParameters.SqsQueueParameters.BatchSize)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := pipes.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := pipes.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
