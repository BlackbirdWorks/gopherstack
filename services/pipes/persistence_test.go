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

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := pipes.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestSnapshot_PersistsEnrichmentCallCount verifies that Snapshot includes
// enrichment call counters and Restore brings them back.
func TestSnapshot_PersistsEnrichmentCallCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeName  string
		callCount int
	}{
		{name: "single_call", pipeName: "enrich-pipe-a", callCount: 1},
		{name: "multiple_calls", pipeName: "enrich-pipe-b", callCount: 5},
		{name: "zero_calls", pipeName: "enrich-pipe-c", callCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			b3CreatePipe(t, b, tt.pipeName, b3LambdaTarget)

			for range tt.callCount {
				b.RecordEnrichmentCall(context.Background(), tt.pipeName)
			}

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap, "Snapshot should not return nil")

			b2 := b3Backend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			got := b2.GetEnrichmentCallCount(context.Background(), tt.pipeName)
			assert.Equal(t, int64(tt.callCount), got, "enrichment call count should survive snapshot/restore")
		})
	}
}

// TestRestore_MissingEnrichmentCallCount verifies backward compatibility:
// restoring from a snapshot without the enrichment field initialises an empty map.
func TestRestore_MissingEnrichmentCallCount(t *testing.T) {
	t.Parallel()

	legacySnap := []byte(`{"pipes":{},"accountID":"111122223333","region":"eu-west-1"}`)

	b := b3Backend()
	require.NoError(t, b.Restore(t.Context(), legacySnap))

	// Must not panic; count for unknown pipe is zero.
	assert.Equal(t, int64(0), b.GetEnrichmentCallCount(context.Background(), "any-pipe"))
}
