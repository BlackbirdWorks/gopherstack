package pipes_test

import (
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

				assert.Empty(t, b.ListPipes())
			},
		},
		{
			name: "pipe_preserved",
			setup: func(b *pipes.InMemoryBackend) {
				_, _ = b.CreatePipe("my-pipe", "arn:aws:iam::123:role/r", "arn:sqs", "arn:sns", "desc", "RUNNING", nil)
			},
			verify: func(t *testing.T, b *pipes.InMemoryBackend) {
				t.Helper()

				ps := b.ListPipes()
				require.Len(t, ps, 1)
				assert.Equal(t, "my-pipe", ps[0].Name)
				// Verify ARN index is rebuilt (tag by ARN should work)
				err := b.TagResource(ps[0].ARN, map[string]string{"env": "test"})
				require.NoError(t, err)
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
