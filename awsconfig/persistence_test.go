package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/awsconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *awsconfig.InMemoryBackend) string
		verify func(t *testing.T, b *awsconfig.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *awsconfig.InMemoryBackend) string {
				err := b.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test")
				if err != nil {
					return ""
				}

				return "test-recorder"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders()
				require.Len(t, recorders, 1)
				assert.Equal(t, id, recorders[0].Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *awsconfig.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, _ string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders()
				assert.Empty(t, recorders)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := awsconfig.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := awsconfig.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
