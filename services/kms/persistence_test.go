package kms_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *kms.InMemoryBackend) string
		verify func(t *testing.T, b *kms.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *kms.InMemoryBackend) string {
				out, err := b.CreateKey(&kms.CreateKeyInput{Description: "test key"})
				if err != nil {
					return ""
				}

				return out.KeyMetadata.KeyID
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.DescribeKey(&kms.DescribeKeyInput{KeyID: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.KeyMetadata.KeyID)
				assert.Equal(t, "test key", out.KeyMetadata.Description)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *kms.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *kms.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListKeys(&kms.ListKeysInput{})
				require.NoError(t, err)
				assert.Empty(t, out.Keys)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
