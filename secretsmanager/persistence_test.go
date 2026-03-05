package secretsmanager_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *secretsmanager.InMemoryBackend) string
		verify func(t *testing.T, b *secretsmanager.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *secretsmanager.InMemoryBackend) string {
				out, err := b.CreateSecret(&secretsmanager.CreateSecretInput{
					Name:         "test-secret",
					Description:  "test description",
					SecretString: "my-secret-value",
				})
				if err != nil {
					return ""
				}

				return out.Name
			},
			verify: func(t *testing.T, b *secretsmanager.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.Name)
				assert.Equal(t, "test description", out.Description)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *secretsmanager.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *secretsmanager.InMemoryBackend, _ string) {
				t.Helper()

				secrets := b.ListAll()
				assert.Empty(t, secrets)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestSecretsManagerHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "snap-secret", SecretString: "snap-value"})
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	freshH := secretsmanager.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	out, err := fresh.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "snap-secret"})
	require.NoError(t, err)
	assert.Equal(t, "snap-secret", out.Name)
}
