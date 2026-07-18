package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
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
				out, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
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

				out, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: id})
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

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestSnapshotRestore_ExtendedFields verifies KmsKeyId, RotationLambdaARN, and
// LastRotatedDate survive round-trip — distinct from the round-trip tests above in
// that it exercises rotation-specific fields, not just Description.
func TestSnapshotRestore_ExtendedFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "snap-test",
		SecretString: "v",
		KmsKeyID:     "alias/snap-key",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "snap-test",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123:function:rotator",
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := secretsmanager.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	desc, err := b2.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "snap-test"})
	require.NoError(t, err)
	assert.Equal(t, "alias/snap-key", desc.KmsKeyID)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:rotator", desc.RotationLambdaARN)
	assert.NotNil(t, desc.LastRotatedDate)
}

func TestSecretsManagerHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "snap-secret", SecretString: "snap-value"},
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	freshH := secretsmanager.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	out, err := fresh.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "snap-secret"})
	require.NoError(t, err)
	assert.Equal(t, "snap-secret", out.Name)
}
