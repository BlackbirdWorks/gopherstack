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

// TestSnapshotRestore_ManagedExternalSecretFields verifies that Type (set via
// CreateSecret) and ExternalSecretRotationRoleArn/ExternalSecretRotationMetadata
// (set via RotateSecret) survive a Snapshot/Restore round trip, since these are
// new additive (omitempty) fields on the Secret/secretSnapshot DTO (gopherstack-9wuh).
func TestSnapshotRestore_ManagedExternalSecretFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "snap-mes",
		SecretString: "v",
		Type:         "SomePartner",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:                      "snap-mes",
		RotationLambdaARN:             "arn:aws:lambda:us-east-1:123:function:rotator",
		ExternalSecretRotationRoleArn: "arn:aws:iam::123:role/mes-rotator",
		ExternalSecretRotationMetadata: []secretsmanager.ExternalSecretRotationMetadataItem{
			{Key: "k", Value: "v"},
		},
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := secretsmanager.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	desc, err := b2.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "snap-mes"})
	require.NoError(t, err)
	assert.Equal(t, "SomePartner", desc.Type)
	assert.Equal(t, "arn:aws:iam::123:role/mes-rotator", desc.ExternalSecretRotationRoleArn)
	require.Len(t, desc.ExternalSecretRotationMetadata, 1)
	assert.Equal(t, "k", desc.ExternalSecretRotationMetadata[0].Key)
	assert.Equal(t, "v", desc.ExternalSecretRotationMetadata[0].Value)
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
