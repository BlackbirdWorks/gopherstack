package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestKMSGrantTokenIndexConsistency exercises the grantsByToken index that backs
// findGrantByToken on the encrypt/decrypt hot path. It verifies that a grant
// token authorizes an operation after CreateGrant and stops authorizing it once
// the grant is removed (RevokeGrant / RetireGrant by token / RetireGrant by ID),
// proving the index stays consistent across add and delete.
func TestKMSGrantTokenIndexConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		// remove deletes the grant via the path under test, given the backend,
		// keyID, grantID and grantToken.
		remove func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, grantToken string)
		name   string
	}{
		{
			name: "revoke_grant_by_id",
			remove: func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, _ string) {
				t.Helper()
				require.NoError(
					t,
					b.RevokeGrant(context.Background(), &kms.RevokeGrantInput{KeyID: keyID, GrantID: grantID}),
				)
			},
		},
		{
			name: "retire_grant_by_token",
			remove: func(t *testing.T, b *kms.InMemoryBackend, _, _, grantToken string) {
				t.Helper()
				require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantToken: grantToken}))
			},
		},
		{
			name: "retire_grant_by_id",
			remove: func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, _ string) {
				t.Helper()
				require.NoError(
					t,
					b.RetireGrant(context.Background(), &kms.RetireGrantInput{KeyID: keyID, GrantID: grantID}),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := key.KeyMetadata.KeyID

			grant, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            keyID,
				GranteePrincipal: "arn:aws:iam::000000000000:role/test",
				Operations:       []string{"Encrypt", "Decrypt"},
			})
			require.NoError(t, err)
			require.NotEmpty(t, grant.GrantToken)

			// The token resolves through the index, so the encrypt succeeds.
			_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:       keyID,
				Plaintext:   []byte("secret"),
				GrantTokens: []string{grant.GrantToken},
			})
			require.NoError(t, err)

			// Remove the grant via the path under test.
			tt.remove(t, b, keyID, grant.GrantID, grant.GrantToken)

			// The index must no longer resolve the token: the same encrypt now
			// fails with an invalid-grant-token error.
			_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:       keyID,
				Plaintext:   []byte("secret"),
				GrantTokens: []string{grant.GrantToken},
			})
			require.ErrorIs(t, err, kms.ErrInvalidGrantToken)
		})
	}
}
