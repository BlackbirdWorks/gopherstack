package kms_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestKMSGrantsByKeyIndexConsistency verifies that the grantsByKey secondary index
// stays consistent with the canonical grants map across CreateGrant, RevokeGrant,
// and RetireGrant operations.
func TestKMSGrantsByKeyIndexConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		remove func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, grantToken string)
		name   string
	}{
		{
			name: "revoke_by_id",
			remove: func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, _ string) {
				t.Helper()
				require.NoError(t, b.RevokeGrant(context.Background(), &kms.RevokeGrantInput{
					KeyID:   keyID,
					GrantID: grantID,
				}))
			},
		},
		{
			name: "retire_by_token",
			remove: func(t *testing.T, b *kms.InMemoryBackend, _, _, grantToken string) {
				t.Helper()
				require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
					GrantToken: grantToken,
				}))
			},
		},
		{
			name: "retire_by_id",
			remove: func(t *testing.T, b *kms.InMemoryBackend, keyID, grantID, _ string) {
				t.Helper()
				require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
					KeyID:   keyID,
					GrantID: grantID,
				}))
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

			// Index must contain the grant right after creation.
			ok, msg := kms.GrantIndexesConsistent(b)
			require.True(t, ok, "index inconsistent after create: %s", msg)
			assert.Equal(t, 1, kms.GrantsByKeyCount(b, "us-east-1", keyID))

			tt.remove(t, b, keyID, grant.GrantID, grant.GrantToken)

			// Index must be consistent after removal.
			ok, msg = kms.GrantIndexesConsistent(b)
			require.True(t, ok, "index inconsistent after remove: %s", msg)
			assert.Equal(t, 0, kms.GrantsByKeyCount(b, "us-east-1", keyID))
		})
	}
}

// TestKMSListGrantsUsesIndex verifies that ListGrants returns correct results
// using the grantsByKey index when there are many grants across multiple keys.
func TestKMSListGrantsUsesIndex(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	// Create two keys and several grants on each.
	key1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	key2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	const grantsPerKey = 5
	for i := range grantsPerKey {
		_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key1.KeyMetadata.KeyID,
			GranteePrincipal: fmt.Sprintf("arn:aws:iam::000000000000:role/key1-%d", i),
			Operations:       []string{"Encrypt"},
		})
		require.NoError(t, err)

		_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key2.KeyMetadata.KeyID,
			GranteePrincipal: fmt.Sprintf("arn:aws:iam::000000000000:role/key2-%d", i),
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)
	}

	// ListGrants for key1 must return exactly grantsPerKey grants.
	out1, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: key1.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Len(t, out1.Grants, grantsPerKey)
	for _, g := range out1.Grants {
		assert.Equal(t, key1.KeyMetadata.KeyID, g.KeyID, "wrong key ID in ListGrants result")
	}

	// ListGrants for key2 must return exactly grantsPerKey grants.
	out2, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: key2.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Len(t, out2.Grants, grantsPerKey)
	for _, g := range out2.Grants {
		assert.Equal(t, key2.KeyMetadata.KeyID, g.KeyID, "wrong key ID in ListGrants result")
	}

	// Index must be consistent after all operations.
	ok, msg := kms.GrantIndexesConsistent(b)
	require.True(t, ok, "index inconsistent: %s", msg)
}

// TestKMSGrantsByKeyIndexAfterRestore verifies that grantsByKey (and grantsByToken)
// are rebuilt correctly after a Snapshot/Restore round-trip.
func TestKMSGrantsByKeyIndexAfterRestore(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	key, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := key.KeyMetadata.KeyID

	grant, err := orig.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::000000000000:role/test",
		Operations:       []string{"Encrypt", "Decrypt"},
	})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// grantsByKey must be rebuilt.
	ok, msg := kms.GrantIndexesConsistent(fresh)
	require.True(t, ok, "index not rebuilt after restore: %s", msg)
	assert.Equal(t, 1, kms.GrantsByKeyCount(fresh, "us-east-1", keyID))

	// grantsByToken must be rebuilt: a grant-validated Encrypt must succeed.
	_, err = fresh.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:       keyID,
		Plaintext:   []byte("secret"),
		GrantTokens: []string{grant.GrantToken},
	})
	require.NoError(t, err, "Encrypt via restored grant token must succeed")
}

// BenchmarkKMSListGrants benchmarks ListGrants with a large number of grants
// to demonstrate the O(grants-for-key) behaviour of the index lookup.
func BenchmarkKMSListGrants(b *testing.B) {
	backend := kms.NewInMemoryBackend()

	key, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	if err != nil {
		b.Fatal(err)
	}
	keyID := key.KeyMetadata.KeyID

	// Create 100 grants on this key and 100 on a different key to populate the grants store.
	key2, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	if err != nil {
		b.Fatal(err)
	}
	for i := range 100 {
		_, err = backend.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            keyID,
			GranteePrincipal: fmt.Sprintf("arn:aws:iam::000000000000:role/target-%d", i),
			Operations:       []string{"Encrypt"},
		})
		if err != nil {
			b.Fatal(err)
		}
		_, err = backend.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key2.KeyMetadata.KeyID,
			GranteePrincipal: fmt.Sprintf("arn:aws:iam::000000000000:role/other-%d", i),
			Operations:       []string{"Decrypt"},
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		out, err2 := backend.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
		if err2 != nil {
			b.Fatal(err2)
		}
		if len(out.Grants) != 100 {
			b.Fatalf("expected 100 grants, got %d", len(out.Grants))
		}
	}
}
