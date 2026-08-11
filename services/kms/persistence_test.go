package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
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
				out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "test key"})
				if err != nil {
					return ""
				}

				return out.KeyMetadata.KeyID
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: id})
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

				out, err := b.ListKeys(context.Background(), &kms.ListKeysInput{})
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

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip exercises a full
// Snapshot->Restore round trip covering every resource kind the Phase 3.3
// pkgs/store conversion touches: the store.Table-backed keys/aliases/grants/
// customKeyStores, and the plain region-nested policies/keyMaterials/
// keyMaterialHistory maps left raw (no store.Table identity field). Crypto
// key material -- both a symmetric key's current AND rotated-out historical
// material, and an asymmetric key's material -- must round-trip byte-for-byte
// since a mismatch would silently break Decrypt/Verify on restored backends.
func TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	orig := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	// Symmetric key: encrypt before rotation, rotate on demand (populates
	// keyMaterialHistory with the pre-rotation material), then confirm the
	// pre-rotation ciphertext still decrypts via history.
	symOut, err := orig.CreateKey(ctx, &kms.CreateKeyInput{Description: "sym"})
	require.NoError(t, err)
	symKeyID := symOut.KeyMetadata.KeyID

	encOut, err := orig.Encrypt(ctx, &kms.EncryptInput{KeyID: symKeyID, Plaintext: []byte("top secret")})
	require.NoError(t, err)

	_, err = orig.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyID: symKeyID})
	require.NoError(t, err)
	require.Equal(t, 1, orig.KeyMaterialHistoryLenForTest(symKeyID))

	_, err = orig.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err, "pre-rotation ciphertext must still decrypt via keyMaterialHistory")

	// Alias.
	require.NoError(t, orig.CreateAlias(ctx, &kms.CreateAliasInput{
		AliasName: "alias/full-state", TargetKeyID: symKeyID,
	}))

	// Grant, with a SourceArn constraint -- added alongside GranteeServicePrincipal/
	// RetiringServicePrincipal/IssuingAccount as real aws-sdk-go-v2/service/kms@v1.55.4
	// GrantConstraints/GrantListEntry fields; must survive Snapshot/Restore like every
	// other field on Grant (this exact codebase's PARITY.md previously found a real
	// persistence gap where fields applied outside InMemoryBackend's own state were
	// silently dropped across a restart -- see handlerSnapshot in persistence.go).
	const grantSourceArn = "arn:aws:cloudtrail:us-east-1:000000000000:trail/full-state-trail"

	grantOut, err := orig.CreateGrant(ctx, &kms.CreateGrantInput{
		KeyID:            symKeyID,
		GranteePrincipal: "arn:aws:iam::000000000000:role/test",
		Operations:       []string{"Encrypt", "Decrypt"},
		Constraints:      &kms.GrantConstraints{SourceArn: grantSourceArn},
	})
	require.NoError(t, err)

	// Key policy (plain region-nested map, no store.Table identity field).
	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"kms:*","Resource":"*"}]}`
	require.NoError(t, orig.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{KeyID: symKeyID, Policy: policy}))

	// Custom key store.
	cksOut, err := orig.CreateCustomKeyStore(ctx, &kms.CreateCustomKeyStoreInput{CustomKeyStoreName: "my-cks"})
	require.NoError(t, err)

	// Asymmetric key material round trip (RSA sign/verify).
	asymOut, err := orig.CreateKey(ctx, &kms.CreateKeyInput{KeyUsage: kms.KeyUsageSignVerify, KeySpec: "RSA_2048"})
	require.NoError(t, err)
	asymKeyID := asymOut.KeyMetadata.KeyID

	sigOut, err := orig.Sign(ctx, &kms.SignInput{
		KeyID: asymKeyID, Message: []byte("msg"), SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)

	snap := orig.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// Symmetric key's CURRENT material persisted: a fresh encrypt/decrypt works.
	freshEnc, err := fresh.Encrypt(ctx, &kms.EncryptInput{KeyID: symKeyID, Plaintext: []byte("after restore")})
	require.NoError(t, err)
	freshDec, err := fresh.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: freshEnc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("after restore"), freshDec.Plaintext)

	// keyMaterialHistory persisted: the pre-rotation ciphertext still decrypts.
	assert.Equal(t, 1, fresh.KeyMaterialHistoryLenForTest(symKeyID))
	decOut, err := fresh.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err, "pre-rotation ciphertext must still decrypt after restore")
	assert.Equal(t, []byte("top secret"), decOut.Plaintext)

	// Alias persisted and resolves.
	aliasOut, err := fresh.ListAliases(ctx, &kms.ListAliasesInput{KeyID: symKeyID})
	require.NoError(t, err)
	require.Len(t, aliasOut.Aliases, 1)
	assert.Equal(t, "alias/full-state", aliasOut.Aliases[0].AliasName)

	// Grant persisted: the grant token still authorizes via the rebuilt indexes.
	_, err = fresh.Encrypt(ctx, &kms.EncryptInput{
		KeyID: symKeyID, Plaintext: []byte("via grant"), GrantTokens: []string{grantOut.GrantToken},
	})
	require.NoError(t, err)

	// Grant's SourceArn constraint and IssuingAccount persisted (not just the
	// token/indexes checked above) -- these are plain fields on the Grant struct
	// with no dedicated snapshot plumbing, so a regression here would mean the
	// generic store.Table JSON round trip silently dropped them.
	grantListOut, err := fresh.ListGrants(ctx, &kms.ListGrantsInput{KeyID: symKeyID, GrantID: grantOut.GrantID})
	require.NoError(t, err)
	require.Len(t, grantListOut.Grants, 1)
	require.NotNil(t, grantListOut.Grants[0].Constraints)
	assert.Equal(t, grantSourceArn, grantListOut.Grants[0].Constraints.SourceArn)
	assert.Equal(t, kms.MockAccountID, grantListOut.Grants[0].IssuingAccount)

	// Key policy persisted.
	polOut, err := fresh.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyID: symKeyID})
	require.NoError(t, err)
	assert.Equal(t, policy, polOut.Policy)

	// Custom key store persisted.
	cksListOut, err := fresh.DescribeCustomKeyStores(
		ctx, &kms.DescribeCustomKeyStoresInput{CustomKeyStoreID: cksOut.CustomKeyStoreID},
	)
	require.NoError(t, err)
	require.Len(t, cksListOut.CustomKeyStores, 1)
	assert.Equal(t, "my-cks", cksListOut.CustomKeyStores[0].CustomKeyStoreName)

	// Asymmetric key material persisted: the original signature still verifies.
	verOut, err := fresh.Verify(ctx, &kms.VerifyInput{
		KeyID: asymKeyID, Message: []byte("msg"), Signature: sigOut.Signature,
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, verOut.SignatureValid)
}
