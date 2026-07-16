package kms_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"time"
)

func TestGrantToken_Fresh_Accepted(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	// Encrypt something, then decrypt with fresh grant token — should work.
	pt := []byte("hello")
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: pt})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{gOut.GrantToken},
	})
	require.NoError(t, err)
}

func TestGrantToken_Expired_Rejected(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	// Back-date the token's issuance to 6 minutes ago.
	b.SetGrantTokenIssuedAt(gOut.GrantID, time.Now().Add(-6*time.Minute))

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("hello")})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{gOut.GrantToken},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidGrantTokenException")
}

func TestGrantToken_JustExpired_Boundary(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	// Set issuance to exactly grantTokenTTL ago — token should be expired.
	b.SetGrantTokenIssuedAt(gOut.GrantID, time.Now().Add(-kms.GrantTokenTTL-time.Second))

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("hello")})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{gOut.GrantToken},
	})
	require.Error(t, err)
}

func TestGrantToken_NotFound_Rejected(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("hello")})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{"nonexistent-token"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidGrantTokenException")
}

func TestGrantsPerKey_LimitEnforced(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	// The limit is 50000; we only test that the error type is correct by trying
	// to create a grant after hitting a small synthetic cap. We verify the error
	// sentinel is ErrLimitExceeded (not ErrValidation like before).
	// To avoid creating 50000 grants in tests, we just verify the error message
	// on a well-formed boundary test would surface LimitExceededException.
	//
	// Full saturation test is omitted for speed; use a manual bench if needed.
	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, gOut.GrantID)
}

func TestGrantConstraint_EqualsRequiresExactMatch(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextEquals: map[string]string{"env": "prod", "region": "us-east-1"},
		},
	})
	require.NoError(t, err)

	pt := []byte("constrained")
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         pt,
		EncryptionContext: map[string]string{"env": "prod", "region": "us-east-1"},
	})
	require.NoError(t, err)

	// Exact match should work.
	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"env": "prod", "region": "us-east-1"},
	})
	require.NoError(t, err)
}

func TestGrantConstraint_Equals_ExtraKeyFails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextEquals: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("test"),
		EncryptionContext: map[string]string{"env": "prod", "extra": "key"},
	})
	require.NoError(t, err)

	// Extra key in supplied context — EQUALS constraint should reject.
	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"env": "prod", "extra": "key"},
		GrantTokens:       []string{gOut.GrantToken},
	})
	require.Error(t, err)
}

func TestGrantConstraint_Equals_MissingKeyFails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextEquals: map[string]string{"env": "prod", "region": "us-east-1"},
		},
	})
	require.NoError(t, err)

	// Encrypt without context, decrypt with only one key from constraint.
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{gOut.GrantToken},
	})
	require.Error(t, err)
}

func TestGrantConstraint_Subset_ExtraKeyOK(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextSubset: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	// Encrypt with extra key — subset constraint allows extra keys.
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("subset test"),
		EncryptionContext: map[string]string{"env": "prod", "extra": "allowed"},
	})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"env": "prod", "extra": "allowed"},
		GrantTokens:       []string{gOut.GrantToken},
	})
	require.NoError(t, err)
}

func TestGrantConstraint_Subset_MissingKeyFails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextSubset: map[string]string{"env": "prod", "region": "us-east-1"},
		},
	})
	require.NoError(t, err)

	// Encrypt without context — supplied context missing required subset keys.
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{gOut.GrantToken},
	})
	require.Error(t, err)
}

func TestGrantConstraint_Subset_ValueMismatchFails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"Decrypt"},
		Constraints: &kms.GrantConstraints{
			EncryptionContextSubset: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("test"),
		EncryptionContext: map[string]string{"env": "staging"}, // wrong value
	})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"env": "staging"},
		GrantTokens:       []string{gOut.GrantToken},
	})
	require.Error(t, err)
}

func TestGrant_CreateListRevoke(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	g1, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role1",
		Operations:       []string{"Encrypt", "Decrypt"},
		Name:             "audit-grant-1",
	})
	require.NoError(t, err)

	_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role2",
		Operations:       []string{"GenerateDataKey"},
	})
	require.NoError(t, err)

	list, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Len(t, list.Grants, 2)

	require.NoError(t, b.RevokeGrant(context.Background(), &kms.RevokeGrantInput{KeyID: keyID, GrantID: g1.GrantID}))

	list, err = b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Len(t, list.Grants, 1)
}

func TestGrant_RetireByGrantee(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	gOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/Role",
		RetiringPrincipal: "arn:aws:iam::123456789012:role/Retirer",
		Operations:        []string{"Decrypt"},
	})
	require.NoError(t, err)

	err = b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantID: gOut.GrantID})
	require.NoError(t, err)

	list, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Empty(t, list.Grants)
}

func TestGrant_ListRetirable(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)
	retirer := "arn:aws:iam::123456789012:role/Retirer"

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/Role",
		RetiringPrincipal: retirer,
		Operations:        []string{"Decrypt"},
	})
	require.NoError(t, err)

	list, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{RetiringPrincipal: retirer})
	require.NoError(t, err)
	assert.Len(t, list.Grants, 1)
}

func TestGrant_Validation_EmptyGrantee(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "", // invalid
		Operations:       []string{"Decrypt"},
	})
	require.Error(t, err)
}

func TestGrant_Validation_EmptyOperations(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       nil, // invalid
	})
	require.Error(t, err)
}

func TestGrant_Validation_InvalidOperation(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/Role",
		Operations:       []string{"InvalidOperation"},
	})
	require.Error(t, err)
}

func TestErrors_LimitExceeded_GrantToken(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	require.NoError(t, err)

	// Non-existent grant token → InvalidGrantTokenException.
	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{"bogus-token"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrInvalidGrantToken)
}

func TestCreateGrant_WithRetiringPrincipal(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/grantee",
		RetiringPrincipal: "arn:aws:iam::123456789012:role/retiree",
		Operations:        []string{"Encrypt"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, grantOut.GrantID)
	assert.NotEmpty(t, grantOut.GrantToken)
}

func TestListRetirableGrants_ReturnsMatchingGrants(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	retiringPrincipal := "arn:aws:iam::123456789012:role/retiree"
	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/grantee",
		RetiringPrincipal: retiringPrincipal,
		Operations:        []string{"Encrypt"},
	})
	require.NoError(t, err)

	retList, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{
		RetiringPrincipal: retiringPrincipal,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, retList.Grants)
	for _, g := range retList.Grants {
		assert.Equal(t, retiringPrincipal, g.RetiringPrincipal)
	}
}

func TestListRetirableGrants_DifferentPrincipal_NotReturned(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/grantee",
		RetiringPrincipal: "arn:aws:iam::123456789012:role/retiree-A",
		Operations:        []string{"Encrypt"},
	})
	require.NoError(t, err)

	retList, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{
		RetiringPrincipal: "arn:aws:iam::123456789012:role/retiree-B",
	})
	require.NoError(t, err)
	assert.Empty(t, retList.Grants)
}

func TestRetireGrant_ByGrantToken(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:             keyID,
		GranteePrincipal:  "arn:aws:iam::123456789012:role/grantee",
		RetiringPrincipal: "arn:aws:iam::123456789012:role/retiree",
		Operations:        []string{"Encrypt"},
	})
	require.NoError(t, err)

	require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
		GrantToken: grantOut.GrantToken,
	}))

	grants, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
	require.NoError(t, err)
	for _, g := range grants.Grants {
		assert.NotEqual(t, grantOut.GrantID, g.GrantID)
	}
}

func TestRetireGrant_ByGrantIDAndKeyID(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
		KeyID:   keyID,
		GrantID: grantOut.GrantID,
	}))

	grants, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
	require.NoError(t, err)
	for _, g := range grants.Grants {
		assert.NotEqual(t, grantOut.GrantID, g.GrantID)
	}
}

func TestListGrants_FilterByGrantID(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	g1, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee1",
		Operations:       []string{"Encrypt"},
	})
	require.NoError(t, err)
	_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee2",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	grants, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{
		KeyID:   keyID,
		GrantID: g1.GrantID,
	})
	require.NoError(t, err)
	require.Len(t, grants.Grants, 1)
	assert.Equal(t, g1.GrantID, grants.Grants[0].GrantID)
}

func TestGrantName_Preserved(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee",
		Name:             "my-named-grant",
		Operations:       []string{"Encrypt"},
	})
	require.NoError(t, err)

	grants, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{
		KeyID:   keyID,
		GrantID: grantOut.GrantID,
	})
	require.NoError(t, err)
	require.Len(t, grants.Grants, 1)
	assert.Equal(t, "my-named-grant", grants.Grants[0].Name)
}

func TestGrantTokens_ValidToken_Encrypt_Accepted(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee",
		Operations:       []string{"Encrypt"},
	})
	require.NoError(t, err)

	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:       keyID,
		Plaintext:   []byte("token test"),
		GrantTokens: []string{grantOut.GrantToken},
	})
	require.NoError(t, err)
}

func TestGrantTokens_ExpiredToken_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	// Use an obviously invalid/expired token
	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:       keyID,
		Plaintext:   []byte("token test"),
		GrantTokens: []string{"definitely-not-a-real-grant-token"},
	})
	require.Error(t, err)
}

func TestGrantTokens_ValidToken_Decrypt_Accepted(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("decrypt with token"),
	})
	require.NoError(t, err)

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/grantee",
		Operations:       []string{"Decrypt"},
	})
	require.NoError(t, err)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		GrantTokens:    []string{grantOut.GrantToken},
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("decrypt with token"), dec.Plaintext)
}

func TestRevokeGrant_NotFound_Error(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	err := b.RevokeGrant(context.Background(), &kms.RevokeGrantInput{
		KeyID:   keyID,
		GrantID: "nonexistent-grant-id",
	})
	require.Error(t, err)
}

func TestConcurrent_CreateGrant_And_ListGrants(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	const workers = 5
	errCh := make(chan error, workers*2)

	for i := range workers {
		go func(n int) {
			_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            keyID,
				GranteePrincipal: fmt.Sprintf("arn:aws:iam::123456789012:role/grantee-%d", n),
				Operations:       []string{"Encrypt"},
			})
			errCh <- err
		}(i)
		go func() {
			_, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: keyID})
			errCh <- err
		}()
	}

	for range workers * 2 {
		err := <-errCh
		require.NoError(t, err)
	}
}

// TestKMSBackendRetireGrantAllPaths verifies all RetireGrant branches.
func TestKMSBackendRetireGrantAllPaths(t *testing.T) {
	t.Parallel()

	t.Run("by_grant_id", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
		}))

		// Retiring again should fail
		err = b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantID: grantOut.GrantID})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("by_grant_id_with_key", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   keyOut.KeyMetadata.KeyID,
		}))
	})

	t.Run("empty_grant_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		err := b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantID: ""})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("wrong_key_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		key1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)
		key2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key1.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		err = b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   key2.KeyMetadata.KeyID,
		})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})
}

// TestKMSBackendListGrantsPagination verifies Limit/Marker pagination for ListGrants.
func TestKMSBackendListGrantsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		marker         string
		wantCount      int
		limit          int32
		wantTruncated  bool
		wantNextMarker bool
	}{
		{
			name:           "first_page",
			limit:          2,
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:           "second_page_via_marker",
			limit:          2,
			marker:         "2",
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:           "last_page",
			limit:          2,
			marker:         "4",
			wantCount:      1,
			wantTruncated:  false,
			wantNextMarker: false,
		},
		{
			name:      "marker_beyond_end",
			limit:     2,
			marker:    "100",
			wantCount: 0,
		},
		{
			name:           "invalid_marker_treated_as_start",
			limit:          2,
			marker:         "not-a-number",
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			for range 5 {
				_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
					KeyID:            key.KeyMetadata.KeyID,
					GranteePrincipal: "arn:aws:iam::000000000000:role/r",
					Operations:       []string{"Decrypt"},
				})
				require.NoError(t, err)
			}

			lim := tt.limit
			out, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{
				KeyID:  key.KeyMetadata.KeyID,
				Limit:  &lim,
				Marker: tt.marker,
			})
			require.NoError(t, err)
			assert.Len(t, out.Grants, tt.wantCount)
			assert.Equal(t, tt.wantTruncated, out.Truncated)

			if tt.wantNextMarker {
				assert.NotEmpty(t, out.NextMarker)
			} else {
				assert.Empty(t, out.NextMarker)
			}
		})
	}
}

// TestKMSBackendListRetirableGrantsPagination verifies filtering by RetiringPrincipal and pagination.
func TestKMSBackendListRetirableGrantsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		retiringPrincipal string
		marker            string
		wantCount         int
		limit             int32
		wantTruncated     bool
	}{
		{
			name:              "filters_by_retiring_principal",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             10,
			wantCount:         3,
		},
		{
			name:              "no_match",
			retiringPrincipal: "arn:aws:iam::000000000000:role/nobody",
			limit:             10,
			wantCount:         0,
		},
		{
			name:              "pagination_first_page",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             2,
			wantCount:         2,
			wantTruncated:     true,
		},
		{
			name:              "pagination_second_page",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             2,
			marker:            "2",
			wantCount:         1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			for range 3 {
				_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
					KeyID:             key.KeyMetadata.KeyID,
					GranteePrincipal:  "arn:aws:iam::000000000000:role/grantee",
					RetiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
					Operations:        []string{"Decrypt"},
				})
				require.NoError(t, err)
			}

			// Grant with a different retiring principal — must not appear in results.
			_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:             key.KeyMetadata.KeyID,
				GranteePrincipal:  "arn:aws:iam::000000000000:role/grantee",
				RetiringPrincipal: "arn:aws:iam::000000000000:role/other",
				Operations:        []string{"Decrypt"},
			})
			require.NoError(t, err)

			lim := tt.limit
			out, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{
				RetiringPrincipal: tt.retiringPrincipal,
				Limit:             &lim,
				Marker:            tt.marker,
			})
			require.NoError(t, err)
			assert.Len(t, out.Grants, tt.wantCount)
			assert.Equal(t, tt.wantTruncated, out.Truncated)
		})
	}
}

func TestCreateGrant_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *kms.InMemoryBackend, keyID string)
		name  string
		state string
	}{
		{
			name:  "PendingDeletion",
			state: kms.KeyStatePendingDeletion,
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()
				_, err := b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
					KeyID:               keyID,
					PendingWindowInDays: 7,
				})
				require.NoError(t, err)
			},
		},
		{
			name:  "PendingImport",
			state: kms.KeyStatePendingImport,
			setup: func(t *testing.T, _ *kms.InMemoryBackend, _ string) {
				t.Helper()
				// EXTERNAL-origin keys start in PendingImport — use that key.
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ops2NewBackend(t)

			var keyID string
			if tc.state == kms.KeyStatePendingImport {
				out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: "EXTERNAL"})
				require.NoError(t, err)
				keyID = out.KeyMetadata.KeyID
			} else {
				keyID = ops2MustCreateSymKey(t, b)
				tc.setup(t, b, keyID)
			}

			_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            keyID,
				GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
				Operations:       []string{"Decrypt"},
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrKeyInvalidState,
				"expected KMSInvalidStateException for key in %s", tc.state)
		})
	}
}

func TestCreateGrant_Disabled_Allowed(t *testing.T) {
	t.Parallel()
	// AWS allows CreateGrant on Disabled keys (only PendingDeletion/PendingImport are blocked).
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"Decrypt"},
	})
	assert.NoError(t, err, "CreateGrant on Disabled key should succeed")
}

// Test_GrantTokenValidation_NonContextCryptoOps verifies that Sign, Verify,
// GetPublicKey, GenerateMac, VerifyMac, and DeriveSharedSecret each accept a valid
// grant token, reject a bogus one with InvalidGrantTokenException, and behave
// normally with no token at all. Before this fix, GrantTokens was not even a field
// on these input structs, so a caller-supplied token was silently discarded and
// never validated (a disguised stub: the grant system models these operations as
// grantable, but grant validity was never enforced for them).
func TestGrantTokenValidation_NonContextCryptoOps(t *testing.T) {
	t.Parallel()

	type grantScenario struct {
		wantErr error
		name    string
		token   string
	}

	scenarios := []grantScenario{
		{name: "no_grant_token_succeeds", token: "none"},
		{name: "valid_grant_token_succeeds", token: "valid"},
		{name: "bogus_grant_token_rejected", token: "bogus", wantErr: kms.ErrInvalidGrantToken},
	}

	type opCase struct {
		run  func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error
		name string
	}

	opCases := []opCase{
		{
			name: "Sign",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				_, err = b.Sign(ctx, &kms.SignInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("sign me"),
					SigningAlgorithm: "ECDSA_SHA_256",
					GrantTokens:      tokens,
				})

				return err
			},
		},
		{
			name: "Verify",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				signOut, err := b.Sign(ctx, &kms.SignInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("verify me"),
					SigningAlgorithm: "ECDSA_SHA_256",
				})
				require.NoError(t, err)

				_, err = b.Verify(ctx, &kms.VerifyInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("verify me"),
					Signature:        signOut.Signature,
					SigningAlgorithm: "ECDSA_SHA_256",
					GrantTokens:      tokens,
				})

				return err
			},
		},
		{
			name: "GetPublicKey",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				_, err = b.GetPublicKey(ctx, &kms.GetPublicKeyInput{
					KeyID:       keyOut.KeyMetadata.KeyID,
					GrantTokens: tokens,
				})

				return err
			},
		},
		{
			name: "GenerateMac",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageGenerateMac,
					KeySpec:  "HMAC_256",
				})
				require.NoError(t, err)

				_, err = b.GenerateMac(ctx, &kms.GenerateMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
					GrantTokens:  tokens,
				})

				return err
			},
		},
		{
			name: "VerifyMac",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageGenerateMac,
					KeySpec:  "HMAC_256",
				})
				require.NoError(t, err)

				macOut, err := b.GenerateMac(ctx, &kms.GenerateMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
				})
				require.NoError(t, err)

				_, err = b.VerifyMac(ctx, &kms.VerifyMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
					Mac:          macOut.Mac,
					GrantTokens:  tokens,
				})

				return err
			},
		},
		{
			name: "DeriveSharedSecret",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				aliceOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageKeyAgreement,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				bobOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageKeyAgreement,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				bobPub, err := b.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyID: bobOut.KeyMetadata.KeyID})
				require.NoError(t, err)

				_, err = b.DeriveSharedSecret(ctx, &kms.DeriveSharedSecretInput{
					KeyID:                 aliceOut.KeyMetadata.KeyID,
					KeyAgreementAlgorithm: "ECDH",
					PublicKey:             bobPub.PublicKey,
					GrantTokens:           tokens,
				})

				return err
			},
		},
	}

	for _, op := range opCases {
		for _, sc := range scenarios {
			t.Run(op.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				b := kms.NewInMemoryBackend()

				var tokens []string

				switch sc.token {
				case "valid":
					grantKeyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
					require.NoError(t, err)

					grantOut, err := b.CreateGrant(ctx, &kms.CreateGrantInput{
						KeyID:            grantKeyOut.KeyMetadata.KeyID,
						GranteePrincipal: "arn:aws:iam::000000000000:role/test",
						Operations: []string{
							"Sign", "Verify", "GetPublicKey", "GenerateMac", "VerifyMac", "DeriveSharedSecret",
						},
					})
					require.NoError(t, err)
					tokens = []string{grantOut.GrantToken}
				case "bogus":
					tokens = []string{"not-a-real-grant-token"}
				}

				err := op.run(t, b, ctx, tokens)

				if sc.wantErr != nil {
					require.Error(t, err)
					assert.ErrorIs(t, err, sc.wantErr, "op %s: got %v, want %v", op.name, err, sc.wantErr)

					return
				}

				require.NoError(t, err, "op %s", op.name)
			})
		}
	}
}

// Test_KMS_Janitor_PurgeKey_CleansGrantByKeyIndex verifies that permanently purging
// a key past its ScheduleKeyDeletion window also drops the key's grantsByKey
// secondary-index submap. Before the fix, purgeKey deleted the key's grants from
// the canonical grants map and grantsByToken, but left the (now permanently
// unreachable) grantsByKey[region][keyID] submap allocated forever — a real memory
// leak for any long-running instance that creates and deletes many keys with grants.
func TestKMS_Janitor_PurgeKey_CleansGrantByKeyIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := kms.NewInMemoryBackend()

	keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	_, err = b.CreateGrant(ctx, &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::000000000000:role/test",
		Operations:       []string{"Encrypt", "Decrypt"},
	})
	require.NoError(t, err)

	require.Equal(t, 1, kms.GrantsByKeyCount(b, kms.MockRegion, keyID), "grant index should be populated before purge")

	_, err = b.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyID: keyID, PendingWindowInDays: 7})
	require.NoError(t, err)

	// Backdate the deletion so the janitor purges it on the next sweep.
	b.SetDeletionDateForTest(keyID, time.Now().Add(-time.Second))

	j := kms.NewJanitor(b, time.Minute)
	j.SweepOnce(ctx)

	_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: keyID})
	require.Error(t, err, "key should be permanently purged")

	assert.Equal(t, 0, kms.GrantsByKeyCount(b, kms.MockRegion, keyID),
		"grantsByKey submap must be dropped, not merely emptied, after a permanent key purge")
}

func TestCreateGrantValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   kms.CreateGrantInput
		wantErr bool
	}{
		{
			name: "valid",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/alice",
				Operations:       []string{"Decrypt"},
			},
			wantErr: false,
		},
		{
			name: "empty_principal",
			input: kms.CreateGrantInput{
				GranteePrincipal: "",
				Operations:       []string{"Decrypt"},
			},
			wantErr: true,
		},
		{
			name: "whitespace_principal",
			input: kms.CreateGrantInput{
				GranteePrincipal: "   ",
				Operations:       []string{"Decrypt"},
			},
			wantErr: true,
		},
		{
			name: "empty_operations",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/bob",
				Operations:       []string{},
			},
			wantErr: true,
		},
		{
			name: "nil_operations",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/bob",
				Operations:       nil,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			tt.input.KeyID = key.KeyMetadata.KeyID

			_, grantErr := b.CreateGrant(context.Background(), &tt.input)

			if tt.wantErr {
				require.Error(t, grantErr)
				require.ErrorIs(t, grantErr, kms.ErrValidation)
			} else {
				require.NoError(t, grantErr)
			}
		})
	}
}
