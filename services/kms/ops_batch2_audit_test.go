package kms_test

// ops_batch2_audit_test.go — batch-2 ops AWS-accuracy audit for services/kms (go-epvx).
// Covers three gaps discovered beyond the earlier batch-2 audit (go-78bt):
//
//  1. CreateGrant missing key-state guard — PendingDeletion and PendingImport keys
//     must reject CreateGrant with KMSInvalidStateException.
//  2. GenerateDataKey / GenerateDataKeyWithoutPlaintext missing GrantTokens — both
//     operations lacked the GrantTokens field and grant-constraint validation.
//  3. CreateAlias missing character validation — AWS only permits letters, digits,
//     colons, forward slashes, underscores, and hyphens in alias names.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func ops2NewBackend(t *testing.T) *kms.InMemoryBackend {
	t.Helper()

	return kms.NewInMemoryBackend()
}

func ops2MustCreateSymKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

// ── 1. CreateGrant key-state guard ───────────────────────────────────────────

func TestOps2_CreateGrant_PendingDeletion_Rejected(t *testing.T) {
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

func TestOps2_CreateGrant_Disabled_Allowed(t *testing.T) {
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

// ── 2. GenerateDataKey GrantTokens ────────────────────────────────────────────

func TestOps2_GenerateDataKey_GrantTokens_Accepted(t *testing.T) {
	t.Parallel()
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"GenerateDataKey"},
	})
	require.NoError(t, err)

	out, err := b.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
		KeyID:       keyID,
		KeySpec:     "AES_256",
		GrantTokens: []string{grantOut.GrantToken},
	})
	require.NoError(t, err)
	assert.Len(t, out.Plaintext, 32, "AES_256 data key should be 32 bytes")
	assert.NotEmpty(t, out.CiphertextBlob)
}

func TestOps2_GenerateDataKey_ExpiredGrantToken_Rejected(t *testing.T) {
	t.Parallel()
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	_, err := b.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
		KeyID:       keyID,
		KeySpec:     "AES_256",
		GrantTokens: []string{"not-a-real-grant-token"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrInvalidGrantToken)
}

// ── 3. GenerateDataKeyWithoutPlaintext GrantTokens ───────────────────────────

func TestOps2_GenerateDataKeyWithoutPlaintext_GrantTokens_Accepted(t *testing.T) {
	t.Parallel()
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"GenerateDataKey"},
	})
	require.NoError(t, err)

	nb := int32(16)
	out, err := b.GenerateDataKeyWithoutPlaintext(context.Background(), &kms.GenerateDataKeyWithoutPlaintextInput{
		KeyID:         keyID,
		NumberOfBytes: &nb,
		GrantTokens:   []string{grantOut.GrantToken},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.CiphertextBlob)
}

func TestOps2_GenerateDataKeyWithoutPlaintext_ExpiredGrantToken_Rejected(t *testing.T) {
	t.Parallel()
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	_, err := b.GenerateDataKeyWithoutPlaintext(context.Background(), &kms.GenerateDataKeyWithoutPlaintextInput{
		KeyID:       keyID,
		KeySpec:     "AES_256",
		GrantTokens: []string{"not-a-real-grant-token"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrInvalidGrantToken)
}

// ── 4. CreateAlias character validation ──────────────────────────────────────

func TestOps2_CreateAlias_InvalidCharacters_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aliasName string
	}{
		{"exclamation", "alias/my!key"},
		{"at-sign", "alias/my@key"},
		{"hash", "alias/my#key"},
		{"dollar", "alias/my$key"},
		{"space", "alias/my key"},
		{"tab", "alias/my\tkey"},
		{"newline", "alias/my\nkey"},
		{"dot", "alias/my.key"},
		{"asterisk", "alias/my*key"},
		{"open-paren", "alias/my(key"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ops2NewBackend(t)
			keyID := ops2MustCreateSymKey(t, b)

			err := b.CreateAlias(context.Background(), &kms.CreateAliasInput{
				AliasName:   tc.aliasName,
				TargetKeyID: keyID,
			})
			require.Error(t, err, "alias name %q should be rejected", tc.aliasName)
			assert.ErrorIs(t, err, kms.ErrValidation,
				"expected ValidationException for %q", tc.aliasName)
		})
	}
}

func TestOps2_CreateAlias_ValidCharacters_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aliasName string
	}{
		{"letters", "alias/myKey"},
		{"digits", "alias/key123"},
		{"hyphen", "alias/my-key"},
		{"underscore", "alias/my_key"},
		{"colon", "alias/my:key"},
		{"nested-slash", "alias/team/mykey"},
		{"mixed", "alias/prod_us-east-1:kms/key-01"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ops2NewBackend(t)
			keyID := ops2MustCreateSymKey(t, b)

			err := b.CreateAlias(context.Background(), &kms.CreateAliasInput{
				AliasName:   tc.aliasName,
				TargetKeyID: keyID,
			})
			assert.NoError(t, err, "alias name %q should be accepted", tc.aliasName)
		})
	}
}
