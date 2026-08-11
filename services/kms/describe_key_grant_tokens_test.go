package kms_test

// describe_key_grant_tokens_test.go — wire-shape + validation regression for
// DescribeKeyInput.GrantTokens.
//
// The real aws-sdk-go-v2 DescribeKeyInput (kms@v1.55.4) carries GrantTokens []string, and
// the DescribeKey operation declares InvalidGrantTokenException in its error set.
// gopherstack's DescribeKeyInput previously had no GrantTokens field at all, so a
// caller-supplied token was silently dropped by the bare json.Unmarshal in dispatch.
// DescribeKey is a valid grant operation (isValidGrantOperation lists it), with no
// encryption context, so it validates grant-token PRESENCE only (existence + TTL) --
// consistent with Sign/Verify/GetPublicKey/DeriveSharedSecret.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestDescribeKey_GrantTokens_WireField(t *testing.T) {
	t.Parallel()

	// The field must deserialize from the AWS wire name "GrantTokens".
	var input kms.DescribeKeyInput
	require.NoError(t, json.Unmarshal(
		[]byte(`{"KeyId":"k-123","GrantTokens":["tok-a","tok-b"]}`), &input))
	assert.Equal(t, "k-123", input.KeyID)
	assert.Equal(t, []string{"tok-a", "tok-b"}, input.GrantTokens)
}

func TestDescribeKey_GrantTokens_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := kms.NewInMemoryBackend()

	created, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "grant-token-desc"})
	require.NoError(t, err)
	keyID := created.KeyMetadata.KeyID

	// A real grant on the key yields a usable token.
	grant, err := b.CreateGrant(ctx, &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::000000000000:role/app",
		Operations:       []string{"DescribeKey"},
	})
	require.NoError(t, err)

	tests := []struct {
		wantErr error
		name    string
		tokens  []string
	}{
		{
			name:   "no tokens is a no-op (the only case Terraform exercises)",
			tokens: nil,
		},
		{
			name:   "valid grant token accepted",
			tokens: []string{grant.GrantToken},
		},
		{
			name:    "bogus grant token rejected as InvalidGrantTokenException",
			tokens:  []string{"not-a-real-token"},
			wantErr: kms.ErrInvalidGrantToken,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, describeErr := b.DescribeKey(ctx, &kms.DescribeKeyInput{
				KeyID:       keyID,
				GrantTokens: tc.tokens,
			})

			if tc.wantErr != nil {
				require.ErrorIs(t, describeErr, tc.wantErr)

				return
			}

			require.NoError(t, describeErr)
			assert.Equal(t, keyID, out.KeyMetadata.KeyID)
		})
	}
}
