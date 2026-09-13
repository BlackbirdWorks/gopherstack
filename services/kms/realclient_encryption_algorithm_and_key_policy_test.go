package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_EncryptionAlgorithm drives Encrypt/Decrypt/ReEncrypt's
// EncryptionAlgorithm(s) fields (gopherstack-xhu2t) through the real client.
// Previously these fields were entirely undeclared:
// a caller-requested algorithm was silently dropped, and the response always
// echoed the key-spec default regardless of what was requested.
func TestRealClient_EncryptionAlgorithm(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "encrypt_honors_requested_rsa_algorithm",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
					KeySpec:  types.KeySpecRsa2048,
					KeyUsage: types.KeyUsageTypeEncryptDecrypt,
				})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				encOut, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
					KeyId:               aws.String(keyID),
					Plaintext:           []byte("hello"),
					EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha1,
				})
				require.NoError(t, err)
				assert.Equal(t, types.EncryptionAlgorithmSpecRsaesOaepSha1, encOut.EncryptionAlgorithm)
			},
		},
		{
			name: "encrypt_rejects_algorithm_not_valid_for_key",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				_, err = client.Encrypt(ctx, &kmssdk.EncryptInput{
					KeyId:               aws.String(keyID),
					Plaintext:           []byte("hello"),
					EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256,
				})
				require.Error(t, err)
			},
		},
		{
			name: "decrypt_rejects_algorithm_not_valid_for_key",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				encOut, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
					KeyId:     aws.String(keyID),
					Plaintext: []byte("hello"),
				})
				require.NoError(t, err)

				_, err = client.Decrypt(ctx, &kmssdk.DecryptInput{
					CiphertextBlob:      encOut.CiphertextBlob,
					EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256,
				})
				require.Error(t, err)
			},
		},
		{
			name: "reencrypt_honors_both_requested_algorithms",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				srcOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
					KeySpec:  types.KeySpecRsa2048,
					KeyUsage: types.KeyUsageTypeEncryptDecrypt,
				})
				require.NoError(t, err)
				srcKeyID := aws.ToString(srcOut.KeyMetadata.KeyId)

				dstOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
					KeySpec:  types.KeySpecRsa2048,
					KeyUsage: types.KeyUsageTypeEncryptDecrypt,
				})
				require.NoError(t, err)
				dstKeyID := aws.ToString(dstOut.KeyMetadata.KeyId)

				encOut, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
					KeyId:               aws.String(srcKeyID),
					Plaintext:           []byte("hello"),
					EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha1,
				})
				require.NoError(t, err)

				reOut, err := client.ReEncrypt(ctx, &kmssdk.ReEncryptInput{
					CiphertextBlob:                 encOut.CiphertextBlob,
					DestinationKeyId:               aws.String(dstKeyID),
					SourceEncryptionAlgorithm:      types.EncryptionAlgorithmSpecRsaesOaepSha1,
					DestinationEncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256,
				})
				require.NoError(t, err)
				assert.Equal(t, types.EncryptionAlgorithmSpecRsaesOaepSha1, reOut.SourceEncryptionAlgorithm)
				assert.Equal(t, types.EncryptionAlgorithmSpecRsaesOaepSha256, reOut.DestinationEncryptionAlgorithm)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_GetKeyPolicyPolicyName proves GetKeyPolicy now rejects a
// PolicyName other than "default" (dropped: the field was declared but
// completely unvalidated -- api_op_GetKeyPolicy.go: "The only valid name is
// default"), matching PutKeyPolicy's existing validation.
func TestRealClient_GetKeyPolicyPolicyName(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = client.GetKeyPolicy(ctx, &kmssdk.GetKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("not-default"),
	})
	require.Error(t, err)

	out, err := client.GetKeyPolicy(ctx, &kmssdk.GetKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
	})
	require.NoError(t, err)
	assert.Equal(t, "default", aws.ToString(out.PolicyName))
}

// TestRealClient_PutKeyPolicyBypassLockoutSafetyCheck proves
// BypassPolicyLockoutSafetyCheck (dropped: undeclared) round-trips onto the
// wire without erroring -- accepted as a no-op, matching the established
// CreateKeyInput/ReplicateKeyInput precedent for the same field name (no IAM
// layer exists in this mock to enforce the lockout check it waives).
func TestRealClient_PutKeyPolicyBypassLockoutSafetyCheck(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"kms:*","Resource":"*"}]}`

	_, err = client.PutKeyPolicy(ctx, &kmssdk.PutKeyPolicyInput{
		KeyId:                          aws.String(keyID),
		PolicyName:                     aws.String("default"),
		Policy:                         aws.String(policy),
		BypassPolicyLockoutSafetyCheck: true,
	})
	require.NoError(t, err)

	out, err := client.GetKeyPolicy(ctx, &kmssdk.GetKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(out.Policy))
}
