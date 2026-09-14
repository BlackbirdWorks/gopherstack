package kms_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireDryRun asserts err is a real, typed DryRunOperationException (not an
// undeclared code degrading to a generic smithy error), matching how a real
// client's errors.As would match it. DryRunOperationException.ErrorFault is
// smithy.FaultClient (types/errors.go), so its wire HTTP status is 400 -- the
// same default this package's kmsErrorTable gives every client-fault entry
// with no explicit httpStatus override.
func requireDryRun(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)

	var dryRunErr *kmstypes.DryRunOperationException

	require.ErrorAs(t, err, &dryRunErr)
}

// TestDryRun_AllOps_RealClient drives every one of the 15 KMS operations
// whose input carries a DryRun member (grepped `DryRun \*bool` across
// aws-sdk-go-v2/service/kms@v1.54.0's api_op_*.go) through a real SDK
// client with DryRun: true on otherwise-valid input, and confirms each
// returns DryRunOperationException instead of performing the operation.
func TestDryRun_AllOps_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, ctx context.Context, client *kmssdk.Client)
		name string
	}{
		{name: "encrypt", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			_, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
				KeyId:     aws.String(keyID),
				Plaintext: []byte("dry-run-plaintext"),
				DryRun:    aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{name: "decrypt", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			encOut, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
				KeyId:     aws.String(keyID),
				Plaintext: []byte("dry-run-plaintext"),
			})
			require.NoError(t, err)

			_, err = client.Decrypt(ctx, &kmssdk.DecryptInput{
				CiphertextBlob: encOut.CiphertextBlob,
				DryRun:         aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{name: "reencrypt", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			srcKeyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)
			dstKeyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			encOut, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
				KeyId:     aws.String(srcKeyID),
				Plaintext: []byte("dry-run-plaintext"),
			})
			require.NoError(t, err)

			_, err = client.ReEncrypt(ctx, &kmssdk.ReEncryptInput{
				CiphertextBlob:   encOut.CiphertextBlob,
				DestinationKeyId: aws.String(dstKeyID),
				DryRun:           aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{
			name: "generate_data_key",
			run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
				t.Helper()

				keyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecSymmetricDefault,
					kmstypes.KeyUsageTypeEncryptDecrypt,
				)

				_, err := client.GenerateDataKey(ctx, &kmssdk.GenerateDataKeyInput{
					KeyId:   aws.String(keyID),
					KeySpec: kmstypes.DataKeySpecAes256,
					DryRun:  aws.Bool(true),
				})
				requireDryRun(t, err)
			},
		},
		{
			name: "generate_data_key_without_plaintext",
			run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
				t.Helper()

				keyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecSymmetricDefault,
					kmstypes.KeyUsageTypeEncryptDecrypt,
				)

				_, err := client.GenerateDataKeyWithoutPlaintext(
					ctx,
					&kmssdk.GenerateDataKeyWithoutPlaintextInput{
						KeyId:   aws.String(keyID),
						KeySpec: kmstypes.DataKeySpecAes256,
						DryRun:  aws.Bool(true),
					},
				)
				requireDryRun(t, err)
			},
		},
		{
			name: "generate_data_key_pair",
			run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
				t.Helper()

				keyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecSymmetricDefault,
					kmstypes.KeyUsageTypeEncryptDecrypt,
				)

				_, err := client.GenerateDataKeyPair(ctx, &kmssdk.GenerateDataKeyPairInput{
					KeyId:       aws.String(keyID),
					KeyPairSpec: kmstypes.DataKeyPairSpecEccNistP256,
					DryRun:      aws.Bool(true),
				})
				requireDryRun(t, err)
			},
		},
		{
			name: "generate_data_key_pair_without_plaintext",
			run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
				t.Helper()

				keyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecSymmetricDefault,
					kmstypes.KeyUsageTypeEncryptDecrypt,
				)

				_, err := client.GenerateDataKeyPairWithoutPlaintext(
					ctx,
					&kmssdk.GenerateDataKeyPairWithoutPlaintextInput{
						KeyId:       aws.String(keyID),
						KeyPairSpec: kmstypes.DataKeyPairSpecEccNistP256,
						DryRun:      aws.Bool(true),
					},
				)
				requireDryRun(t, err)
			},
		},
		{name: "sign", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecEccNistP256,
				kmstypes.KeyUsageTypeSignVerify,
			)

			_, err := client.Sign(ctx, &kmssdk.SignInput{
				KeyId:            aws.String(keyID),
				Message:          []byte("dry-run-message"),
				SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
				DryRun:           aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{name: "verify", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecEccNistP256,
				kmstypes.KeyUsageTypeSignVerify,
			)

			signOut, err := client.Sign(ctx, &kmssdk.SignInput{
				KeyId:            aws.String(keyID),
				Message:          []byte("dry-run-message"),
				SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
			})
			require.NoError(t, err)

			_, err = client.Verify(ctx, &kmssdk.VerifyInput{
				KeyId:            aws.String(keyID),
				Message:          []byte("dry-run-message"),
				Signature:        signOut.Signature,
				SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
				DryRun:           aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{name: "generate_mac", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecHmac256,
				kmstypes.KeyUsageTypeGenerateVerifyMac,
			)

			_, err := client.GenerateMac(ctx, &kmssdk.GenerateMacInput{
				KeyId:        aws.String(keyID),
				Message:      []byte("dry-run-message"),
				MacAlgorithm: kmstypes.MacAlgorithmSpecHmacSha256,
				DryRun:       aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{name: "verify_mac", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecHmac256,
				kmstypes.KeyUsageTypeGenerateVerifyMac,
			)

			macOut, err := client.GenerateMac(ctx, &kmssdk.GenerateMacInput{
				KeyId:        aws.String(keyID),
				Message:      []byte("dry-run-message"),
				MacAlgorithm: kmstypes.MacAlgorithmSpecHmacSha256,
			})
			require.NoError(t, err)

			_, err = client.VerifyMac(ctx, &kmssdk.VerifyMacInput{
				KeyId:        aws.String(keyID),
				Message:      []byte("dry-run-message"),
				Mac:          macOut.Mac,
				MacAlgorithm: kmstypes.MacAlgorithmSpecHmacSha256,
				DryRun:       aws.Bool(true),
			})
			requireDryRun(t, err)
		}},
		{
			name: "derive_shared_secret",
			run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
				t.Helper()

				keyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecEccNistP256,
					kmstypes.KeyUsageTypeKeyAgreement,
				)
				peerKeyID := createTestKey(
					ctx,
					t,
					client,
					kmstypes.KeySpecEccNistP256,
					kmstypes.KeyUsageTypeKeyAgreement,
				)

				peerPub, err := client.GetPublicKey(
					ctx,
					&kmssdk.GetPublicKeyInput{KeyId: aws.String(peerKeyID)},
				)
				require.NoError(t, err)

				_, err = client.DeriveSharedSecret(ctx, &kmssdk.DeriveSharedSecretInput{
					KeyId:                 aws.String(keyID),
					PublicKey:             peerPub.PublicKey,
					KeyAgreementAlgorithm: kmstypes.KeyAgreementAlgorithmSpecEcdh,
					DryRun:                aws.Bool(true),
				})
				requireDryRun(t, err)
			},
		},
		{name: "create_grant", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			_, err := client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
				KeyId:            aws.String(keyID),
				GranteePrincipal: aws.String("arn:aws:iam::000000000000:role/dry-run-grantee"),
				Operations:       []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
				DryRun:           aws.Bool(true),
			})
			requireDryRun(t, err)

			listOut, err := client.ListGrants(
				ctx,
				&kmssdk.ListGrantsInput{KeyId: aws.String(keyID)},
			)
			require.NoError(t, err)
			assert.Empty(t, listOut.Grants, "DryRun CreateGrant must not create a grant")
		}},
		{name: "revoke_grant", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			grantOut, err := client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
				KeyId:            aws.String(keyID),
				GranteePrincipal: aws.String("arn:aws:iam::000000000000:role/dry-run-grantee"),
				Operations:       []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
			})
			require.NoError(t, err)

			_, err = client.RevokeGrant(ctx, &kmssdk.RevokeGrantInput{
				KeyId:   aws.String(keyID),
				GrantId: grantOut.GrantId,
				DryRun:  aws.Bool(true),
			})
			requireDryRun(t, err)

			listOut, err := client.ListGrants(
				ctx,
				&kmssdk.ListGrantsInput{KeyId: aws.String(keyID)},
			)
			require.NoError(t, err)
			require.Len(t, listOut.Grants, 1, "DryRun RevokeGrant must not revoke the grant")
			assert.Equal(t, aws.ToString(grantOut.GrantId), aws.ToString(listOut.Grants[0].GrantId))
		}},
		{name: "retire_grant", run: func(t *testing.T, ctx context.Context, client *kmssdk.Client) {
			t.Helper()

			keyID := createTestKey(
				ctx,
				t,
				client,
				kmstypes.KeySpecSymmetricDefault,
				kmstypes.KeyUsageTypeEncryptDecrypt,
			)

			grantOut, err := client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
				KeyId:            aws.String(keyID),
				GranteePrincipal: aws.String("arn:aws:iam::000000000000:role/dry-run-grantee"),
				Operations:       []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
			})
			require.NoError(t, err)

			_, err = client.RetireGrant(ctx, &kmssdk.RetireGrantInput{
				GrantToken: grantOut.GrantToken,
				DryRun:     aws.Bool(true),
			})
			requireDryRun(t, err)

			listOut, err := client.ListGrants(
				ctx,
				&kmssdk.ListGrantsInput{KeyId: aws.String(keyID)},
			)
			require.NoError(t, err)
			require.Len(t, listOut.Grants, 1, "DryRun RetireGrant must not retire the grant")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			tt.run(t, t.Context(), client)
		})
	}
}

// TestEncrypt_DryRun_ValidKey_RealClient proves a DryRun Encrypt produces no
// ciphertext at all: the SDK call fails with DryRunOperationException, so
// there is nothing for a subsequent Decrypt to act on.
func TestEncrypt_DryRun_ValidKey_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	keyID := createTestKey(
		ctx,
		t,
		client,
		kmstypes.KeySpecSymmetricDefault,
		kmstypes.KeyUsageTypeEncryptDecrypt,
	)

	out, err := client.Encrypt(ctx, &kmssdk.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("dry-run-plaintext"),
		DryRun:    aws.Bool(true),
	})
	requireDryRun(t, err)
	assert.Nil(t, out, "a failed Encrypt call must not return a CiphertextBlob to decrypt")
}

// TestEncrypt_DryRun_DisabledKey_RealClient proves the pre-existing
// disabled-key validation still wins over DryRun: DryRun only short-circuits
// AFTER every other check this backend performs passes, so a request that
// would fail anyway must surface that real failure, not DryRunOperationException.
func TestEncrypt_DryRun_DisabledKey_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	keyID := createTestKey(
		ctx,
		t,
		client,
		kmstypes.KeySpecSymmetricDefault,
		kmstypes.KeyUsageTypeEncryptDecrypt,
	)

	_, err := client.DisableKey(ctx, &kmssdk.DisableKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	_, err = client.Encrypt(ctx, &kmssdk.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("dry-run-plaintext"),
		DryRun:    aws.Bool(true),
	})
	require.Error(t, err)

	var disabledErr *kmstypes.DisabledException

	require.ErrorAs(
		t,
		err,
		&disabledErr,
		"a disabled key must surface DisabledException, not DryRunOperationException",
	)

	var dryRunErr *kmstypes.DryRunOperationException

	require.NotErrorAs(t, err, &dryRunErr)
}

// createTestKey creates a KMS key with the given spec/usage via the real SDK
// client and returns its KeyId.
func createTestKey(
	ctx context.Context,
	t *testing.T,
	client *kmssdk.Client,
	spec kmstypes.KeySpec,
	usage kmstypes.KeyUsageType,
) string {
	t.Helper()

	out, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
		KeySpec:  spec,
		KeyUsage: usage,
	})
	require.NoError(t, err)

	return aws.ToString(out.KeyMetadata.KeyId)
}
