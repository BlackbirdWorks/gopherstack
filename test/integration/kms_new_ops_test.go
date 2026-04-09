package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_KMS_CustomKeyStore verifies the full custom key store lifecycle via the AWS SDK.
func TestIntegration_KMS_CustomKeyStore(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	storeName := "integration-store-" + uuid.NewString()

	// CreateCustomKeyStore
	createOut, err := client.CreateCustomKeyStore(ctx, &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: aws.String(storeName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.CustomKeyStoreId)
	storeID := *createOut.CustomKeyStoreId

	// DescribeCustomKeyStores – should find the store by ID
	descOut, err := client.DescribeCustomKeyStores(ctx, &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.CustomKeyStores, 1)
	assert.Equal(t, storeName, *descOut.CustomKeyStores[0].CustomKeyStoreName)
	assert.Equal(t, kmstypes.ConnectionStateTypeDisconnected, descOut.CustomKeyStores[0].ConnectionState)

	// DescribeCustomKeyStores – find by name
	descByName, err := client.DescribeCustomKeyStores(ctx, &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreName: aws.String(storeName),
	})
	require.NoError(t, err)
	require.Len(t, descByName.CustomKeyStores, 1)
	assert.Equal(t, storeID, *descByName.CustomKeyStores[0].CustomKeyStoreId)

	// ConnectCustomKeyStore
	_, err = client.ConnectCustomKeyStore(ctx, &kms.ConnectCustomKeyStoreInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)

	// Verify CONNECTED state.
	descAfterConnect, err := client.DescribeCustomKeyStores(ctx, &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)
	assert.Equal(t, kmstypes.ConnectionStateTypeConnected, descAfterConnect.CustomKeyStores[0].ConnectionState)

	// DisconnectCustomKeyStore
	_, err = client.DisconnectCustomKeyStore(ctx, &kms.DisconnectCustomKeyStoreInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)

	// DeleteCustomKeyStore
	_, err = client.DeleteCustomKeyStore(ctx, &kms.DeleteCustomKeyStoreInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)

	// Confirm deletion.
	descAfterDelete, err := client.DescribeCustomKeyStores(ctx, &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: aws.String(storeID),
	})
	require.NoError(t, err)
	assert.Empty(t, descAfterDelete.CustomKeyStores)
}

// TestIntegration_KMS_GenerateRandom verifies GenerateRandom returns the requested bytes.
func TestIntegration_KMS_GenerateRandom(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Default (no size requested) – 32 bytes.
	out, err := client.GenerateRandom(ctx, &kms.GenerateRandomInput{})
	require.NoError(t, err)
	assert.Len(t, out.Plaintext, 32)

	// Explicit 64 bytes.
	out64, err := client.GenerateRandom(ctx, &kms.GenerateRandomInput{
		NumberOfBytes: aws.Int32(64),
	})
	require.NoError(t, err)
	assert.Len(t, out64.Plaintext, 64)

	// Verify uniqueness.
	assert.NotEqual(t, out.Plaintext, out64.Plaintext)
}

// TestIntegration_KMS_GenerateMac verifies HMAC key creation and MAC generation.
func TestIntegration_KMS_GenerateMac(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Create an HMAC key.
	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		KeySpec:  kmstypes.KeySpecHmac256,
		KeyUsage: kmstypes.KeyUsageTypeGenerateVerifyMac,
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	// GenerateMac.
	macOut, err := client.GenerateMac(ctx, &kms.GenerateMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: kmstypes.MacAlgorithmSpecHmacSha256,
		Message:      []byte("hello world"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, macOut.Mac)
	assert.Equal(t, kmstypes.MacAlgorithmSpecHmacSha256, macOut.MacAlgorithm)
	assert.NotEmpty(t, *macOut.KeyId)

	// Same message → same MAC (HMAC is deterministic).
	macOut2, err := client.GenerateMac(ctx, &kms.GenerateMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: kmstypes.MacAlgorithmSpecHmacSha256,
		Message:      []byte("hello world"),
	})
	require.NoError(t, err)
	assert.Equal(t, macOut.Mac, macOut2.Mac)
}

// TestIntegration_KMS_GenerateDataKeyPair verifies asymmetric data key pair generation.
func TestIntegration_KMS_GenerateDataKeyPair(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Create a symmetric wrapping key.
	wrapOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("wrap-key-" + uuid.NewString()),
	})
	require.NoError(t, err)
	wrapKeyID := *wrapOut.KeyMetadata.KeyId

	tests := []struct {
		name        string
		keyPairSpec kmstypes.DataKeyPairSpec
	}{
		{name: "rsa_2048", keyPairSpec: kmstypes.DataKeyPairSpecRsa2048},
		{name: "ecc_p256", keyPairSpec: kmstypes.DataKeyPairSpecEccNistP256},
		{name: "ecc_p384", keyPairSpec: kmstypes.DataKeyPairSpecEccNistP384},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pairOut, pairErr := client.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{
				KeyId:       aws.String(wrapKeyID),
				KeyPairSpec: tt.keyPairSpec,
			})
			require.NoError(t, pairErr)
			assert.NotEmpty(t, pairOut.PublicKey)
			assert.NotEmpty(t, pairOut.PrivateKeyPlaintext)
			assert.NotEmpty(t, pairOut.PrivateKeyCiphertextBlob)
			assert.Equal(t, tt.keyPairSpec, pairOut.KeyPairSpec)
		})
	}
}

// TestIntegration_KMS_GenerateDataKeyPairWithoutPlaintext verifies key pair generation without plaintext.
func TestIntegration_KMS_GenerateDataKeyPairWithoutPlaintext(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	wrapOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("wrap-no-plaintext-" + uuid.NewString()),
	})
	require.NoError(t, err)
	wrapKeyID := *wrapOut.KeyMetadata.KeyId

	out, err := client.GenerateDataKeyPairWithoutPlaintext(ctx, &kms.GenerateDataKeyPairWithoutPlaintextInput{
		KeyId:       aws.String(wrapKeyID),
		KeyPairSpec: kmstypes.DataKeyPairSpecEccNistP256,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PublicKey)
	assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
}

// TestIntegration_KMS_DeriveSharedSecret verifies ECDH shared secret derivation.
func TestIntegration_KMS_DeriveSharedSecret(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Create a KEY_AGREEMENT ECC key.
	keyAOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		KeySpec:  kmstypes.KeySpecEccNistP256,
		KeyUsage: kmstypes.KeyUsageTypeKeyAgreement,
	})
	require.NoError(t, err)
	keyAID := *keyAOut.KeyMetadata.KeyId

	// Create a second KEY_AGREEMENT key for the peer.
	keyBOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		KeySpec:  kmstypes.KeySpecEccNistP256,
		KeyUsage: kmstypes.KeyUsageTypeKeyAgreement,
	})
	require.NoError(t, err)
	keyBID := *keyBOut.KeyMetadata.KeyId

	// Get B's public key.
	pubBOut, err := client.GetPublicKey(ctx, &kms.GetPublicKeyInput{
		KeyId: aws.String(keyBID),
	})
	require.NoError(t, err)

	// Get A's public key.
	pubAOut, err := client.GetPublicKey(ctx, &kms.GetPublicKeyInput{
		KeyId: aws.String(keyAID),
	})
	require.NoError(t, err)

	// Derive shared secret from A's side using B's public key.
	ssA, err := client.DeriveSharedSecret(ctx, &kms.DeriveSharedSecretInput{
		KeyId:                 aws.String(keyAID),
		KeyAgreementAlgorithm: kmstypes.KeyAgreementAlgorithmSpecEcdh,
		PublicKey:             pubBOut.PublicKey,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, ssA.SharedSecret)

	// Derive shared secret from B's side using A's public key.
	ssB, err := client.DeriveSharedSecret(ctx, &kms.DeriveSharedSecretInput{
		KeyId:                 aws.String(keyBID),
		KeyAgreementAlgorithm: kmstypes.KeyAgreementAlgorithmSpecEcdh,
		PublicKey:             pubAOut.PublicKey,
	})
	require.NoError(t, err)

	// Both sides should yield the same shared secret (ECDH property).
	assert.Equal(t, ssA.SharedSecret, ssB.SharedSecret)
}
