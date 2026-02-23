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

func TestIntegration_KMS_KeyLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// CreateKey
	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("integration test key " + uuid.NewString()),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.KeyMetadata)
	keyID := *createOut.KeyMetadata.KeyId

	// DescribeKey
	descOut, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)
	assert.Equal(t, keyID, *descOut.KeyMetadata.KeyId)
	assert.Equal(t, kmstypes.KeyStateEnabled, descOut.KeyMetadata.KeyState)

	// ListKeys — key should appear
	listOut, err := client.ListKeys(ctx, &kms.ListKeysInput{})
	require.NoError(t, err)
	found := false
	for _, k := range listOut.Keys {
		if *k.KeyId == keyID {
			found = true
			break
		}
	}
	assert.True(t, found, "created key should appear in ListKeys")
}

func TestIntegration_KMS_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Create a key
	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("encrypt-decrypt test " + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	plaintext := []byte("my-secret-plaintext-" + uuid.NewString())

	// Encrypt
	encOut, err := client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	require.NotEmpty(t, encOut.CiphertextBlob)

	// Decrypt
	decOut, err := client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

func TestIntegration_KMS_Aliases(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	// Create a key
	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("alias test " + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	aliasName := "alias/test-alias-" + uuid.NewString()

	// CreateAlias
	_, err = client.CreateAlias(ctx, &kms.CreateAliasInput{
		AliasName:   aws.String(aliasName),
		TargetKeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	// ListAliases — alias should appear
	listOut, err := client.ListAliases(ctx, &kms.ListAliasesInput{})
	require.NoError(t, err)
	found := false
	for _, a := range listOut.Aliases {
		if *a.AliasName == aliasName {
			found = true
			assert.Equal(t, keyID, *a.TargetKeyId)
			break
		}
	}
	assert.True(t, found, "created alias should appear in ListAliases")
}

func TestIntegration_KMS_KeyRotation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("rotation test " + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	// EnableKeyRotation
	_, err = client.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	// GetKeyRotationStatus — should be enabled
	statusOut, err := client.GetKeyRotationStatus(ctx, &kms.GetKeyRotationStatusInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)
	assert.True(t, statusOut.KeyRotationEnabled)

	// DisableKeyRotation
	_, err = client.DisableKeyRotation(ctx, &kms.DisableKeyRotationInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	// GetKeyRotationStatus — should be disabled
	statusOut2, err := client.GetKeyRotationStatus(ctx, &kms.GetKeyRotationStatusInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)
	assert.False(t, statusOut2.KeyRotationEnabled)
}

func TestIntegration_KMS_GenerateDataKey(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("data key test " + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	// GenerateDataKey
	dataKeyOut, err := client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: kmstypes.DataKeySpecAes256,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, dataKeyOut.Plaintext)
	assert.NotEmpty(t, dataKeyOut.CiphertextBlob)
	assert.Equal(t, keyID, *dataKeyOut.KeyId)
	// AES-256 plaintext key is 32 bytes
	assert.Len(t, dataKeyOut.Plaintext, 32)
}
