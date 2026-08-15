package kms_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestImportKeyMaterial_ReturnsKeyID covers gopherstack-7185: the real
// ImportKeyMaterialOutput carries KeyId (aws-sdk-go-v2 kms@v1.55.4
// api_op_ImportKeyMaterial.go), but the handler returned an empty envelope.
// Drives the real client end-to-end: CreateKey -> GetParametersForImport ->
// wrap material with the returned RSA public key -> ImportKeyMaterial, and
// asserts the returned KeyId matches the key that was actually imported into.
func TestImportKeyMaterial_ReturnsKeyID(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{Origin: kmstypes.OriginTypeExternal})
	require.NoError(t, err)
	keyID := aws.ToString(created.KeyMetadata.KeyId)

	params, err := client.GetParametersForImport(ctx, &kmssdk.GetParametersForImportInput{
		KeyId:             aws.String(keyID),
		WrappingAlgorithm: kmstypes.AlgorithmSpecRsaesOaepSha256,
		WrappingKeySpec:   kmstypes.WrappingKeySpecRsa2048,
	})
	require.NoError(t, err)

	pub, err := x509.ParsePKIXPublicKey(params.PublicKey)
	require.NoError(t, err)
	rsaPub, ok := pub.(*rsa.PublicKey)
	require.True(t, ok)

	material := make([]byte, 32)
	_, err = rand.Read(material)
	require.NoError(t, err)
	wrapped, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rsaPub, material, nil)
	require.NoError(t, err)

	importOut, err := client.ImportKeyMaterial(ctx, &kmssdk.ImportKeyMaterialInput{
		KeyId:                aws.String(keyID),
		ImportToken:          params.ImportToken,
		EncryptedKeyMaterial: wrapped,
		ExpirationModel:      kmstypes.ExpirationModelTypeKeyMaterialDoesNotExpire,
	})
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(importOut.KeyId),
		"ImportKeyMaterial must return the id of the key that was imported into")

	deleteOut, err := client.DeleteImportedKeyMaterial(
		ctx, &kmssdk.DeleteImportedKeyMaterialInput{KeyId: aws.String(keyID)},
	)
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(deleteOut.KeyId),
		"DeleteImportedKeyMaterial must return the id of the key its material was deleted from")
}

// TestReplicateKey_ReturnsPolicyAndTags covers gopherstack-7185: the real
// ReplicateKeyOutput carries ReplicaPolicy and ReplicaTags (aws-sdk-go-v2
// kms@v1.55.4 api_op_ReplicateKey.go), but the handler's response only ever
// carried ReplicaKeyMetadata even though the backend already computes/stores
// both (copyTagsToReplica, GetKeyPolicy's default-policy synthesis).
func TestReplicateKey_ReturnsPolicyAndTags(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
		MultiRegion: aws.Bool(true),
		Tags: []kmstypes.Tag{
			{TagKey: aws.String("team"), TagValue: aws.String("platform")},
		},
	})
	require.NoError(t, err)
	keyID := aws.ToString(created.KeyMetadata.KeyId)

	out, err := client.ReplicateKey(ctx, &kmssdk.ReplicateKeyInput{
		KeyId:         aws.String(keyID),
		ReplicaRegion: aws.String("eu-west-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicaKeyMetadata)

	assert.NotEmpty(t, out.ReplicaPolicy, "ReplicaPolicy must be populated")
	require.Len(t, out.ReplicaTags, 1, "ReplicaTags must carry the tags copied from the source key")
	assert.Equal(t, "team", aws.ToString(out.ReplicaTags[0].TagKey))
	assert.Equal(t, "platform", aws.ToString(out.ReplicaTags[0].TagValue))
}
