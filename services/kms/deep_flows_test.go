package kms_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeepFlows_MultiRegionKey(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	west := func(o *kmssdk.Options) { o.Region = "us-west-2" }

	created, err := client.CreateKey(t.Context(), &kmssdk.CreateKeyInput{MultiRegion: aws.Bool(true)})
	require.NoError(t, err)

	primaryID := aws.ToString(created.KeyMetadata.KeyId)
	assert.True(t, strings.HasPrefix(primaryID, "mrk-"), "multi-Region key IDs carry the mrk- prefix")

	rep, err := client.ReplicateKey(t.Context(), &kmssdk.ReplicateKeyInput{
		KeyId: aws.String(primaryID), ReplicaRegion: aws.String("us-west-2"),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(aws.ToString(rep.ReplicaKeyMetadata.KeyId), "mrk-"))

	for _, region := range []string{"us-west-2", "us-east-1"} {
		_, err = client.ReplicateKey(t.Context(), &kmssdk.ReplicateKeyInput{
			KeyId: aws.String(primaryID), ReplicaRegion: aws.String(region),
		})

		var exists *kmstypes.AlreadyExistsException
		require.ErrorAs(t, err, &exists, region)
	}

	enc, err := client.Encrypt(t.Context(), &kmssdk.EncryptInput{
		KeyId: aws.String(primaryID), Plaintext: []byte("hello"),
	})
	require.NoError(t, err)

	dec, err := client.Decrypt(t.Context(), &kmssdk.DecryptInput{CiphertextBlob: enc.CiphertextBlob}, west)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(dec.Plaintext))
}

func TestDeepFlows_CreateGrantIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		second   func(in *kmssdk.CreateGrantInput)
		name     string
		wantSame bool
	}{
		{name: "identical_named", second: func(*kmssdk.CreateGrantInput) {}, wantSame: true},
		{name: "different_name", second: func(in *kmssdk.CreateGrantInput) { in.Name = aws.String("other") }},
		{name: "different_ops", second: func(in *kmssdk.CreateGrantInput) {
			in.Operations = []kmstypes.GrantOperation{kmstypes.GrantOperationEncrypt}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			keyID := createTestKey(t.Context(), t, client,
				kmstypes.KeySpecSymmetricDefault, kmstypes.KeyUsageTypeEncryptDecrypt)

			in := &kmssdk.CreateGrantInput{
				KeyId:            aws.String(keyID),
				GranteePrincipal: aws.String("arn:aws:iam::000000000000:role/r"),
				Operations: []kmstypes.GrantOperation{
					kmstypes.GrantOperationDecrypt,
					kmstypes.GrantOperationEncrypt,
				},
				Name: aws.String("g"),
			}

			first, err := client.CreateGrant(t.Context(), in)
			require.NoError(t, err)

			again := *in
			tt.second(&again)

			second, err := client.CreateGrant(t.Context(), &again)
			require.NoError(t, err)

			if tt.wantSame {
				assert.Equal(t, aws.ToString(first.GrantId), aws.ToString(second.GrantId))
			} else {
				assert.NotEqual(t, aws.ToString(first.GrantId), aws.ToString(second.GrantId))
			}
		})
	}
}
