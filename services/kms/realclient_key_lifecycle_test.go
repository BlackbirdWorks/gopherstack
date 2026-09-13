package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_KeyLifecycle drives kms's key lifecycle op families
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_KeyLifecycle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "cancel key deletion and enable key",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				_, err = client.ScheduleKeyDeletion(ctx, &kmssdk.ScheduleKeyDeletionInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)

				cancelOut, err := client.CancelKeyDeletion(ctx, &kmssdk.CancelKeyDeletionInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				assert.Equal(t, keyID, aws.ToString(cancelOut.KeyId))

				_, err = client.EnableKey(ctx, &kmssdk.EnableKeyInput{KeyId: aws.String(keyID)})
				require.NoError(t, err)

				descOut, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(keyID)})
				require.NoError(t, err)
				assert.Equal(t, types.KeyStateEnabled, descOut.KeyMetadata.KeyState)
				assert.True(t, descOut.KeyMetadata.Enabled)
			},
		},
		{
			name: "key policy",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Principal":{"AWS":"*"},"Action":"kms:*","Resource":"*"}]}`
				_, err = client.PutKeyPolicy(ctx, &kmssdk.PutKeyPolicyInput{
					KeyId:  aws.String(keyID),
					Policy: aws.String(policy),
				})
				require.NoError(t, err)

				getOut, err := client.GetKeyPolicy(ctx, &kmssdk.GetKeyPolicyInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				assert.JSONEq(t, policy, aws.ToString(getOut.Policy))

				listOut, err := client.ListKeyPolicies(ctx, &kmssdk.ListKeyPoliciesInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				assert.Contains(t, listOut.PolicyNames, "default")
			},
		},
		{
			name: "key rotation on demand and list rotations",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				_, err = client.RotateKeyOnDemand(ctx, &kmssdk.RotateKeyOnDemandInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)

				listOut, err := client.ListKeyRotations(ctx, &kmssdk.ListKeyRotationsInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				require.Len(t, listOut.Rotations, 1)
				assert.Equal(t, types.RotationTypeOnDemand, listOut.Rotations[0].RotationType)
			},
		},
		{
			name: "get key last usage",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				before, err := client.GetKeyLastUsage(ctx, &kmssdk.GetKeyLastUsageInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				assert.Nil(t, before.KeyLastUsage)

				_, err = client.Encrypt(ctx, &kmssdk.EncryptInput{
					KeyId:     aws.String(keyID),
					Plaintext: []byte("s11-plaintext"),
				})
				require.NoError(t, err)

				after, err := client.GetKeyLastUsage(ctx, &kmssdk.GetKeyLastUsageInput{
					KeyId: aws.String(keyID),
				})
				require.NoError(t, err)
				require.NotNil(t, after.KeyLastUsage)
				assert.Equal(t, types.KeyLastUsageTrackingOperationEncrypt, after.KeyLastUsage.Operation)
			},
		},
		{
			name: "update key description",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				keyID := aws.ToString(createOut.KeyMetadata.KeyId)

				_, err = client.UpdateKeyDescription(ctx, &kmssdk.UpdateKeyDescriptionInput{
					KeyId:       aws.String(keyID),
					Description: aws.String("s11 updated description"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(keyID)})
				require.NoError(t, err)
				assert.Equal(t, "s11 updated description", aws.ToString(descOut.KeyMetadata.Description))
			},
		},
		{
			name: "alias update and delete",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				key1Out, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				key1ID := aws.ToString(key1Out.KeyMetadata.KeyId)

				key2Out, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, err)
				key2ID := aws.ToString(key2Out.KeyMetadata.KeyId)

				_, err = client.CreateAlias(ctx, &kmssdk.CreateAliasInput{
					AliasName:   aws.String("alias/s11-alias"),
					TargetKeyId: aws.String(key1ID),
				})
				require.NoError(t, err)

				_, err = client.UpdateAlias(ctx, &kmssdk.UpdateAliasInput{
					AliasName:   aws.String("alias/s11-alias"),
					TargetKeyId: aws.String(key2ID),
				})
				require.NoError(t, err)

				listOut, err := client.ListAliases(ctx, &kmssdk.ListAliasesInput{
					KeyId: aws.String(key2ID),
				})
				require.NoError(t, err)
				var found bool
				for _, a := range listOut.Aliases {
					if aws.ToString(a.AliasName) == "alias/s11-alias" {
						found = true
					}
				}
				assert.True(t, found, "UpdateAlias must retarget the alias to key2")

				_, err = client.DeleteAlias(ctx, &kmssdk.DeleteAliasInput{
					AliasName: aws.String("alias/s11-alias"),
				})
				require.NoError(t, err)

				afterOut, err := client.ListAliases(ctx, &kmssdk.ListAliasesInput{
					KeyId: aws.String(key2ID),
				})
				require.NoError(t, err)
				for _, a := range afterOut.Aliases {
					assert.NotEqual(t, "alias/s11-alias", aws.ToString(a.AliasName))
				}
			},
		},
		{
			name: "update custom key store name",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateCustomKeyStore(ctx, &kmssdk.CreateCustomKeyStoreInput{
					CustomKeyStoreName: aws.String("s11-key-store"),
				})
				require.NoError(t, err)
				storeID := aws.ToString(createOut.CustomKeyStoreId)

				_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
					CustomKeyStoreId:      aws.String(storeID),
					NewCustomKeyStoreName: aws.String("s11-key-store-renamed"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
					CustomKeyStoreId: aws.String(storeID),
				})
				require.NoError(t, err)
				require.Len(t, descOut.CustomKeyStores, 1)
				assert.Equal(t, "s11-key-store-renamed", aws.ToString(descOut.CustomKeyStores[0].CustomKeyStoreName))
			},
		},
		{
			name: "update primary region",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestKMSClient(t, newTestKMSHandler())
				ctx := t.Context()

				createOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
					MultiRegion: aws.Bool(true),
				})
				require.NoError(t, err)
				primaryID := aws.ToString(createOut.KeyMetadata.KeyId)

				replicaOut, err := client.ReplicateKey(ctx, &kmssdk.ReplicateKeyInput{
					KeyId:         aws.String(primaryID),
					ReplicaRegion: aws.String("us-west-2"),
				})
				require.NoError(t, err)
				require.NotNil(t, replicaOut.ReplicaKeyMetadata)

				_, err = client.UpdatePrimaryRegion(ctx, &kmssdk.UpdatePrimaryRegionInput{
					KeyId:         aws.String(primaryID),
					PrimaryRegion: aws.String("us-west-2"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(primaryID)})
				require.NoError(t, err)
				require.NotNil(t, descOut.KeyMetadata.MultiRegionConfiguration)
				assert.Equal(t, types.MultiRegionKeyTypeReplica,
					descOut.KeyMetadata.MultiRegionConfiguration.MultiRegionKeyType)
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
