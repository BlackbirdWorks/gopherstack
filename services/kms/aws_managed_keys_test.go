package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestDescribeKey_AWSManagedAlias_LazyProvision locks in gopherstack-6u8p4:
// alias/aws/<service> is provisioned on first reference and stable afterwards.
func TestDescribeKey_AWSManagedAlias_LazyProvision(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	first, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
		KeyId: aws.String("alias/aws/dynamodb"),
	})
	require.NoError(t, err)
	require.NotNil(t, first.KeyMetadata)
	assert.Equal(t, kmstypes.KeyManagerTypeAws, first.KeyMetadata.KeyManager)
	assert.Equal(t, kmstypes.KeyStateEnabled, first.KeyMetadata.KeyState)
	assert.Equal(t, kmstypes.KeySpecSymmetricDefault, first.KeyMetadata.KeySpec)
	assert.Equal(t,
		"Default key that protects my DynamoDB data when no other key is defined",
		aws.ToString(first.KeyMetadata.Description),
	)

	second, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
		KeyId: aws.String("alias/aws/dynamodb"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.KeyMetadata.KeyId), aws.ToString(second.KeyMetadata.KeyId))
	assert.Equal(t, aws.ToString(first.KeyMetadata.Arn), aws.ToString(second.KeyMetadata.Arn))
}

// TestListAliases_AWSManagedAlias_ShowsAfterProvision checks ListAliases
// surfaces the alias once DescribeKey has provisioned it.
func TestListAliases_AWSManagedAlias_ShowsAfterProvision(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	desc, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
		KeyId: aws.String("alias/aws/s3"),
	})
	require.NoError(t, err)
	keyID := aws.ToString(desc.KeyMetadata.KeyId)

	out, err := client.ListAliases(ctx, &kmssdk.ListAliasesInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	require.Len(t, out.Aliases, 1)
	assert.Equal(t, "alias/aws/s3", aws.ToString(out.Aliases[0].AliasName))
	assert.Equal(t, keyID, aws.ToString(out.Aliases[0].TargetKeyId))
}

// TestAWSManagedKey_SnapshotRestoreRoundTrip locks in additive persistence of
// Key.KeyManager (gopherstack-6u8p4): a lazily-provisioned AWS managed key
// survives Snapshot/Restore and is not re-provisioned as a second key.
func TestAWSManagedKey_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	orig := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	descOut, err := orig.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: "alias/aws/lambda"})
	require.NoError(t, err)
	origKeyID := descOut.KeyMetadata.KeyID

	snap := orig.Snapshot(ctx)

	fresh := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	restoredDesc, err := fresh.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: "alias/aws/lambda"})
	require.NoError(t, err)
	assert.Equal(t, origKeyID, restoredDesc.KeyMetadata.KeyID, "must not re-provision a second key")
	assert.Equal(t, "AWS", restoredDesc.KeyMetadata.KeyManager)

	aliasesOut, err := fresh.ListAliases(ctx, &kms.ListAliasesInput{KeyID: origKeyID})
	require.NoError(t, err)
	require.Len(t, aliasesOut.Aliases, 1)
	assert.Equal(t, "alias/aws/lambda", aliasesOut.Aliases[0].AliasName)
}

// TestAWSManagedKey_RestrictedOps locks in the real per-op restrictions on an
// AWS-managed key (developerguide/concepts.html: "you cannot change any
// properties of AWS managed keys, rotate them, change their key policies, or
// schedule them for deletion"), each returning a typed error the real SDK
// client can match with errors.As.
func TestAWSManagedKey_RestrictedOps(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	_, provisionErr := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
		KeyId: aws.String("alias/aws/rds"),
	})
	require.NoError(t, provisionErr)

	tests := []struct {
		call     func() error
		name     string
		wantCode string
	}{
		{
			name: "ScheduleKeyDeletion",
			call: func() error {
				_, err := client.ScheduleKeyDeletion(ctx, &kmssdk.ScheduleKeyDeletionInput{
					KeyId: aws.String("alias/aws/rds"),
				})

				return err
			},
			wantCode: "KMSInvalidStateException",
		},
		{
			name: "DisableKey",
			call: func() error {
				_, err := client.DisableKey(ctx, &kmssdk.DisableKeyInput{
					KeyId: aws.String("alias/aws/rds"),
				})

				return err
			},
			wantCode: "KMSInvalidStateException",
		},
		{
			name: "PutKeyPolicy",
			call: func() error {
				_, err := client.PutKeyPolicy(ctx, &kmssdk.PutKeyPolicyInput{
					KeyId:  aws.String("alias/aws/rds"),
					Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				})

				return err
			},
			wantCode: "UnsupportedOperationException",
		},
		{
			name: "DeleteAlias",
			call: func() error {
				_, err := client.DeleteAlias(ctx, &kmssdk.DeleteAliasInput{
					AliasName: aws.String("alias/aws/rds"),
				})

				return err
			},
			wantCode: "KMSInvalidStateException",
		},
		{
			name: "UpdateAlias",
			call: func() error {
				keyOut, keyErr := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
				require.NoError(t, keyErr)

				_, err := client.UpdateAlias(ctx, &kmssdk.UpdateAliasInput{
					AliasName:   aws.String("alias/aws/rds"),
					TargetKeyId: keyOut.KeyMetadata.KeyId,
				})

				return err
			},
			wantCode: "KMSInvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call()
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.ErrorCode())
		})
	}
}

// TestCreateAlias_AWSReservedPrefix_RealClient locks in the real-client shape
// of CreateAlias's rejection of the alias/aws/ prefix ("reserved for AWS
// managed keys" -- developerguide/kms-alias.html).
func TestCreateAlias_AWSReservedPrefix_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	keyOut, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)

	_, err = client.CreateAlias(ctx, &kmssdk.CreateAliasInput{
		AliasName:   aws.String("alias/aws/my-service"),
		TargetKeyId: keyOut.KeyMetadata.KeyId,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidAliasNameException", apiErr.ErrorCode())
}
