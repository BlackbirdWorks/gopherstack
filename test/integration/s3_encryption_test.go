package integration_test

import (
	"testing"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_Encryption verifies PutBucketEncryption, GetBucketEncryption, and DeleteBucketEncryption.
func TestIntegration_S3_Encryption(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup   func(t *testing.T, client *s3.Client, bucket string)
		name    string
		wantErr bool
	}{
		{
			name: "put and get AES256 encryption configuration",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketEncryption(t.Context(), &s3.PutBucketEncryptionInput{
					Bucket: aws.String(bucket),
					ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
						Rules: []types.ServerSideEncryptionRule{
							{
								ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
									SSEAlgorithm: types.ServerSideEncryptionAes256,
								},
							},
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "put and get aws:kms encryption configuration",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketEncryption(t.Context(), &s3.PutBucketEncryptionInput{
					Bucket: aws.String(bucket),
					ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
						Rules: []types.ServerSideEncryptionRule{
							{
								ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
									SSEAlgorithm:   types.ServerSideEncryptionAwsKms,
									KMSMasterKeyID: aws.String("arn:aws:kms:us-east-1:000000000000:key/test-key"),
								},
							},
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "get returns ServerSideEncryptionConfigurationNotFoundError when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No encryption configuration stored.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "encryption-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			_, err = client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr, "expected smithy.APIError")
				assert.Equal(t, "ServerSideEncryptionConfigurationNotFoundError", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestIntegration_S3_DeleteBucketEncryption verifies that deleting the encryption configuration works.
func TestIntegration_S3_DeleteBucketEncryption(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()

	bucket := "encryption-delete-integ-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Put an encryption config.
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify it is stored.
	out, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.NotEmpty(t, out.ServerSideEncryptionConfiguration.Rules)
	assert.Equal(
		t,
		types.ServerSideEncryptionAes256,
		out.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm,
	)

	// Delete it.
	_, err = client.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Verify it is gone with the expected error code.
	_, err = client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})

	var apiErr smithy.APIError
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ServerSideEncryptionConfigurationNotFoundError", apiErr.ErrorCode())
}
