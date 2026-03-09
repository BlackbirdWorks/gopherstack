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

// TestIntegration_S3_Website verifies PutBucketWebsite, GetBucketWebsite, and DeleteBucketWebsite.
func TestIntegration_S3_Website(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup   func(t *testing.T, client *s3.Client, bucket string)
		name    string
		wantErr bool
	}{
		{
			name: "put and get index/error document configuration",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketWebsite(t.Context(), &s3.PutBucketWebsiteInput{
					Bucket: aws.String(bucket),
					WebsiteConfiguration: &types.WebsiteConfiguration{
						IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
						ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "put redirect all requests configuration",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketWebsite(t.Context(), &s3.PutBucketWebsiteInput{
					Bucket: aws.String(bucket),
					WebsiteConfiguration: &types.WebsiteConfiguration{
						RedirectAllRequestsTo: &types.RedirectAllRequestsTo{
							HostName: aws.String("example.com"),
							Protocol: types.ProtocolHttps,
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "get returns NoSuchWebsiteConfiguration when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No website configuration stored.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "website-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			_, err = client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr, "expected smithy.APIError")
				assert.Equal(t, "NoSuchWebsiteConfiguration", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestIntegration_S3_DeleteBucketWebsite verifies that deleting the website configuration works.
func TestIntegration_S3_DeleteBucketWebsite(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()

	bucket := "website-delete-integ-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Put a website config.
	_, err = client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &types.WebsiteConfiguration{
			IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		},
	})
	require.NoError(t, err)

	// Verify it is stored.
	_, err = client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Delete it.
	_, err = client.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Verify it is gone with the expected error code.
	_, err = client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})

	var apiErr smithy.APIError
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchWebsiteConfiguration", apiErr.ErrorCode())
}
