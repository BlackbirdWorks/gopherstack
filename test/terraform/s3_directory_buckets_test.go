package terraform_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3controlsvc "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_S3DirectoryBuckets provisions an S3 Express One Zone directory
// bucket via aws_s3_directory_bucket plus an access point scope via
// aws_s3control_directory_bucket_access_point_scope, and verifies both.
// Regression test for gopherstack-z2w1a: aws_s3_directory_bucket previously
// failed with 403 SignatureDoesNotMatch because the pinned aws-sdk-go-v2
// client signs every S3 Express request (including the CreateSession call it
// issues automatically) with the "s3express" signing name, which
// verifyHeaderAuth rejected outright.
func TestTerraform_S3DirectoryBuckets(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "s3-directory-buckets",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				id := uuid.NewString()[:8]

				return map[string]any{
					"BucketName": "tf-s3xb-" + id + "--use1-az4--x-s3",
					"AZID":       "use1-az4",
					// S3 Express access point names follow a distinct
					// convention from bucket names: "--xa-s3", not "--x-s3"
					// (confirmed via the real terraform-provider-aws's own
					// client-side validation regexp).
					"AccessPointName": "tf-s3xb-ap-" + id + "--use1-az4--xa-s3",
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				bucketName := vars["BucketName"].(string)  //nolint:forcetypeassert // test fixture var
				apName := vars["AccessPointName"].(string) //nolint:forcetypeassert // test fixture var

				s3Client := createS3Client(t)

				_, err := s3Client.HeadBucket(ctx, &s3svc.HeadBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err, "HeadBucket on the directory bucket should succeed after terraform apply")

				_, err = s3Client.PutObject(ctx, &s3svc.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("hello.txt"),
					Body:   strings.NewReader("s3 express one zone"),
				})
				require.NoError(t, err, "PutObject via the SDK's own CreateSession flow should succeed")

				getOut, err := s3Client.GetObject(ctx, &s3svc.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("hello.txt"),
				})
				require.NoError(t, err, "GetObject via the SDK's own CreateSession flow should succeed")
				body, err := io.ReadAll(getOut.Body)
				require.NoError(t, err)
				assert.Equal(t, "s3 express one zone", string(body))

				s3controlClient := createS3ControlClient(t)
				scopeOut, err := s3controlClient.GetAccessPointScope(ctx, &s3controlsvc.GetAccessPointScopeInput{
					AccountId: aws.String("000000000000"),
					Name:      aws.String(apName),
				})
				require.NoError(t, err, "GetAccessPointScope should succeed after terraform apply")
				require.NotNil(t, scopeOut.Scope)
				assert.Contains(t, scopeOut.Scope.Prefixes, "logs/")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
