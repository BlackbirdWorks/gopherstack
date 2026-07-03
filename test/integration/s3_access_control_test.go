package integration_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_AccessControl exercises data-plane authorization: an
// explicit Deny bucket policy blocks object access even for the (owner)
// caller, mirroring real S3 where a bucket-policy Deny overrides the account
// root. It also verifies GetBucketAcl reflects the stored canned ACL instead of
// a hardcoded owner grant.
func TestIntegration_S3_AccessControl(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "explicit deny policy blocks GetObject",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucketName := "deny-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("locked.txt"),
					Body:   strings.NewReader("secret"),
				})
				require.NoError(t, err)

				policy := `{"Version":"2012-10-17","Statement":[{` +
					`"Effect":"Deny","Principal":"*","Action":"s3:GetObject",` +
					`"Resource":"arn:aws:s3:::` + bucketName + `/*"}]}`
				_, err = client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
					Bucket: aws.String(bucketName),
					Policy: aws.String(policy),
				})
				require.NoError(t, err)

				_, err = client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("locked.txt"),
				})
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "AccessDenied", apiErr.ErrorCode())
			},
		},
		{
			name: "get bucket acl reflects public-read canned ACL",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucketName := "acl-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutBucketAcl(ctx, &s3.PutBucketAclInput{
					Bucket: aws.String(bucketName),
					ACL:    types.BucketCannedACLPublicRead,
				})
				require.NoError(t, err)

				out, err := client.GetBucketAcl(ctx, &s3.GetBucketAclInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				var sawPublicRead bool
				for _, g := range out.Grants {
					if g.Grantee != nil && g.Grantee.URI != nil &&
						strings.Contains(*g.Grantee.URI, "AllUsers") &&
						g.Permission == types.PermissionRead {
						sawPublicRead = true
					}
				}
				assert.True(t, sawPublicRead,
					"GetBucketAcl must expose the AllUsers READ grant for a public-read bucket")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			tt.verify(t, client)
		})
	}
}
