package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiErrorCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, err)

	return apiErr.ErrorCode()
}

func newRealismBucket(t *testing.T) (*sdk_s3.Client, string) {
	t.Helper()

	client := newRealS3ClientTest(t)
	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("realism")})
	require.NoError(t, err)

	return client, "realism"
}

func TestExtractResource_DoesNotWriteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "invalid bucket", path: "/AB", want: ""},
		{name: "valid bucket", path: "/good-bucket/key", want: "good-bucket"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPut, tc.path, nil), rec)

			assert.Equal(t, tc.want, handler.ExtractResource(c))
			assert.Empty(t, rec.Body.String())
		})
	}
}

func TestListObjects_ArgumentValidation_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(c *sdk_s3.Client, bucket string) error
		name string
		want string
	}{
		{
			name: "bogus continuation token",
			want: "InvalidArgument",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.ListObjectsV2(context.Background(), &sdk_s3.ListObjectsV2Input{
					Bucket: aws.String(bucket), ContinuationToken: aws.String("bogus"),
				})

				return err
			},
		},
		{
			name: "negative max keys",
			want: "InvalidArgument",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.ListObjects(context.Background(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String(bucket), MaxKeys: aws.Int32(-1),
				})

				return err
			},
		},
		{
			name: "oversized metadata",
			want: "MetadataTooLarge",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.PutObject(context.Background(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"),
					Metadata: map[string]string{"big": strings.Repeat("x", 2100)},
				})

				return err
			},
		},
		{
			name: "unsupported cors method",
			want: "InvalidRequest",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.PutBucketCors(context.Background(), &sdk_s3.PutBucketCorsInput{
					Bucket: aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{
						{AllowedMethods: []string{"BAD"}, AllowedOrigins: []string{"*"}},
					}},
				})

				return err
			},
		},
		{
			name: "lifecycle zero days",
			want: "InvalidArgument",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.PutBucketLifecycleConfiguration(
					context.Background(),
					&sdk_s3.PutBucketLifecycleConfigurationInput{
						Bucket: aws.String(bucket),
						LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
							ID: aws.String("r"), Status: types.ExpirationStatusEnabled,
							Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
							Expiration: &types.LifecycleExpiration{Days: aws.Int32(0)},
						}}},
					},
				)

				return err
			},
		},
		{
			name: "lifecycle duplicate ids",
			want: "InvalidArgument",
			run: func(c *sdk_s3.Client, bucket string) error {
				rule := types.LifecycleRule{
					ID: aws.String("same"), Status: types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
				}
				_, err := c.PutBucketLifecycleConfiguration(
					context.Background(),
					&sdk_s3.PutBucketLifecycleConfigurationInput{
						Bucket: aws.String(bucket),
						LifecycleConfiguration: &types.BucketLifecycleConfiguration{
							Rules: []types.LifecycleRule{rule, rule},
						},
					},
				)

				return err
			},
		},
		{
			name: "versioning bad status",
			want: "MalformedXML",
			run: func(c *sdk_s3.Client, bucket string) error {
				_, err := c.PutBucketVersioning(context.Background(), &sdk_s3.PutBucketVersioningInput{
					Bucket:                  aws.String(bucket),
					VersioningConfiguration: &types.VersioningConfiguration{Status: "Bogus"},
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, bucket := newRealismBucket(t)
			assert.Equal(t, tc.want, apiErrorCode(t, tc.run(client, bucket)))
		})
	}
}

func TestListObjectsV2_OpaqueTokenRoundTrip_SDK(t *testing.T) {
	t.Parallel()

	client, bucket := newRealismBucket(t)

	for _, k := range []string{"a", "b", "c"} {
		_, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(k), Body: strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	var keys []string

	var token *string

	for {
		out, err := client.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), MaxKeys: aws.Int32(1), ContinuationToken: token,
		})
		require.NoError(t, err)

		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}

		token = out.NextContinuationToken
		assert.NotContains(t, []string{"a", "b", "c"}, aws.ToString(token))
	}

	assert.Equal(t, []string{"a", "b", "c"}, keys)
}
