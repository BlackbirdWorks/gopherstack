package integration_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func httpStatusOf(t *testing.T, err error) int {
	t.Helper()

	var re *smithyhttp.ResponseError
	require.ErrorAs(t, err, &re)

	return re.HTTPStatusCode()
}

func TestIntegration_S3_Parity_ConditionalEqualBoundary(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createS3Client(t)
	ctx := t.Context()

	bucket := "cond-" + uuid.NewString()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("data"),
	})
	require.NoError(t, err)

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	require.NoError(t, err)
	lm := head.LastModified

	tests := []struct {
		call       func() error
		name       string
		wantStatus int
	}{
		{
			name: "get if-modified-since equal",
			call: func() error {
				_, e := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"), IfModifiedSince: lm,
				})

				return e
			},
			wantStatus: http.StatusNotModified,
		},
		{
			name: "head if-modified-since equal",
			call: func() error {
				_, e := client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"), IfModifiedSince: lm,
				})

				return e
			},
			wantStatus: http.StatusNotModified,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.call())
			assert.Equal(t, tt.wantStatus, httpStatusOf(t, tt.call()))
		})
	}

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), IfUnmodifiedSince: lm,
	})
	require.NoError(t, err)
}

func TestIntegration_S3_Parity_VersionIDErrors(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createS3Client(t)
	ctx := t.Context()

	bucket := "verr-" + uuid.NewString()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("data"),
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		versionID  string
		wantCode   string
		wantStatus int
	}{
		{"malformed", "garbage", "InvalidArgument", http.StatusBadRequest},
		{"well formed absent", "3HL4kqtJlcpXroDTDmJ.rSpS_GYIzbCs", "NoSuchVersion", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, getErr := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(tt.versionID),
			})

			var apiErr smithy.APIError
			require.ErrorAs(t, getErr, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

			_, headErr := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(tt.versionID),
			})
			require.Error(t, headErr)
			assert.Equal(t, tt.wantStatus, httpStatusOf(t, headErr))
		})
	}
}
