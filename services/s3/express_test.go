package s3_test

import (
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// newRealS3ExpressClientTest is newRealS3ClientTest plus
// DisableS3ExpressSessionAuth, which every case in this file needs: without
// it the pinned SDK (s3@v1.111.0) can still drive the CreateSession/
// PutObject/GetObject flow against a directory bucket fine, but its own
// S3Express identity resolver refuses ListDirectoryBuckets (a no-bucket op)
// with a client-side "bucket name is missing" error before any request
// reaches the wire -- see isListDirectoryBucketsRequest's doc comment.
func newRealS3ExpressClientTest(t *testing.T) *sdk_s3.Client {
	t.Helper()

	client := newRealS3ClientTest(t)

	return sdk_s3.New(client.Options(), func(o *sdk_s3.Options) {
		o.DisableS3ExpressSessionAuth = aws.Bool(true)
	})
}

// TestS3Express_FullFlow drives the real aws-sdk-go-v2 client through the
// complete S3 Express One Zone flow -- CreateBucket (directory) triggers the
// SDK's automatic CreateSession, then PutObject/GetObject/ListObjectsV2 sign
// with the returned session credentials and the x-amz-s3session-token header
// -- exercising exactly the flow gopherstack-z2w1a reported as a 403
// SignatureDoesNotMatch. Regression test for the root cause: verifyHeaderAuth
// rejected any Authorization header whose credential scope wasn't literally
// "s3"/"s3-object-lambda", but S3 Express requests are always signed with the
// "s3express" signing name.
func TestS3Express_FullFlow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		bucketName string
		key        string
		body       string
	}{
		{
			name:       "small text object",
			bucketName: "expr-flow-a--use1-az4--x-s3",
			key:        "hello.txt",
			body:       "hello from s3 express",
		},
		{
			name:       "empty object",
			bucketName: "expr-flow-b--use1-az4--x-s3",
			key:        "empty.txt",
			body:       "",
		},
		{
			name:       "nested key",
			bucketName: "expr-flow-c--use1-az4--x-s3",
			key:        "a/b/c.txt",
			body:       "nested",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ExpressClientTest(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucketName),
				CreateBucketConfiguration: &types.CreateBucketConfiguration{
					Bucket: &types.BucketInfo{
						Type:           types.BucketTypeDirectory,
						DataRedundancy: types.DataRedundancySingleAvailabilityZone,
					},
					Location: &types.LocationInfo{
						Type: types.LocationTypeAvailabilityZone,
						Name: aws.String("use1-az4"),
					},
				},
			})
			require.NoError(t, err, "CreateBucket (directory) must succeed")

			_, err = client.HeadBucket(ctx, &sdk_s3.HeadBucketInput{Bucket: aws.String(tt.bucketName)})
			require.NoError(t, err, "HeadBucket on the new directory bucket must succeed")

			_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
				Bucket: aws.String(tt.bucketName),
				Key:    aws.String(tt.key),
				Body:   strings.NewReader(tt.body),
			})
			require.NoError(t, err, "PutObject via the session-credential flow must succeed")

			getOut, err := client.GetObject(ctx, &sdk_s3.GetObjectInput{
				Bucket: aws.String(tt.bucketName),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err, "GetObject via the session-credential flow must succeed")
			gotBody, err := io.ReadAll(getOut.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.body, string(gotBody))

			listOut, err := client.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
				Bucket:    aws.String(tt.bucketName),
				Delimiter: aws.String("/"),
			})
			require.NoError(t, err, "ListObjectsV2 with delimiter \"/\" must succeed on a directory bucket")
			var gotKeys []string
			for _, obj := range listOut.Contents {
				gotKeys = append(gotKeys, aws.ToString(obj.Key))
			}
			if !strings.Contains(tt.key, "/") {
				assert.Contains(t, gotKeys, tt.key)
			}

			_, err = client.DeleteObject(ctx, &sdk_s3.DeleteObjectInput{
				Bucket: aws.String(tt.bucketName),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err, "DeleteObject via the session-credential flow must succeed")

			_, err = client.DeleteBucket(ctx, &sdk_s3.DeleteBucketInput{Bucket: aws.String(tt.bucketName)})
			require.NoError(t, err, "DeleteBucket on the now-empty directory bucket must succeed")
		})
	}
}

// TestS3Express_ListDirectoryBuckets confirms ListDirectoryBuckets reaches
// gopherstack and returns only directory buckets, excluding general-purpose
// ones, and that ListBuckets excludes directory buckets in turn.
func TestS3Express_ListDirectoryBuckets(t *testing.T) {
	t.Parallel()

	client := newRealS3ExpressClientTest(t)
	ctx := t.Context()

	const (
		dirBucket = "expr-list-dir--use1-az4--x-s3"
		gpBucket  = "expr-list-gp"
	)

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(dirBucket)})
	require.NoError(t, err)
	_, err = client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(gpBucket)})
	require.NoError(t, err)

	dirOut, err := client.ListDirectoryBuckets(ctx, &sdk_s3.ListDirectoryBucketsInput{})
	require.NoError(t, err, "ListDirectoryBuckets must reach the wire and succeed")

	dirNames := make([]string, 0, len(dirOut.Buckets))
	for _, b := range dirOut.Buckets {
		dirNames = append(dirNames, aws.ToString(b.Name))
	}
	assert.Contains(t, dirNames, dirBucket)
	assert.NotContains(t, dirNames, gpBucket)

	listOut, err := client.ListBuckets(ctx, &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)

	gpNames := make([]string, 0, len(listOut.Buckets))
	for _, b := range listOut.Buckets {
		gpNames = append(gpNames, aws.ToString(b.Name))
	}
	assert.Contains(t, gpNames, gpBucket)
	assert.NotContains(t, gpNames, dirBucket)
}

// TestS3Express_DirectoryBucketSemantics covers the two cheap-to-model
// directory-bucket restrictions: ListObjectsV2 requires Delimiter "/" (or
// none), and ListObjects (V1) is not supported at all (s3@v1.111.0
// api_op_ListObjects.go:13).
func TestS3Express_DirectoryBucketSemantics(t *testing.T) {
	t.Parallel()

	client := newRealS3ExpressClientTest(t)
	ctx := t.Context()

	const bucket = "expr-semantics--use1-az4--x-s3"

	{
		_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
	}

	t.Run("ListObjectsV2 rejects a non-slash delimiter", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket:    aws.String(bucket),
			Delimiter: aws.String(","),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidArgument", apiErr.ErrorCode())
	})

	t.Run("ListObjects (V1) is not supported", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListObjects(ctx, &sdk_s3.ListObjectsInput{Bucket: aws.String(bucket)})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NotImplemented", apiErr.ErrorCode())
	})
}

// TestS3ExpressSession_Expiry is a synctest regression test for the 5-minute
// CreateSession TTL: a session must authenticate requests right up to (but
// not past) its expiry.
func TestS3ExpressSession_Expiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
		mustCreateBucket(t, backend, "expiry--use1-az4--x-s3")

		creds, err := backend.CreateSession(t.Context(), "expiry--use1-az4--x-s3", types.SessionModeReadWrite)
		require.NoError(t, err)

		bucket, secret, ok := backend.ExpressSessionSecret(creds.AccessKeyID, creds.SessionToken)
		require.True(t, ok, "a freshly issued session must resolve")
		assert.Equal(t, "expiry--use1-az4--x-s3", bucket)
		assert.Equal(t, creds.SecretAccessKey, secret)

		time.Sleep(4 * time.Minute)

		_, _, ok = backend.ExpressSessionSecret(creds.AccessKeyID, creds.SessionToken)
		assert.True(t, ok, "a session must still resolve within its 5-minute TTL")

		time.Sleep(2 * time.Minute) // total elapsed: 6 minutes, past the 5-minute TTL

		_, _, ok = backend.ExpressSessionSecret(creds.AccessKeyID, creds.SessionToken)
		assert.False(t, ok, "a session must stop resolving once its TTL has passed")
	})
}

// TestS3ExpressSession_TTLBoundsGrowth locks in that CreateSession's store
// cannot leak: expired sessions are swept the next time CreateSession is
// called, not retained forever.
func TestS3ExpressSession_TTLBoundsGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
		mustCreateBucket(t, backend, "sweep--use1-az4--x-s3")

		old := make([]s3.SessionCredentials, 0, 5)
		for range 5 {
			c, err := backend.CreateSession(t.Context(), "sweep--use1-az4--x-s3", types.SessionModeReadWrite)
			require.NoError(t, err)
			old = append(old, c)
		}

		time.Sleep(6 * time.Minute)

		fresh, err := backend.CreateSession(t.Context(), "sweep--use1-az4--x-s3", types.SessionModeReadWrite)
		require.NoError(t, err)

		for _, c := range old {
			_, _, ok := backend.ExpressSessionSecret(c.AccessKeyID, c.SessionToken)
			assert.False(t, ok, "expired sessions must be swept, not retained")
		}

		_, _, ok := backend.ExpressSessionSecret(fresh.AccessKeyID, fresh.SessionToken)
		assert.True(t, ok, "the freshly created session must still resolve")
	})
}
