package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectRangeOmitsChecksum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rng          *string
		name         string
		wantChecksum bool
	}{
		{name: "full", wantChecksum: true},
		{name: "range", rng: aws.String("bytes=0-3")},
		{name: "suffix", rng: aws.String("bytes=-2")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("range-ck")})
			require.NoError(t, err)

			_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
				Bucket: aws.String("range-ck"), Key: aws.String("k"), Body: strings.NewReader("0123456789"),
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			})
			require.NoError(t, err)

			out, err := client.GetObject(ctx, &sdk_s3.GetObjectInput{
				Bucket: aws.String("range-ck"), Key: aws.String("k"), Range: tt.rng,
				ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)

			defer out.Body.Close()

			assert.Equal(t, tt.wantChecksum, out.ChecksumCRC32 != nil)
		})
	}
}

func TestConditionalWriteMissingKeyAndMultipart(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("cond-bkt")})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String("cond-bkt"), Key: aws.String("absent"), Body: strings.NewReader("x"),
		IfMatch: aws.String(`"abc"`),
	})
	assert.Equal(t, "NoSuchKey", apiCode(err))

	_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String("cond-bkt"), Key: aws.String("exists"), Body: strings.NewReader("x"),
	})
	require.NoError(t, err)

	mpu, err := client.CreateMultipartUpload(ctx, &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String("cond-bkt"), Key: aws.String("exists"),
	})
	require.NoError(t, err)

	part, err := client.UploadPart(ctx, &sdk_s3.UploadPartInput{
		Bucket: aws.String("cond-bkt"), Key: aws.String("exists"), UploadId: mpu.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("data"),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &sdk_s3.CompleteMultipartUploadInput{
		Bucket: aws.String("cond-bkt"), Key: aws.String("exists"), UploadId: mpu.UploadId,
		IfNoneMatch: aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	assert.Equal(t, "PreconditionFailed", apiCode(err))
}

func TestCORSPreflightHeaderWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reqHdrs string
		want    int
	}{
		{name: "prefix wildcard", reqHdrs: "x-amz-meta-foo", want: http.StatusOK},
		{name: "list with spaces", reqHdrs: "x-amz-meta-foo, content-type", want: http.StatusOK},
		{name: "case insensitive", reqHdrs: "X-Amz-Meta-Foo", want: http.StatusOK},
		{name: "unlisted", reqHdrs: "authorization", want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, sdkClient := newTestHandler(t)
			bucket := "cors-wild"
			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			cfg := `<CORSConfiguration><CORSRule><AllowedMethod>PUT</AllowedMethod>` +
				`<AllowedOrigin>https://*.example.com</AllowedOrigin>` +
				`<AllowedHeader>x-amz-*</AllowedHeader><AllowedHeader>content-type</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`
			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(cfg))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodOptions, "/"+bucket+"/k", nil)
			req.Header.Set("Origin", "https://app.example.com")
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", tt.reqHdrs)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusOK {
				assert.Contains(t, rec.Header().Get("Vary"), "Origin")
			}
		})
	}
}

func TestLifecycleRuleRequiresAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		rule    types.LifecycleRule
	}{
		{
			name: "no action",
			rule: types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("a/")},
			},
			wantErr: "InvalidRequest",
		},
		{
			name: "expiration",
			rule: types.LifecycleRule{
				Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("a/")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(3)},
			},
		},
		{
			name: "delete marker cleanup",
			rule: types.LifecycleRule{
				Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("a/")},
				Expiration: &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)},
			},
		},
		{
			name: "abort multipart only",
			rule: types.LifecycleRule{
				Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("a/")},
				AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
					DaysAfterInitiation: aws.Int32(2),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("lc-action")})
			require.NoError(t, err)

			_, err = client.PutBucketLifecycleConfiguration(ctx, &sdk_s3.PutBucketLifecycleConfigurationInput{
				Bucket:                 aws.String("lc-action"),
				LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{tt.rule}},
			})
			assert.Equal(t, tt.wantErr, apiCode(err))
		})
	}
}
