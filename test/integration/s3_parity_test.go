package integration_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
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

func TestIntegration_S3_Parity_PutObjectChecksumType(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createS3Client(t)
	ctx := t.Context()

	bucket := "cktype-" + uuid.NewString()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	put, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("k"),
		Body:              strings.NewReader("checksum me"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	require.NoError(t, err)

	tests := []struct {
		call func() (types.ChecksumType, error)
		name string
	}{
		{name: "put", call: func() (types.ChecksumType, error) { return put.ChecksumType, nil }},
		{
			name: "head",
			call: func() (types.ChecksumType, error) {
				out, e := client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
				})
				if e != nil {
					return "", e
				}

				return out.ChecksumType, nil
			},
		},
		{
			name: "get",
			call: func() (types.ChecksumType, error) {
				out, e := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
				})
				if e != nil {
					return "", e
				}
				defer out.Body.Close()

				return out.ChecksumType, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, callErr := tt.call()
			require.NoError(t, callErr)
			assert.Equal(t, types.ChecksumTypeFullObject, got)
		})
	}
}

func TestIntegration_S3_Parity_ConditionalPutRace(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createS3Client(t)

	const writers = 8

	tests := []struct {
		name string
		seed bool
	}{
		{name: "if-none-match star", seed: false},
		{name: "if-match etag", seed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "race-" + uuid.NewString()
			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
				Bucket:                  aws.String(bucket),
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			require.NoError(t, err)

			in := func(i int) *s3.PutObjectInput {
				return &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String("k"),
					Body:   strings.NewReader(fmt.Sprintf("writer-%d", i)),
				}
			}

			wantVersions := 1
			cond := func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") }

			if tt.seed {
				seed, seedErr := client.PutObject(ctx, in(-1))
				require.NoError(t, seedErr)

				etag := seed.ETag
				cond = func(in *s3.PutObjectInput) { in.IfMatch = etag }
				wantVersions = 1 + 1
			}

			errs := make([]error, writers)

			var wg sync.WaitGroup

			start := make(chan struct{})

			for i := range writers {
				wg.Go(func() {
					input := in(i)
					cond(input)
					<-start
					_, errs[i] = client.PutObject(ctx, input)
				})
			}

			close(start)
			wg.Wait()

			wins := 0

			for _, e := range errs {
				if e == nil {
					wins++

					continue
				}

				var apiErr smithy.APIError
				require.ErrorAs(t, e, &apiErr)
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
				assert.Equal(t, http.StatusPreconditionFailed, httpStatusOf(t, e))
			}

			assert.Equal(t, 1, wins)

			list, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			assert.Len(t, list.Versions, wantVersions)
		})
	}
}

func TestIntegration_S3_Parity_CompleteMultipartLoserKeepsUpload(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createS3Client(t)
	ctx := t.Context()

	bucket := "mpucond-" + uuid.NewString()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	create := func() (*string, []types.CompletedPart) {
		created, createErr := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, createErr)

		part, partErr := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: created.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader("part"),
		})
		require.NoError(t, partErr)

		return created.UploadId, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}
	}

	complete := func(id *string, parts []types.CompletedPart) error {
		_, completeErr := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: id,
			IfNoneMatch:     aws.String("*"),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		})

		return completeErr
	}

	winnerID, winnerParts := create()
	loserID, loserParts := create()

	require.NoError(t, complete(winnerID, winnerParts))

	loseErr := complete(loserID, loserParts)
	require.Error(t, loseErr)
	assert.Equal(t, http.StatusPreconditionFailed, httpStatusOf(t, loseErr))

	listed, err := client.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: loserID,
	})
	require.NoError(t, err)
	assert.Len(t, listed.Parts, 1)

	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	require.NoError(t, err)

	require.NoError(t, complete(loserID, loserParts))
}
