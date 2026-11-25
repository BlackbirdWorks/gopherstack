package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

type mpuFixture struct {
	id   *string
	etag *string
}

func newMPUFixture(t *testing.T, backend s3.StorageBackend, bucket, key string) mpuFixture {
	t.Helper()

	created, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)

	part, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("part"),
	})
	require.NoError(t, err)

	return mpuFixture{id: created.UploadId, etag: part.ETag}
}

func completeMPU(
	t *testing.T, backend s3.StorageBackend, bucket, key string, f mpuFixture, ifNoneMatch *string,
) error {
	t.Helper()

	_, err := backend.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: f.id,
		IfNoneMatch: ifNoneMatch,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: f.etag, PartNumber: aws.Int32(1)}},
		},
	})

	return err
}

func TestCompleteMultipartUpload_LoserKeepsUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		writers int
	}{
		{name: "two", writers: 2},
		{name: "many", writers: atomicWriters},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b")

			fixtures := make([]mpuFixture, tt.writers)
			for i := range fixtures {
				fixtures[i] = newMPUFixture(t, backend, "b", "k")
			}

			failed := make([]bool, tt.writers)

			wins, losers := raceWriters(tt.writers, func(i int) error {
				err := completeMPU(t, backend, "b", "k", fixtures[i], aws.String("*"))
				failed[i] = err != nil

				return err
			})

			require.Equal(t, 1, wins)
			require.Len(t, losers, tt.writers-1)

			for _, err := range losers {
				require.ErrorIs(t, err, s3.ErrPreconditionFailed)
			}

			listed, err := backend.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
				Bucket: aws.String("b"),
			})
			require.NoError(t, err)
			assert.Len(t, listed.Uploads, tt.writers-1)

			_, err = backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
				Bucket: aws.String("b"), Key: aws.String("k"),
			})
			require.NoError(t, err)

			for i, f := range fixtures {
				if !failed[i] {
					continue
				}

				parts, listErr := backend.ListParts(t.Context(), &sdk_s3.ListPartsInput{
					Bucket: aws.String("b"), Key: aws.String("k"), UploadId: f.id,
				})
				require.NoError(t, listErr)
				assert.Len(t, parts.Parts, 1)

				require.NoError(t, completeMPU(t, backend, "b", "k", f, aws.String("*")))

				// Object exists again, so the next loser must fail the same way.
				_, err = backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
					Bucket: aws.String("b"), Key: aws.String("k"),
				})
				require.NoError(t, err)
			}
		})
	}
}

func TestCompleteMultipartUpload_SameUploadSingleWinner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ifNoneMatch *string
		name        string
	}{
		{name: "unconditional"},
		{name: "if none match", ifNoneMatch: aws.String("*")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b")

			f := newMPUFixture(t, backend, "b", "k")

			wins, losers := raceWriters(atomicWriters, func(int) error {
				return completeMPU(t, backend, "b", "k", f, tt.ifNoneMatch)
			})

			assert.Equal(t, 1, wins)
			require.Len(t, losers, atomicWriters-1)

			for _, err := range losers {
				assert.ErrorIs(t, err, s3.ErrNoSuchUpload)
			}
		})
	}
}
