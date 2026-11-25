package s3_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

const atomicWriters = 16

func raceWriters(n int, fn func(i int) error) (int, []error) {
	var wins int
	var losers []error

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		start = make(chan struct{})
	)

	for i := range n {
		wg.Go(func() {
			<-start

			err := fn(i)

			mu.Lock()
			defer mu.Unlock()

			if err == nil {
				wins++

				return
			}

			losers = append(losers, err)
		})
	}

	close(start)
	wg.Wait()

	return wins, losers
}

func TestConditionalPutObjectAtomic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seed     bool
		ifMatch  bool
		wantWins int
	}{
		{name: "none match star on absent key", wantWins: 1},
		{name: "if match on existing etag", seed: true, ifMatch: true, wantWins: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b")

			put := func(body string, mutate func(*sdk_s3.PutObjectInput)) (*sdk_s3.PutObjectOutput, error) {
				in := &sdk_s3.PutObjectInput{
					Bucket: aws.String("b"), Key: aws.String("k"), Body: strings.NewReader(body),
				}
				if mutate != nil {
					mutate(in)
				}

				return backend.PutObject(t.Context(), in)
			}

			var etag *string

			if tt.seed {
				out, err := put("seed", nil)
				require.NoError(t, err)

				etag = out.ETag
			}

			wins, losers := raceWriters(atomicWriters, func(int) error {
				_, err := put("x", func(in *sdk_s3.PutObjectInput) {
					if tt.ifMatch {
						in.IfMatch = etag

						return
					}

					in.IfNoneMatch = aws.String("*")
				})

				return err
			})

			assert.Equal(t, tt.wantWins, wins)
			require.Len(t, losers, atomicWriters-tt.wantWins)

			for _, err := range losers {
				assert.ErrorIs(t, err, s3.ErrPreconditionFailed)
			}
		})
	}
}

func TestConditionalCompleteMultipartUploadAtomic(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b")

	type upload struct {
		id   *string
		etag *string
	}

	uploads := make([]upload, atomicWriters)

	for i := range uploads {
		created, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String("b"), Key: aws.String("k"),
		})
		require.NoError(t, err)

		part, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
			Bucket: aws.String("b"), Key: aws.String("k"), UploadId: created.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader("part"),
		})
		require.NoError(t, err)

		uploads[i] = upload{id: created.UploadId, etag: part.ETag}
	}

	wins, losers := raceWriters(atomicWriters, func(i int) error {
		_, err := backend.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
			Bucket: aws.String("b"), Key: aws.String("k"), UploadId: uploads[i].id,
			IfNoneMatch: aws.String("*"),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{{ETag: uploads[i].etag, PartNumber: aws.Int32(1)}},
			},
		})

		return err
	})

	assert.Equal(t, 1, wins)
	require.Len(t, losers, atomicWriters-1)

	for _, err := range losers {
		assert.ErrorIs(t, err, s3.ErrPreconditionFailed)
	}
}

func TestRealClient_PutObjectChecksumType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		algo     types.ChecksumAlgorithm
		wantType types.ChecksumType
	}{
		{name: "crc32", algo: types.ChecksumAlgorithmCrc32, wantType: types.ChecksumTypeFullObject},
		{name: "sha256", algo: types.ChecksumAlgorithmSha256, wantType: types.ChecksumTypeFullObject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("cs-bucket")})
			require.NoError(t, err)

			put, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("cs-bucket"), Key: aws.String("k"), Body: strings.NewReader("hello"),
				ChecksumAlgorithm: tt.algo,
			})
			require.NoError(t, err)

			head, err := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
				Bucket: aws.String("cs-bucket"), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)

			get, err := client.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("cs-bucket"), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)
			defer get.Body.Close()

			assert.Equal(t, tt.wantType, head.ChecksumType)
			assert.Equal(t, tt.wantType, get.ChecksumType)
			assert.Equal(t, tt.wantType, put.ChecksumType)
		})
	}
}
