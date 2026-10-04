package s3_test

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestSnapshot_RacesWithObjectAndUploadWrites reproduces gopherstack-fwd0g: Snapshot marshals buckets under b.mu.RLock.
// PutObject/UploadPart mutate stored fields under bucket.mu/obj.mu/upload.mu alone; run with -race.
func TestSnapshot_RacesWithObjectAndUploadWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, b *s3.InMemoryBackend, bucket string)
		name   string
	}{
		{
			name: "concurrent_snapshot_and_put_object",
			mutate: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()

				for i := range 100 {
					mustPutObject(t, b, bucket, fmt.Sprintf("key-%03d", i), []byte("payload"))
				}
			},
		},
		{
			name: "concurrent_snapshot_and_upload_part",
			mutate: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()

				created, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String("mp-key"),
				})
				require.NoError(t, err)

				for i := int32(1); i <= 100; i++ {
					_, uploadErr := b.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
						Bucket:     aws.String(bucket),
						Key:        aws.String("mp-key"),
						UploadId:   created.UploadId,
						PartNumber: aws.Int32(i),
						Body:       bytes.NewReader([]byte("part-" + strconv.Itoa(int(i)))),
					})
					require.NoError(t, uploadErr)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			const bucket = "race-snapshot-bucket"

			mustCreateBucket(t, b, bucket)

			var wg sync.WaitGroup

			stop := make(chan struct{})

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					_ = b.Snapshot(t.Context())
				}
			})

			tt.mutate(t, b, bucket)
			close(stop)
			wg.Wait()
		})
	}
}
