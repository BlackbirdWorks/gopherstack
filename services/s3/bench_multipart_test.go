package s3_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func BenchmarkCompleteMultipartUpload_10x5MiB(b *testing.B) {
	const (
		partCount = 10
		partSize  = 5 * 1024 * 1024
	)

	random := make([]byte, partSize)
	rng := rand.NewChaCha8([32]byte{1})
	_, _ = rng.Read(random)

	tests := []struct {
		name string
		part []byte
	}{
		{name: "compressible", part: bytes.Repeat([]byte("abcdefgh"), partSize/8)},
		{name: "random", part: random},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{})
			bucket := "bench-mpu-10x5m"
			_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})

			b.ReportAllocs()

			for i := 0; b.Loop(); i++ {
				b.StopTimer()

				key := fmt.Sprintf("key-%d", i)
				created, err := backend.CreateMultipartUpload(b.Context(), &sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket), Key: aws.String(key),
				})
				if err != nil {
					b.Fatal(err)
				}

				completed := make([]sdk_s3_types.CompletedPart, 0, partCount)
				for n := int32(1); n <= partCount; n++ {
					out, upErr := backend.UploadPart(b.Context(), &sdk_s3.UploadPartInput{
						Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
						PartNumber: aws.Int32(n), Body: bytes.NewReader(tt.part),
					})
					if upErr != nil {
						b.Fatal(upErr)
					}

					completed = append(completed, sdk_s3_types.CompletedPart{PartNumber: aws.Int32(n), ETag: out.ETag})
				}

				b.StartTimer()

				_, err = backend.CompleteMultipartUpload(b.Context(), &sdk_s3.CompleteMultipartUploadInput{
					Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
					MultipartUpload: &sdk_s3_types.CompletedMultipartUpload{Parts: completed},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
