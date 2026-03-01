package s3_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func BenchmarkPutObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}
}

func BenchmarkGetObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	for i := range 1000 {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.GetObject(b.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("key-%d", i%1000)),
		})
	}
}

func BenchmarkCalculateChecksum(b *testing.B) {
	data := []byte("some data to calculate checksum for benchmarking purpose")

	b.Run("SHA256", func(b *testing.B) {
		for range b.N {
			_ = s3.CalculateChecksum(data, "SHA256")
		}
	})

	b.Run("CRC32", func(b *testing.B) {
		for range b.N {
			_ = s3.CalculateChecksum(data, "CRC32")
		}
	})
}

// BenchmarkDeleteObjects measures DeleteObjects throughput when removing many
// keys from the same bucket. The single-lock-per-batch implementation avoids
// the per-object lock churn of the previous per-object DeleteObject loop.
func BenchmarkDeleteObjects(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("%d_objects", count), func(b *testing.B) {
			b.StopTimer()
			backend := s3.NewInMemoryBackend(nil)
			bucketName := "bench-delete-bucket"
			_, _ = backend.CreateBucket(
				b.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
			)
			objects := make([]sdk_s3_types.ObjectIdentifier, count)
			for i := range count {
				key := aws.String(fmt.Sprintf("key-%d", i))
				_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    key,
					Body:   bytes.NewReader([]byte("data")),
				})
				objects[i] = sdk_s3_types.ObjectIdentifier{Key: key}
			}
			b.StartTimer()

			for range b.N {
				_, _ = backend.DeleteObjects(b.Context(), &sdk_s3.DeleteObjectsInput{
					Bucket: aws.String(bucketName),
					Delete: &sdk_s3_types.Delete{Objects: objects},
				})
			}
		})
	}
}
