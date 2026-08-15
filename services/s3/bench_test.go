package s3_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

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

// BenchmarkListObjectsV2 measures listing throughput over a large bucket,
// with and without a prefix/delimiter, to give the s3 deep pass (gopherstack-3dqa)
// a real number for its previously-unmeasured optimization axis.
func BenchmarkListObjectsV2(b *testing.B) {
	const objectCount = 50_000

	backend := s3.NewInMemoryBackend(nil)
	bucketName := "bench-list-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("x")
	for i := range objectCount {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("dir%d/key-%d", i%100, i)),
			Body:   bytes.NewReader(data),
		})
	}

	b.Run("flat_maxkeys1000", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:  aws.String(bucketName),
				MaxKeys: aws.Int32(1000),
			})
		}
	})

	b.Run("prefix_delimiter", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:    aws.String(bucketName),
				Prefix:    aws.String("dir1/"),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(1000),
			})
		}
	})

	b.Run("common_prefix_only", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:    aws.String(bucketName),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(1000),
			})
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
