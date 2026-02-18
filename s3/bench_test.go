package s3_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"Gopherstack/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func BenchmarkPutObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_, _ = backend.CreateBucket(
		context.Background(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.PutObject(context.Background(), &sdk_s3.PutObjectInput{
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
		context.Background(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	for i := range 1000 {
		_, _ = backend.PutObject(context.Background(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
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
