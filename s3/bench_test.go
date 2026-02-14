package s3_test

import (
	"fmt"
	"testing"

	"Gopherstack/s3"
)

func BenchmarkPutObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_ = backend.CreateBucket(bucketName)
	data := []byte("benchmarking data")

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.PutObject(bucketName, fmt.Sprintf("key-%d", i), data, s3.ObjectMetadata{})
	}
}

func BenchmarkGetObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_ = backend.CreateBucket(bucketName)
	data := []byte("benchmarking data")

	for i := range 1000 {
		_, _ = backend.PutObject(bucketName, fmt.Sprintf("key-%d", i), data, s3.ObjectMetadata{})
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.GetObject(bucketName, fmt.Sprintf("key-%d", i%1000), "")
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
