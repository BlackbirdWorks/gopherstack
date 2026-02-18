package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func createS3BenchmarkClient(b *testing.B) *s3.Client {
	b.Helper()

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		b.Fatalf("unable to load SDK config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func BenchmarkS3PutObject(b *testing.B) {
	client := createS3BenchmarkClient(b)
	ctx := context.Background()
	bucket := "bench-put-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(b, err)

	data := strings.Repeat("x", 1024) // 1KB

	b.ResetTimer()
	for range b.N {
		_, pErr := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj"),
			Body:   strings.NewReader(data),
		})
		if pErr != nil {
			b.Fatalf("PutObject failed: %v", pErr)
		}
	}
	b.StopTimer()

	// Cleanup
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

func BenchmarkS3GetObject(b *testing.B) {
	client := createS3BenchmarkClient(b)
	ctx := context.Background()
	bucket := "bench-get-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(b, err)

	data := strings.Repeat("x", 1024) // 1KB
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader(data),
	})
	require.NoError(b, err)

	b.ResetTimer()
	for range b.N {
		resp, gErr := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj"),
		})
		if gErr != nil {
			b.Fatalf("GetObject failed: %v", gErr)
		}
		// Drain body to simulate real usage and avoid connection issues
		_ = resp.Body.Close()
	}
	b.StopTimer()

	// Cleanup
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

func BenchmarkS3DeleteObject(b *testing.B) {
	client := createS3BenchmarkClient(b)
	ctx := context.Background()
	bucket := "bench-del-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(b, err)

	// Since we need an object to delete, this benchmark is tricky correctly.
	// If we delete a non-existent object, it's fast but effectively a no-op S3-wise (usually success).
	// If we want to measure Delete overhead, we just call DeleteObject.
	// Whether the object exists or not might implicitly affect performance dependent on implementation.
	// But usually "DeleteObject" calls are what we measure.
	// Let's just call DeleteObject on a key.

	b.ResetTimer()
	for range b.N {
		// To make it more realistic, we might want to put then delete, but that measures PUT+DELETE.
		// Measuring just Delete on non-existing key is pure overhead of the handler/routing/lock.
		// Measuring Delete on existing key requires setup every iteration.
		// Let's measure DeleteObject on arbitrary key (idempotent, effectively S3 "delete marker" or "return 204")
		_, dErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj"),
		})
		if dErr != nil {
			b.Fatalf("DeleteObject failed: %v", dErr)
		}
	}
	b.StopTimer()

	// Cleanup
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
