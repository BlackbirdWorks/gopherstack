package cloudtrail

import (
	"context"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// BenchmarkLogFileBody proves the gzip.Writer pooling in logFileBody: before
// the fix, every call allocated a fresh flate compressor (window + Huffman
// tables) via gzip.NewWriter; pooling reuses it via Reset.
func BenchmarkLogFileBody(b *testing.B) {
	ev := Event{
		EventName:       "PutObject",
		CloudTrailEvent: `{"eventName":"PutObject","eventSource":"s3.amazonaws.com","eventTime":"2026-09-19T00:00:00Z"}`,
	}

	b.ReportAllocs()

	for range b.N {
		if _, err := logFileBody(ev); err != nil {
			b.Fatal(err)
		}
	}
}

// benchS3 is a minimal cloudtrail.S3Backend double for benchmarking delivery
// without depending on services/s3.
type benchS3 struct{}

func (benchS3) HeadBucket(context.Context, *sdk_s3.HeadBucketInput) (*sdk_s3.HeadBucketOutput, error) {
	return &sdk_s3.HeadBucketOutput{}, nil
}

func (benchS3) PutObject(context.Context, *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error) {
	return &sdk_s3.PutObjectOutput{}, nil
}

// BenchmarkRecordManagementEvent_Concurrent proves that RecordEvent no longer
// serializes every mutating API call in the emulator behind one goroutine's
// gzip+S3-PutObject: before the fix, b.mu was held for that entire call,
// so concurrent RecordManagementEvent calls (invoked on every mutating
// operation, across every registered service) could not overlap even though
// most of the work here (marshal, gzip, S3 write) touches no shared state.
func BenchmarkRecordManagementEvent_Concurrent(b *testing.B) {
	be := NewInMemoryBackend("123456789012", "us-east-1")
	be.SetS3Backend(benchS3{})

	_, err := be.CreateTrail(
		"bench-trail", "bench-bucket", "", "", "", "", "", false, false, false, nil, false,
	)
	if err != nil {
		b.Fatal(err)
	}

	if startErr := be.StartLogging("bench-trail"); startErr != nil {
		b.Fatal(startErr)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			be.RecordManagementEvent(service.CloudTrailEventInput{
				EventName:   "PutObject",
				EventSource: "s3.amazonaws.com",
				AwsRegion:   "us-east-1",
			})
		}
	})
}
