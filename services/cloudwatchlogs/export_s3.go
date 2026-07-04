package cloudwatchlogs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// s3ExportSink adapts the in-process S3 backend to the ExportSink interface so
// CreateExportTask writes real gzipped objects into the emulated S3 service.
type s3ExportSink struct {
	backend s3.StorageBackend
}

// newS3ExportSink builds an export sink backed by the given S3 storage backend.
func newS3ExportSink(backend s3.StorageBackend) *s3ExportSink {
	return &s3ExportSink{backend: backend}
}

// PutObject writes body to bucket/key via the S3 backend's PutObject operation.
func (s *s3ExportSink) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := s.backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("s3 export PutObject: %w", err)
	}

	return nil
}
