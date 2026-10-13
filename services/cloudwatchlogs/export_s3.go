package cloudwatchlogs

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// s3ExportSink adapts the in-process S3 backend to the ExportSink interface so
// CreateExportTask writes real gzipped objects into the emulated S3 service.
type s3ExportSink struct {
	backend  s3.StorageBackend
	provider s3HandlerProvider
}

var errS3Unavailable = errors.New("s3 backend unavailable")

// PutObject writes body to bucket/key via the S3 backend's PutObject operation.
func (s *s3ExportSink) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	backend, err := s.resolve()
	if err != nil {
		return err
	}

	_, err = backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("s3 export PutObject: %w", err)
	}

	return nil
}

// resolve looks the S3 backend up per call: the CLI stores sibling handlers
// only after every provider's Init has run.
func (s *s3ExportSink) resolve() (s3.StorageBackend, error) {
	if s.backend != nil {
		return s.backend, nil
	}

	sh, ok := s.provider.GetS3Handler().(*s3.S3Handler)
	if !ok || sh == nil || sh.Backend == nil {
		return nil, errS3Unavailable
	}

	return sh.Backend, nil
}
