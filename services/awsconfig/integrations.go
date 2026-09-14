package awsconfig

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
)

// s3WriterAdapter adapts s3pkg.StorageBackend to S3Writer, used to deliver
// ConfigSnapshot objects. Mirrors stepfunctions' s3ResultWriterAdapter
// (services/stepfunctions/integrations.go).
type s3WriterAdapter struct {
	backend s3pkg.StorageBackend
}

var _ S3Writer = (*s3WriterAdapter)(nil)

// NewS3WriterIntegration creates an S3 integration adapter for
// DeliverConfigSnapshot.
func NewS3WriterIntegration(backend s3pkg.StorageBackend) S3Writer {
	return &s3WriterAdapter{backend: backend}
}

// PutObjectBytes implements S3Writer.
func (a *s3WriterAdapter) PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error {
	_, err := a.backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	return err
}
