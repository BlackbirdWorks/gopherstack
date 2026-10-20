package kinesisanalyticsv2

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const flinkMaxCodeBytes = 512 << 20

// siblingServices is the slice of *CLI used to read application code from emulated S3.
type siblingServices interface {
	GetS3Handler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) s3Backend() (s3backend.StorageBackend, bool) {
	b.mu.RLock("s3Backend")
	cfg := b.appConfig
	b.mu.RUnlock()

	s, ok := cfg.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// fetchFlinkCode reads an application JAR from emulated S3.
func (b *InMemoryBackend) fetchFlinkCode(ctx context.Context, bucketARN, fileKey, version string) ([]byte, error) {
	backend, ok := b.s3Backend()
	if !ok {
		return nil, errFlinkS3Missing
	}

	bucket := bucketARN[strings.LastIndex(bucketARN, ":")+1:]
	if !arn.HasService(bucketARN, "s3") || bucket == "" {
		return nil, errFlinkBucketARN
	}

	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fileKey)}
	if version != "" {
		in.VersionId = aws.String(version)
	}

	out, err := backend.GetObject(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("get application code: %w", err)
	}

	defer out.Body.Close()

	return io.ReadAll(io.LimitReader(out.Body, flinkMaxCodeBytes)) //nolint:wrapcheck // read errors are self-describing
}
