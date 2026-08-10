package sagemaker

import (
	"context"
	"errors"
	"fmt"
	"io"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

// maxPipelineDefinitionBytes caps how many bytes CreatePipeline/UpdatePipeline
// read from a PipelineDefinitionS3Location object, matching mgn's identical
// import-source safety cap (services/mgn/s3import.go).
const maxPipelineDefinitionBytes = 64 * 1024 * 1024

// S3Accessor is the subset of S3 operations CreatePipeline/UpdatePipeline need
// to fetch a pipeline definition referenced by PipelineDefinitionS3Location.
// Satisfied by the in-process S3 backend (services/s3), wired in cli.go
// alongside the DynamoDB/MGN->S3 wiring -- see SetS3Backend.
type S3Accessor interface {
	GetObject(ctx context.Context, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error)
}

// SetS3Backend wires the S3 backend CreatePipeline/UpdatePipeline read a
// PipelineDefinitionS3Location's object from. Until this is called, a
// PipelineDefinitionS3Location fails with errPipelineDefinitionUnreadable
// rather than fabricating a definition.
func (b *InMemoryBackend) SetS3Backend(s3 S3Accessor) {
	b.mu.Lock("SetS3Backend")
	defer b.mu.Unlock()

	b.s3 = s3
}

// s3Backend returns the wired S3 accessor, or nil when none is configured.
func (b *InMemoryBackend) s3Backend() S3Accessor {
	b.mu.RLock("s3Backend")
	defer b.mu.RUnlock()

	return b.s3
}

// errPipelineDefinitionUnreadable wraps every reason a
// PipelineDefinitionS3Location's object could not be read: no backend wired,
// a missing bucket/key, or a real GetObject/read failure.
var errPipelineDefinitionUnreadable = errors.New(
	"sagemaker: pipeline definition S3 object could not be read",
)

// readPipelineDefinitionFromS3 fetches and returns the pipeline definition a
// PipelineDefinitionS3Location's bucket/key/version reference, the way real
// CreatePipeline/UpdatePipeline retrieve it ("If specified, SageMaker will
// retrieve the pipeline definition from this location.",
// api_op_CreatePipeline.go:60-61, sagemaker@v1.263.2).
func (b *InMemoryBackend) readPipelineDefinitionFromS3(
	ctx context.Context, bucket, key, versionID string,
) (string, error) {
	s3 := b.s3Backend()
	if s3 == nil {
		return "", fmt.Errorf("%w: no S3 backend configured", errPipelineDefinitionUnreadable)
	}

	in := &s3sdk.GetObjectInput{Bucket: &bucket, Key: &key}
	if versionID != "" {
		in.VersionId = &versionID
	}

	out, err := s3.GetObject(ctx, in)
	if err != nil {
		return "", fmt.Errorf("%w: %s/%s: %w", errPipelineDefinitionUnreadable, bucket, key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxPipelineDefinitionBytes))
	if err != nil {
		return "", fmt.Errorf("%w: %s/%s: %w", errPipelineDefinitionUnreadable, bucket, key, err)
	}

	if len(data) == 0 {
		return "", fmt.Errorf("%w: %s/%s: object is empty", errPipelineDefinitionUnreadable, bucket, key)
	}

	return string(data), nil
}
