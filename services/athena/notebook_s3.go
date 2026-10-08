package athena

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// loadNotebookFromS3 reads the ipynb object at an s3://bucket/key URI through the wired S3 backend.
func (b *InMemoryBackend) loadNotebookFromS3(uri string) (string, error) {
	bucket, key, ok := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")
	if !strings.HasPrefix(uri, "s3://") || !ok || bucket == "" || key == "" {
		return "", fmt.Errorf("%w: NotebookS3LocationUri must be an s3://bucket/key URI", ErrValidation)
	}

	getter, ok := b.s3.(S3Getter)
	if !ok {
		return "", fmt.Errorf("%w: NotebookS3LocationUri cannot be read: S3 is not available", ErrValidation)
	}

	out, err := getter.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("%w: cannot read NotebookS3LocationUri %q: %w", ErrValidation, uri, err)
	}

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("%w: cannot read NotebookS3LocationUri %q: %w", ErrValidation, uri, err)
	}

	return string(data), nil
}
