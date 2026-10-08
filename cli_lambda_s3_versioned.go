package main

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// lambdaS3Fetcher reads Lambda deployment packages from S3, optionally at a specific object version.
type lambdaS3Fetcher struct {
	backend s3backend.StorageBackend
}

func (f lambdaS3Fetcher) GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error) {
	return f.get(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
}

func (f lambdaS3Fetcher) GetObjectVersionBytes(ctx context.Context, bucket, key, versionID string) ([]byte, error) {
	return f.get(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
}

func (f lambdaS3Fetcher) get(ctx context.Context, in *awss3.GetObjectInput) ([]byte, error) {
	out, err := f.backend.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}
