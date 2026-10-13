package main

import (
	"bytes"
	"context"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// lambdaESMS3Destination writes event source mapping on-failure records to S3.
type lambdaESMS3Destination struct{ s3 s3backend.StorageBackend }

func (d lambdaESMS3Destination) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := d.s3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("put on-failure record: %w", err)
	}

	return nil
}

// wireLambdaESMS3Destination lets event source mapping on-failure destinations deliver to S3.
func wireLambdaESMS3Destination(byName map[string]service.Registerable) {
	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return
	}

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	if !ok {
		return
	}

	if bk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
		bk.SetESMS3Destination(lambdaESMS3Destination{s3: s3H.Backend})
	}
}
