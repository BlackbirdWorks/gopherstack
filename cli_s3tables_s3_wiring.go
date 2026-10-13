package main

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3tablesbackend "github.com/blackbirdworks/gopherstack/services/s3tables"
)

// wireS3TablesS3 lets S3 Tables materialize each new table's initial Iceberg metadata.json in S3.
func wireS3TablesS3(s3tablesReg, s3Reg service.Registerable) {
	tablesH, ok := s3tablesReg.(*s3tablesbackend.Handler)
	if !ok {
		return
	}

	s3H, ok := s3Reg.(*s3backend.S3Handler)
	if !ok {
		return
	}

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	if !ok {
		return
	}

	tablesH.SetMetadataWriter(&s3TablesWarehouseWriter{s3: s3Bk})
}

type s3TablesWarehouseWriter struct {
	s3 *s3backend.InMemoryBackend
}

func (w *s3TablesWarehouseWriter) WriteWarehouseObject(
	ctx context.Context, region, bucket, key string, body []byte,
) error {
	in := &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != config.DefaultRegion {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}

	// Bucket already exists after the first table in this table bucket.
	_, _ = w.s3.CreateBucket(ctx, in)

	_, err := w.s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})

	return err
}
