package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type redshiftWorkgroupEngines struct {
	h *redshiftbackend.ServerlessHandler
}

func (r redshiftWorkgroupEngines) WorkgroupEngineLogin(region, name string) (redshiftdatabackend.EngineLogin, bool) {
	l, ok := r.h.WorkgroupEngineLogin(region, name)

	return redshiftdatabackend.EngineLogin(l), ok
}

// wireRedshiftWorkgroupEngines lets the Redshift Data API run SQL on serverless workgroups backed by a live engine.
func wireRedshiftWorkgroupEngines(byName map[string]service.Registerable) {
	serverless, ok := byName["RedshiftServerless"].(*redshiftbackend.ServerlessHandler)
	if !ok {
		return
	}

	if data, dok := byName["RedshiftData"].(*redshiftdatabackend.Handler); dok {
		data.SetWorkgroupEngines(redshiftWorkgroupEngines{h: serverless})
	}
}

// redshiftObjectStore gives COPY FROM and UNLOAD TO the emulator's S3.
type redshiftObjectStore struct{ s3 *s3backend.InMemoryBackend }

var errRedshiftS3Body = errors.New("S3 object has no body")

func (o redshiftObjectStore) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := o.s3.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get s3://%s/%s: %w", bucket, key, err)
	}

	if out.Body == nil {
		return nil, errRedshiftS3Body
	}

	defer func() { _ = out.Body.Close() }()

	return io.ReadAll(out.Body)
}

func (o redshiftObjectStore) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := o.s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", bucket, key, err)
	}

	return nil
}

func (o redshiftObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := o.s3.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("delete s3://%s/%s: %w", bucket, key, err)
	}

	return nil
}

func (o redshiftObjectStore) ListKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := o.s3.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}

		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

// wireRedshiftObjectStore points the Data API's COPY FROM s3 and UNLOAD TO s3 at the emulator's S3.
func wireRedshiftObjectStore(byName map[string]service.Registerable) {
	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	if !ok {
		return
	}

	bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	if !ok {
		return
	}

	if data, dok := byName["RedshiftData"].(*redshiftdatabackend.Handler); dok {
		data.SetObjectStore(redshiftObjectStore{s3: bk})
	}
}
