package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func TestLambdaS3FetcherObjectVersion(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ctx := t.Context()

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("code")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String("code"),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	first, err := c.PutObject(
		ctx,
		&s3.PutObjectInput{Bucket: aws.String("code"), Key: aws.String("fn.zip"), Body: strings.NewReader("one")},
	)
	require.NoError(t, err)

	_, err = c.PutObject(
		ctx,
		&s3.PutObjectInput{Bucket: aws.String("code"), Key: aws.String("fn.zip"), Body: strings.NewReader("two")},
	)
	require.NoError(t, err)

	h, ok := serviceByName(fx.services)["S3"].(*s3backend.S3Handler)
	require.True(t, ok)

	f := lambdaS3Fetcher{backend: h.Backend}

	latest, err := f.GetObjectBytes(ctx, "code", "fn.zip")
	require.NoError(t, err)
	assert.Equal(t, "two", string(latest))

	old, err := f.GetObjectVersionBytes(ctx, "code", "fn.zip", aws.ToString(first.VersionId))
	require.NoError(t, err)
	assert.Equal(t, "one", string(old))
}
