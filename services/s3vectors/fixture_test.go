package s3vectors_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/s3vectors"
)

const testAccount = awsmeta.DefaultAccount

type fixture struct {
	backend *s3vectors.InMemoryBackend
	client  *sdk.Client
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := s3vectors.NewInMemoryBackend(testAccount, "us-east-1")
	b.SetClock(func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) })

	f := &fixture{backend: b, url: sdktest.Serve(t, s3vectors.NewHandler(b))}
	f.client = f.clientIn(t, "us-east-1")

	return f
}

func (f *fixture) clientIn(t *testing.T, region string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func (f *fixture) bucket(t *testing.T, name string) {
	t.Helper()

	_, err := f.client.CreateVectorBucket(t.Context(), &sdk.CreateVectorBucketInput{VectorBucketName: aws.String(name)})
	require.NoError(t, err)
}

func (f *fixture) index(t *testing.T, bucket, name, metric string, dim int32, nonFilterable ...string) {
	t.Helper()

	in := &sdk.CreateIndexInput{
		VectorBucketName: aws.String(bucket), IndexName: aws.String(name), DataType: types.DataTypeFloat32,
		Dimension: aws.Int32(dim), DistanceMetric: types.DistanceMetric(metric),
	}
	if len(nonFilterable) > 0 {
		in.MetadataConfiguration = &types.MetadataConfiguration{NonFilterableMetadataKeys: nonFilterable}
	}

	_, err := f.client.CreateIndex(t.Context(), in)
	require.NoError(t, err)
}

func vec(key string, meta map[string]any, data ...float32) types.PutInputVector {
	v := types.PutInputVector{Key: aws.String(key), Data: &types.VectorDataMemberFloat32{Value: data}}
	if meta != nil {
		v.Metadata = document.NewLazyDocument(meta)
	}

	return v
}

func (f *fixture) put(t *testing.T, bucket, index string, vs ...types.PutInputVector) {
	t.Helper()

	_, err := f.client.PutVectors(t.Context(), &sdk.PutVectorsInput{
		VectorBucketName: aws.String(bucket), IndexName: aws.String(index), Vectors: vs,
	})
	require.NoError(t, err)
}
