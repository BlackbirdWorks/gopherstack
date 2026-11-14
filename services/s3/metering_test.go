package s3_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestRequestObserverAndStorageHoldings(t *testing.T) {
	t.Parallel()

	bk := s3.NewInMemoryBackend(nil)
	h := s3.NewHandler(bk)

	var (
		mu     sync.Mutex
		events []s3.RequestEvent
	)

	h.SetRequestObserver(func(e s3.RequestEvent) {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, e)
	})

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
	)
	require.NoError(t, err)

	url := sdktest.Serve(t, h)
	client := sdk_s3.NewFromConfig(cfg, func(o *sdk_s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("obs")})
	require.NoError(t, err)

	_, err = client.PutBucketTagging(t.Context(), &sdk_s3.PutBucketTaggingInput{
		Bucket: aws.String("obs"),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String("Team"), Value: aws.String("core")},
		}},
	})
	require.NoError(t, err)

	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("obs"), Key: aws.String("k"), Body: bytes.NewReader([]byte("12345")),
		StorageClass: types.StorageClassStandardIa,
	})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, events, 3)
	assert.Equal(t, "PutObject", events[2].Operation)
	assert.True(t, events[2].HasKey)
	assert.Equal(t, "core", events[2].Tags["Team"])
	assert.Equal(t, "us-east-1", events[2].Region)

	held := bk.StorageHoldings()
	require.Len(t, held, 1)
	assert.Equal(t, s3.StorageHolding{
		Bucket:       "obs",
		Region:       "us-east-1",
		StorageClass: "STANDARD_IA",
		Bytes:        5,
		Tags:         map[string]string{"Team": "core"},
	}, held[0])
}
