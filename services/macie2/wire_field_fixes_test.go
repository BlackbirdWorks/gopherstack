package macie2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// newTestMacie2SDKClient stands up the real aws-sdk-go-v2 macie2 client
// against an httptest server running this package's Handler.
func newTestMacie2SDKClient(t *testing.T, h *macie2.Handler) *macie2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return macie2sdk.NewFromConfig(cfg, func(o *macie2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetBucketStatistics_RealClient drives GetBucketStatistics through a
// real SDK client. Real GetBucketStatisticsOutput.ClassifiableObjectCount is
// the total number of classifiable OBJECTS across the buckets (confirmed at
// aws-sdk-go-v2/service/macie2's api_op_GetBucketStatistics.go), not a count
// of buckets that have any -- the pre-fix "classifiableBucketCount" key
// didn't exist on the real shape at all, so a real client's
// ClassifiableObjectCount was always 0 regardless of what DescribeBuckets
// showed for the same data. ObjectCount/SizeInBytes were missing entirely,
// even though the backend already tracks both per bucket.
func TestGetBucketStatistics_RealClient(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(b)
	client := newTestMacie2SDKClient(t, h)

	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:               "000000000000",
		BucketArn:               "arn:aws:s3:::bucket-a",
		BucketName:              "bucket-a",
		Region:                  "us-east-1",
		ClassifiableObjectCount: 10,
		ClassifiableSizeInBytes: 1000,
		ObjectCount:             25,
		SizeInBytes:             4096,
		PublicAccess:            "NOT_PUBLIC",
		EncryptionType:          "AES256",
		SharedAccess:            "NOT_SHARED",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:               "000000000000",
		BucketArn:               "arn:aws:s3:::bucket-b",
		BucketName:              "bucket-b",
		Region:                  "us-east-1",
		ClassifiableObjectCount: 5,
		ClassifiableSizeInBytes: 500,
		ObjectCount:             8,
		SizeInBytes:             2048,
		PublicAccess:            "NOT_PUBLIC",
		EncryptionType:          "AES256",
		SharedAccess:            "NOT_SHARED",
	})

	out, err := client.GetBucketStatistics(t.Context(), &macie2sdk.GetBucketStatisticsInput{})
	require.NoError(t, err)

	assert.Equal(t, int64(15), aws.ToInt64(out.ClassifiableObjectCount))
	assert.Equal(t, int64(1500), aws.ToInt64(out.ClassifiableSizeInBytes))
	assert.Equal(t, int64(33), aws.ToInt64(out.ObjectCount))
	assert.Equal(t, int64(6144), aws.ToInt64(out.SizeInBytes))
}

// TestUpdateResourceProfile_SensitivityScoreOverridden_RealClient drives
// UpdateResourceProfile then GetResourceProfile through a real SDK client.
// Real GetResourceProfileOutput's flag is "sensitivityScoreOverridden"
// (confirmed at api_op_GetResourceProfile.go); the pre-fix
// "sensitivityScoreOverride" key doesn't exist on the real shape, so a real
// client's SensitivityScoreOverridden stayed false even after a manual
// override was set.
func TestUpdateResourceProfile_SensitivityScoreOverridden_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	resourceARN := "arn:aws:s3:::override-bucket"

	_, err := client.UpdateResourceProfile(t.Context(), &macie2sdk.UpdateResourceProfileInput{
		ResourceArn:              aws.String(resourceARN),
		SensitivityScoreOverride: aws.Int32(100),
	})
	require.NoError(t, err)

	out, err := client.GetResourceProfile(t.Context(), &macie2sdk.GetResourceProfileInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)

	assert.True(t, aws.ToBool(out.SensitivityScoreOverridden))
	assert.Equal(t, int32(100), aws.ToInt32(out.SensitivityScore))
}
