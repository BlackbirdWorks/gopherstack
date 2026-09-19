package integration_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cfkvssdk "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"
	cfkvstypes "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore/types"
	"github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticCFKVSEndpointResolver pins every request to the shared test
// container. cloudfrontkeyvaluestore's endpoint ruleset derives a
// per-account-ID virtual host from the KvsARN input (endpoints.go's
// EndpointParameters.KvsARN), which BaseEndpoint alone does not suppress --
// see services/cloudfrontkeyvaluestore/handler_test.go's identical
// staticEndpointResolver. Without this override every request fails with a
// DNS lookup on "<accountID>.<host>", not a gopherstack bug.
type staticCFKVSEndpointResolver struct{ url string }

func (r staticCFKVSEndpointResolver) ResolveEndpoint(
	_ context.Context, _ cfkvssdk.EndpointParameters,
) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(r.url)
	if err != nil {
		return smithyendpoints.Endpoint{}, err
	}

	return smithyendpoints.Endpoint{URI: *u}, nil
}

// createCloudFrontKeyValueStoreClient returns a CloudFront KeyValueStore
// (data-plane) client pointed at the shared test container.
func createCloudFrontKeyValueStoreClient(t *testing.T) *cfkvssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cfkvssdk.NewFromConfig(cfg, func(o *cfkvssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.EndpointResolverV2 = staticCFKVSEndpointResolver{url: endpoint}
	})
}

// TestIntegration_CloudFrontKeyValueStore_KeyLifecycle drives the CloudFront
// KeyValueStore data-plane API through the real AWS SDK v2 client: create
// the store via the cloudfront control-plane client, then
// Describe/Put/Get/List/UpdateKeys/Delete against the data plane, and
// confirm the stale-IfMatch and quota-violation errors surface as the real
// typed exceptions.
func TestIntegration_CloudFrontKeyValueStore_KeyLifecycle(t *testing.T) { //nolint:tparallel // sequential subtests
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	cfClient := createCloudFrontClient(t)
	kvsClient := createCloudFrontKeyValueStoreClient(t)

	storeName := "it-kvs-" + uuid.NewString()[:8]

	createOut, createErr := cfClient.CreateKeyValueStore(ctx, &cloudfront.CreateKeyValueStoreInput{
		Name:    aws.String(storeName),
		Comment: aws.String("integration test store"),
	})
	require.NoError(t, createErr)
	require.NotNil(t, createOut.KeyValueStore)

	kvsARN := aws.ToString(createOut.KeyValueStore.ARN)
	require.NotEmpty(t, kvsARN)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = cfClient.DeleteKeyValueStore(cleanupCtx, &cloudfront.DeleteKeyValueStoreInput{
			Name:    createOut.KeyValueStore.Name,
			IfMatch: createOut.ETag,
		})
	})

	// DescribeKeyValueStore gives the data-plane ETag required by every
	// mutating op below.
	descOut, descErr := kvsClient.DescribeKeyValueStore(ctx, &cfkvssdk.DescribeKeyValueStoreInput{
		KvsARN: aws.String(kvsARN),
	})
	require.NoError(t, descErr)
	assert.Equal(t, kvsARN, aws.ToString(descOut.KvsARN))
	assert.Equal(t, int32(0), aws.ToInt32(descOut.ItemCount))
	require.NotEmpty(t, aws.ToString(descOut.ETag))

	etag := aws.ToString(descOut.ETag)

	t.Run("put_and_get_key", func(t *testing.T) { //nolint:paralleltest // sequential by design
		putOut, err := kvsClient.PutKey(ctx, &cfkvssdk.PutKeyInput{
			KvsARN:  aws.String(kvsARN),
			Key:     aws.String("greeting"),
			Value:   aws.String("hello"),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(putOut.ETag))
		assert.Equal(t, int32(1), aws.ToInt32(putOut.ItemCount))
		etag = aws.ToString(putOut.ETag)

		getOut, err := kvsClient.GetKey(ctx, &cfkvssdk.GetKeyInput{
			KvsARN: aws.String(kvsARN),
			Key:    aws.String("greeting"),
		})
		require.NoError(t, err)
		assert.Equal(t, "greeting", aws.ToString(getOut.Key))
		assert.Equal(t, "hello", aws.ToString(getOut.Value))
	})

	t.Run("list_keys", func(t *testing.T) { //nolint:paralleltest // sequential by design
		listOut, err := kvsClient.ListKeys(ctx, &cfkvssdk.ListKeysInput{
			KvsARN: aws.String(kvsARN),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)
		assert.Equal(t, "greeting", aws.ToString(listOut.Items[0].Key))
		assert.Equal(t, "hello", aws.ToString(listOut.Items[0].Value))
	})

	t.Run("update_keys_batch", func(t *testing.T) { //nolint:paralleltest // sequential by design
		updOut, err := kvsClient.UpdateKeys(ctx, &cfkvssdk.UpdateKeysInput{
			KvsARN:  aws.String(kvsARN),
			IfMatch: aws.String(etag),
			Puts: []cfkvstypes.PutKeyRequestListItem{
				{Key: aws.String("second"), Value: aws.String("value2")},
			},
			Deletes: []cfkvstypes.DeleteKeyRequestListItem{
				{Key: aws.String("greeting")},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(updOut.ETag))
		assert.Equal(t, int32(1), aws.ToInt32(updOut.ItemCount))
		etag = aws.ToString(updOut.ETag)

		listOut, err := kvsClient.ListKeys(ctx, &cfkvssdk.ListKeysInput{
			KvsARN: aws.String(kvsARN),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)
		assert.Equal(t, "second", aws.ToString(listOut.Items[0].Key))
		assert.Equal(t, "value2", aws.ToString(listOut.Items[0].Value))
	})

	t.Run("delete_key", func(t *testing.T) { //nolint:paralleltest // sequential by design
		delOut, err := kvsClient.DeleteKey(ctx, &cfkvssdk.DeleteKeyInput{
			KvsARN:  aws.String(kvsARN),
			Key:     aws.String("second"),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(0), aws.ToInt32(delOut.ItemCount))
		etag = aws.ToString(delOut.ETag)

		listOut, err := kvsClient.ListKeys(ctx, &cfkvssdk.ListKeysInput{
			KvsARN: aws.String(kvsARN),
		})
		require.NoError(t, err)
		assert.Empty(t, listOut.Items)
	})

	t.Run("stale_ifmatch_conflict", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := kvsClient.PutKey(ctx, &cfkvssdk.PutKeyInput{
			KvsARN:  aws.String(kvsARN),
			Key:     aws.String("third"),
			Value:   aws.String("value3"),
			IfMatch: aws.String("stale-etag-does-not-match"),
		})
		require.Error(t, err)

		var conflict *cfkvstypes.ConflictException
		require.ErrorAs(t, err, &conflict, "expected a real ConflictException from the SDK deserializer")
	})

	t.Run("quota_violation_key_too_large", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := kvsClient.PutKey(ctx, &cfkvssdk.PutKeyInput{
			KvsARN:  aws.String(kvsARN),
			Key:     aws.String(strings.Repeat("k", 513)),
			Value:   aws.String("v"),
			IfMatch: aws.String(etag),
		})
		require.Error(t, err)

		var quota *cfkvstypes.ServiceQuotaExceededException
		require.ErrorAs(t, err, &quota, "expected a real ServiceQuotaExceededException from the SDK deserializer")

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ServiceQuotaExceededException", apiErr.ErrorCode())
	})
}
