package cloudfrontkeyvaluestore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfkvssdk "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"
	"github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKClient_QuotaExceeded proves the per-store/per-request size quotas
// (gopherstack-4ara: previously "no per-store size/count quotas enforced")
// surface as the real client's ServiceQuotaExceededException, not a silent
// success or the wrong exception type -- PutKey/DeleteKey/UpdateKeys are the
// only ops carrying ServiceQuotaExceededException in their deserializer
// error sets (verified against deserializers.go), GetKey/ListKeys/
// DescribeKeyValueStore do not.
func TestSDKClient_QuotaExceeded(t *testing.T) {
	t.Parallel()

	type quotaTestCase struct {
		run  func(t *testing.T, client *cfkvssdk.Client, ctx context.Context, kvsARN, etag string)
		name string
	}

	tests := []quotaTestCase{
		{
			name: "key over 512 bytes",
			run: func(t *testing.T, client *cfkvssdk.Client, ctx context.Context, kvsARN, etag string) {
				t.Helper()

				_, err := client.PutKey(ctx, &cfkvssdk.PutKeyInput{
					KvsARN:  aws.String(kvsARN),
					Key:     aws.String(strings.Repeat("k", 513)),
					Value:   aws.String("v"),
					IfMatch: aws.String(etag),
				})
				requireQuotaExceeded(t, err)
			},
		},
		{
			name: "value over 1024 bytes",
			run: func(t *testing.T, client *cfkvssdk.Client, ctx context.Context, kvsARN, etag string) {
				t.Helper()

				_, err := client.PutKey(ctx, &cfkvssdk.PutKeyInput{
					KvsARN:  aws.String(kvsARN),
					Key:     aws.String("k"),
					Value:   aws.String(strings.Repeat("v", 1025)),
					IfMatch: aws.String(etag),
				})
				requireQuotaExceeded(t, err)
			},
		},
		{
			name: "update batch over 50 keys",
			run: func(t *testing.T, client *cfkvssdk.Client, ctx context.Context, kvsARN, etag string) {
				t.Helper()

				puts := make([]types.PutKeyRequestListItem, 51)
				for i := range puts {
					puts[i] = types.PutKeyRequestListItem{
						Key:   aws.String(fmt.Sprintf("k%d", i)),
						Value: aws.String("v"),
					}
				}

				_, err := client.UpdateKeys(ctx, &cfkvssdk.UpdateKeysInput{
					KvsARN:  aws.String(kvsARN),
					Puts:    puts,
					IfMatch: aws.String(etag),
				})
				requireQuotaExceeded(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			kvs, err := backend.CreateKeyValueStore("quota-kvs-"+tt.name, "", nil)
			require.NoError(t, err)

			client := newSDKClient(t, h)
			ctx := context.Background()

			describeOut, err := client.DescribeKeyValueStore(ctx, &cfkvssdk.DescribeKeyValueStoreInput{
				KvsARN: aws.String(kvs.ARN),
			})
			require.NoError(t, err)

			tt.run(t, client, ctx, kvs.ARN, *describeOut.ETag)
		})
	}
}

// TestSDKClient_StoreSizeQuotaExceeded proves the per-store 5 MB quota
// (gopherstack-4ara): items are seeded directly through the backend (fast,
// no HTTP round trip) up to just under the quota, then one real SDK PutKey
// call pushes the store over it and must surface ServiceQuotaExceededException.
func TestSDKClient_StoreSizeQuotaExceeded(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)
	kvs, err := backend.CreateKeyValueStore("store-quota-kvs", "", nil)
	require.NoError(t, err)

	// Just under the per-item 1 KB value quota so this exercises the
	// per-store 5 MB quota, not the per-value one.
	bigValue := strings.Repeat("v", 1024)

	// 3440 * (500-byte key + 1024-byte value) = 5,242,560 bytes, 320 bytes
	// under the 5 MB (5,242,880 byte) quota -- not enough headroom for the
	// next Put's 1,036 bytes ("one-more-key" + bigValue).
	const seedItems = 3440

	var etag string

	for i := range seedItems {
		key := fmt.Sprintf("%0500d", i)
		etag, err = backend.PutKVSValue(kvs.ID, key, bigValue, "")
		require.NoError(t, err)
	}

	client := newSDKClient(t, h)

	_, err = client.PutKey(context.Background(), &cfkvssdk.PutKeyInput{
		KvsARN:  aws.String(kvs.ARN),
		Key:     aws.String("one-more-key"),
		Value:   aws.String(bigValue),
		IfMatch: aws.String(etag),
	})
	requireQuotaExceeded(t, err)
}

func requireQuotaExceeded(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ServiceQuotaExceededException", apiErr.ErrorCode())
}

// TestSDKClient_ListKeysItemFields proves ListKeys' per-item
// ListKeysResponseListItem shape (Key/Value, both required per
// cloudfrontkeyvaluestore@v1.15.4 types/types.go) round-trips through the
// real client -- the per-item field sweep gopherstack-21my checks for every
// service, recorded here since this is the only op in this SDK returning a
// nested item list.
func TestSDKClient_ListKeysItemFields(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)
	kvs, err := backend.CreateKeyValueStore("list-fields-kvs", "", nil)
	require.NoError(t, err)

	_, err = backend.PutKVSValue(kvs.ID, "greeting", "hello", "")
	require.NoError(t, err)

	client := newSDKClient(t, h)

	out, err := client.ListKeys(context.Background(), &cfkvssdk.ListKeysInput{KvsARN: aws.String(kvs.ARN)})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotNil(t, out.Items[0].Key)
	require.NotNil(t, out.Items[0].Value)
	assert.Equal(t, "greeting", *out.Items[0].Key)
	assert.Equal(t, "hello", *out.Items[0].Value)
}
