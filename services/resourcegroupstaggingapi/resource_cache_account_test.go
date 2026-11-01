package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	rgt "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestGetResources_CacheIsPerAccount(t *testing.T) {
	t.Parallel()

	b := rgt.NewInMemoryBackend("000000000000", "us-east-1")
	b.RegisterProvider(func(ctx context.Context) []rgt.TaggedResource {
		return []rgt.TaggedResource{{
			ResourceARN: "arn:aws:sqs:us-east-1:" + awsmeta.Account(ctx) + ":q", Tags: map[string]string{"k": "v"},
		}}
	})

	for _, acct := range []string{"000000000000", "111111111111", "000000000000"} {
		ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: "us-east-1", Account: acct})
		out, err := b.GetResources(ctx, &rgt.GetResourcesInput{})
		require.NoError(t, err)
		require.Len(t, out.ResourceTagMappingList, 1)
		assert.Contains(t, out.ResourceTagMappingList[0].ResourceARN, ":"+acct+":")
	}
}
