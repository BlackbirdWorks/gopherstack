package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/outposts"
)

// TestPersistence_SnapshotRestoreRoundTrip proves Handler.Snapshot/Restore
// (persistence.go) round-trips every collection this backend owns --
// Outpost (with tags, which deserialize with a generic Prometheus lock name
// after a JSON round-trip and must be rebuilt under the real per-resource
// name -- see restoreOutpostAndSiteTagsLocked), Site, Order, Quote,
// CapacityTask, and the seeded Asset.
func TestPersistence_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)

	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	_, err := client.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: created.OutpostArn,
		Tags:        map[string]string{"env": "staging"},
	})
	require.NoError(t, err)

	order, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("OR-RACKM05"), Quantity: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	quote, err := client.CreateQuote(t.Context(), minimalCreateQuoteInput())
	require.NoError(t, err)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)
	require.Len(t, assets.Assets, 1)

	task, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String("m5.xlarge"), Count: 1},
		},
	})
	require.NoError(t, err)

	// Snapshot the populated backend, then restore the bytes into a
	// completely fresh backend -- only the snapshot carries the state across.
	snapshot := h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)

	restoredBackend := outposts.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(restoredBackend.Close)
	require.NoError(t, restoredBackend.Restore(t.Context(), snapshot))

	restoredHandler := outposts.NewHandler(restoredBackend)
	restoredClient := newRoundTripClient(t, restoredHandler)

	gotOutpost, err := restoredClient.GetOutpost(t.Context(), &outpostssdk.GetOutpostInput{
		OutpostId: created.OutpostId,
	})
	require.NoError(t, err)
	require.Equal(t, "staging", gotOutpost.Outpost.Tags["env"])

	// Prove the restored Outpost's tags.Tags is genuinely usable post-restore
	// (not just a JSON echo) by mutating it through the native API.
	_, err = restoredClient.TagResource(t.Context(), &outpostssdk.TagResourceInput{
		ResourceArn: created.OutpostArn,
		Tags:        map[string]string{"post-restore": "true"},
	})
	require.NoError(t, err)

	gotSite, err := restoredClient.GetSite(t.Context(), &outpostssdk.GetSiteInput{SiteId: aws.String(siteID)})
	require.NoError(t, err)
	require.Equal(t, siteID, aws.ToString(gotSite.Site.SiteId))

	gotOrder, err := restoredClient.GetOrder(t.Context(), &outpostssdk.GetOrderInput{OrderId: order.Order.OrderId})
	require.NoError(t, err)
	require.Equal(t, types.OrderStatusPreparing, gotOrder.Order.Status)

	gotQuote, err := restoredClient.GetQuote(t.Context(), &outpostssdk.GetQuoteInput{
		QuoteIdentifier: quote.Quote.QuoteId,
	})
	require.NoError(t, err)
	require.Equal(t, types.QuoteStatusCreated, gotQuote.Quote.QuoteStatus)

	gotTask, err := restoredClient.GetCapacityTask(t.Context(), &outpostssdk.GetCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		CapacityTaskId:    task.CapacityTaskId,
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusRequested, gotTask.CapacityTaskStatus)

	gotAssets, err := restoredClient.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, gotAssets.Assets, 1)
}

// TestPersistence_IncompatibleVersionStartsEmpty proves Restore discards
// (rather than partially decodes) a snapshot whose version does not match
// outpostsSnapshotVersion.
func TestPersistence_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	backend := outposts.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	err := backend.Restore(t.Context(), []byte(`{"version":999,"tables":{},"accountId":"x","region":"y"}`))
	require.NoError(t, err)

	_, err = backend.GetOutpost("op-anything")
	require.Error(t, err)
}
