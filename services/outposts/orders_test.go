package outposts_test

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

// orderTransitionWait is long enough for the backend's 100ms simulated
// PREPARING -> COMPLETED order transition to have fired.
const orderTransitionWait = 250 * time.Millisecond

func TestCreateOrder_Lifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("cat-rack-m5"), Quantity: aws.Int32(2)},
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.OrderStatusPreparing, out.Order.Status)
	require.Equal(t, types.OrderTypeOutpost, out.Order.OrderType)
	require.Len(t, out.Order.LineItems, 1)

	time.Sleep(orderTransitionWait)

	got, err := client.GetOrder(t.Context(), &outpostssdk.GetOrderInput{OrderId: out.Order.OrderId})
	require.NoError(t, err)
	require.Equal(t, types.OrderStatusCompleted, got.Order.Status)
	require.Equal(t, types.LineItemStatusInstalled, got.Order.LineItems[0].Status)
	require.NotNil(t, got.Order.OrderFulfilledDate)

	// Order completion must have recorded an ORIGINAL subscription.
	billing, err := client.GetOutpostBillingInformation(t.Context(), &outpostssdk.GetOutpostBillingInformationInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, billing.Subscriptions, 1)
	require.Equal(t, types.SubscriptionTypeOriginal, billing.Subscriptions[0].SubscriptionType)
}

// TestCreateOrder_ConcurrentReadDuringAsyncCompletion exercises a copy of an
// Order returned to a caller (via CreateOrder/GetOrder/ListOrders)
// concurrently with scheduleOrderCompletion's async PREPARING -> COMPLETED
// transition, which mutates Status/LineItems/OrderFulfilledDate on the
// backend's stored Order in place. Before the fix, CreateOrder/GetOrder/
// ListOrders returned a shallow copy whose LineItems slice header aliased
// the same backing array as the stored Order, so reading the returned
// copy's LineItems here raced with that async write under -race. It must
// fail under `go test -race` without the deep-copy fix in orders.go's
// Order.clone, and pass with it.
func TestCreateOrder_ConcurrentReadDuringAsyncCompletion(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("cat-rack-m5"), Quantity: aws.Int32(2)},
		},
	})
	require.NoError(t, err)

	// Read the CreateOrder response's own LineItems concurrently with
	// GetOrder/ListOrders calls, spanning the 100ms async completion window.
	var wg sync.WaitGroup

	readOrderFields := func(status types.OrderStatus, items []types.LineItem) {
		_ = status
		for _, li := range items {
			_ = li.Status
		}
	}

	readOrderFields(out.Order.Status, out.Order.LineItems)

	deadline := time.Now().Add(orderTransitionWait)

	for range 10 {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				got, getErr := client.GetOrder(t.Context(), &outpostssdk.GetOrderInput{OrderId: out.Order.OrderId})
				if getErr != nil {
					continue
				}

				readOrderFields(got.Order.Status, got.Order.LineItems)

				listed, listErr := client.ListOrders(t.Context(), &outpostssdk.ListOrdersInput{
					OutpostIdentifierFilter: created.OutpostId,
				})
				if listErr != nil {
					continue
				}

				for _, o := range listed.Orders {
					_ = o.Status
				}
			}
		})
	}

	wg.Wait()
}

func TestCreateOrder_UnknownCatalogItem(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	_, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("no-such-catalog-item"), Quantity: aws.Int32(1)},
		},
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestCancelOrder(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("cat-rack-m5"), Quantity: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	_, err = client.CancelOrder(t.Context(), &outpostssdk.CancelOrderInput{OrderId: out.Order.OrderId})
	require.NoError(t, err)

	got, err := client.GetOrder(t.Context(), &outpostssdk.GetOrderInput{OrderId: out.Order.OrderId})
	require.NoError(t, err)
	require.Equal(t, types.OrderStatusCancelled, got.Order.Status)

	// Cancelling an already-cancelled order is rejected.
	_, err = client.CancelOrder(t.Context(), &outpostssdk.CancelOrderInput{OrderId: out.Order.OrderId})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

func TestListOrders_FiltersByOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)
	otherOutpost := createTestOutpost(t, client, siteID)

	_, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("cat-rack-m5"), Quantity: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	out, err := client.ListOrders(t.Context(), &outpostssdk.ListOrdersInput{
		OutpostIdentifierFilter: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, out.Orders, 1)

	out, err = client.ListOrders(t.Context(), &outpostssdk.ListOrdersInput{
		OutpostIdentifierFilter: otherOutpost.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, out.Orders)
}
