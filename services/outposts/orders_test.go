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

// orderTransitionTimeout/orderTransitionTick bound require.Eventually polls
// for the backend's chained 100ms-per-hop PREPARING -> IN_PROGRESS ->
// DELIVERED -> COMPLETED order transition (orders.go's
// scheduleOrderCompletion) to reach a given status. The tick is well under
// the per-hop delay so a 3-hop timeline's intermediate stops are actually
// observed, not skipped over.
const (
	orderTransitionTimeout = 2 * time.Second
	orderTransitionTick    = 10 * time.Millisecond
)

// waitForOrderStatus polls GetOrder until it reports want, returning the
// last response once it does.
func waitForOrderStatus(
	t *testing.T, client *outpostssdk.Client, orderID *string, want types.OrderStatus,
) *outpostssdk.GetOrderOutput {
	t.Helper()

	var got *outpostssdk.GetOrderOutput

	require.Eventually(t, func() bool {
		out, err := client.GetOrder(t.Context(), &outpostssdk.GetOrderInput{OrderId: orderID})
		if err != nil {
			return false
		}

		got = out

		return out.Order.Status == want
	}, orderTransitionTimeout, orderTransitionTick, "order never reached status %s", want)

	return got
}

func createTestOrder(
	t *testing.T,
	client *outpostssdk.Client,
	outpostID *string,
) *outpostssdk.CreateOrderOutput {
	t.Helper()

	out, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: outpostID,
		PaymentOption:     types.PaymentOptionAllUpfront,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("OR-RACKM05"), Quantity: aws.Int32(2)},
		},
	})
	require.NoError(t, err)

	return out
}

func TestCreateOrder_Lifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out := createTestOrder(t, client, created.OutpostId)
	require.Equal(t, types.OrderStatusPreparing, out.Order.Status)
	require.Equal(t, types.OrderTypeOutpost, out.Order.OrderType)
	require.Len(t, out.Order.LineItems, 1)

	got := waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusCompleted)
	require.Equal(t, types.LineItemStatusInstalled, got.Order.LineItems[0].Status)
	require.NotNil(t, got.Order.OrderFulfilledDate)

	// Order completion must have recorded an ORIGINAL subscription.
	billing, err := client.GetOutpostBillingInformation(
		t.Context(),
		&outpostssdk.GetOutpostBillingInformationInput{
			OutpostIdentifier: created.OutpostId,
		},
	)
	require.NoError(t, err)
	require.Len(t, billing.Subscriptions, 1)
	require.Equal(t, types.SubscriptionTypeOriginal, billing.Subscriptions[0].SubscriptionType)
}

// TestCreateOrder_LifecycleTransitions proves the order genuinely moves
// through the real intermediate SDK-declared states, not just PREPARING and
// COMPLETED -- and that LineItems move in lockstep at each hop.
func TestCreateOrder_LifecycleTransitions(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out := createTestOrder(t, client, created.OutpostId)

	got := waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusInProgress)
	require.Equal(t, types.LineItemStatusBuilding, got.Order.LineItems[0].Status)

	got = waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusDelivered)
	require.Equal(t, types.LineItemStatusDelivered, got.Order.LineItems[0].Status)

	got = waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusCompleted)
	require.Equal(t, types.LineItemStatusInstalled, got.Order.LineItems[0].Status)
}

// TestCreateOrder_ConcurrentReadDuringAsyncCompletion exercises a copy of an
// Order returned to a caller (via CreateOrder/GetOrder/ListOrders)
// concurrently with scheduleOrderCompletion's async chained status
// transition, which mutates Status/LineItems/OrderFulfilledDate on the
// backend's stored Order in place. Before the original fix, CreateOrder/
// GetOrder/ListOrders returned a shallow copy whose LineItems slice header
// aliased the same backing array as the stored Order, so reading the
// returned copy's LineItems here raced with that async write under -race.
// It must fail under `go test -race` without the deep-copy fix in
// orders.go's Order.clone, and pass with it.
func TestCreateOrder_ConcurrentReadDuringAsyncCompletion(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out := createTestOrder(t, client, created.OutpostId)

	// Read the CreateOrder response's own LineItems concurrently with
	// GetOrder/ListOrders calls, spanning the full 3-hop async transition
	// window.
	var wg sync.WaitGroup

	readOrderFields := func(status types.OrderStatus, items []types.LineItem) {
		_ = status
		for _, li := range items {
			_ = li.Status
		}
	}

	readOrderFields(out.Order.Status, out.Order.LineItems)

	deadline := time.Now().Add(orderTransitionTimeout)

	for range 10 {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				got, getErr := client.GetOrder(
					t.Context(),
					&outpostssdk.GetOrderInput{OrderId: out.Order.OrderId},
				)
				if getErr != nil {
					continue
				}

				readOrderFields(got.Order.Status, got.Order.LineItems)

				if got.Order.Status == types.OrderStatusCompleted {
					return
				}

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

	tests := []struct {
		waitFor types.OrderStatus
		name    string
	}{
		{name: "while preparing", waitFor: types.OrderStatusPreparing},
		{name: "while in progress", waitFor: types.OrderStatusInProgress},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			siteID := createTestSite(t, client)
			created := createTestOutpost(t, client, siteID)

			out := createTestOrder(t, client, created.OutpostId)
			waitForOrderStatus(t, client, out.Order.OrderId, tt.waitFor)

			_, err := client.CancelOrder(
				t.Context(),
				&outpostssdk.CancelOrderInput{OrderId: out.Order.OrderId},
			)
			require.NoError(t, err)

			got := waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusCancelled)
			for _, li := range got.Order.LineItems {
				require.Equal(t, types.LineItemStatusCancelled, li.Status)
			}

			// Cancelling an already-cancelled order is rejected.
			_, err = client.CancelOrder(
				t.Context(),
				&outpostssdk.CancelOrderInput{OrderId: out.Order.OrderId},
			)
			require.Error(t, err)

			var ce *types.ConflictException
			require.ErrorAs(t, err, &ce)
		})
	}
}

// TestCancelOrder_RejectedOnceDelivered proves the cancellable window closes
// once the order reaches DELIVERED -- the real hardware is presumed already
// shipped/delivered at that point.
func TestCancelOrder_RejectedOnceDelivered(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out := createTestOrder(t, client, created.OutpostId)
	waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusDelivered)

	_, err := client.CancelOrder(
		t.Context(),
		&outpostssdk.CancelOrderInput{OrderId: out.Order.OrderId},
	)
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

// TestCreateOrder_CompletionSetsContractEndDate proves a completed order
// establishes the Outpost's ContractEndDate from the order's own
// PaymentTerm (not just CreateRenewal), and that OUTPOST_RENEWAL_REQUIRED_ERROR
// reads it on a subsequent quote.
func TestCreateOrder_CompletionSetsContractEndDate(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out := createTestOrder(t, client, created.OutpostId)
	waitForOrderStatus(t, client, out.Order.OrderId, types.OrderStatusCompleted)

	billing, err := client.GetOutpostBillingInformation(
		t.Context(),
		&outpostssdk.GetOutpostBillingInformationInput{
			OutpostIdentifier: created.OutpostId,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		aws.ToString(billing.ContractEndDate),
		"a completed order must establish a contract end date",
	)

	in := minimalCreateQuoteInput()
	in.OutpostIdentifier = created.OutpostId

	quote, err := client.CreateQuote(t.Context(), in)
	require.NoError(t, err)

	for _, req := range quote.Quote.OrderingRequirements {
		if req.OrderingRequirementType == types.OrderingRequirementTypeOutpostRenewalRequiredError {
			require.Equal(t, types.OrderingRequirementStatusPass, req.Status,
				"a freshly-completed order's contract should not yet need renewal")
		}
	}
}

func TestListOrders_FiltersByOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)
	otherOutpost := createTestOutpost(t, client, siteID)

	createTestOrder(t, client, created.OutpostId)

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
