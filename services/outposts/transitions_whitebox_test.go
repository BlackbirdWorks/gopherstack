package outposts

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncTestAccountID = "000000000000"
	syncTestRegion    = "us-east-1"
)

// newSyncTestOutpost creates a backend, Site, and Outpost (with its seeded
// COMPUTE asset) by calling backend methods directly -- no HTTP server, so
// callers can run this inside a synctest bubble, where httptest and real
// network clients do not work. Returns the backend, the Outpost ID, and the
// seeded Asset ID.
func newSyncTestOutpost(t *testing.T) (*InMemoryBackend, string, string) {
	t.Helper()

	b := NewInMemoryBackend(context.Background(), syncTestAccountID, syncTestRegion)
	t.Cleanup(b.Close)

	site, err := b.CreateSite(&createSiteRequest{Name: "site-1"})
	require.NoError(t, err)

	outpost, err := b.CreateOutpost(&createOutpostRequest{Name: "outpost-1", SiteId: site.ID})
	require.NoError(t, err)

	assets, err := b.ListAssets(outpost.ID, assetFilter{})
	require.NoError(t, err)
	require.Len(t, assets, 1)

	return b, outpost.ID, assets[0].ID
}

// advanceAndSettle advances the synctest bubble's fake clock by d and lets
// every goroutine woken by that advance run to completion (block again)
// before returning -- the pattern shutdown_leak_test.go already uses in this
// package.
func advanceAndSettle(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

// TestStartCapacityTask_LifecycleTransitions_Synctest proves the capacity
// task genuinely moves through IN_PROGRESS before COMPLETED, and that
// InstanceTypeCapacities are only applied once COMPLETED -- against real
// (fake-clocked) timers, not a real-time poll, so it cannot flake by missing
// the IN_PROGRESS window under scheduler contention (the CI failure this
// test exists to prevent -- gopherstack-yf2hu).
func TestStartCapacityTask_LifecycleTransitions_Synctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, assetID := newSyncTestOutpost(t)

		task, err := b.StartCapacityTask(outpostID, &startCapacityTaskRequest{
			AssetId:       assetID,
			InstancePools: []instanceTypeCapacityWire{{InstanceType: "m5.xlarge", Count: 2}},
		})
		require.NoError(t, err)
		assert.Equal(t, CapacityTaskStatusRequested, task.Status)

		advanceAndSettle(capacityTaskTransitionDelay)

		got, err := b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusInProgress, got.Status)

		_, pools, err := b.GetOutpostInstanceTypes(outpostID)
		require.NoError(t, err)
		require.Empty(t, pools, "capacity must not apply until COMPLETED")

		advanceAndSettle(capacityTaskTransitionDelay)

		got, err = b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusCompleted, got.Status)

		_, pools, err = b.GetOutpostInstanceTypes(outpostID)
		require.NoError(t, err)
		require.NotEmpty(t, pools)
	})
}

// TestCancelCapacityTask_WhileInProgress_Synctest cancels a capacity task
// while it is IN_PROGRESS and proves the async CANCELLATION_IN_PROGRESS ->
// CANCELLED hop resolves deterministically. This is the case
// require.Eventually could miss under scheduler contention (same root cause
// as gopherstack-yf2hu) -- "while requested" has no async wait to miss and
// stays covered by the SDK-level TestCancelCapacityTask.
func TestCancelCapacityTask_WhileInProgress_Synctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, assetID := newSyncTestOutpost(t)

		task, err := b.StartCapacityTask(outpostID, &startCapacityTaskRequest{
			AssetId:       assetID,
			InstancePools: []instanceTypeCapacityWire{{InstanceType: "m5.xlarge", Count: 1}},
		})
		require.NoError(t, err)

		advanceAndSettle(capacityTaskTransitionDelay)

		got, err := b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusInProgress, got.Status)

		require.NoError(t, b.CancelCapacityTask(outpostID, task.ID))

		got, err = b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusCancellationInProgress, got.Status,
			"cancellation must pause at the transient state before resolving async")

		advanceAndSettle(capacityTaskTransitionDelay)

		got, err = b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusCancelled, got.Status)

		err = b.CancelCapacityTask(outpostID, task.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, errConflictSentinel,
			"cancelling an already-terminal task must be rejected")
	})
}

// TestCreateOrder_LifecycleTransitions_Synctest proves the order genuinely
// moves through the real intermediate SDK-declared states -- PREPARING ->
// IN_PROGRESS -> DELIVERED -> COMPLETED -- with LineItems moving in
// lockstep, against fake-clocked timers so intermediate stops cannot be
// skipped over under scheduler contention.
func TestCreateOrder_LifecycleTransitions_Synctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, _ := newSyncTestOutpost(t)

		order, err := b.CreateOrder(&createOrderRequest{
			OutpostIdentifier: outpostID,
			PaymentOption:     PaymentOptionAllUpfront,
			LineItems:         []lineItemRequestWire{{CatalogItemId: "OR-RACKM05", Quantity: 1}},
		})
		require.NoError(t, err)
		assert.Equal(t, OrderStatusPreparing, order.Status)

		advanceAndSettle(orderTransitionDelay)

		got, err := b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusInProgress, got.Status)
		require.Equal(t, LineItemStatusBuilding, got.LineItems[0].Status)

		advanceAndSettle(orderTransitionDelay)

		got, err = b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusDelivered, got.Status)
		require.Equal(t, LineItemStatusDelivered, got.LineItems[0].Status)

		advanceAndSettle(orderTransitionDelay)

		got, err = b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusCompleted, got.Status)
		require.Equal(t, LineItemStatusInstalled, got.LineItems[0].Status)
	})
}

// TestCancelOrder_Synctest cancels an order from each cancellable status.
// "while in progress" is the case require.Eventually could miss under
// scheduler contention; "while preparing" has no async wait to miss and
// stays covered by the SDK-level TestCancelOrder.
func TestCancelOrder_Synctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, _ := newSyncTestOutpost(t)

		order, err := b.CreateOrder(&createOrderRequest{
			OutpostIdentifier: outpostID,
			PaymentOption:     PaymentOptionAllUpfront,
			LineItems:         []lineItemRequestWire{{CatalogItemId: "OR-RACKM05", Quantity: 1}},
		})
		require.NoError(t, err)

		advanceAndSettle(orderTransitionDelay)

		got, err := b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusInProgress, got.Status)

		require.NoError(t, b.CancelOrder(order.ID))

		got, err = b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusCancelled, got.Status)

		for _, li := range got.LineItems {
			assert.Equal(t, LineItemStatusCancelled, li.Status)
		}

		err = b.CancelOrder(order.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, errConflictSentinel,
			"cancelling an already-cancelled order must be rejected")
	})
}

// TestCancelOrder_RejectedOnceDelivered_Synctest proves the cancellable
// window closes once the order reaches DELIVERED -- the real hardware is
// presumed already shipped/delivered at that point. Reaching DELIVERED
// deterministically (rather than via a real-time poll two hops deep) is
// exactly the class of wait gopherstack-yf2hu's fix targets.
func TestCancelOrder_RejectedOnceDelivered_Synctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, _ := newSyncTestOutpost(t)

		order, err := b.CreateOrder(&createOrderRequest{
			OutpostIdentifier: outpostID,
			PaymentOption:     PaymentOptionAllUpfront,
			LineItems:         []lineItemRequestWire{{CatalogItemId: "OR-RACKM05", Quantity: 1}},
		})
		require.NoError(t, err)

		advanceAndSettle(2 * orderTransitionDelay)

		got, err := b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusDelivered, got.Status)

		err = b.CancelOrder(order.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, errConflictSentinel)
	})
}
