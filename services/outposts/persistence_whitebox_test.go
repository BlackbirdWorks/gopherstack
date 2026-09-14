package outposts

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// TestPersistence_SnapshotRestoreRoundTrip_MidFlightOrderTransition proves
// an Order's intermediate status (not just the initial or final one)
// survives a snapshot/restore round trip, and that the ORIGINAL backend's
// pending timer -- unaffected by the snapshot -- genuinely carries the order
// on to COMPLETED. Restore does not re-arm the pending async timer
// (worker.Group timers are never persisted, matching services/grafana's
// identical behavior for its own workspace transitions), so the restored
// copy is expected to stay parked at IN_PROGRESS rather than continue
// advancing on its own.
//
// Runs inside a synctest bubble, driving the backend directly rather than
// through the SDK client: httptest servers and real network clients do not
// work inside a bubble, and this test needs a fake clock to reach
// IN_PROGRESS deterministically -- see gopherstack-yf2hu, which this test
// replaces a require.Eventually-over-the-network version of.
func TestPersistence_SnapshotRestoreRoundTrip_MidFlightOrderTransition(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, _ := newSyncTestOutpost(t)

		order, err := b.CreateOrder(&createOrderRequest{
			OutpostIdentifier: outpostID,
			PaymentOption:     PaymentOptionAllUpfront,
			LineItems:         []lineItemRequestWire{{CatalogItemId: "OR-RACKM05", Quantity: 2}},
		})
		require.NoError(t, err)

		advanceAndSettle(orderTransitionDelay)

		got, err := b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusInProgress, got.Status)

		snapshot := b.Snapshot(context.Background())
		require.NotEmpty(t, snapshot)

		restored := NewInMemoryBackend(context.Background(), syncTestAccountID, syncTestRegion)
		t.Cleanup(restored.Close)
		require.NoError(t, restored.Restore(context.Background(), snapshot))

		gotRestored, err := restored.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusInProgress, gotRestored.Status)
		require.Equal(t, LineItemStatusBuilding, gotRestored.LineItems[0].Status)

		// Advancing further must not move the restored copy: no timer was
		// re-armed for it.
		advanceAndSettle(2 * orderTransitionDelay)

		gotRestored, err = restored.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusInProgress, gotRestored.Status,
			"restore must not re-arm the pending timer")

		// The ORIGINAL backend's own timer is unaffected by the snapshot and
		// really does carry the order on to COMPLETED.
		gotOriginal, err := b.GetOrder(order.ID)
		require.NoError(t, err)
		require.Equal(t, OrderStatusCompleted, gotOriginal.Status)
	})
}

// TestPersistence_SnapshotRestoreRoundTrip_MidFlightCapacityTaskTransition is
// TestPersistence_SnapshotRestoreRoundTrip_MidFlightOrderTransition's
// CapacityTask counterpart -- see that test's doc comment.
func TestPersistence_SnapshotRestoreRoundTrip_MidFlightCapacityTaskTransition(t *testing.T) {
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

		snapshot := b.Snapshot(context.Background())
		require.NotEmpty(t, snapshot)

		restored := NewInMemoryBackend(context.Background(), syncTestAccountID, syncTestRegion)
		t.Cleanup(restored.Close)
		require.NoError(t, restored.Restore(context.Background(), snapshot))

		gotRestored, err := restored.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusInProgress, gotRestored.Status)

		// Advancing further must not move the restored copy: no timer was
		// re-armed for it.
		advanceAndSettle(2 * capacityTaskTransitionDelay)

		gotRestored, err = restored.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusInProgress, gotRestored.Status,
			"restore must not re-arm the pending timer")

		// The ORIGINAL backend's own timer is unaffected by the snapshot and
		// really does carry the task on to COMPLETED.
		gotOriginal, err := b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		require.Equal(t, CapacityTaskStatusCompleted, gotOriginal.Status)
	})
}
