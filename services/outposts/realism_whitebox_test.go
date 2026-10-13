package outposts

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetTransitionDelay_StretchesOrderHops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "first hop pending", delay: 5 * time.Second, wait: 4 * time.Second, want: OrderStatusPreparing},
		{name: "in progress", delay: 5 * time.Second, wait: 5 * time.Second, want: OrderStatusInProgress},
		{name: "delivered", delay: 5 * time.Second, wait: 10 * time.Second, want: OrderStatusDelivered},
		{name: "completed", delay: 5 * time.Second, wait: 15 * time.Second, want: OrderStatusCompleted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, outpostID, _ := newSyncTestOutpost(t)
				b.SetTransitionDelay(tt.delay)

				order, err := b.CreateOrder(&createOrderRequest{
					OutpostIdentifier: outpostID,
					PaymentOption:     PaymentOptionAllUpfront,
					LineItems:         []lineItemRequestWire{{CatalogItemId: "OR-RACKM05", Quantity: 1}},
				})
				require.NoError(t, err)

				advanceAndSettle(tt.wait)

				got, err := b.GetOrder(order.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.Status)
			})
		})
	}
}

func TestSetTransitionDelay_StretchesCapacityTask(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, outpostID, assetID := newSyncTestOutpost(t)
		b.SetTransitionDelay(5 * time.Second)

		task, err := b.StartCapacityTask(outpostID, &startCapacityTaskRequest{
			AssetId:       assetID,
			InstancePools: []instanceTypeCapacityWire{{InstanceType: "m5.xlarge", Count: 1}},
		})
		require.NoError(t, err)

		advanceAndSettle(4 * time.Second)

		got, err := b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		assert.Equal(t, CapacityTaskStatusRequested, got.Status)

		advanceAndSettle(time.Second)

		got, err = b.GetCapacityTask(outpostID, task.ID)
		require.NoError(t, err)
		assert.Equal(t, CapacityTaskStatusInProgress, got.Status)
	})
}
