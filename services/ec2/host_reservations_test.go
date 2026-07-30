package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHostReservations_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("offerings catalog is static and filterable", func(t *testing.T) { //nolint:paralleltest // existing issue.
		all := b.DescribeHostReservationOfferings("", 0, 0)
		require.NotEmpty(t, all)

		one := b.DescribeHostReservationOfferings(all[0].OfferingID, 0, 0)
		require.Len(t, one, 1)
		assert.Equal(t, all[0].OfferingID, one[0].OfferingID)

		byDuration := b.DescribeHostReservationOfferings("", 31536000, 31536000)
		for _, o := range byDuration {
			assert.Equal(t, int32(31536000), o.Duration)
		}
	})

	hosts, err := b.AllocateHosts("us-east-1a", "m5.large", 2)
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	hostIDs := []string{hosts[0].HostID, hosts[1].HostID}
	offeringID := b.DescribeHostReservationOfferings("", 0, 0)[0].OfferingID

	t.Run("purchase preview requires real hosts+offering", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, previewErr := b.GetHostReservationPurchasePreview(hostIDs, "hro-doesnotexist")
		require.Error(t, previewErr)

		_, previewErr = b.GetHostReservationPurchasePreview([]string{"h-doesnotexist"}, offeringID)
		require.Error(t, previewErr)

		preview, previewErr := b.GetHostReservationPurchasePreview(hostIDs, offeringID)
		require.NoError(t, previewErr)
		assert.Equal(t, "USD", preview.CurrencyCode)
		require.Len(t, preview.Purchases, 1)
		assert.Empty(t, preview.Purchases[0].HostReservationID)
	})

	var reservationID string

	t.Run("purchase creates real reservation state", func(t *testing.T) { //nolint:paralleltest // existing issue.
		purchase, purchaseErr := b.PurchaseHostReservation(hostIDs, offeringID, map[string]string{"Name": "test"})
		require.NoError(t, purchaseErr)
		require.Len(t, purchase.Purchases, 1)
		assert.NotEmpty(t, purchase.Purchases[0].HostReservationID)
		reservationID = purchase.Purchases[0].HostReservationID

		reservations := b.DescribeHostReservations([]string{reservationID})
		require.Len(t, reservations, 1)
		assert.Equal(t, "active", reservations[0].State)
		assert.Equal(t, hostIDs, reservations[0].HostIDSet)
		assert.Equal(t, "test", b.TagsForResource(reservationID)["Name"])
	})

	t.Run("describe all returns the reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.NotEmpty(t, b.DescribeHostReservations(nil))
	})

	t.Run("release hosts uses real dedicated-host state", func(t *testing.T) { //nolint:paralleltest // existing issue.
		successful, unsuccessful := b.ReleaseHosts([]string{hostIDs[0], "h-doesnotexist"})
		assert.Equal(t, []string{hostIDs[0]}, successful)
		require.Len(t, unsuccessful, 1)
		assert.Equal(t, "h-doesnotexist", unsuccessful[0].HostID)
		assert.NotEmpty(t, unsuccessful[0].Code)

		remaining := b.DescribeHosts([]string{hostIDs[0]})
		assert.Empty(t, remaining)
	})
}
