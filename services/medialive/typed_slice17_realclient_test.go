package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestSlice17_MediaLive_DeleteReservation_RealClient covers medialive's last
// typed-client-uncovered op (gopherstack-n3zi slice 17): DeleteReservation
// only allows deleting an EXPIRED reservation, so this test uses the
// existing ForceReservationEnd test export to backdate a purchased
// reservation's term instead of waiting out a real lease.
func TestSlice17_MediaLive_DeleteReservation_RealClient(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	h := newTestHandler(t)
	client := newTestMediaLiveClient(t, h)

	offerings, err := client.ListOfferings(ctx, &medialivesdk.ListOfferingsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.Offerings)

	purchased, err := client.PurchaseOffering(ctx, &medialivesdk.PurchaseOfferingInput{
		OfferingId: offerings.Offerings[0].OfferingId,
		Count:      aws.Int32(1),
		Name:       aws.String("slice17-reservation"),
	})
	require.NoError(t, err)

	reservationID := aws.ToString(purchased.Reservation.ReservationId)
	medialive.ForceReservationEnd(h.Backend.(*medialive.InMemoryBackend), reservationID, "2000-01-01T00:00:00Z")

	deleted, err := client.DeleteReservation(ctx, &medialivesdk.DeleteReservationInput{
		ReservationId: aws.String(reservationID),
	})
	require.NoError(t, err)
	assert.Equal(t, reservationID, aws.ToString(deleted.ReservationId))
	assert.Equal(t, "CANCELED", string(deleted.State))

	_, err = client.DescribeReservation(ctx, &medialivesdk.DescribeReservationInput{
		ReservationId: aws.String(reservationID),
	})
	require.Error(t, err)
}
